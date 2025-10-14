pub mod websocket;

use std::net::ToSocketAddrs;
use std::sync::Arc;

use clap::Parser;
use eyre::WrapErr as _;
use http2byond::ByondTopicValue;
use reqwest::redirect::Policy;
use reqwest::Client;
use tokio::sync::Mutex;
use twitch_api::twitch_oauth2::{self, AppAccessToken, Scope, TwitchToken as _, UserToken};
use twitch_api::{
    client::ClientDefault,
    eventsub::{self, Event, Message, Payload},
    HelixClient,
};

#[derive(Parser, Debug, Clone)]
#[clap(about, version)]
pub struct Cli {
    /// Client ID of twitch application
    #[clap(long, env, hide_env = true)]
    pub client_id: twitch_api::twitch_oauth2::ClientId,
    /// Client secret of twitch application
    #[clap(long, env, hide_env = true)]
    pub client_secret: twitch_api::twitch_oauth2::ClientSecret,
    #[clap(long, env, hide_env = true)]
    pub broadcaster_login: twitch_api::types::UserName,
    /// Path to config file
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/config.toml"))]
    pub config: std::path::PathBuf,
    /// Path to persistence file
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/persist/token.txt"))]
    pub persist: std::path::PathBuf,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug)]
pub struct Config {
    byond_host: String,
    comms_key: String,
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, eyre::Report> {
        let config = std::fs::read_to_string(path)?;
        toml::from_str(&config).wrap_err("Failed to parse config")
    }
}

#[tokio::main]
async fn main() -> Result<(), eyre::Report> {
    color_eyre::install()?;
    tracing_subscriber::fmt::fmt()
        .with_writer(std::io::stderr)
        .init();
    _ = dotenvy::dotenv();
    let opts = Cli::parse();
    let config = Config::load(&opts.config)?;

    let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
        ClientDefault::default_client_with_name(Some("my_chatbot".parse()?))?,
    );

    let token_path = &opts.persist;

    let mut user_token: Option<UserToken> = None;

    if let Ok(exists) = std::fs::exists(token_path) {
        if exists {
            let read_file = std::fs::read_to_string(token_path)?;

            let tokens: Vec<&str> = read_file.split("|").collect();

            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?;

            user_token = Some(
                twitch_oauth2::UserToken::from_existing_or_refresh_token(
                    &client,
                    tokens[0].into(),
                    tokens[1].into(),
                    opts.client_id.clone(),
                    Some(opts.client_secret.clone()),
                )
                .await?,
            );
        }
    }

    if user_token.is_none() {
        let mut builder = twitch_api::twitch_oauth2::tokens::DeviceUserTokenBuilder::new(
            opts.client_id.clone(),
            vec![Scope::UserReadChat, Scope::UserWriteChat],
        );
        builder.set_secret(Some(opts.client_secret.clone()));
        let code = builder.start(&client).await?;
        println!("Please go to: {}", code.verification_uri);
        user_token = Some(builder.wait_for_code(&client, tokio::time::sleep).await?);
    }

    if user_token.is_none() {
        return Ok(());
    }

    let user_token = user_token.unwrap();

    if let Some(refresh_token) = &user_token.refresh_token {
        let _ = std::fs::write(
            token_path,
            format!(
                "{}|{}",
                user_token.access_token.clone().take(),
                refresh_token.clone().take()
            ),
        );
    }

    let app_token = AppAccessToken::get_app_access_token(
        &Client::builder().redirect(Policy::none()).build()?,
        opts.client_id.clone(),
        opts.client_secret.clone(),
        vec![Scope::UserWriteChat, Scope::UserBot, Scope::ChannelBot],
    )
    .await?;

    let Some(twitch_api::helix::users::User {
        id: broadcaster, ..
    }) = client
        .get_user_from_login(&opts.broadcaster_login, &user_token)
        .await?
    else {
        eyre::bail!(
            "No broadcaster found with login: {}",
            opts.broadcaster_login
        );
    };

    let user_token = Arc::new(Mutex::new(user_token));
    let app_token = Arc::new(Mutex::new(app_token));

    let bot = Bot {
        opts,
        client,
        user_token,
        app_token,
        config,
        broadcaster,
    };
    bot.start().await?;
    Ok(())
}

pub struct Bot {
    pub opts: Cli,
    pub client: HelixClient<'static, reqwest::Client>,
    pub user_token: Arc<Mutex<twitch_api::twitch_oauth2::UserToken>>,
    pub app_token: Arc<Mutex<twitch_api::twitch_oauth2::AppAccessToken>>,
    pub config: Config,
    pub broadcaster: twitch_api::types::UserId,
}

impl Bot {
    pub async fn start(&self) -> Result<(), eyre::Report> {
        // To make a connection to the chat we need to use a websocket connection.
        // This is a wrapper for the websocket connection that handles the reconnects and handles all messages from eventsub.
        let websocket = websocket::ChatWebsocketClient {
            session_id: None,
            token: self.user_token.clone(),
            client: self.client.clone(),
            connect_url: twitch_api::TWITCH_EVENTSUB_WEBSOCKET_URL.clone(),
            chats: vec![self.broadcaster.clone()],
        };
        let refresh_token = async move {
            let token = self.user_token.clone();
            let client = self.client.clone();
            // We check constantly if the token is valid.
            // We also need to refresh the token if it's about to be expired.
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                let mut token = token.lock().await;
                if token.expires_in() < std::time::Duration::from_secs(60) {
                    token
                        .refresh_token(&self.client)
                        .await
                        .wrap_err("couldn't refresh token")?;
                }
                token
                    .validate_token(&client)
                    .await
                    .wrap_err("couldn't validate token")?;
            }
            #[allow(unreachable_code)]
            Ok(())
        };
        let ws = websocket.run(|e, ts| async { self.handle_event(e, ts).await });
        futures::future::try_join(ws, refresh_token).await?;
        Ok(())
    }

    async fn handle_event(
        &self,
        event: Event,
        timestamp: twitch_api::types::Timestamp,
    ) -> Result<(), eyre::Report> {
        let token = self.app_token.lock().await;
        match event {
            Event::ChannelChatMessageV1(Payload {
                message: Message::Notification(payload),
                subscription,
                ..
            }) => {
                println!(
                    "[{}] {}: {}",
                    timestamp, payload.chatter_user_name, payload.message.text
                );
                if let Some(full_command) = payload.message.text.strip_prefix("!") {
                    let mut split_whitespace = full_command.split_whitespace();
                    let command = split_whitespace.next().unwrap();
                    let rest = full_command.replace(command, "");

                    self.command(&payload, &subscription, command, rest.trim(), &token)
                        .await?;
                }
            }
            Event::ChannelChatNotificationV1(Payload {
                message: Message::Notification(payload),
                ..
            }) => {
                println!(
                    "[{}] {}: {}",
                    timestamp,
                    match &payload.chatter {
                        eventsub::channel::chat::notification::Chatter::Chatter {
                            chatter_user_name: user,
                            ..
                        } => user.as_str(),
                        _ => "anonymous",
                    },
                    payload.message.text
                );
            }
            _ => {}
        }
        Ok(())
    }

    async fn command(
        &self,
        payload: &eventsub::channel::ChannelChatMessageV1Payload,
        subscription: &eventsub::EventSubscriptionInformation<
            eventsub::channel::ChannelChatMessageV1,
        >,
        command: &str,
        _rest: &str,
        token: &AppAccessToken,
    ) -> Result<(), eyre::Report> {
        tracing::info!("Command: {}", command);

        let mut is_moderator = false;

        for badge in payload.badges.iter() {
            if badge.set_id.clone().take() == "moderator" {
                is_moderator = true;
                break;
            }
        }

        let json = serde_json::to_string(&GameRequest {
            query: "cmtv".to_string(),
            command: command.to_string(),
            username: payload.chatter_user_login.to_string(),
            is_moderator,
            auth: Some(self.config.comms_key.clone()),
            args: _rest.to_string(),
            source: "byond-twitch".to_string(),
        })?;

        let ByondTopicValue::String(mut received) = http2byond::send_byond(
            &self.config.byond_host.to_socket_addrs()?.next().unwrap(),
            &json,
        )?
        else {
            return Ok(());
        };

        received.pop();

        let response = serde_json::from_str::<GameResponse>(&received)?;

        let _ = self
            .client
            .send_chat_message_reply(
                &subscription.condition.broadcaster_user_id,
                &subscription.condition.user_id,
                &payload.message_id,
                &*response.response,
                token,
            )
            .await;

        Ok(())
    }
}

#[derive(serde::Serialize)]
struct GameRequest {
    query: String,
    command: String,
    args: String,
    username: String,
    is_moderator: bool,
    auth: Option<String>,
    source: String,
}

#[derive(serde::Deserialize)]
struct GameResponse {
    #[allow(dead_code)]
    statuscode: i32,
    response: String,
}
