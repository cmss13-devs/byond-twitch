#![warn(clippy::all, clippy::pedantic, clippy::suspicious)]
#![allow(clippy::missing_errors_doc, clippy::too_many_lines)]

pub mod server;
pub mod websocket;

use base64::Engine;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use eyre::eyre;
use futures::StreamExt;
use hmac::{Hmac, Mac};
use ordinal::ToOrdinal;
use regex::Regex;
use serde_json::json;
use sha2::Sha256;
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use tracing::instrument;
use twitch_api::helix::clips::Clip;

use chrono::Datelike;
use clap::Parser;
use eyre::WrapErr as _;
use http2byond::ByondTopicValue;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use jwt::VerifyWithKey;
use rand::seq::IndexedRandom;
use reqwest::redirect::Policy;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use twitch_api::helix;
use twitch_api::helix::predictions::create_prediction::{self, NewPredictionOutcome};
use twitch_api::helix::predictions::{end_prediction, get_predictions};
use twitch_api::twitch_oauth2::{
    self, AppAccessToken, ClientId, ClientSecret, Scope, TwitchToken, UserToken,
};
use twitch_api::types::{PredictionStatus, Timestamp, UserId};
use twitch_api::{
    client::ClientDefault,
    eventsub::{self, Event, Message, Payload},
    HelixClient,
};

#[derive(Parser, Debug, Clone)]
#[clap(about, version)]
pub struct Cli {
    pub broadcaster_login: twitch_api::types::UserName,
    /// Path to config file
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/config.toml"))]
    pub config: std::path::PathBuf,
    /// Path to persistence file
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/persist"))]
    pub persist: std::path::PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    twitch: TwitchConfig,
    game: GameConfig,
    discord: Option<DiscordConfig>,
    redis_url: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TwitchConfig {
    client_id: ClientId,
    client_secret: ClientSecret,
    api_secret: Option<String>,
    response_user_id: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct GameConfig {
    host: String,
    comms_key: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DiscordConfig {
    webhook: String,
    token: String,
    leaderboard_channel: u64,
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

    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        prev(info);
        std::process::exit(1);
    }));

    tracing_subscriber::fmt::fmt()
        .with_writer(std::io::stderr)
        .init();
    _ = dotenvy::dotenv();
    let opts = Cli::parse();
    let config = Config::load(&opts.config)?;

    let game_config = config.game.clone();
    let twitch_key = config.twitch.api_secret.clone();

    let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
        ClientDefault::default_client_with_name(Some("byond-twitch".parse()?))?,
    );

    let token_path = &opts.persist.join("token.txt");

    let mut broadcaster_user_token: Option<UserToken> = None;

    if let Ok(exists) = std::fs::exists(token_path) {
        if exists {
            let read_file = std::fs::read_to_string(token_path)?;

            let tokens: Vec<&str> = read_file.split('|').collect();

            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?;

            broadcaster_user_token = Some(
                twitch_oauth2::UserToken::from_existing_or_refresh_token(
                    &client,
                    tokens[0].into(),
                    tokens[1].into(),
                    config.twitch.client_id.clone(),
                    Some(config.twitch.client_secret.clone()),
                )
                .await?,
            );
        }
    }

    if broadcaster_user_token.is_none() {
        let mut builder = twitch_api::twitch_oauth2::tokens::DeviceUserTokenBuilder::new(
            config.twitch.client_id.clone(),
            vec![Scope::UserReadChat, Scope::ChannelManagePredictions],
        );
        builder.set_secret(Some(config.twitch.client_secret.clone()));
        let code = builder.start(&client).await?;
        println!("Please go to: {}", code.verification_uri);
        broadcaster_user_token = Some(builder.wait_for_code(&client, tokio::time::sleep).await?);
    }

    if broadcaster_user_token.is_none() {
        return Ok(());
    }

    let Some(broadcaster_user_token) = broadcaster_user_token else {
        return Err(eyre!("Could not unwrap token"));
    };

    if let Some(refresh_token) = &broadcaster_user_token.refresh_token {
        let _ = std::fs::write(
            token_path,
            format!(
                "{}|{}",
                broadcaster_user_token.access_token.clone().take(),
                refresh_token.clone().take()
            ),
        );
    }

    let bot_app_token = AppAccessToken::get_app_access_token(
        &Client::builder().redirect(Policy::none()).build()?,
        config.twitch.client_id.clone(),
        config.twitch.client_secret.clone(),
        vec![Scope::UserWriteChat, Scope::UserBot, Scope::ChannelBot],
    )
    .await?;

    let Some(twitch_api::helix::users::User {
        id: broadcaster, ..
    }) = client
        .get_user_from_login(&opts.broadcaster_login, &broadcaster_user_token)
        .await?
    else {
        eyre::bail!(
            "No broadcaster found with login: {}",
            opts.broadcaster_login
        );
    };

    let mut published_clips: Vec<String> = Vec::new();

    let published_path = &opts.persist.join("published.txt");
    if fs::exists(published_path)? {
        let read = fs::read_to_string(published_path)?;

        published_clips = read.split('|').map(ToString::to_string).collect();
    }

    let broadcaster_user_token = Arc::new(Mutex::new(broadcaster_user_token));
    let bot_app_token = Arc::new(Mutex::new(bot_app_token));

    let webserver_token = bot_app_token.clone();
    let response_user_id = config.twitch.response_user_id.clone();
    let webserver_broadcaster = broadcaster.clone();
    let persist = opts.persist.clone();
    tokio::spawn(async move {
        let _ = start_webserver(
            game_config,
            twitch_key,
            webserver_token,
            response_user_id,
            webserver_broadcaster,
            persist,
        )
        .await;
    });

    let published_clips = Arc::new(Mutex::new(published_clips));

    let bot = Bot {
        opts,
        client,
        broadcaster_user_token,
        bot_app_token,
        published_clips,
        config,
        broadcaster,
    };
    bot.start().await?;
    Ok(())
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct RedisRoundEvent {
    source: String,
    round_id: String,

    #[serde(rename = "type")]
    string_type: String,

    round_name: Option<String>,
    round_finished: Option<String>,
}

async fn start_webserver(
    config: GameConfig,
    twitch_secret: Option<String>,
    app_token: Arc<Mutex<AppAccessToken>>,
    responder_user_id: String,
    broadcaster_user_id: UserId,
    persist: PathBuf,
) -> Result<(), eyre::Report> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    let cache = Arc::new(Mutex::new(RequestCache::default()));
    if tokio::fs::try_exists(persist.join("cache.json")).await? {
        let cache_string = tokio::fs::read_to_string(persist.join("cache.json")).await?;

        let icons = serde_json::from_str::<HashMap<String, String>>(&cache_string)?;

        cache.lock().await.role_icons = icons;
    }

    loop {
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);

        let config = config.clone();
        let twitch_secret = twitch_secret.clone();
        let app_access_token = app_token.clone();
        let responder_user_id = responder_user_id.clone();
        let broadcaster_user_id = broadcaster_user_id.clone();

        let persist = persist.clone();

        let cache = cache.clone();

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            config.clone(),
                            cache.clone(),
                            twitch_secret.clone(),
                            app_access_token.clone(),
                            responder_user_id.clone(),
                            broadcaster_user_id.clone(),
                            persist.clone(),
                        )
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {err:?}");
            }
        });
    }
}

#[derive(Default)]
struct RequestCache {
    role_icons: HashMap<String, String>,

    cached_status: Option<GameResponse>,
    cached_time: Option<DateTime<Utc>>,
}

#[derive(Deserialize)]
struct SetRoleIcons {
    auth_key: String,
    role_icons: HashMap<String, String>,
}

#[derive(Deserialize)]
struct FollowPlayerRequest {
    token: String,
    name: String,
}

#[derive(Deserialize)]
#[allow(dead_code)]
struct TwitchExtToken {
    exp: i32,
    opaque_user_id: String,
    user_id: Option<String>,
    channel_id: String,
    role: String,
    is_unlinked: bool,
}

#[allow(clippy::too_many_arguments)]
async fn handle_request(
    request: Request<hyper::body::Incoming>,
    config: GameConfig,
    request_cache: Arc<Mutex<RequestCache>>,
    twitch_secret: Option<String>,
    app_token: Arc<Mutex<AppAccessToken>>,
    responder_user_id: String,
    broadcaster_user_id: UserId,
    persist: PathBuf,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match request.uri().path() {
        "/role_icons" => match *request.method() {
            Method::GET => {
                let locked = request_cache.lock().await;

                let Ok(response) = Response::builder()
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(
                        serde_json::to_string(&locked.role_icons).unwrap_or_default(),
                    )))
                else {
                    return get_response_with_code("An error occured while preparing data!", 501);
                };

                Ok(response)
            }
            Method::POST => {
                let Ok(collected) = request.collect().await else {
                    return get_response_with_code("An error occured.", 501);
                };

                let Ok(body_str) = String::from_utf8(collected.to_bytes().to_vec()) else {
                    return get_response_with_code("Internal server error.", 501);
                };

                let Ok(deserialised_body) = serde_json::from_str::<SetRoleIcons>(&body_str) else {
                    return get_response_with_code("Internal server error.", 501);
                };

                if deserialised_body.auth_key != config.comms_key {
                    return get_response_with_code("Invalid comms key!", 401);
                }

                let mut locked = request_cache.lock().await;
                locked.role_icons.clear();

                for (key, value) in &deserialised_body.role_icons {
                    locked.role_icons.insert(key.to_owned(), value.to_owned());
                }

                let _ = tokio::fs::write(
                    persist.join("cache.json"),
                    serde_json::to_string(&deserialised_body.role_icons)
                        .unwrap()
                        .as_bytes(),
                )
                .await;

                get_response_with_code("Accepted.", 200)
            }
            _ => get_response_with_code("Bad method.", 404),
        },
        "/active_players" => {
            {
                let cache = request_cache.lock().await;

                if let Some(cached_time) = cache.cached_time {
                    if let Some(cached_data) = &cache.cached_status {
                        let five_seconds_ago = chrono::Utc::now() - Duration::seconds(5);

                        if cached_time > five_seconds_ago {
                            let Ok(response) = Response::builder()
                                .header("Content-Type", "application/json")
                                .body(Full::new(Bytes::from(
                                    serde_json::to_string(&cached_data.data).unwrap_or_default(),
                                )))
                            else {
                                return get_response_with_code(
                                    "An error occured while preparing data!",
                                    501,
                                );
                            };

                            return Ok(response);
                        }
                    }
                }
            }

            let Ok(query) = serde_json::to_string(&GameRequest {
                query: "active_mobs".to_string(),
                auth: Some(config.comms_key),
                source: "byond-twitch-web".to_string(),
            }) else {
                return get_response_with_code("An error occured preparing to fetch data!", 501);
            };

            let Ok(mut address) = config.host.to_socket_addrs() else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Some(next) = address.next() else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(&next, &query)
            else {
                return get_response_with_code("An error occured fetching data!", 501);
            };

            received.pop();

            let game_response = match serde_json::from_str::<GameResponse>(&received) {
                Ok(response) => response,
                Err(err) => {
                    tracing::error!(err = ?err, "failed to deserialize game response");
                    return get_response_with_code("An error occured deserializing data!", 501);
                }
            };

            let Ok(response) = Response::builder()
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(
                    serde_json::to_string(&game_response.data).unwrap_or_default(),
                )))
            else {
                return get_response_with_code("An error occured while preparing data!", 501);
            };

            {
                let mut cache = request_cache.lock().await;

                cache.cached_status = Some(game_response.clone());
                cache.cached_time = Some(chrono::Utc::now());
            }

            Ok(response)
        }
        "/follow_player" => {
            let Some(twitch_secret) = twitch_secret else {
                return get_response_with_code("Server not set up.", 500);
            };

            let Ok(collected) = request.collect().await else {
                return get_response_with_code("An error occured.", 501);
            };

            let Ok(body_str) = String::from_utf8(collected.to_bytes().to_vec()) else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Ok(request) = serde_json::from_str::<FollowPlayerRequest>(&body_str) else {
                return get_response_with_code("Invalid request.", 503);
            };

            let Ok(base_64) = base64::prelude::BASE64_STANDARD.decode(twitch_secret.as_bytes())
            else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Ok(hmac): Result<Hmac<Sha256>, _> = Hmac::new_from_slice(&base_64) else {
                return get_response_with_code("Internal server error.", 500);
            };

            let Ok(claim): Result<TwitchExtToken, jwt::Error> =
                request.token.verify_with_key(&hmac)
            else {
                return get_response_with_code("Internal server error.", 500);
            };

            let Some(user_id) = claim.user_id else {
                return get_response_with_code(
                    "You need to allow the extension to view your identity.",
                    200,
                );
            };

            let is_moderator = claim.role == "moderator" || claim.role == "broadcaster";

            let Ok(query) = serde_json::to_string(&GameCMTVCommand {
                query: "cmtv".to_string(),
                auth: Some(config.comms_key),
                source: "byond-twitch-web".to_string(),
                command: "follow".to_string(),
                args: &request.name,
                is_moderator,
                user_id: &user_id,
                username: None,
            }) else {
                return get_response_with_code("An error occured preparing to fetch data!", 501);
            };

            let Ok(mut address) = config.host.to_socket_addrs() else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Some(next) = address.next() else {
                return get_response_with_code("Internal server error.", 501);
            };

            let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(&next, &query)
            else {
                return get_response_with_code("An error occured fetching data!", 501);
            };

            received.pop();

            let Ok(response) = serde_json::from_str::<GameResponse>(&received) else {
                return get_response_with_code("An error occured deserializing data!", 501);
            };

            if let Ok(code) = response.statuscode.try_into() {
                if code == 200 {
                    let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
                        ClientDefault::default_client_with_name(Some(
                            "byond-twitch-web".parse().unwrap(),
                        ))
                        .unwrap(),
                    );

                    let app_token = app_token.lock().await.clone();
                    if let Ok(Some(user)) = client.get_user_from_id(&user_id, &app_token).await {
                        let _ = client
                            .send_chat_message(
                                broadcaster_user_id,
                                responder_user_id,
                                &*format!(
                                    "{} has requested to follow '{}' via Overlay!",
                                    user.display_name, request.name
                                ),
                                &app_token,
                            )
                            .await;
                    }
                }

                get_response_with_code(&response.response, code)
            } else {
                get_response_with_code("Internal server error.", 501)
            }
        }
        _ => get_response_with_code("Bad path.", 200),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn get_response_with_code(
    what_to_say: &str,
    code: u16,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = Response::builder()
        .status(code)
        .body(Full::new(Bytes::from(what_to_say.to_string())))
        .unwrap();

    Ok(response)
}

pub struct Bot {
    pub opts: Cli,
    pub client: HelixClient<'static, reqwest::Client>,
    pub broadcaster_user_token: Arc<Mutex<twitch_api::twitch_oauth2::UserToken>>,
    pub bot_app_token: Arc<Mutex<twitch_api::twitch_oauth2::AppAccessToken>>,
    pub published_clips: Arc<Mutex<Vec<String>>>,
    pub config: Config,
    pub broadcaster: twitch_api::types::UserId,
}

impl Bot {
    pub async fn start(&self) -> Result<(), eyre::Report> {
        self.start_api_loop();

        if let Some(redis_url) = self.config.redis_url.clone() {
            let _ = self.start_redis_websocket(&redis_url);
        }

        // To make a connection to the chat we need to use a websocket connection.
        // This is a wrapper for the websocket connection that handles the reconnects and handles all messages from eventsub.
        let websocket = websocket::ChatWebsocketClient {
            session_id: None,
            token: self.broadcaster_user_token.clone(),
            client: self.client.clone(),
            connect_url: twitch_api::TWITCH_EVENTSUB_WEBSOCKET_URL.clone(),
            chats: vec![self.broadcaster.clone()],
        };
        let refresh_token = async move {
            let token = self.broadcaster_user_token.clone();
            let client = self.client.clone();
            // We check constantly if the token is valid.
            // We also need to refresh the token if it's about to be expired.
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            loop {
                interval.tick().await;
                let mut inner_token = token.lock().await.clone();

                if inner_token.expires_in() < std::time::Duration::from_secs(60) {
                    tracing::info!("refreshing token...");
                    inner_token
                        .refresh_token(&self.client)
                        .await
                        .wrap_err("couldn't refresh token")?;
                    let mut relocked = token.lock().await;
                    tracing::info!(
                        "refreshed token successfully, now {:?}",
                        inner_token.expires_in().as_secs()
                    );
                    *relocked = inner_token;

                    inner_token = token.lock().await.clone();
                }
                if inner_token
                    .validate_token(&client)
                    .await
                    .wrap_err("couldn't validate token")
                    .is_err()
                {
                    let _ = inner_token.refresh_token(&self.client).await;
                    let mut relocked = token.lock().await;
                    *relocked = inner_token;

                    tracing::info!("refreshed token after failed to validate token");
                }
            }
            #[allow(unreachable_code)]
            Ok(())
        };
        let ws = websocket.run(|e, ts| async { self.handle_event(e, ts).await });
        futures::future::try_join(ws, refresh_token).await?;
        Ok(())
    }

    fn start_api_loop(&self) {
        tracing::info!("starting api loop...");

        let broadcaster = self.broadcaster.clone();
        let sender_id = self.config.twitch.response_user_id.clone();
        let inner_client = self.client.clone();
        let app_token = self.bot_app_token.clone();
        let published_clips = self.published_clips.clone();
        let discord_config = self.config.discord.clone();

        let persist = self.opts.persist.clone();
        tokio::spawn(async move {
            let Some(discord) = discord_config else {
                return;
            };

            let mut every_25 = -2;
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                every_25 += 1;

                let token = app_token.lock().await.clone();

                if every_25 > 25 || every_25 == -1 {
                    Bot::handle_clip_leaderboard(&inner_client, &broadcaster, &token, &discord)
                        .await;
                }

                if every_25 > 25 {
                    let app_token = app_token.lock().await.clone();

                    let options = [
                        "Go to our website at https://cm-ss13.com/play to learn how to get involved in the action!",
                        "View available chat commands using !help.",
                        "You can switch the perspective of the camera every 30 minutes using !follow.",
                        "Join our Discord at https://discord.gg/cmss13 to get involved with the community!",
                        "Browse our forums at https://forum.cm-ss13.com for community discussion, guides and more!",
                        "Did you know: The combat correspondent's camera will always be followed by the stream when they are broadcasting.",
                        "Worried about joining the game? Don't be! Our team of mentors are always ready to help.",
                        "Browse our wiki at https://cm-ss13.com/wiki to get an idea of the game.",
                        "CM-SS13 is open-source! You can download and contribute to the code at https://github.com/cmss13-devs/cmss13 on our GitHub."
                    ];
                    let chosen = options.choose(&mut rand::rng());

                    if let Some(chosen) = chosen {
                        let _ = inner_client
                            .send_chat_message(&broadcaster, &sender_id, *chosen, &app_token)
                            .await;
                    }

                    every_25 = 0;
                }

                let Some(prev_1) = chrono::Utc::now().date_naive().pred_opt() else {
                    continue;
                };

                let Some(hms_opt) = prev_1.and_hms_opt(0, 0, 0) else {
                    continue;
                };

                let Ok(timestamp) = Timestamp::from_str(
                    &hms_opt
                        .and_utc()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                ) else {
                    continue;
                };

                let request = helix::clips::GetClipsRequest::broadcaster_id(&broadcaster)
                    .started_at(timestamp)
                    .to_owned();

                let Ok(responses) = inner_client.req_get(request, &token).await else {
                    continue;
                };

                let cloned_clips = published_clips.lock().await.clone();

                let request_client = reqwest::Client::new();
                let mut wait_interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

                let mut all_published: Vec<String> = Vec::new();
                for response in responses.data {
                    if cloned_clips.contains(&response.id) {
                        all_published.push(response.id.clone());
                        continue;
                    }

                    wait_interval.tick().await;

                    let outbound_webhook = Webhook {
                        username: Some("twitch.tv/cm_ss13".to_string()),
                        content: Some(format!(
                            "## New clip by {}\n\n{}",
                            response.creator_name, response.url
                        )),
                        ..Default::default()
                    };

                    let Ok(webhook_str) = serde_json::to_string(&outbound_webhook) else {
                        continue;
                    };

                    let _ = request_client
                        .request(Method::POST, &discord.webhook)
                        .body(webhook_str)
                        .header("Content-Type", "application/json")
                        .send()
                        .await;

                    let _ = inner_client
                        .send_chat_message(
                            &broadcaster,
                            &sender_id,
                            &*format!("New clip by @{} {}", response.creator_name, response.url),
                            &token,
                        )
                        .await;

                    tracing::info!("Clip Published: {}", &response.id);
                    all_published.push(response.id);
                }

                let our_clips = {
                    let mut published_clips = published_clips.lock().await;

                    published_clips.clear();
                    for clip in all_published {
                        published_clips.push(clip);
                    }

                    published_clips.clone()
                };

                if !our_clips.is_empty() {
                    let _ =
                        tokio::fs::write(persist.join("published.txt"), our_clips.join("|")).await;
                }

                interval.tick().await;
            }
        });
    }

    #[instrument(skip_all)]
    async fn handle_clip_leaderboard(
        client: &HelixClient<'_, reqwest::Client>,
        broadcaster: &twitch_api::types::UserId,
        twitch_app_token: &AppAccessToken,
        discord: &DiscordConfig,
    ) {
        tracing::info!("beginning update of leaderboard");

        let local: DateTime<Utc> = Utc::now();
        let Some(nd) = NaiveDate::from_ymd_opt(local.year(), local.month(), 1) else {
            tracing::warn!("failed to create date from year/month");
            return;
        };

        let Some(date_time) = nd.and_hms_opt(0, 0, 0) else {
            tracing::warn!("failed to create date/time");
            return;
        };

        let Ok(timestamp) = Timestamp::from_str(
            &date_time
                .and_utc()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ) else {
            tracing::warn!("could not convert date to rfc3339 string");
            return;
        };

        let mut all_clips: Vec<Clip> = Vec::new();

        let request = helix::clips::GetClipsRequest::broadcaster_id(broadcaster)
            .first(100)
            .started_at(&timestamp)
            .to_owned();

        if let Ok(mut responses) = client.req_get(request, twitch_app_token).await {
            for response in responses.data.clone() {
                all_clips.push(response);
            }

            while let Ok(Some(next_clip)) = responses.get_next(client, twitch_app_token).await {
                responses = next_clip;

                for response in responses.data.clone() {
                    all_clips.push(response);
                }
            }
        }

        all_clips.sort_by(|x, y| y.view_count.cmp(&x.view_count));

        let leaderboard_channel = discord.leaderboard_channel;

        let discord_http = serenity::http::Http::new(&discord.token);
        if let Ok(messages) = discord_http
            .get_messages(leaderboard_channel.into(), None, Some(5))
            .await
        {
            if messages
                .iter()
                .all(|message| message.content.contains("Place:"))
            {
                for n in 0..5 {
                    let Some(message) = messages.get(n) else {
                        tracing::warn!("no message when there should be");
                        continue;
                    };

                    let format_string = if let Some(clip) = all_clips.get(n) {
                        format!(
                            "## {} Place: {} by {} ({} views)\n\n{}",
                            (n + 1).to_ordinal_string(),
                            clip.title,
                            clip.creator_name,
                            clip.view_count,
                            clip.url
                        )
                    } else {
                        format!("## {} Place: No clip yet!", (n + 1).to_ordinal_string())
                    };

                    let _ = discord_http
                        .edit_message(
                            leaderboard_channel.into(),
                            message.id,
                            &json!({ "content": format_string }),
                            Vec::new(),
                        )
                        .await;
                }

                tracing::info!("updated existing messages");
                return;
            }
        }

        let mut wait_interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

        for n in (0..5).rev() {
            let format_string = if let Some(clip) = all_clips.get(n) {
                format!(
                    "## {} Place: {} by {} ({} views)\n\n{}",
                    (n + 1).to_ordinal_string(),
                    clip.title,
                    clip.creator_name,
                    clip.view_count,
                    clip.url
                )
            } else {
                format!("## {} Place: No clip yet!", (n + 1).to_ordinal_string())
            };

            let _ = discord_http
                .send_message(
                    leaderboard_channel.into(),
                    Vec::new(),
                    &json!({ "content": format_string }),
                )
                .await;

            wait_interval.tick().await;
        }

        tracing::info!("posted new messages");
    }

    #[tracing::instrument(skip_all)]
    fn start_redis_websocket(&self, redis_url: &str) -> Result<(), eyre::Report> {
        tracing::info!("connecting to redis");

        let redis_url = redis_url.to_string();

        let broadcaster_id = self.broadcaster.clone();
        let inner_client = self.client.clone();
        let broadcaster_user_token = self.broadcaster_user_token.clone();
        let app_access_token = self.bot_app_token.clone();
        let response_user_id = self.config.twitch.response_user_id.clone();

        tokio::spawn(async move {
            let redis_client = match redis::Client::open(redis_url.clone()) {
                Ok(ok) => ok,
                Err(err) => {
                    tracing::error!(err = ?err, "could not create redis client");
                    return;
                }
            };

            let pubsub = match redis_client.get_async_pubsub().await {
                Ok(ok) => ok,
                Err(err) => {
                    tracing::error!(err = ?err, "could not get pubsub");
                    return;
                }
            };
            let (mut sink, mut stream) = pubsub.split();

            if let Err(err) = sink.subscribe(&["byond.round"]).await {
                tracing::error!(err = ?err, "unable to subscribe on pubsub");
                return;
            }

            tracing::info!("connected to redis");
            while let Some(unwrapped) = stream.next().await {
                let Ok(unwrapped) = unwrapped.get_payload::<String>() else {
                    tracing::error!("unable to get payload as string");
                    continue;
                };

                let Ok(deserialized) = serde_json::from_str::<RedisRoundEvent>(&unwrapped) else {
                    tracing::info!("could not deserialise");
                    continue;
                };

                if deserialized.source != "cm13-live" {
                    tracing::info!("non-primary source");
                    continue;
                }

                match deserialized.string_type.as_str() {
                    "round-start" => {
                        tracing::info!("redis: round start");

                        let broadcaster_user_token = broadcaster_user_token.lock().await.clone();
                        let app_access_token = app_access_token.lock().await.clone();

                        let request =
                            get_predictions::GetPredictionsRequest::broadcaster_id(&broadcaster_id);

                        let existing_response_result = match inner_client
                            .req_get(request, &broadcaster_user_token)
                            .await
                        {
                            Ok(res) => res,
                            Err(err) => {
                                tracing::error!(err = ?err, "error getting existing predictions");
                                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                                continue;
                            }
                        };

                        let existing_response: Vec<get_predictions::Prediction> =
                            existing_response_result.data;

                        if let Some(existing) = existing_response.first() {
                            if existing.status == PredictionStatus::Active
                                || existing.status == PredictionStatus::Locked
                            {
                                let request = end_prediction::EndPredictionRequest::new();
                                let body = end_prediction::EndPredictionBody::new(
                                    &broadcaster_id,
                                    &existing.id,
                                    PredictionStatus::Canceled,
                                );

                                let _ = inner_client
                                    .req_patch(request, body, &broadcaster_user_token)
                                    .await;
                            }
                        }

                        let outcomes = vec![
                            NewPredictionOutcome::new("Marines"),
                            NewPredictionOutcome::new("Xenos"),
                        ];
                        let body = create_prediction::CreatePredictionBody::new(
                            &broadcaster_id,
                            format!("Who will win Round {}?", &deserialized.round_id),
                            &outcomes,
                            1200,
                        );

                        let request = create_prediction::CreatePredictionRequest::new();

                        let _ = inner_client
                            .req_post(request, body, &broadcaster_user_token)
                            .await
                            .inspect_err(|err| {
                                tracing::error!("error in creating prediction {}", err);
                            });

                        let _ = inner_client
                            .send_chat_message(
                                &broadcaster_id,
                                &response_user_id,
                                "New round beginning! Vote on the outcome for the next 20 minutes.",
                                &app_access_token,
                            )
                            .await
                            .inspect_err(|err| tracing::error!("error in posting to chat {}", err));

                        tracing::info!("created new prediction");
                    }
                    "round-complete" => {
                        tracing::info!("redis: round complete");

                        let broadcaster_user_token = broadcaster_user_token.lock().await.clone();
                        let app_access_token = app_access_token.lock().await.clone();

                        let request =
                            get_predictions::GetPredictionsRequest::broadcaster_id(&broadcaster_id);

                        let Ok(existing_response) =
                            inner_client.req_get(request, &broadcaster_user_token).await
                        else {
                            continue;
                        };

                        let Some(first_response) = existing_response.first() else {
                            tracing::warn!("no responses from api");
                            continue;
                        };

                        if !&first_response
                            .title
                            .contains(&deserialized.round_id.to_string())
                        {
                            tracing::warn!(
                                "most recent prediction not our roundid: {} vs {}",
                                &first_response.title,
                                &deserialized.round_id
                            );
                            continue;
                        }

                        let Some(outcome) = deserialized.round_finished else {
                            tracing::warn!("could not deserialize information from game");
                            continue;
                        };

                        let search_string = if outcome.contains("Xenomorph") {
                            "Xenos"
                        } else if outcome.contains("Marine") {
                            "Marines"
                        } else {
                            tracing::warn!("unexpected outcome, cancelling");

                            let request = end_prediction::EndPredictionRequest::new();
                            let body = end_prediction::EndPredictionBody::new(
                                &broadcaster_id,
                                &first_response.id,
                                PredictionStatus::Canceled,
                            );

                            let _ = inner_client
                                .req_patch(request, body, &broadcaster_user_token)
                                .await;
                            continue;
                        };

                        for prediction in &first_response.outcomes {
                            if prediction.title != search_string {
                                continue;
                            }

                            let request = end_prediction::EndPredictionRequest::new();
                            let body = end_prediction::EndPredictionBody::new(
                                &broadcaster_id,
                                &first_response.id,
                                PredictionStatus::Resolved,
                            )
                            .winning_outcome_id(&prediction.id);

                            let _ = inner_client
                                .req_patch(request, body, &broadcaster_user_token)
                                .await;
                            let _ = inner_client
                                .send_chat_message(
                                    &broadcaster_id,
                                    &response_user_id,
                                    &*format!("Round finished! The result was {outcome}."),
                                    &app_access_token,
                                )
                                .await;

                            break;
                        }
                    }
                    _ => {}
                }
            }
        });

        Ok(())
    }

    async fn handle_event(
        &self,
        event: Event,
        timestamp: twitch_api::types::Timestamp,
    ) -> Result<(), eyre::Report> {
        let bot_app_token = self.bot_app_token.lock().await.clone();
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

                if JOIN_REGEX
                    .find(&payload.message.text.to_lowercase())
                    .is_some()
                    && payload.chatter_user_id.as_str() != &*self.config.twitch.response_user_id
                {
                    let _ = self.client
                        .send_chat_message_reply(
                            &subscription.condition.broadcaster_user_id,
                            &self.config.twitch.response_user_id,
                            &payload.message_id,
                            "Wanting to join the game? Download BYOND at https://www.byond.com/download and head to https://cm-ss13.com/play/main to get involved!",
                            &bot_app_token,
                        )
                        .await;
                }

                if let Some(full_command) = payload.message.text.strip_prefix("!") {
                    let mut split_whitespace = full_command.split_whitespace();

                    let Some(command) = split_whitespace.next() else {
                        return Err(eyre!("Unable to find command."));
                    };

                    let rest = full_command.replace(command, "");

                    self.command(
                        &payload,
                        &subscription,
                        command,
                        rest.trim(),
                        &bot_app_token,
                    )
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
        rest: &str,
        token: &AppAccessToken,
    ) -> Result<(), eyre::Report> {
        tracing::info!("Command: {}", command);

        let mut is_moderator = false;

        for badge in &payload.badges {
            if badge.set_id.clone().take() == "moderator" {
                is_moderator = true;
                break;
            }
        }

        let json = serde_json::to_string(&GameCMTVCommand {
            query: "cmtv".to_string(),
            command: command.to_string(),
            username: Some(payload.chatter_user_login.to_string()),
            user_id: &payload.chatter_user_id.to_string(),
            is_moderator,
            auth: Some(self.config.game.comms_key.clone()),
            args: &rest.to_string(),
            source: "byond-twitch".to_string(),
        })?;

        let Some(address) = &self.config.game.host.to_socket_addrs()?.next() else {
            return Err(eyre!("Could not locate address for BYOND host"));
        };

        let ByondTopicValue::String(mut received) = http2byond::send_byond(address, &json)? else {
            return Ok(());
        };

        received.pop();

        let response = serde_json::from_str::<GameResponse>(&received)?;

        if response.statuscode == 200 || response.statuscode == 303 {
            let _ = self
                .client
                .send_chat_message_reply(
                    &subscription.condition.broadcaster_user_id,
                    &self.config.twitch.response_user_id,
                    &payload.message_id,
                    &*response.response,
                    token,
                )
                .await;
        }

        Ok(())
    }
}

static JOIN_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"([dt]o).*(join|play)|(can\si\s(join|play))|how\si\s(join|play)").unwrap()
});

#[derive(serde::Serialize)]
struct GameRequest {
    query: String,
    auth: Option<String>,
    source: String,
}

#[derive(Deserialize, Clone)]
#[allow(dead_code)]
struct GameResponse {
    statuscode: i32,
    response: String,
    data: Option<Vec<Player>>,
}

#[derive(Deserialize, Serialize, Clone)]
struct Player {
    name: String,
    job: String,
    background: String,
}

#[derive(serde::Serialize)]
struct GameCMTVCommand<'a> {
    query: String,
    command: String,
    args: &'a String,
    username: Option<String>,
    user_id: &'a String,
    is_moderator: bool,
    auth: Option<String>,
    source: String,
}

#[derive(serde::Serialize, Default)]
struct Webhook {
    content: Option<String>,
    username: Option<String>,
    avatar_url: Option<String>,
    embeds: Option<Vec<WebhookEmbed>>,
}

#[derive(serde::Serialize, Default)]
struct WebhookEmbed {
    title: String,
    #[serde(rename = "type")]
    _type: String,
    description: String,
    url: String,
    timestamp: String,
    color: i32,
    video: Option<WebhookVideo>,
    author: Option<WebhookAuthor>,
    footer: Option<WebhookFooter>,
}

#[derive(serde::Serialize, Default)]
struct WebhookVideo {
    url: String,
}

#[derive(serde::Serialize)]
struct WebhookAuthor {
    name: String,
}

#[derive(serde::Serialize)]
struct WebhookFooter {
    text: String,
}
