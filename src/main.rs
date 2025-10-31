pub mod server;
pub mod websocket;

use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use eyre::eyre;
use futures::lock::MutexGuard;
use hmac::{Hmac, Mac};
use sha2::{Sha256, Sha384};
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;

use clap::Parser;
use eyre::WrapErr as _;
use http2byond::ByondTopicValue;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use jwt::{FromBase64, VerifyWithKey};
use reqwest::redirect::Policy;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, Mutex};
use twitch_api::helix;
use twitch_api::helix::predictions::create_prediction::{self, NewPredictionOutcome};
use twitch_api::helix::predictions::{end_prediction, get_predictions};
use twitch_api::twitch_oauth2::{self, AppAccessToken, Scope, TwitchToken as _, UserToken};
use twitch_api::types::{PredictionStatus, Timestamp};
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
    #[clap(long, default_value = concat!(env!("CARGO_MANIFEST_DIR"), "/persist"))]
    pub persist: std::path::PathBuf,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug)]
pub struct Config {
    byond_host: String,
    comms_key: String,
    response_user_id: String,
    discord_webhook: Option<String>,
    redis_url: Option<String>,
    twitch_secret: Option<String>,
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

    let byond_host = config.byond_host.clone();
    let comms_key = config.comms_key.clone();
    let twitch_key = config.twitch_secret.clone();

    tokio::spawn(async move {
        let _ = start_webserver(byond_host, comms_key, twitch_key).await;
    });

    let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
        ClientDefault::default_client_with_name(Some("my_chatbot".parse()?))?,
    );

    let token_path = &opts.persist.join("token.txt");

    let mut broadcaster_user_token: Option<UserToken> = None;

    if let Ok(exists) = std::fs::exists(token_path) {
        if exists {
            let read_file = std::fs::read_to_string(token_path)?;

            let tokens: Vec<&str> = read_file.split("|").collect();

            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?;

            broadcaster_user_token = Some(
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

    if broadcaster_user_token.is_none() {
        let mut builder = twitch_api::twitch_oauth2::tokens::DeviceUserTokenBuilder::new(
            opts.client_id.clone(),
            vec![Scope::UserReadChat, Scope::ChannelManagePredictions],
        );
        builder.set_secret(Some(opts.client_secret.clone()));
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
        opts.client_id.clone(),
        opts.client_secret.clone(),
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

        published_clips = read.split("|").map(|split| split.to_string()).collect();
    }

    let broadcaster_user_token = Arc::new(Mutex::new(broadcaster_user_token));
    let bot_app_token = Arc::new(Mutex::new(bot_app_token));

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
struct RedisRoundStart {
    source: String,
    round_id: i32,

    #[serde(rename = "type")]
    _type: String,

    round_name: Option<String>,
    round_finished: Option<String>,
}

async fn start_webserver(
    byond_host: String,
    comms_key: String,
    twitch_secret: Option<String>,
) -> Result<(), eyre::Report> {
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));

    let listener = TcpListener::bind(addr).await?;
    let role_icons = Arc::new(Mutex::new(RequestCache::default()));

    loop {
        let (stream, _) = listener.accept().await?;

        let io = TokioIo::new(stream);

        let byond_host = byond_host.clone();
        let comms_key = comms_key.clone();
        let role_icons = role_icons.clone();
        let twitch_secret = twitch_secret.clone();

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .serve_connection(
                    io,
                    service_fn(move |req| {
                        handle_request(
                            req,
                            byond_host.to_owned(),
                            comms_key.to_owned(),
                            role_icons.to_owned(),
                            twitch_secret.to_owned(),
                        )
                    }),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
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

async fn handle_request(
    request: Request<hyper::body::Incoming>,
    byond_host: String,
    comms_key: String,
    request_cache: Arc<Mutex<RequestCache>>,
    twitch_secret: Option<String>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match request.uri().path() {
        "/role_icons" => match *request.method() {
            Method::GET => {
                let locked = request_cache.lock().await;

                let Ok(response) = Response::builder()
                    .header("Content-Type", "application/json")
                    .body(Full::new(Bytes::from(
                        serde_json::to_string(&locked.role_icons).unwrap(),
                    )))
                else {
                    return get_response_with_code("An error occured while preparing data!", 501);
                };

                Ok(response)
            }
            Method::POST => {
                let full = request.collect().await.unwrap().to_bytes();
                let body_str = String::from_utf8(full.to_vec()).unwrap();

                let deserialised_body = serde_json::from_str::<SetRoleIcons>(&body_str).unwrap();

                if deserialised_body.auth_key != comms_key {
                    return get_response_with_code("Invalid comms key!", 401);
                };

                let mut locked = request_cache.lock().await;
                locked.role_icons.clear();

                for (key, value) in deserialised_body.role_icons.iter() {
                    locked.role_icons.insert(key.to_owned(), value.to_owned());
                }

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
                                    serde_json::to_string(&cached_data.data).unwrap(),
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
                auth: Some(comms_key),
                source: "byond-twitch-web".to_string(),
            }) else {
                return get_response_with_code("An error occured preparing to fetch data!", 501);
            };

            let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(
                &byond_host.to_socket_addrs().unwrap().next().unwrap(),
                &query,
            ) else {
                return get_response_with_code("An error occured fetching data!", 501);
            };

            received.pop();

            let Ok(game_response) = serde_json::from_str::<GameResponse>(&received) else {
                return get_response_with_code("An error occured deserializing data!", 501);
            };

            let Ok(response) = Response::builder()
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(
                    serde_json::to_string(&game_response.data).unwrap(),
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

            let full = request.collect().await.unwrap().to_bytes();
            let body_str = String::from_utf8(full.to_vec()).unwrap();

            let Ok(request) = serde_json::from_str::<FollowPlayerRequest>(&body_str) else {
                return get_response_with_code("Invalid request.", 503);
            };

            let Ok(hmac): Result<Hmac<Sha256>, _> = Hmac::new_from_slice(
                base64::prelude::BASE64_STANDARD
                    .decode(twitch_secret.as_bytes())
                    .unwrap()
                    .as_ref(),
            ) else {
                return get_response_with_code("Internal server errorer.", 500);
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
                auth: Some(comms_key),
                source: "byond-twitch-web".to_string(),
                command: "follow".to_string(),
                args: request.name,
                is_moderator,
                user_id,
                username: None,
            }) else {
                return get_response_with_code("An error occured preparing to fetch data!", 501);
            };

            let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(
                &byond_host.to_socket_addrs().unwrap().next().unwrap(),
                &query,
            ) else {
                return get_response_with_code("An error occured fetching data!", 501);
            };

            received.pop();

            let Ok(response) = serde_json::from_str::<GameResponse>(&received) else {
                return get_response_with_code("An error occured deserializing data!", 501);
            };

            get_response_with_code(&response.response, response.statuscode.try_into().unwrap())
        }
        _ => get_response_with_code("Bad path.", 200),
    }
}

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
        let _ = self.start_clip_polling();

        if let Some(redis_url) = self.config.redis_url.clone() {
            let _ = self.start_redis_websocket(redis_url);
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

    fn start_clip_polling(&self) -> Result<(), eyre::Report> {
        tracing::info!("starting clip polling...");

        let broadcaster = self.broadcaster.clone();
        let inner_client = self.client.clone();
        let app_token = self.bot_app_token.clone();
        let published_clips = self.published_clips.clone();
        let webhook = self.config.discord_webhook.clone();
        let persist = self.opts.persist.clone();
        tokio::spawn(async move {
            let Some(webhook) = webhook else {
                return;
            };

            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
            loop {
                interval.tick().await;

                let token_guard = app_token.lock().await;

                let previous_day_rfc3339 = chrono::Utc::now()
                    .date_naive()
                    .pred_opt()
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
                    .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

                let request = helix::clips::GetClipsRequest::broadcaster_id(&broadcaster)
                    .started_at(Timestamp::from_str(&previous_day_rfc3339).unwrap())
                    .to_owned();

                let Ok(responses) = inner_client.req_get(request, &*token_guard).await else {
                    continue;
                };

                let mut published_clips = published_clips.lock().await;

                let request_client = reqwest::Client::new();
                let mut wait_interval = tokio::time::interval(tokio::time::Duration::from_secs(1));

                let mut all_published: Vec<String> = Vec::new();
                for response in responses.data {
                    if published_clips.contains(&response.id) {
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

                    let _ = request_client
                        .request(Method::POST, &webhook)
                        .body(serde_json::to_string(&outbound_webhook).unwrap())
                        .header("Content-Type", "application/json")
                        .send()
                        .await;

                    tracing::info!("Clip Published: {}", &response.id);
                    all_published.push(response.id);
                }
                published_clips.clear();
                for clip in all_published {
                    published_clips.push(clip);
                }

                let _ = fs::write(persist.join("published.txt"), published_clips.join("|"));
            }
        });

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    fn start_redis_websocket(&self, redis_url: String) -> Result<(), eyre::Report> {
        tracing::info!("connecting to redis");

        let (tx, _) = broadcast::channel::<String>(100);

        let socket_tx = tx.clone();
        tokio::spawn(async move {
            let socket_server = server::WebsocketServer::new(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
                socket_tx,
            );
            let _ = socket_server.run().await;
        });

        let redis_url = redis_url.clone();

        let broadcaster_id = self.broadcaster.clone();
        let inner_client = self.client.clone();
        let broadcaster_user_token = self.broadcaster_user_token.clone();
        let response_user_id = self.config.response_user_id.clone();
        tokio::spawn(async move {
            let redis_client = redis::Client::open(redis_url).unwrap();

            let mut conn = redis_client.get_connection().unwrap();
            let mut pub_sub = conn.as_pubsub();

            pub_sub.subscribe(&["byond.round"]).unwrap();

            tracing::info!("connected to redis");

            loop {
                let msg = pub_sub.get_message().unwrap();

                let Ok(unwrapped) = msg.get_payload::<String>() else {
                    tracing::info!("non-string redis message");
                    continue;
                };

                let deserialized = match serde_json::from_str::<RedisRoundStart>(&unwrapped) {
                    Ok(ok) => ok,
                    Err(err) => panic!("{}", err),
                };

                if deserialized.source != "cm13-live" {
                    tracing::info!("non-primary source");
                    continue;
                }

                dbg!("{}", &deserialized);

                match deserialized._type.as_str() {
                    "round-start" => {
                        tracing::info!("redis: round start");

                        let broadcaster_user_token = broadcaster_user_token.lock().await;

                        let request =
                            get_predictions::GetPredictionsRequest::broadcaster_id(&broadcaster_id);
                        let existing_response: Vec<get_predictions::Prediction> = inner_client
                            .req_get(request, &*broadcaster_user_token)
                            .await
                            .wrap_err("unable to get existing predictions from api")
                            .unwrap()
                            .data;

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
                                    .req_patch(request, body, &*broadcaster_user_token)
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
                            .req_post(request, body, &*broadcaster_user_token)
                            .await
                            .wrap_err("error creating new prediction")
                            .unwrap();
                        let _ = inner_client
                            .send_chat_message(
                                &broadcaster_id,
                                &response_user_id,
                                "New round beginning! Vote on the outcome for the next 20 minutes.",
                                &*broadcaster_user_token,
                            )
                            .await
                            .wrap_err("error writing to chat")
                            .unwrap();

                        tracing::info!("created new prediction");
                    }
                    "round-complete" => {
                        tracing::info!("redis: round complete");

                        let token = broadcaster_user_token.lock().await;

                        let request =
                            get_predictions::GetPredictionsRequest::broadcaster_id(&broadcaster_id);

                        let Ok(existing_response) = inner_client.req_get(request, &*token).await
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

                            let _ = inner_client.req_patch(request, body, &*token).await;
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

                            let _ = inner_client.req_patch(request, body, &*token).await;
                            let _ = inner_client
                                .send_chat_message(
                                    &broadcaster_id,
                                    &response_user_id,
                                    &*format!("Round finished! The result was {}.", outcome),
                                    &*token,
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
        let bot_app_token = self.bot_app_token.lock().await;
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

        let json = serde_json::to_string(&GameCMTVCommand {
            query: "cmtv".to_string(),
            command: command.to_string(),
            username: Some(payload.chatter_user_login.to_string()),
            user_id: payload.chatter_user_id.to_string(),
            is_moderator,
            auth: Some(self.config.comms_key.clone()),
            args: _rest.to_string(),
            source: "byond-twitch".to_string(),
        })?;

        let Some(address) = &self.config.byond_host.to_socket_addrs()?.next() else {
            return Err(eyre!("Could not locate address for BYOND host"));
        };

        let ByondTopicValue::String(mut received) = http2byond::send_byond(address, &json)? else {
            return Ok(());
        };

        received.pop();

        let response = serde_json::from_str::<GameResponse>(&received)?;

        let _ = self
            .client
            .send_chat_message_reply(
                &subscription.condition.broadcaster_user_id,
                &self.config.response_user_id,
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
}

#[derive(serde::Serialize)]
struct GameCMTVCommand {
    query: String,
    command: String,
    args: String,
    username: Option<String>,
    user_id: String,
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
