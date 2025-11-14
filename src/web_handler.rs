use std::convert::Infallible;
use std::net::ToSocketAddrs;
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc};

use base64::Engine;
use chrono::{DateTime, Duration, Utc};
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use http2byond::ByondTopicValue;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{server::conn::http1, Request};
use hyper::{Method, Response};
use hyper_util::rt::TokioIo;
use jwt::VerifyWithKey;
use serde::Deserialize;
use sha2::Sha256;
use tokio::{net::TcpListener, sync::Mutex};
use twitch_api::client::ClientDefault;
use twitch_api::helix::subscriptions::BroadcasterSubscription;
use twitch_api::HelixClient;
use twitch_api::{
    twitch_oauth2::{AppAccessToken, UserToken},
    types::UserId,
};

use crate::GameCMTVCommand;
use crate::GameConfig;
use crate::{GameRequest, GameResponse};

#[derive(Clone)]
pub struct WebServer {
    pub config: GameConfig,
    pub twitch_secret: Option<String>,
    pub app_token: Arc<Mutex<AppAccessToken>>,
    pub broadcaster_user_token: Arc<Mutex<UserToken>>,
    pub responder_user_id: String,
    pub broadcaster_user_id: UserId,
    pub persist: PathBuf,
}

impl WebServer {
    pub async fn start(
        config: GameConfig,
        twitch_secret: Option<String>,
        app_token: Arc<Mutex<AppAccessToken>>,
        broadcaster_user_token: Arc<Mutex<UserToken>>,
        responder_user_id: String,
        broadcaster_user_id: UserId,
        persist: PathBuf,
    ) {
        let server = WebServer {
            config,
            twitch_secret,
            app_token,
            broadcaster_user_token,
            responder_user_id,
            broadcaster_user_id,
            persist,
        };

        server.run().await;
    }

    async fn run(self) {
        let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
        let Ok(listener) = TcpListener::bind(addr).await else {
            return;
        };

        let cache = Arc::new(Mutex::new(RequestCache::default()));

        if tokio::fs::try_exists(self.persist.join("cache.json"))
            .await
            .is_ok()
        {
            let Ok(cache_string) = tokio::fs::read_to_string(self.persist.join("cache.json")).await
            else {
                return;
            };

            let Ok(icons) = serde_json::from_str::<HashMap<String, String>>(&cache_string) else {
                return;
            };

            cache.lock().await.role_icons = icons;
        }

        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };

            let io = TokioIo::new(stream);

            let parent = self.clone();
            let cache = cache.clone();
            tokio::task::spawn(async move {
                if let Err(err) = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            Self::handle_request(parent.clone(), req, cache.clone())
                        }),
                    )
                    .await
                {
                    eprintln!("Error serving connection: {err:?}");
                }
            });
        }
    }

    async fn handle_request(
        parent: WebServer,
        request: Request<hyper::body::Incoming>,
        request_cache: Arc<Mutex<RequestCache>>,
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
                        return Self::get_response_with_code(
                            "An error occured while preparing data!",
                            501,
                        );
                    };

                    Ok(response)
                }
                Method::POST => Self::post_roleicons(parent, request, request_cache).await,
                _ => Self::get_response_with_code("Bad method.", 404),
            },
            "/active_players" => Self::active_players(parent, request_cache).await,
            "/follow_player" => Self::follow_player(parent, request).await,
            "/active_subscribers" => Self::get_active_subscribers(parent, request).await,
            _ => Self::get_response_with_code("Bad path.", 200),
        }
    }

    async fn post_roleicons(
        parent: WebServer,
        request: Request<hyper::body::Incoming>,
        request_cache: Arc<Mutex<RequestCache>>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let Ok(collected) = request.collect().await else {
            return Self::get_response_with_code("An error occured.", 501);
        };

        let Ok(body_str) = String::from_utf8(collected.to_bytes().to_vec()) else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(deserialised_body) = serde_json::from_str::<SetRoleIcons>(&body_str) else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        if deserialised_body.auth_key != parent.config.comms_key {
            return Self::get_response_with_code("Invalid comms key!", 401);
        }

        let mut locked = request_cache.lock().await;
        locked.role_icons.clear();

        for (key, value) in &deserialised_body.role_icons {
            locked.role_icons.insert(key.to_owned(), value.to_owned());
        }

        let _ = tokio::fs::write(
            parent.persist.join("cache.json"),
            serde_json::to_string(&deserialised_body.role_icons)
                .unwrap()
                .as_bytes(),
        )
        .await;

        Self::get_response_with_code("Accepted.", 200)
    }

    async fn active_players(
        parent: WebServer,
        request_cache: Arc<Mutex<RequestCache>>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
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
                            return Self::get_response_with_code(
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
            auth: Some(parent.config.comms_key),
            source: "byond-twitch-web".to_string(),
        }) else {
            return Self::get_response_with_code("An error occured preparing to fetch data!", 501);
        };

        let Ok(mut address) = parent.config.host.to_socket_addrs() else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Some(next) = address.next() else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(&next, &query)
        else {
            return Self::get_response_with_code("An error occured fetching data!", 501);
        };

        received.pop();

        let game_response = match serde_json::from_str::<GameResponse>(&received) {
            Ok(response) => response,
            Err(err) => {
                tracing::error!(err = ?err, "failed to deserialize game response");
                return Self::get_response_with_code("An error occured deserializing data!", 501);
            }
        };

        let Ok(response) = Response::builder()
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                serde_json::to_string(&game_response.data).unwrap_or_default(),
            )))
        else {
            return Self::get_response_with_code("An error occured while preparing data!", 501);
        };

        {
            let mut cache = request_cache.lock().await;

            cache.cached_status = Some(game_response.clone());
            cache.cached_time = Some(chrono::Utc::now());
        }

        Ok(response)
    }

    async fn follow_player(
        parent: WebServer,
        request: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let Some(twitch_secret) = parent.twitch_secret else {
            return Self::get_response_with_code("Server not set up.", 500);
        };

        let Ok(collected) = request.collect().await else {
            return Self::get_response_with_code("An error occured.", 501);
        };

        let Ok(body_str) = String::from_utf8(collected.to_bytes().to_vec()) else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(request) = serde_json::from_str::<FollowPlayerRequest>(&body_str) else {
            return Self::get_response_with_code("Invalid request.", 503);
        };

        let Ok(base_64) = base64::prelude::BASE64_STANDARD.decode(twitch_secret.as_bytes()) else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(hmac): Result<Hmac<Sha256>, _> = Hmac::new_from_slice(&base_64) else {
            return Self::get_response_with_code("Internal server error.", 500);
        };

        let Ok(claim): Result<TwitchExtToken, jwt::Error> = request.token.verify_with_key(&hmac)
        else {
            return Self::get_response_with_code("Internal server error.", 500);
        };

        let Some(user_id) = claim.user_id else {
            return Self::get_response_with_code(
                "You need to allow the extension to view your identity.",
                200,
            );
        };

        let is_moderator = claim.role == "moderator" || claim.role == "broadcaster";

        let Ok(query) = serde_json::to_string(&GameCMTVCommand {
            query: "cmtv".to_string(),
            auth: Some(parent.config.comms_key),
            source: "byond-twitch-web".to_string(),
            command: "follow".to_string(),
            args: &request.name,
            is_moderator,
            user_id: &user_id,
            username: None,
        }) else {
            return Self::get_response_with_code("An error occured preparing to fetch data!", 501);
        };

        let Ok(mut address) = parent.config.host.to_socket_addrs() else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Some(next) = address.next() else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(ByondTopicValue::String(mut received)) = http2byond::send_byond(&next, &query)
        else {
            return Self::get_response_with_code("An error occured fetching data!", 501);
        };

        received.pop();

        let Ok(response) = serde_json::from_str::<GameResponse>(&received) else {
            return Self::get_response_with_code("An error occured deserializing data!", 501);
        };

        if let Ok(code) = response.statuscode.try_into() {
            if code == 200 {
                let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
                    ClientDefault::default_client_with_name(Some(
                        "byond-twitch-web".parse().unwrap(),
                    ))
                    .unwrap(),
                );

                let app_token = parent.app_token.lock().await.clone();
                if let Ok(Some(user)) = client.get_user_from_id(&user_id, &app_token).await {
                    let _ = client
                        .send_chat_message(
                            parent.broadcaster_user_id,
                            parent.responder_user_id,
                            &*format!(
                                "{} has requested to follow '{}' via Overlay!",
                                user.display_name, request.name
                            ),
                            &app_token,
                        )
                        .await;
                }
            }

            Self::get_response_with_code(&response.response, code)
        } else {
            Self::get_response_with_code("Internal server error.", 501)
        }
    }

    async fn get_active_subscribers(
        parent: WebServer,
        request: Request<hyper::body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let Ok(collected) = request.collect().await else {
            return Self::get_response_with_code("An error occured.", 501);
        };

        let Ok(body_str) = String::from_utf8(collected.to_bytes().to_vec()) else {
            return Self::get_response_with_code("Internal server error.", 501);
        };

        let Ok(request) = serde_json::from_str::<GetRequest>(&body_str) else {
            return Self::get_response_with_code("Invalid request.", 503);
        };

        if request.auth_key != parent.config.comms_key {
            return Self::get_response_with_code("Unauthorized.", 401);
        }

        let client: HelixClient<reqwest::Client> = twitch_api::HelixClient::with_client(
            ClientDefault::default_client_with_name(Some("byond-twitch-web".parse().unwrap()))
                .unwrap(),
        );

        let Ok(subs) = client
            .get_broadcaster_subscriptions(&parent.broadcaster_user_token.lock().await.clone())
            .try_collect::<Vec<BroadcasterSubscription>>()
            .await
        else {
            return Self::get_response_with_code("Failed to get subscriptions.", 501);
        };

        let subs: Vec<UserId> = subs.iter().map(|sub| sub.user_id.clone()).collect();

        let Ok(response) = Response::builder()
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                serde_json::to_string(&subs).unwrap_or_default(),
            )))
        else {
            return Self::get_response_with_code("An error occured while preparing data!", 501);
        };

        Ok(response)
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
#[derive(Deserialize)]
struct GetRequest {
    auth_key: String,
}
