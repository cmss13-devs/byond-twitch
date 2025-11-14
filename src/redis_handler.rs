use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;
use twitch_api::{
    helix::predictions::{
        create_prediction::{self, NewPredictionOutcome},
        end_prediction, get_predictions,
    },
    twitch_oauth2::{AppAccessToken, UserToken},
    types::{PredictionStatus, UserId},
    HelixClient,
};

use crate::RedisRoundEvent;

#[derive(Clone)]
pub struct RedisBot<'a> {
    pub broadcaster_id: UserId,
    pub inner_client: HelixClient<'a, reqwest::Client>,
    pub broadcaster_user_token: Arc<Mutex<UserToken>>,
    pub app_access_token: Arc<Mutex<AppAccessToken>>,
    pub response_user_id: String,
    pub redis_url: String,
}

impl RedisBot<'_> {
    pub async fn run(&'_ self) {
        let redis_client = match redis::Client::open(self.redis_url.clone()) {
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

                    let broadcaster_user_token = self.broadcaster_user_token.lock().await.clone();
                    let app_access_token = self.app_access_token.lock().await.clone();

                    let request = get_predictions::GetPredictionsRequest::broadcaster_id(
                        &self.broadcaster_id,
                    );

                    let existing_response_result = match self
                        .inner_client
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
                                &self.broadcaster_id,
                                &existing.id,
                                PredictionStatus::Canceled,
                            );

                            let _ = self
                                .inner_client
                                .req_patch(request, body, &broadcaster_user_token)
                                .await;
                        }
                    }

                    let outcomes = vec![
                        NewPredictionOutcome::new("Marines"),
                        NewPredictionOutcome::new("Xenos"),
                    ];
                    let body = create_prediction::CreatePredictionBody::new(
                        &self.broadcaster_id,
                        format!("Who will win Round {}?", &deserialized.round_id),
                        &outcomes,
                        1200,
                    );

                    let request = create_prediction::CreatePredictionRequest::new();

                    let _ = self
                        .inner_client
                        .req_post(request, body, &broadcaster_user_token)
                        .await
                        .inspect_err(|err| {
                            tracing::error!("error in creating prediction {}", err);
                        });

                    let _ = self
                        .inner_client
                        .send_chat_message(
                            &self.broadcaster_id,
                            &self.response_user_id,
                            "New round beginning! Vote on the outcome for the next 20 minutes.",
                            &app_access_token,
                        )
                        .await
                        .inspect_err(|err| tracing::error!("error in posting to chat {}", err));

                    tracing::info!("created new prediction");
                }
                "round-complete" => {
                    tracing::info!("redis: round complete");

                    let broadcaster_user_token = self.broadcaster_user_token.lock().await.clone();
                    let app_access_token = self.app_access_token.lock().await.clone();

                    let request = get_predictions::GetPredictionsRequest::broadcaster_id(
                        &self.broadcaster_id,
                    );

                    let Ok(existing_response) = self
                        .inner_client
                        .req_get(request, &broadcaster_user_token)
                        .await
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
                            &self.broadcaster_id,
                            &first_response.id,
                            PredictionStatus::Canceled,
                        );

                        let _ = self
                            .inner_client
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
                            &self.broadcaster_id,
                            &first_response.id,
                            PredictionStatus::Resolved,
                        )
                        .winning_outcome_id(&prediction.id);

                        let _ = self
                            .inner_client
                            .req_patch(request, body, &broadcaster_user_token)
                            .await;
                        let _ = self
                            .inner_client
                            .send_chat_message(
                                &self.broadcaster_id,
                                &self.response_user_id,
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
    }
}
