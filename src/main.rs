mod mpd_worker;

use askama::Template;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Router,
};
use futures::stream::Stream;
use mpd_worker::{start_mpd_bridge, ClientCommand, PlayerState};
use std::{convert::Infallible, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_stream::{wrappers::WatchStream, StreamExt};

#[derive(Clone)]
struct AppState {
    state_rx: watch::Receiver<PlayerState>,
    cmd_tx: mpsc::Sender<ClientCommand>,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    title: String,
    artist: String,
    is_playing: bool,
}

#[derive(Template)]
#[template(path = "player_partial.html")]
struct PlayerPartialTemplate {
    title: String,
    artist: String,
    is_playing: bool,
}

#[tokio::main]
async fn main() {
    let (state_tx, state_rx) = watch::channel(PlayerState::default());
    let (cmd_tx, cmd_rx) = mpsc::channel(32);

    tokio::spawn(start_mpd_bridge("127.0.0.1:6600", state_tx, cmd_rx));

    let app_state = AppState { state_rx, cmd_tx };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/events", get(sse_handler))
        .route("/cmd/{action}", post(command_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let current = state.state_rx.borrow().clone();
    IndexTemplate {
        title: current.title,
        artist: current.artist,
        is_playing: current.is_playing,
    }
}

async fn command_handler(
    Path(action): Path<String>,
    State(state): State<AppState>,
) -> StatusCode {
    let cmd = match action.as_str() {
        "toggle" => Some(ClientCommand::TogglePlay),
        "next" => Some(ClientCommand::Next),
        "prev" => Some(ClientCommand::Previous),
        _ => None,
    };

    if let Some(cmd) = cmd {
        let _ = state.cmd_tx.send(cmd).await;
        StatusCode::NO_CONTENT
    } else {
        StatusCode::BAD_REQUEST
    }
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.state_rx.clone();
    let stream = WatchStream::new(rx).map(|player| {
        let html = PlayerPartialTemplate {
            title: player.title,
            artist: player.artist,
            is_playing: player.is_playing,
        }
        .render()
        .unwrap_or_default();

        Ok(Event::default().event("player-update").data(html))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
