mod mpd_worker;

use askama::Template;
use axum::{
    Router,
    body::Body,
    extract::{Form, Path, Query, State},
    http::{StatusCode, header},
    response::{
        Html, IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use futures::stream::Stream;
use mpd_client::{
    Client, commands,
    filter::{Filter, Operator},
    tag::Tag,
};
use mpd_worker::{ClientCommand, PlayerState, QueueItem, start_mpd_bridge};
use std::{
    cmp::min,
    collections::{BTreeSet, HashMap},
    convert::Infallible,
    env,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};
use tokio_stream::{StreamExt, wrappers::WatchStream};

#[derive(Clone)]
struct AppState {
    state_rx: watch::Receiver<PlayerState>,
    cmd_tx: mpsc::Sender<ClientCommand>,
    mpd_addr: String,
}

#[derive(Template)]
#[template(path = "index.html")]
struct IndexTemplate {
    initial_tab_html: String,
}

#[derive(Template)]
#[template(path = "tab_player.html")]
struct TabPlayerTemplate {
    player_html: String,
}

#[derive(Template)]
#[template(path = "player_partial.html")]
struct PlayerPartialTemplate {
    has_connection_error: bool,
    connection_error_message: String,
    has_cover: bool,
    cover_cache_key: String,
    title: String,
    artist: String,
    album: String,
    is_playing: bool,
    volume: u8,
    repeat: bool,
    random: bool,
    consume: bool,
    single: bool,
    elapsed_label: String,
    duration_label: String,
    progress_percent: u8,
    queue: Vec<QueueItemView>,
}

#[derive(Clone)]
struct QueueItemView {
    display_position: usize,
    position: usize,
    title: String,
    artist: String,
    duration_label: String,
    is_current: bool,
}

#[derive(Template)]
#[template(path = "tab_library.html")]
struct TabLibraryTemplate {
    current_dir: String,
    has_parent: bool,
    parent_dir: String,
    sort_key: String,
    sort_order: String,
    title_next_order: String,
    artist_next_order: String,
    album_next_order: String,
    duration_next_order: String,
    title_indicator: String,
    artist_indicator: String,
    album_indicator: String,
    duration_indicator: String,
    per_page: usize,
    total_songs: usize,
    page: usize,
    total_pages: usize,
    page_start: usize,
    page_end: usize,
    has_prev_page: bool,
    prev_page: usize,
    has_next_page: bool,
    next_page: usize,
    directories: Vec<DirectoryView>,
    songs: Vec<SongView>,
    playlists: Vec<String>,
}

#[derive(Clone)]
struct DirectoryView {
    name: String,
    full_path: String,
}

#[derive(Clone)]
struct SongView {
    uri: String,
    title: String,
    artist: String,
    album: String,
    duration_secs: u64,
    duration_label: String,
}

#[derive(Clone)]
struct LibraryParams {
    dir: String,
    sort_key: String,
    sort_order: String,
    page: usize,
    per_page: usize,
}

#[derive(Template)]
#[template(path = "tab_playlists.html")]
struct TabPlaylistsTemplate {
    playlists: Vec<String>,
    selected_name: String,
    songs: Vec<SongView>,
}

#[derive(Template)]
#[template(path = "tab_search.html")]
struct TabSearchTemplate {
    query: String,
    results: Vec<SongView>,
    playlists: Vec<String>,
}

#[tokio::main]
async fn main() {
    let mpd_addr = match mpd_addr_from_env() {
        Ok(addr) => addr,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };

    let (state_tx, state_rx) = watch::channel(PlayerState::default());
    let (cmd_tx, cmd_rx) = mpsc::channel(32);

    println!("Connecting to MPD at {mpd_addr}");
    tokio::spawn(start_mpd_bridge(mpd_addr.clone(), state_tx, cmd_rx));

    let app_state = AppState {
        state_rx,
        cmd_tx,
        mpd_addr,
    };

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/events", get(sse_handler))
        .route("/cover/current", get(current_cover_handler))
        .route("/tab/player", get(tab_player_handler))
        .route(
            "/tab/library",
            get(tab_library_get_handler).post(tab_library_post_handler),
        )
        .route(
            "/tab/playlists",
            get(tab_playlists_get_handler).post(tab_playlists_post_handler),
        )
        .route("/tab/search", get(tab_search_handler))
        .route("/cmd/:action", post(command_handler))
        .route("/cmd/play/:position", post(play_at_handler))
        .route("/library/queue", post(enqueue_uri_handler))
        .route(
            "/library/add-to-playlist",
            post(add_uri_to_playlist_handler),
        )
        .route("/playlist/load", post(load_playlist_handler))
        .route("/playlist/save", post(save_queue_as_playlist_handler))
        .route("/playlist/delete", post(delete_playlist_handler))
        .route("/playlist/clear", post(clear_playlist_handler))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Listening on http://localhost:3000");
    axum::serve(listener, app).await.unwrap();
}

fn mpd_addr_from_env() -> Result<String, String> {
    if let Ok(addr) = env::var("MPD_ADDR") {
        let trimmed = addr.trim();
        if trimmed.is_empty() {
            return Err("MPD_ADDR is set but empty".to_string());
        }
        return Ok(trimmed.to_string());
    }

    let host = env::var("MPD_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let host = host.trim();
    if host.is_empty() {
        return Err("MPD_HOST is set but empty".to_string());
    }

    let port_raw = env::var("MPD_PORT").unwrap_or_else(|_| "6600".to_string());
    let port = port_raw
        .trim()
        .parse::<u16>()
        .map_err(|_| format!("MPD_PORT must be a valid TCP port, got `{port_raw}`"))?;

    let endpoint = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };

    Ok(endpoint)
}

async fn index_handler(State(state): State<AppState>) -> impl IntoResponse {
    let current = state.state_rx.borrow().clone();
    let player_html = player_template_from_state(&current)
        .render()
        .unwrap_or_default();
    let initial_tab_html = TabPlayerTemplate { player_html }
        .render()
        .unwrap_or_default();

    IndexTemplate { initial_tab_html }
}

async fn tab_player_handler(State(state): State<AppState>) -> Html<String> {
    let current = state.state_rx.borrow().clone();
    let player_html = player_template_from_state(&current)
        .render()
        .unwrap_or_default();
    Html(
        TabPlayerTemplate { player_html }
            .render()
            .unwrap_or_default(),
    )
}

async fn tab_library_get_handler(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Html<String> {
    let params = library_params_from_map(&query);
    Html(render_library_tab(&state, &params).await)
}

async fn tab_library_post_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> Html<String> {
    let params = library_params_from_map(&form);
    Html(render_library_tab(&state, &params).await)
}

async fn tab_playlists_get_handler(State(state): State<AppState>) -> Html<String> {
    Html(render_playlists_tab(&state, "").await)
}

async fn tab_playlists_post_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> Html<String> {
    let name = form.get("name").map_or("", String::as_str);
    Html(render_playlists_tab(&state, name).await)
}

async fn tab_search_handler(
    State(state): State<AppState>,
    Query(query): Query<HashMap<String, String>>,
) -> Html<String> {
    let q = query.get("q").map_or("", String::as_str).trim();
    Html(render_search_tab(&state, q).await)
}

async fn command_handler(Path(action): Path<String>, State(state): State<AppState>) -> StatusCode {
    let cmd = match action.as_str() {
        "toggle" => Some(ClientCommand::TogglePlay),
        "next" => Some(ClientCommand::Next),
        "prev" => Some(ClientCommand::Previous),
        "stop" => Some(ClientCommand::Stop),
        "vol-up" => Some(ClientCommand::VolumeDelta(5)),
        "vol-down" => Some(ClientCommand::VolumeDelta(-5)),
        "repeat" => Some(ClientCommand::ToggleRepeat),
        "random" => Some(ClientCommand::ToggleRandom),
        "consume" => Some(ClientCommand::ToggleConsume),
        "single" => Some(ClientCommand::ToggleSingle),
        _ => None,
    };

    if let Some(cmd) = cmd {
        if state.cmd_tx.send(cmd).await.is_ok() {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        }
    } else {
        StatusCode::BAD_REQUEST
    }
}

async fn play_at_handler(Path(position): Path<usize>, State(state): State<AppState>) -> StatusCode {
    if state
        .cmd_tx
        .send(ClientCommand::PlayAt(position))
        .await
        .is_ok()
    {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn enqueue_uri_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(uri) = form.get("uri").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if uri.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if client.command(commands::Add::uri(uri)).await.is_ok() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn add_uri_to_playlist_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(uri) = form.get("uri").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    let Some(playlist) = form.get("playlist").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if uri.trim().is_empty() || playlist.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if client
                .command(commands::AddToPlaylist::new(playlist, uri))
                .await
                .is_ok()
            {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn load_playlist_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(name) = form.get("name").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if name.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    let replace_queue = form.get("replace").is_some_and(|v| v == "1");

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if replace_queue && client.command(commands::ClearQueue).await.is_err() {
                return StatusCode::BAD_GATEWAY;
            }
            if client
                .command(commands::LoadPlaylist::name(name))
                .await
                .is_ok()
            {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn save_queue_as_playlist_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(name) = form.get("name").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if name.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if client
                .command(commands::SaveQueueAsPlaylist(name))
                .await
                .is_ok()
            {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn delete_playlist_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(name) = form.get("name").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if name.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if client.command(commands::DeletePlaylist(name)).await.is_ok() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn clear_playlist_handler(
    State(state): State<AppState>,
    Form(form): Form<HashMap<String, String>>,
) -> StatusCode {
    let Some(name) = form.get("name").map(String::as_str) else {
        return StatusCode::BAD_REQUEST;
    };
    if name.trim().is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    match connect_mpd(&state.mpd_addr).await {
        Ok(client) => {
            if client.command(commands::ClearPlaylist(name)).await.is_ok() {
                StatusCode::NO_CONTENT
            } else {
                StatusCode::BAD_GATEWAY
            }
        }
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn current_cover_handler(State(state): State<AppState>) -> Response {
    let Some(uri) = state.state_rx.borrow().current_uri.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let client = match connect_mpd(&state.mpd_addr).await {
        Ok(client) => client,
        Err(_) => return StatusCode::SERVICE_UNAVAILABLE.into_response(),
    };

    match client.album_art(&uri).await {
        Ok(Some((data, mime))) => {
            let content_type = mime.unwrap_or_else(|| "image/jpeg".to_string());
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, content_type)
                .header(header::CACHE_CONTROL, "no-store")
                .body(Body::from(data.to_vec()))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}

async fn sse_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.state_rx.clone();
    let stream = WatchStream::new(rx).map(|player| {
        let html = player_template_from_state(&player)
            .render()
            .unwrap_or_default();

        Ok(Event::default().event("player-update").data(html))
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}

fn player_template_from_state(player: &PlayerState) -> PlayerPartialTemplate {
    let queue = player
        .queue
        .iter()
        .map(|item| queue_item_to_view(item, player.current_position))
        .collect::<Vec<_>>();
    let progress_percent = if player.duration_secs == 0 {
        0
    } else {
        (((player.elapsed_secs as f64 / player.duration_secs as f64) * 100.0).round() as u8)
            .min(100)
    };

    PlayerPartialTemplate {
        has_connection_error: player.connection_error.is_some(),
        connection_error_message: player.connection_error.clone().unwrap_or_default(),
        has_cover: player.current_uri.is_some(),
        cover_cache_key: format!(
            "{}-{}",
            player.current_position.unwrap_or_default(),
            player.duration_secs
        ),
        title: player.title.clone(),
        artist: player.artist.clone(),
        album: player.album.clone(),
        is_playing: player.is_playing,
        volume: player.volume,
        repeat: player.repeat,
        random: player.random,
        consume: player.consume,
        single: player.single,
        elapsed_label: format_duration(player.elapsed_secs),
        duration_label: format_duration(player.duration_secs),
        progress_percent,
        queue,
    }
}

fn queue_item_to_view(item: &QueueItem, current_position: Option<usize>) -> QueueItemView {
    QueueItemView {
        display_position: item.position + 1,
        position: item.position,
        title: item.title.clone(),
        artist: item.artist.clone(),
        duration_label: format_duration(item.duration_secs),
        is_current: current_position == Some(item.position),
    }
}

async fn render_library_tab(state: &AppState, params: &LibraryParams) -> String {
    let dir = params.dir.trim_matches('/');
    let client = match connect_mpd(&state.mpd_addr).await {
        Ok(client) => client,
        Err(err) => return format!("<div class=\"panel\">Failed to connect to MPD: {err}</div>"),
    };

    let songs = match client.command(commands::ListAllIn::directory(dir)).await {
        Ok(songs) => songs,
        Err(err) => return format!("<div class=\"panel\">Failed to load library: {err}</div>"),
    };
    let playlists = fetch_playlist_names(&client).await;

    let mut directories = BTreeSet::new();
    let mut song_rows = Vec::new();
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{dir}/")
    };

    for song in songs {
        let remainder = if dir.is_empty() {
            song.url.as_str()
        } else if let Some(rest) = song.url.strip_prefix(&prefix) {
            rest
        } else {
            continue;
        };

        if remainder.is_empty() {
            continue;
        }

        if let Some((child, _)) = remainder.split_once('/') {
            directories.insert(child.to_string());
            continue;
        }

        song_rows.push(song_to_view(
            &song.url,
            &song.tags,
            song.duration.map(|d| d.as_secs()),
        ));
    }

    let directories = directories
        .into_iter()
        .map(|name| DirectoryView {
            full_path: join_path(dir, &name),
            name,
        })
        .collect::<Vec<_>>();

    sort_songs(&mut song_rows, &params.sort_key, &params.sort_order);

    let total_songs = song_rows.len();
    let total_pages = usize::max(1, total_songs.div_ceil(params.per_page));
    let page = min(params.page, total_pages);
    let page_start_idx = (page - 1) * params.per_page;
    let page_end_idx = min(page_start_idx + params.per_page, total_songs);
    let songs = song_rows[page_start_idx..page_end_idx].to_vec();

    let page_start = if total_songs == 0 {
        0
    } else {
        page_start_idx + 1
    };
    let page_end = page_end_idx;
    let parent_dir = parent_dir(dir).unwrap_or_default();
    let title_indicator = sort_indicator("title", &params.sort_key, &params.sort_order);
    let artist_indicator = sort_indicator("artist", &params.sort_key, &params.sort_order);
    let album_indicator = sort_indicator("album", &params.sort_key, &params.sort_order);
    let duration_indicator = sort_indicator("duration", &params.sort_key, &params.sort_order);

    TabLibraryTemplate {
        current_dir: dir.to_string(),
        has_parent: !dir.is_empty(),
        parent_dir,
        sort_key: params.sort_key.clone(),
        sort_order: params.sort_order.clone(),
        title_next_order: next_order_for_column("title", &params.sort_key, &params.sort_order),
        artist_next_order: next_order_for_column("artist", &params.sort_key, &params.sort_order),
        album_next_order: next_order_for_column("album", &params.sort_key, &params.sort_order),
        duration_next_order: next_order_for_column(
            "duration",
            &params.sort_key,
            &params.sort_order,
        ),
        title_indicator,
        artist_indicator,
        album_indicator,
        duration_indicator,
        per_page: params.per_page,
        total_songs,
        page,
        total_pages,
        page_start,
        page_end,
        has_prev_page: page > 1,
        prev_page: page.saturating_sub(1).max(1),
        has_next_page: page < total_pages,
        next_page: page + 1,
        directories,
        songs,
        playlists,
    }
    .render()
    .unwrap_or_default()
}

async fn render_playlists_tab(state: &AppState, requested_name: &str) -> String {
    let client = match connect_mpd(&state.mpd_addr).await {
        Ok(client) => client,
        Err(err) => return format!("<div class=\"panel\">Failed to connect to MPD: {err}</div>"),
    };
    let playlists = fetch_playlist_names(&client).await;
    let selected_name = if requested_name.is_empty() {
        playlists.first().cloned().unwrap_or_default()
    } else {
        requested_name.to_string()
    };

    let songs = if selected_name.is_empty() {
        Vec::new()
    } else {
        match client.command(commands::GetPlaylist(&selected_name)).await {
            Ok(items) => items
                .into_iter()
                .map(|song| song_to_view(&song.url, &song.tags, song.duration.map(|d| d.as_secs())))
                .collect(),
            Err(_) => Vec::new(),
        }
    };

    TabPlaylistsTemplate {
        playlists,
        selected_name,
        songs,
    }
    .render()
    .unwrap_or_default()
}

async fn render_search_tab(state: &AppState, q: &str) -> String {
    let client = match connect_mpd(&state.mpd_addr).await {
        Ok(client) => client,
        Err(err) => return format!("<div class=\"panel\">Failed to connect to MPD: {err}</div>"),
    };
    let playlists = fetch_playlist_names(&client).await;
    let query = q.trim();
    let results = if query.is_empty() {
        Vec::new()
    } else {
        let filter = Filter::new(Tag::any(), Operator::Contain, query);
        match client
            .command(commands::Find::new(filter).window(0..200))
            .await
        {
            Ok(items) => items
                .into_iter()
                .map(|song| song_to_view(&song.url, &song.tags, song.duration.map(|d| d.as_secs())))
                .collect(),
            Err(_) => Vec::new(),
        }
    };

    TabSearchTemplate {
        query: query.to_string(),
        results,
        playlists,
    }
    .render()
    .unwrap_or_default()
}

async fn fetch_playlist_names(client: &Client) -> Vec<String> {
    let mut names = match client.command(commands::GetPlaylists).await {
        Ok(playlists) => playlists
            .into_iter()
            .map(|playlist| playlist.name)
            .collect::<Vec<_>>(),
        Err(_) => Vec::new(),
    };
    names.sort();
    names
}

fn song_to_view(
    uri: &str,
    tags: &std::collections::HashMap<Tag, Vec<String>>,
    duration_secs: Option<u64>,
) -> SongView {
    let title = tags
        .get(&Tag::Title)
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_else(|| file_name(uri));
    let artist = tags
        .get(&Tag::Artist)
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_else(|| "Unknown Artist".to_string());
    let album = tags
        .get(&Tag::Album)
        .and_then(|v| v.first())
        .cloned()
        .unwrap_or_else(|| "Unknown Album".to_string());

    SongView {
        uri: uri.to_string(),
        title,
        artist,
        album,
        duration_secs: duration_secs.unwrap_or(0),
        duration_label: format_duration(duration_secs.unwrap_or(0)),
    }
}

fn file_name(uri: &str) -> String {
    uri.rsplit('/').next().unwrap_or(uri).to_string()
}

fn parent_dir(dir: &str) -> Option<String> {
    if dir.is_empty() {
        None
    } else if let Some((parent, _)) = dir.rsplit_once('/') {
        Some(parent.to_string())
    } else {
        Some(String::new())
    }
}

fn join_path(base: &str, child: &str) -> String {
    if base.is_empty() {
        child.to_string()
    } else {
        format!("{base}/{child}")
    }
}

async fn connect_mpd(addr: &str) -> Result<Client, String> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|err| format!("connect failed: {err}"))?;
    let (client, _) = Client::connect(stream)
        .await
        .map_err(|err| format!("handshake failed: {err}"))?;
    Ok(client)
}

fn format_duration(total_secs: u64) -> String {
    let minutes = total_secs / 60;
    let seconds = total_secs % 60;
    format!("{minutes:02}:{seconds:02}")
}

fn library_params_from_map(values: &HashMap<String, String>) -> LibraryParams {
    let dir = values.get("dir").cloned().unwrap_or_default();
    let mut sort_key = values
        .get("sort")
        .map(|v| v.to_lowercase())
        .unwrap_or_else(|| "title".to_string());
    if !matches!(sort_key.as_str(), "title" | "artist" | "album" | "duration") {
        sort_key = "title".to_string();
    }

    let mut sort_order = values
        .get("order")
        .map(|v| v.to_lowercase())
        .unwrap_or_else(|| "asc".to_string());
    if !matches!(sort_order.as_str(), "asc" | "desc") {
        sort_order = "asc".to_string();
    }

    let per_page = values
        .get("per_page")
        .and_then(|v| v.parse::<usize>().ok())
        .map(|v| v.clamp(25, 500))
        .unwrap_or(100);
    let page = values
        .get("page")
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);

    LibraryParams {
        dir,
        sort_key,
        sort_order,
        page,
        per_page,
    }
}

fn next_order_for_column(column: &str, current_sort: &str, current_order: &str) -> String {
    if current_sort == column && current_order == "asc" {
        "desc".to_string()
    } else {
        "asc".to_string()
    }
}

fn sort_indicator(column: &str, current_sort: &str, current_order: &str) -> String {
    if current_sort != column {
        return String::new();
    }
    if current_order == "asc" {
        "↑".to_string()
    } else {
        "↓".to_string()
    }
}

fn sort_songs(songs: &mut [SongView], sort_key: &str, sort_order: &str) {
    match sort_key {
        "artist" => songs.sort_by_key(|song| song.artist.to_lowercase()),
        "album" => songs.sort_by_key(|song| song.album.to_lowercase()),
        "duration" => songs.sort_by_key(|song| song.duration_secs),
        _ => songs.sort_by_key(|song| song.title.to_lowercase()),
    }
    if sort_order == "desc" {
        songs.reverse();
    }
}
