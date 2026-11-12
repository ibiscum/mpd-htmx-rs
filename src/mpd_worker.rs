use mpd_client::{
    Client,
    client::{ConnectionEvent, Subsystem},
    commands,
    commands::{SingleMode, SongPosition},
    responses::PlayState,
    tag::Tag,
};
use std::time::Duration;
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlayerState {
    pub connection_error: Option<String>,
    pub current_uri: Option<String>,
    pub title: String,
    pub artist: String,
    pub album: String,
    pub is_playing: bool,
    pub volume: u8,
    pub repeat: bool,
    pub random: bool,
    pub consume: bool,
    pub single: bool,
    pub elapsed_secs: u64,
    pub duration_secs: u64,
    pub current_position: Option<usize>,
    pub queue: Vec<QueueItem>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueItem {
    pub position: usize,
    pub uri: String,
    pub title: String,
    pub artist: String,
    pub duration_secs: u64,
}

#[derive(Clone, Debug)]
pub enum ClientCommand {
    TogglePlay,
    Next,
    Previous,
    Stop,
    VolumeDelta(i8),
    ToggleRepeat,
    ToggleRandom,
    ToggleConsume,
    ToggleSingle,
    PlayAt(usize),
}

pub async fn start_mpd_bridge(
    mpd_addr: String,
    state_tx: watch::Sender<PlayerState>,
    mut cmd_rx: mpsc::Receiver<ClientCommand>,
) {
    const RECONNECT_DELAY: Duration = Duration::from_secs(3);

    loop {
        let stream = match TcpStream::connect(&mpd_addr).await {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("Failed to connect to MPD at {mpd_addr}: {err}");
                let _ = state_tx.send(PlayerState {
                    connection_error: Some(format!("Disconnected from MPD: {err}")),
                    ..PlayerState::default()
                });
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        let (client, mut state_changes) = match Client::connect(stream).await {
            Ok(conn) => conn,
            Err(err) => {
                eprintln!("MPD handshake failed for {mpd_addr}: {err}");
                let _ = state_tx.send(PlayerState {
                    connection_error: Some(format!("MPD handshake failed: {err}")),
                    ..PlayerState::default()
                });
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        if let Err(err) = push_latest_state(&client, &state_tx).await {
            eprintln!("Failed to fetch initial MPD state: {err}");
        }

        let mut progress_tick = tokio::time::interval(Duration::from_secs(1));
        progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                event = state_changes.next() => {
                    match event {
                        Some(ConnectionEvent::SubsystemChange(
                            Subsystem::Player
                            | Subsystem::Queue
                            | Subsystem::Mixer
                            | Subsystem::Options,
                        )) => {
                            if let Err(err) = push_latest_state(&client, &state_tx).await {
                                eprintln!("Failed to refresh MPD state: {err}");
                            }
                        }
                        Some(_) => {}
                        None => {
                            eprintln!("MPD connection closed; reconnecting");
                            break;
                        }
                    }
                }

                _ = progress_tick.tick() => {
                    if let Err(err) = push_progress_state(&client, &state_tx).await {
                            eprintln!("Failed to refresh MPD progress: {err}");
                            break;
                    }
                }

                incoming = cmd_rx.recv() => {
                    let Some(cmd) = incoming else {
                        return;
                    };

                    if let Err(err) = run_command(&client, cmd).await {
                        eprintln!("Failed to send command to MPD: {err}");
                    } else if let Err(err) = push_latest_state(&client, &state_tx).await {
                        eprintln!("Failed to refresh MPD state after command: {err}");
                    }
                }
            }
        }

        tokio::time::sleep(RECONNECT_DELAY).await;
    }
}

async fn push_latest_state(
    client: &Client,
    state_tx: &watch::Sender<PlayerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let state = fetch_state(client).await?;
    let _ = state_tx.send(state);
    Ok(())
}

async fn push_progress_state(
    client: &Client,
    state_tx: &watch::Sender<PlayerState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let status = client.command(commands::Status).await?;
    let mut next = state_tx.borrow().clone();

    next.is_playing = status.state == PlayState::Playing;
    next.elapsed_secs = status.elapsed.map(|d| d.as_secs()).unwrap_or(0);
    next.duration_secs = status.duration.map(|d| d.as_secs()).unwrap_or(0);
    next.current_position = status.current_song.map(|(pos, _)| pos.0);

    let _ = state_tx.send(next);
    Ok(())
}

async fn run_command(
    client: &Client,
    cmd: ClientCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        ClientCommand::TogglePlay => {
            let status = client.command(commands::Status).await?;
            let pause = status.state == PlayState::Playing;
            client.command(commands::SetPause(pause)).await?;
        }
        ClientCommand::Next => {
            client.command(commands::Next).await?;
        }
        ClientCommand::Previous => {
            client.command(commands::Previous).await?;
        }
        ClientCommand::Stop => {
            client.command(commands::Stop).await?;
        }
        ClientCommand::VolumeDelta(delta) => {
            let status = client.command(commands::Status).await?;
            let target = ((status.volume as i16) + (delta as i16)).clamp(0, 100) as u8;
            client.command(commands::SetVolume(target)).await?;
        }
        ClientCommand::ToggleRepeat => {
            let status = client.command(commands::Status).await?;
            client.command(commands::SetRepeat(!status.repeat)).await?;
        }
        ClientCommand::ToggleRandom => {
            let status = client.command(commands::Status).await?;
            client.command(commands::SetRandom(!status.random)).await?;
        }
        ClientCommand::ToggleConsume => {
            let status = client.command(commands::Status).await?;
            client
                .command(commands::SetConsume(!status.consume))
                .await?;
        }
        ClientCommand::ToggleSingle => {
            let status = client.command(commands::Status).await?;
            let mode = if status.single == SingleMode::Disabled {
                SingleMode::Enabled
            } else {
                SingleMode::Disabled
            };
            client.command(commands::SetSingle(mode)).await?;
        }
        ClientCommand::PlayAt(position) => {
            client
                .command(commands::Play::song(SongPosition(position)))
                .await?;
        }
    }

    Ok(())
}

async fn fetch_state(client: &Client) -> Result<PlayerState, Box<dyn std::error::Error>> {
    let status = client.command(commands::Status).await?;
    let current = client.command(commands::CurrentSong).await?;
    let queue = client.command(commands::Queue::all()).await?;

    let (title, artist, album, current_uri) = if let Some(song_in_queue) = current {
        let tags = song_in_queue.song.tags;
        let current_uri = song_in_queue.song.url;
        let title = tags
            .get(&Tag::Title)
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_else(|| "Unknown Title".into());
        let artist = tags
            .get(&Tag::Artist)
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_else(|| "Unknown Artist".into());
        let album = tags
            .get(&Tag::Album)
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_else(|| "Unknown Album".into());
        (title, artist, album, Some(current_uri))
    } else {
        ("Stopped".into(), "No track loaded".into(), "—".into(), None)
    };

    let queue = queue
        .into_iter()
        .map(|item| {
            let title = item
                .song
                .title()
                .map(str::to_string)
                .unwrap_or_else(|| "Unknown Title".into());
            let artist = item
                .song
                .artists()
                .first()
                .cloned()
                .unwrap_or_else(|| "Unknown Artist".into());
            QueueItem {
                position: item.position.0,
                uri: item.song.url,
                title,
                artist,
                duration_secs: item.song.duration.map(|d| d.as_secs()).unwrap_or(0),
            }
        })
        .collect();

    Ok(PlayerState {
        connection_error: None,
        current_uri,
        title,
        artist,
        album,
        is_playing: status.state == PlayState::Playing,
        volume: status.volume,
        repeat: status.repeat,
        random: status.random,
        consume: status.consume,
        single: status.single != SingleMode::Disabled,
        elapsed_secs: status.elapsed.map(|d| d.as_secs()).unwrap_or(0),
        duration_secs: status.duration.map(|d| d.as_secs()).unwrap_or(0),
        current_position: status.current_song.map(|(pos, _)| pos.0),
        queue,
    })
}
