use mpd_client::{
    client::{ConnectionEvent, Subsystem},
    commands,
    responses::PlayState,
    tag::Tag,
    Client,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, watch},
};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PlayerState {
    pub title: String,
    pub artist: String,
    pub is_playing: bool,
}

#[derive(Clone, Debug)]
pub enum ClientCommand {
    TogglePlay,
    Next,
    Previous,
}

pub async fn start_mpd_bridge(
    mpd_addr: &str,
    state_tx: watch::Sender<PlayerState>,
    mut cmd_rx: mpsc::Receiver<ClientCommand>,
) {
    let stream = TcpStream::connect(mpd_addr)
        .await
        .expect("Failed to connect to MPD");
    let (client, mut state_changes) = Client::connect(stream)
        .await
        .expect("MPD handshake failed");

    let client_reader = client.clone();
    tokio::spawn(async move {
        if let Ok(state) = fetch_state(&client_reader).await {
            let _ = state_tx.send(state);
        }

        while let Some(event) = state_changes.next().await {
            if let ConnectionEvent::SubsystemChange(Subsystem::Player | Subsystem::Queue) = event {
                if let Ok(state) = fetch_state(&client_reader).await {
                    let _ = state_tx.send(state);
                }
            }
        }
    });

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ClientCommand::TogglePlay => {
                if let Ok(status) = client.command(commands::Status).await {
                    let pause = status.state == PlayState::Playing;
                    let _ = client.command(commands::SetPause(pause)).await;
                }
            }
            ClientCommand::Next => {
                let _ = client.command(commands::Next).await;
            }
            ClientCommand::Previous => {
                let _ = client.command(commands::Previous).await;
            }
        }
    }
}

async fn fetch_state(client: &Client) -> Result<PlayerState, Box<dyn std::error::Error>> {
    let status = client.command(commands::Status).await?;
    let current = client.command(commands::CurrentSong).await?;

    let (title, artist) = if let Some(song_in_queue) = current {
        let tags = song_in_queue.song.tags;
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
        (title, artist)
    } else {
        ("Stopped".into(), "No track loaded".into())
    };

    Ok(PlayerState {
        title,
        artist,
        is_playing: status.state == PlayState::Playing,
    })
}
