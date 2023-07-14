use crate::prepare::Chunk;
use anyhow::{Context, Result};
use async_channel::{Receiver, Sender};
use tracing::trace;

/// Dispatches `Chunk`s
pub struct Manager {
    chunks: Vec<Chunk>,
    tx: Sender<Chunk>,
    pub rx: Receiver<Chunk>,
}

impl Manager {
    pub fn new(chunks: Vec<Chunk>) -> Self {
        let (tx, rx) = async_channel::bounded(1);
        Manager { chunks, tx, rx }
    }

    pub async fn dispatch_work(&mut self) -> Result<()> {
        while let Some(chunk) = self.chunks.pop() {
            trace!("dispatching chunk work item");
            self.tx
                .send(chunk)
                .await
                .with_context(|| "couldn't dispatch work item")?;
        }
        self.tx.close();
        Ok(())
    }
}
