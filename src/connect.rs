use anyhow::Result;
use rustls::{
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{ring, verify_tls12_signature, verify_tls13_signature, CryptoProvider},
    pki_types::{CertificateDer, ServerName, UnixTime},
    ClientConfig, DigitallySignedStruct, SignatureScheme,
};
use std::sync::Arc;
use tokio_postgres::IsolationLevel::Serializable;
use tokio_postgres::{config::SslMode, Client, Config, NoTls, SimpleQueryMessage, Transaction};
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::{info, trace, warn};

/// Accepts any server certificate, but still checks handshake signatures.
#[derive(Debug)]
struct NoCertVerifier(Arc<CryptoProvider>);

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

async fn connect_ssl(config: &Config) -> Result<Client, tokio_postgres::Error> {
    let provider = Arc::new(ring::default_provider());
    let tls_config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring provider supports the default protocol versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerifier(provider)))
        .with_no_client_auth();

    let tls = MakeRustlsConnect::new(tls_config);
    let (client, connection) = config.connect(tls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::runtime::Handle::current().spawn(async move {
        if let Err(e) = connection.await {
            warn!("connection error: {e}");
        }
    });
    Ok(client)
}

async fn connect_nossl(config: &Config) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) = config.connect(NoTls).await?;
    tokio::runtime::Handle::current().spawn(async move {
        if let Err(e) = connection.await {
            warn!("connection error: {e}");
        }
    });
    Ok(client)
}

/// Returns a `PostgreSQL` client, Depending on the sslmode, it may
/// try to connect twice (with and without ssl).
pub async fn connect(config: &Config) -> Result<Client, tokio_postgres::Error> {
    let sslmode = config.get_ssl_mode();

    let connection = match sslmode {
        SslMode::Disable => connect_nossl(config).await,
        SslMode::Prefer => {
            let ssl = connect_ssl(config).await;
            match ssl {
                Err(e) => {
                    trace!("Failed to connect using ssl, falling back to nossl: {e:?}");
                    connect_nossl(config).await
                }
                Ok(_) => ssl,
            }
        }
        _ => connect_ssl(config).await,
    };

    match connection {
        Ok(c) => {
            // trace!("Testing if connection is working properly");
            // for query in &[
            //     "SET search_path TO pg_catalog,pg_temp",
            //     "SET client_encoding TO utf8",
            //     "SET timezone TO utc",
            //     // We don't want restrictive settings for the following
            //     // to impact our migration (by losing connections)
            //     "SET idle_in_transaction_session_timeout TO 0",
            //     "SET statement_timeout TO 0",
            //     "SET lock_timeout TO 0",
            //     // We do parallelism on our side already, for a more
            //     // stable experience, we disable parallelism on the PG side
            //     "SET max_parallel_workers TO 0",
            //     "SET max_parallel_maintenance_workers TO 0",
            // ] {
            //     c.simple_query(query).await?;
            // }
            // let pg_version: i32 = c.query_one(PG_VERSION_SQL, &[]).await?.get(0);
            // if pg_version >= 140_000 {
            //     c.simple_query("SET idle_session_timeout TO 0").await?;
            // }
            let rows = c
                .simple_query(
                    "\
SELECT
    format('%s@%s:%s/%s (pid: %s)',
            current_user,
            inet_server_addr(),
            current_setting('port'),
            current_catalog,
            pg_backend_pid()
    )",
                )
                .await?;
            let conninfo: String = match rows.first() {
                Some(SimpleQueryMessage::Row(row)) => row
                    .get(0)
                    .map_or_else(String::new, std::borrow::ToOwned::to_owned),
                _ => String::new(),
            };
            trace!("Connected successfully: {conninfo}");
            Ok(c)
        }
        Err(e) => {
            info!("Error while establishing connection: {e:?} ({config:?})",);
            Err(e)
        }
    }
}

pub struct Source {
    pub client: Client,
}

impl Source {
    pub(crate) async fn connect(config: &Config) -> Result<Self> {
        let client = connect(config).await?;
        Ok(Self { client })
    }

    pub(crate) async fn transaction(&mut self) -> Result<Transaction<'_>> {
        Ok(self
            .client
            .build_transaction()
            .isolation_level(Serializable)
            .read_only(true)
            .deferrable(true)
            .start()
            .await?)
    }
}

pub struct Target {
    pub client: Client,
}

impl Target {
    pub(crate) async fn connect(config: &Config) -> Result<Self> {
        let client = connect(config).await?;
        Ok(Self { client })
    }
}
