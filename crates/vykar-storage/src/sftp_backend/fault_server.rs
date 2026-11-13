//! In-process SSH + SFTP server over a temp directory, with injectable faults,
//! for exercising `SftpBackend`'s atomic upload path end to end.

// Handler methods mirror the async trait signatures; most need no `.await`.
#![allow(clippy::unused_async_trait_impl)]

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use russh::keys::ssh_key::{private::Ed25519Keypair, LineEnding};
use russh::keys::PrivateKey;
use russh::server::{Auth, ChannelOpenHandle, Msg, Session};
use russh::{Channel, ChannelId, Disconnect};
use russh_sftp::protocol::{
    Attrs, Data, File, FileAttributes, Handle, Name, OpenFlags, Packet, Status, StatusCode, Version,
};

/// Faults armed by a test. Each `*_once` flag fires on the next matching
/// request and then clears itself.
#[derive(Default)]
pub(super) struct Faults {
    pub hide_posix_rename: AtomicBool,
    pub refuse_rename: AtomicBool,
    /// Perform the rename, then kill the connection instead of replying.
    pub drop_rename_reply_once: AtomicBool,
    /// Kill the connection on the next WRITE, before writing anything.
    pub drop_on_write_once: AtomicBool,
    /// Seconds added to every mtime the server reports, as if its clock
    /// differed from the client's.
    pub mtime_skew_secs: AtomicI64,
    pub writes: AtomicUsize,
    pub renames: AtomicUsize,
}

pub(super) struct FaultServer {
    pub dir: tempfile::TempDir,
    pub port: u16,
    pub faults: Arc<Faults>,
    client_key: PathBuf,
    known_hosts: PathBuf,
    _runtime: tokio::runtime::Runtime,
}

impl FaultServer {
    pub fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("repo")).unwrap();

        let client_key = dir.path().join("id_ed25519");
        let key = PrivateKey::from(Ed25519Keypair::from_seed(&[7; 32]));
        std::fs::write(
            &client_key,
            key.to_openssh(LineEnding::LF).unwrap().as_bytes(),
        )
        .unwrap();
        let known_hosts = dir.path().join("known_hosts");

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let faults = Arc::new(Faults::default());
        let root = dir.path().to_path_buf();
        let listener = runtime
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .unwrap();
        let port = listener.local_addr().unwrap().port();

        let config = Arc::new(russh::server::Config {
            keys: vec![PrivateKey::from(Ed25519Keypair::from_seed(&[9; 32]))],
            auth_rejection_time: Duration::from_millis(1),
            auth_rejection_time_initial: Some(Duration::ZERO),
            ..Default::default()
        });
        let accept_faults = Arc::clone(&faults);
        runtime.spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let handler = SshHandler {
                    root: root.clone(),
                    faults: Arc::clone(&accept_faults),
                    channels: HashMap::new(),
                };
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    if let Ok(session) = russh::server::run_stream(config, stream, handler).await {
                        let _ = session.await;
                    }
                });
            }
        });

        Self {
            dir,
            port,
            faults,
            client_key,
            known_hosts,
            _runtime: runtime,
        }
    }

    pub fn backend(&self, max_retries: usize) -> super::SftpBackend {
        super::SftpBackend::new(
            "127.0.0.1",
            Some("vykar"),
            Some(self.port),
            "/repo",
            Some(self.client_key.to_str().unwrap()),
            Some(self.known_hosts.to_str().unwrap()),
            Some(1),
            Some(5),
            crate::RetryConfig {
                max_retries,
                retry_delay_ms: 1,
                retry_max_delay_ms: 1,
            },
        )
        .unwrap()
    }

    /// Path of a repository key on the server's disk.
    pub fn path(&self, key: &str) -> PathBuf {
        self.dir.path().join("repo").join(key)
    }

    /// Names of temp files in the directory holding `key`.
    pub fn temps_next_to(&self, key: &str) -> Vec<String> {
        let parent = self.path(key).parent().unwrap().to_path_buf();
        let mut out: Vec<String> = std::fs::read_dir(parent)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| vykar_protocol::is_temp_file(n))
            .collect();
        out.sort();
        out
    }
}

struct SshHandler {
    root: PathBuf,
    faults: Arc<Faults>,
    channels: HashMap<ChannelId, Channel<Msg>>,
}

impl russh::server::Handler for SshHandler {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        _user: &str,
        _key: russh::server::VerifiedPublicKey<'_>,
    ) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        reply: ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.channels.insert(channel.id(), channel);
        reply.accept().await;
        Ok(())
    }

    async fn subsystem_request(
        &mut self,
        channel_id: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        match (name, self.channels.remove(&channel_id)) {
            ("sftp", Some(channel)) => {
                session.channel_success(channel_id)?;
                let sftp = SftpHandler {
                    root: self.root.clone(),
                    faults: Arc::clone(&self.faults),
                    session: session.handle(),
                    files: HashMap::new(),
                    dirs: HashMap::new(),
                    next_handle: 0,
                };
                russh_sftp::server::run(channel.into_stream(), sftp).await;
            }
            _ => session.channel_failure(channel_id)?,
        }
        Ok(())
    }
}

struct SftpHandler {
    root: PathBuf,
    faults: Arc<Faults>,
    session: russh::server::Handle,
    files: HashMap<String, std::fs::File>,
    dirs: HashMap<String, Option<Vec<File>>>,
    next_handle: u64,
}

fn ok(id: u32) -> Status {
    Status {
        id,
        status_code: StatusCode::Ok,
        error_message: String::new(),
        language_tag: "en-US".into(),
    }
}

fn io_status(e: &std::io::Error) -> StatusCode {
    match e.kind() {
        std::io::ErrorKind::NotFound => StatusCode::NoSuchFile,
        std::io::ErrorKind::PermissionDenied => StatusCode::PermissionDenied,
        _ => StatusCode::Failure,
    }
}

fn sftp_string(buf: &[u8]) -> Option<(String, &[u8])> {
    let (len, rest) = buf.split_at_checked(4)?;
    let len = u32::from_be_bytes(len.try_into().ok()?) as usize;
    let (s, rest) = rest.split_at_checked(len)?;
    Some((String::from_utf8(s.to_vec()).ok()?, rest))
}

impl SftpHandler {
    fn local(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_start_matches('/'))
    }

    fn new_handle(&mut self) -> String {
        self.next_handle += 1;
        self.next_handle.to_string()
    }

    /// Drop the whole SSH connection. Outstanding requests never get a reply.
    async fn kill_connection(&self) {
        let _ = self
            .session
            .disconnect(Disconnect::ByApplication, "fault".into(), String::new())
            .await;
    }
}

fn attrs_of(path: &Path, faults: &Faults) -> Result<FileAttributes, StatusCode> {
    let mut attrs = std::fs::metadata(path)
        .map(|m| FileAttributes::from(&m))
        .map_err(|e| io_status(&e))?;
    let skew = faults.mtime_skew_secs.load(Ordering::SeqCst);
    if let Some(mtime) = attrs.mtime.as_mut() {
        *mtime = (i64::from(*mtime) + skew).clamp(0, i64::from(u32::MAX)) as u32;
    }
    Ok(attrs)
}

impl russh_sftp::server::Handler for SftpHandler {
    type Error = StatusCode;

    fn unimplemented(&self) -> Self::Error {
        StatusCode::OpUnsupported
    }

    async fn init(
        &mut self,
        _version: u32,
        _extensions: HashMap<String, String>,
    ) -> Result<Version, Self::Error> {
        let mut version = Version::new();
        if !self.faults.hide_posix_rename.load(Ordering::SeqCst) {
            version
                .extensions
                .insert("posix-rename@openssh.com".into(), "1".into());
        }
        Ok(version)
    }

    async fn open(
        &mut self,
        id: u32,
        filename: String,
        pflags: OpenFlags,
        _attrs: FileAttributes,
    ) -> Result<Handle, Self::Error> {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(pflags.contains(OpenFlags::READ))
            .write(pflags.contains(OpenFlags::WRITE))
            .truncate(pflags.contains(OpenFlags::TRUNCATE));
        if pflags.contains(OpenFlags::EXCLUDE) {
            opts.create_new(true);
        } else if pflags.contains(OpenFlags::CREATE) {
            opts.create(true);
        }
        let file = opts
            .open(self.local(&filename))
            .map_err(|e| io_status(&e))?;
        let handle = self.new_handle();
        self.files.insert(handle.clone(), file);
        Ok(Handle { id, handle })
    }

    async fn close(&mut self, id: u32, handle: String) -> Result<Status, Self::Error> {
        self.files.remove(&handle);
        self.dirs.remove(&handle);
        Ok(ok(id))
    }

    async fn read(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        len: u32,
    ) -> Result<Data, Self::Error> {
        let file = self.files.get_mut(&handle).ok_or(StatusCode::Failure)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_status(&e))?;
        let mut data = vec![0; len as usize];
        let n = file.read(&mut data).map_err(|e| io_status(&e))?;
        if n == 0 {
            return Err(StatusCode::Eof);
        }
        data.truncate(n);
        Ok(Data { id, data })
    }

    async fn write(
        &mut self,
        id: u32,
        handle: String,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<Status, Self::Error> {
        if self.faults.drop_on_write_once.swap(false, Ordering::SeqCst) {
            self.kill_connection().await;
            std::future::pending::<()>().await;
        }
        self.faults.writes.fetch_add(1, Ordering::SeqCst);
        let file = self.files.get_mut(&handle).ok_or(StatusCode::Failure)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_status(&e))?;
        file.write_all(&data).map_err(|e| io_status(&e))?;
        Ok(ok(id))
    }

    async fn stat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        Ok(Attrs {
            id,
            attrs: attrs_of(&self.local(&path), &self.faults)?,
        })
    }

    async fn lstat(&mut self, id: u32, path: String) -> Result<Attrs, Self::Error> {
        self.stat(id, path).await
    }

    async fn mkdir(
        &mut self,
        id: u32,
        path: String,
        _attrs: FileAttributes,
    ) -> Result<Status, Self::Error> {
        std::fs::create_dir(self.local(&path)).map_err(|e| match e.kind() {
            std::io::ErrorKind::AlreadyExists => StatusCode::Failure,
            _ => io_status(&e),
        })?;
        Ok(ok(id))
    }

    async fn remove(&mut self, id: u32, filename: String) -> Result<Status, Self::Error> {
        std::fs::remove_file(self.local(&filename)).map_err(|e| io_status(&e))?;
        Ok(ok(id))
    }

    async fn opendir(&mut self, id: u32, path: String) -> Result<Handle, Self::Error> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(self.local(&path)).map_err(|e| io_status(&e))? {
            let entry = entry.map_err(|e| io_status(&e))?;
            let filename = entry.file_name().to_string_lossy().into_owned();
            files.push(File {
                longname: filename.clone(),
                filename,
                attrs: attrs_of(&entry.path(), &self.faults)?,
            });
        }
        let handle = self.new_handle();
        self.dirs.insert(handle.clone(), Some(files));
        Ok(Handle { id, handle })
    }

    async fn readdir(&mut self, id: u32, handle: String) -> Result<Name, Self::Error> {
        match self.dirs.get_mut(&handle).and_then(Option::take) {
            Some(files) => Ok(Name { id, files }),
            None => Err(StatusCode::Eof),
        }
    }

    async fn extended(
        &mut self,
        id: u32,
        request: String,
        data: Vec<u8>,
    ) -> Result<Packet, Self::Error> {
        if request != "posix-rename@openssh.com"
            || self.faults.hide_posix_rename.load(Ordering::SeqCst)
        {
            return Err(StatusCode::OpUnsupported);
        }
        let (old, rest) = sftp_string(&data).ok_or(StatusCode::BadMessage)?;
        let (new, _) = sftp_string(rest).ok_or(StatusCode::BadMessage)?;
        if self.faults.refuse_rename.load(Ordering::SeqCst) {
            return Err(StatusCode::Failure);
        }
        std::fs::rename(self.local(&old), self.local(&new)).map_err(|e| io_status(&e))?;
        self.faults.renames.fetch_add(1, Ordering::SeqCst);
        if self
            .faults
            .drop_rename_reply_once
            .swap(false, Ordering::SeqCst)
        {
            self.kill_connection().await;
            std::future::pending::<()>().await;
        }
        Ok(Packet::Status(ok(id)))
    }
}
