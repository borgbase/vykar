//! StatusNotifierWatcher presence. Slint's ksni backend fails once and never
//! retries when no watcher is registered, e.g. when autostart races the panel.

use futures_lite::StreamExt;
use zbus::fdo::DBusProxy;
use zbus::names::BusName;

const WATCHER: &str = "org.kde.StatusNotifierWatcher";

async fn dbus() -> zbus::Result<DBusProxy<'static>> {
    let conn = zbus::Connection::session().await?;
    DBusProxy::new(&conn).await
}

pub(crate) fn watcher_present() -> bool {
    zbus::block_on(async {
        dbus()
            .await?
            .name_has_owner(BusName::from_static_str(WATCHER)?)
            .await
            .map_err(zbus::Error::from)
    })
    .unwrap_or(false)
}

/// Block until a StatusNotifierWatcher owns its bus name.
pub(crate) fn wait_for_watcher() -> zbus::Result<()> {
    zbus::block_on(async {
        let dbus = dbus().await?;
        // Subscribe before checking so an owner appearing in between isn't missed.
        let mut changes = dbus
            .receive_name_owner_changed_with_args(&[(0, WATCHER)])
            .await?;
        if dbus
            .name_has_owner(BusName::from_static_str(WATCHER)?)
            .await?
        {
            return Ok(());
        }
        while let Some(signal) = changes.next().await {
            if signal.args()?.new_owner().is_some() {
                return Ok(());
            }
        }
        Err(zbus::Error::Failure("D-Bus signal stream ended".into()))
    })
}
