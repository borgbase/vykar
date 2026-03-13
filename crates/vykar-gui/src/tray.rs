use tray_icon::menu::{Menu, MenuId, MenuItem, Submenu};
use tray_icon::{Icon, TrayIconBuilder};

use crate::APP_TITLE;

pub(crate) fn build_tray_icon() -> Result<
    (
        tray_icon::TrayIcon,
        MenuId,
        MenuId,
        MenuId,
        Submenu,
        MenuItem,
    ),
    String,
> {
    let menu = Menu::new();

    let open_item = MenuItem::new(format!("Open {APP_TITLE}"), true, None);
    let run_now_item = MenuItem::new("Full Backup", true, None);
    let source_submenu = Submenu::new("Backup Source", true);
    let cancel_item = MenuItem::new("Cancel Backup", false, None);
    let quit_item = MenuItem::new("Quit", true, None);

    menu.append(&open_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&run_now_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&source_submenu)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&cancel_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&quit_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;

    let logo_bytes = include_bytes!("../../../docs/src/images/logo_simple.png");
    let logo_img = image::load_from_memory(logo_bytes)
        .map_err(|e| format!("failed to decode logo: {e}"))?
        .resize(44, 44, image::imageops::FilterType::Lanczos3)
        .into_rgba8();
    let (w, h) = logo_img.dimensions();
    let icon =
        Icon::from_rgba(logo_img.into_raw(), w, h).map_err(|e| format!("tray icon error: {e}"))?;

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip(APP_TITLE)
        .with_icon(icon)
        .with_icon_as_template(true)
        .build()
        .map_err(|e| format!("tray icon build failed: {e}"))?;

    Ok((
        tray,
        open_item.id().clone(),
        run_now_item.id().clone(),
        quit_item.id().clone(),
        source_submenu,
        cancel_item,
    ))
}
