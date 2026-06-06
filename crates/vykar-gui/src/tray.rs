use tray_icon::menu::{Menu, MenuId, MenuItem, Submenu};
use tray_icon::{Icon, TrayIconBuilder};

use crate::APP_TITLE;

#[cfg(target_os = "linux")]
fn should_invert_icon() -> bool {
    use gtk::prelude::*;
    if let Some(settings) = gtk::Settings::default() {
        if settings.is_gtk_application_prefer_dark_theme() {
            return true;
        }
        if let Some(name) = settings.gtk_theme_name() {
            let lower = name.to_lowercase();
            return lower.contains("dark") || lower.contains("inverse");
        }
    }
    false
}

#[cfg(target_os = "windows")]
fn should_invert_icon() -> bool {
    use winreg::enums::HKEY_CURRENT_USER;
    use winreg::RegKey;
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    hkcu.open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize")
        .ok()
        .and_then(|k| k.get_value::<u32, _>("SystemUsesLightTheme").ok())
        .map_or(false, |v| v == 0)
}

#[cfg(target_os = "macos")]
fn should_invert_icon() -> bool {
    false // macOS template mode handles light/dark automatically
}

fn invert_icon_rgb(img: &mut image::RgbaImage) {
    for pixel in img.pixels_mut() {
        pixel[0] = 255 - pixel[0];
        pixel[1] = 255 - pixel[1];
        pixel[2] = 255 - pixel[2];
    }
}

pub(crate) fn build_tray_icon(
) -> Result<(tray_icon::TrayIcon, MenuId, MenuId, MenuId, Submenu, MenuId), String> {
    let menu = Menu::new();

    let open_item = MenuItem::new(format!("Open {APP_TITLE}"), true, None);
    let run_now_item = MenuItem::new("Full Backup Cycle", true, None);
    let source_submenu = Submenu::new("Backup Source", true);
    let cancel_item = MenuItem::new("Cancel Backup", true, None);
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
    let mut logo_img = image::load_from_memory(logo_bytes)
        .map_err(|e| format!("failed to decode logo: {e}"))?
        .resize(44, 44, image::imageops::FilterType::Lanczos3)
        .into_rgba8();
    if should_invert_icon() {
        invert_icon_rgb(&mut logo_img);
    }
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
        cancel_item.id().clone(),
    ))
}
