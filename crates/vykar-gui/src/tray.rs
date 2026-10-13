//! Tray icon lifecycle. The `VykarTray` component lives in a UI-thread slot
//! so it can be rebuilt when the platform tray host (re)appears: Slint never
//! retries a failed registration, and on Windows it misses Explorer restarts.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use slint::{Image, ModelRc, Rgba8Pixel, SharedPixelBuffer, SharedString, VecModel};

use crate::{VykarTray, APP_TITLE};

#[cfg(target_os = "linux")]
pub(crate) mod linux;
#[cfg(target_os = "windows")]
pub(crate) mod windows;

type Factory = Box<dyn Fn() -> Result<VykarTray, String>>;

thread_local! {
    static FACTORY: RefCell<Option<Factory>> = const { RefCell::new(None) };
    static TRAY: RefCell<Option<VykarTray>> = const { RefCell::new(None) };
    // Shared by every tray instance so a rebuilt tray keeps its submenu.
    static SOURCE_LABELS: Rc<VecModel<SharedString>> = Rc::new(VecModel::default());
    static OPERATION_BUSY: Cell<bool> = const { Cell::new(false) };
}

/// Set how [`create`] builds a fully wired tray. UI thread only.
pub(crate) fn set_factory(factory: impl Fn() -> Result<VykarTray, String> + 'static) {
    FACTORY.with(|f| *f.borrow_mut() = Some(Box::new(factory)));
}

/// Build the tray, replacing any existing instance. UI thread only.
pub(crate) fn create() -> Result<(), String> {
    let tray = FACTORY.with(|f| f.borrow().as_ref().map(|make| make()));
    let tray = tray.ok_or("tray already shut down")??;
    tray.set_source_labels(SOURCE_LABELS.with(|m| ModelRc::from(m.clone())));
    tray.set_operation_busy(OPERATION_BUSY.get());
    let old = TRAY.with(|t| t.borrow_mut().replace(tray));
    drop(old);
    Ok(())
}

pub(crate) fn is_installed() -> bool {
    TRAY.with(|t| t.borrow().is_some())
}

pub(crate) fn set_source_labels(labels: Vec<SharedString>) {
    SOURCE_LABELS.with(|m| m.set_vec(labels));
}

/// Mirrors the window's `operation_busy`; shows the Cancel item.
pub(crate) fn set_operation_busy(busy: bool) {
    OPERATION_BUSY.set(busy);
    TRAY.with(|t| {
        if let Some(tray) = t.borrow().as_ref() {
            tray.set_operation_busy(busy);
        }
    });
}

/// Remove the icon and stop any rebuilds. Main-thread thread-locals are not
/// guaranteed to be dropped at exit, and Windows leaves a ghost icon behind.
pub(crate) fn shutdown() {
    FACTORY.with(|f| f.borrow_mut().take());
    let old = TRAY.with(|t| t.borrow_mut().take());
    drop(old);
}

/// Windows taskbar theme, which can differ from the app theme `Palette`
/// follows. Returns the `scheme-override` value for `VykarTray`.
#[cfg(target_os = "windows")]
fn taskbar_scheme_override() -> i32 {
    use winreg::enums::HKEY_CURRENT_USER;
    use winreg::RegKey;
    let hkcu = RegKey::predef(HKEY_CURRENT_USER);
    match hkcu
        .open_subkey(r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize")
        .ok()
        .and_then(|k| k.get_value::<u32, _>("SystemUsesLightTheme").ok())
    {
        Some(0) => 2,
        Some(_) => 1,
        None => 0,
    }
}

fn invert_icon_rgb(img: &mut image::RgbaImage) {
    for pixel in img.pixels_mut() {
        pixel[0] = 255 - pixel[0];
        pixel[1] = 255 - pixel[1];
        pixel[2] = 255 - pixel[2];
    }
}

fn to_slint_image(img: &image::RgbaImage) -> Image {
    let (w, h) = img.dimensions();
    Image::from_rgba8(SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
        img.as_raw(),
        w,
        h,
    ))
}

/// Build an unwired `VykarTray` with icons and theme set.
pub(crate) fn build_tray() -> Result<VykarTray, String> {
    let logo_bytes = include_bytes!("../../../docs/src/images/logo_simple.png");
    let mut logo_img = image::load_from_memory(logo_bytes)
        .map_err(|e| format!("failed to decode logo: {e}"))?
        .resize(44, 44, image::imageops::FilterType::Lanczos3)
        .into_rgba8();
    let light_icon = to_slint_image(&logo_img);
    invert_icon_rgb(&mut logo_img);
    let dark_icon = to_slint_image(&logo_img);

    let tray = VykarTray::new().map_err(|e| format!("tray icon build failed: {e}"))?;
    tray.set_app_title(APP_TITLE.into());
    tray.set_light_icon(light_icon);
    tray.set_dark_icon(dark_icon);
    #[cfg(target_os = "windows")]
    tray.set_scheme_override(taskbar_scheme_override());
    Ok(tray)
}
