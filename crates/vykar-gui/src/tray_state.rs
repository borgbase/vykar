use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use tray_icon::menu::{MenuId, MenuItem, Submenu};

thread_local! {
    static TRAY_SUBMENU: RefCell<Option<Submenu>> = const { RefCell::new(None) };
}

pub(crate) fn set_submenu(submenu: Submenu) {
    TRAY_SUBMENU.with(|s| *s.borrow_mut() = Some(submenu));
}

pub(crate) fn rebuild_submenu(
    labels: &[String],
    tray_source_items: &Arc<Mutex<Vec<(MenuId, String)>>>,
) {
    TRAY_SUBMENU.with(|s| {
        if let Some(submenu) = s.borrow().as_ref() {
            while submenu.remove_at(0).is_some() {}
            let mut new_items = Vec::new();
            for label in labels {
                let mi = MenuItem::new(label, true, None);
                new_items.push((mi.id().clone(), label.clone()));
                let _ = submenu.append(&mi);
            }
            if let Ok(mut tsi) = tray_source_items.lock() {
                *tsi = new_items;
            }
        }
    });
}
