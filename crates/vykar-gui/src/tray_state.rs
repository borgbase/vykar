use std::cell::RefCell;

use slint::SharedString;
use tray_icon::menu::{MenuId, MenuItem, Submenu};

thread_local! {
    static TRAY_SUBMENU: RefCell<Option<Submenu>> = const { RefCell::new(None) };
}

pub(crate) fn set_submenu(submenu: Submenu) {
    TRAY_SUBMENU.with(|s| *s.borrow_mut() = Some(submenu));
}

pub(crate) fn rebuild_submenu(labels: &[SharedString]) -> Vec<(MenuId, String)> {
    let mut new_items = Vec::new();
    TRAY_SUBMENU.with(|s| {
        if let Some(submenu) = s.borrow().as_ref() {
            while submenu.remove_at(0).is_some() {}
            for label in labels {
                let label = label.to_string();
                let mi = MenuItem::new(&label, true, None);
                new_items.push((mi.id().clone(), label));
                let _ = submenu.append(&mi);
            }
        }
    });
    new_items
}
