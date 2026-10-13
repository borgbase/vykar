//! Explorer-restart recovery. Slint's tray window is message-only, and
//! message-only windows never receive the `TaskbarCreated` broadcast, so its
//! own re-add path never runs. A hidden top-level window listens instead.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::OnceLock;

use windows_sys::Win32::Foundation::{HWND, LPARAM, LRESULT, WPARAM};
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::UI::WindowsAndMessaging::{
    ChangeWindowMessageFilterEx, CreateWindowExW, DefWindowProcW, DispatchMessageW, GetMessageW,
    RegisterClassW, RegisterWindowMessageW, TranslateMessage, MSG, MSGFLT_ALLOW, WNDCLASSW,
    WS_EX_TOOLWINDOW, WS_POPUP,
};

static TASKBAR_CREATED: AtomicU32 = AtomicU32::new(0);
static ON_RESTART: OnceLock<fn()> = OnceLock::new();

fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Call `on_restart` (from a background thread) whenever Explorer recreates
/// the taskbar. Call at most once.
pub(crate) fn spawn_taskbar_listener(on_restart: fn()) {
    if ON_RESTART.set(on_restart).is_err() {
        return;
    }
    std::thread::spawn(run_listener);
}

unsafe extern "system" fn wndproc(hwnd: HWND, msg: u32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    let taskbar_created = TASKBAR_CREATED.load(Ordering::Relaxed);
    if taskbar_created != 0 && msg == taskbar_created {
        if let Some(on_restart) = ON_RESTART.get() {
            on_restart();
        }
        return 0;
    }
    // SAFETY: forwarding the unmodified arguments Windows passed to this wndproc.
    unsafe { DefWindowProcW(hwnd, msg, wparam, lparam) }
}

fn run_listener() {
    let message_name = wide("TaskbarCreated");
    let class_name = wide("VykarTaskbarListener");

    // SAFETY: `message_name` is a NUL-terminated UTF-16 string that outlives the call.
    let id = unsafe { RegisterWindowMessageW(message_name.as_ptr()) };
    if id == 0 {
        return;
    }
    TASKBAR_CREATED.store(id, Ordering::Relaxed);

    // SAFETY: a null module name returns the handle of the current executable.
    let hinstance = unsafe { GetModuleHandleW(std::ptr::null()) };
    // SAFETY: WNDCLASSW is plain data; all-zero is valid (null pointers, `None` wndproc).
    let mut class: WNDCLASSW = unsafe { std::mem::zeroed() };
    class.lpfnWndProc = Some(wndproc);
    class.hInstance = hinstance;
    class.lpszClassName = class_name.as_ptr();
    // SAFETY: `class` is fully initialised and `class_name` outlives the class.
    if unsafe { RegisterClassW(&class) } == 0 {
        return;
    }

    // A null parent makes this a top-level window (unlike HWND_MESSAGE), so it
    // receives broadcasts while staying hidden: no WS_VISIBLE, never shown.
    // SAFETY: the class was registered above; all pointer arguments are valid or null.
    let hwnd = unsafe {
        CreateWindowExW(
            WS_EX_TOOLWINDOW,
            class_name.as_ptr(),
            std::ptr::null(),
            WS_POPUP,
            0,
            0,
            0,
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            hinstance,
            std::ptr::null(),
        )
    };
    if hwnd.is_null() {
        return;
    }

    // Explorer runs at medium integrity; without this, UIPI drops its
    // broadcast when Vykar runs elevated.
    // SAFETY: `hwnd` is a live window owned by this thread; a null filter-info
    // pointer is allowed.
    unsafe { ChangeWindowMessageFilterEx(hwnd, id, MSGFLT_ALLOW, std::ptr::null_mut()) };

    // SAFETY: MSG is plain data; all-zero is a valid initial value.
    let mut msg: MSG = unsafe { std::mem::zeroed() };
    // SAFETY: standard message loop for windows owned by this thread.
    while unsafe { GetMessageW(&mut msg, std::ptr::null_mut(), 0, 0) } > 0 {
        // SAFETY: `msg` was filled in by GetMessageW.
        unsafe {
            TranslateMessage(&msg);
            DispatchMessageW(&msg);
        }
    }
}
