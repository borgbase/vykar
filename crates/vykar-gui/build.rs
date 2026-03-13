fn main() {
    slint_build::compile("ui/app.slint").unwrap();

    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "windows" {
        let mut res = winresource::WindowsResource::new();
        res.set_icon("windows/icon.ico");
        res.compile().expect("failed to compile Windows resources");
    }
}
