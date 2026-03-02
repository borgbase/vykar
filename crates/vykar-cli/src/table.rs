use std::io::IsTerminal;

use comfy_table::{presets::NOTHING, Attribute, Cell, Table};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CliTableTheme {
    pub use_unicode: bool,
    pub use_color: bool,
}

impl CliTableTheme {
    pub(crate) fn detect() -> Self {
        let is_tty = std::io::stdout().is_terminal();
        let no_color = std::env::var_os("NO_COLOR").is_some();
        resolve_table_theme(is_tty, no_color)
    }

    pub(crate) fn new_data_table(self, headers: &[&str]) -> Table {
        let mut table = Table::new();
        table.load_preset(NOTHING);
        let header_cells: Vec<Cell> = headers.iter().map(|h| self.header_cell(h)).collect();
        table.set_header(header_cells);
        table
    }

    pub(crate) fn new_kv_table(self) -> Table {
        let mut table = Table::new();
        table.load_preset(NOTHING);
        table
    }

    fn header_cell(self, text: &str) -> Cell {
        let mut cell = Cell::new(text);
        if self.use_color {
            cell = cell.add_attribute(Attribute::Bold);
        }
        cell
    }

    pub(crate) fn key_cell(self, text: &str) -> Cell {
        let mut cell = Cell::new(text);
        if self.use_color {
            cell = cell.add_attribute(Attribute::Bold);
        }
        cell
    }
}

fn resolve_table_theme(is_tty: bool, no_color: bool) -> CliTableTheme {
    CliTableTheme {
        use_unicode: is_tty,
        use_color: is_tty && !no_color,
    }
}

pub(crate) fn add_kv_row(
    table: &mut Table,
    theme: CliTableTheme,
    field: &str,
    value: impl ToString,
) {
    table.add_row(vec![theme.key_cell(field), Cell::new(value.to_string())]);
}

#[cfg(test)]
mod tests {
    use comfy_table::presets::NOTHING;

    use super::resolve_table_theme;

    #[test]
    fn resolve_table_theme_enables_unicode_and_color_for_tty() {
        let theme = resolve_table_theme(true, false);
        assert!(theme.use_unicode);
        assert!(theme.use_color);
    }

    #[test]
    fn resolve_table_theme_disables_color_when_no_color_is_set() {
        let theme = resolve_table_theme(true, true);
        assert!(theme.use_unicode);
        assert!(!theme.use_color);
    }

    #[test]
    fn resolve_table_theme_uses_plain_style_when_not_tty() {
        let theme = resolve_table_theme(false, false);
        assert!(!theme.use_unicode);
        assert!(!theme.use_color);
    }

    #[test]
    fn data_table_uses_nothing_preset() {
        let theme = resolve_table_theme(false, false);
        let mut table = theme.new_data_table(&["A", "B"]);
        assert_eq!(table.current_style_as_preset(), NOTHING);
    }

    #[test]
    fn kv_table_uses_nothing_preset() {
        let theme = resolve_table_theme(true, false);
        let mut table = theme.new_kv_table();
        table.add_row(vec!["key", "value"]);
        assert_eq!(table.current_style_as_preset(), NOTHING);
    }
}
