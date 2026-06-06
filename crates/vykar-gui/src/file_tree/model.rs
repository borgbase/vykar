#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CheckState {
    Unchecked = 0,
    Checked = 1,
    Partial = 2,
}

#[derive(Debug, Clone)]
pub(crate) struct TreeNode {
    pub name: String,
    /// Display path (used in tests and for node identification).
    #[cfg_attr(not(test), allow(dead_code))]
    pub full_path: String,
    pub entry_type: String,
    pub permissions: String,
    pub size_str: String,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
    pub expanded: bool,
    pub check_state: CheckState,
    pub depth: usize,
    pub is_dir: bool,
    /// Original item paths for restore. Empty = synthetic node (never collapse).
    /// Multiple entries = merged from overlapping sources (emit all on collapse).
    pub item_paths: Vec<String>,
    /// True if this node or any descendant has merged origins (item_paths.len() > 1)
    /// or is synthetic. When true, a Checked node with a single item_path must
    /// recurse instead of collapsing, because descendants may have origins not
    /// covered by this node's item_path prefix.
    pub mixed_origins: bool,
}

pub(crate) struct FileTree {
    pub arena: Vec<TreeNode>,
    pub roots: Vec<usize>,
    pub visible_rows: Vec<usize>,
}
