/// Helper for building dot/index annotated paths through nested structures.
#[derive(Debug, Clone)]
pub(super) struct Path {
    pub(super) column: usize,
    pub(super) path: String,
}

impl Path {
    pub(super) fn new(column: usize, name: &str) -> Self {
        Self {
            column,
            path: name.to_string(),
        }
    }

    pub(super) fn push_field(&self, name: &str) -> Self {
        let mut next = self.path.clone();
        if !next.is_empty() {
            next.push('.');
        }
        next.push_str(name);
        Self {
            column: self.column,
            path: next,
        }
    }

    pub(super) fn push_index(&self, index: usize) -> Self {
        let mut next = self.path.clone();
        next.push('[');
        next.push_str(&index.to_string());
        next.push(']');
        Self {
            column: self.column,
            path: next,
        }
    }

    pub(super) fn push_key(&self) -> Self {
        let mut next = self.path.clone();
        next.push_str(".<key>");
        Self {
            column: self.column,
            path: next,
        }
    }

    pub(super) fn push_value(&self) -> Self {
        let mut next = self.path.clone();
        next.push_str(".<value>");
        Self {
            column: self.column,
            path: next,
        }
    }

    pub(super) fn push_variant(&self, name: &str, tag: i8) -> Self {
        let mut next = self.path.clone();
        if !next.is_empty() {
            next.push('.');
        }
        next.push_str(name);
        next.push_str(&format!("#{}", tag));
        Self {
            column: self.column,
            path: next,
        }
    }
}
