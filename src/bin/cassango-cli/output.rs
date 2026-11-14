//! Rendering results as pretty JSON, a table, or JSON Lines.

use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Format {
    /// Indented JSON plus a summary line (default).
    Pretty,
    /// Aligned columns of top-level fields.
    Table,
    /// One compact JSON value per line, nothing else (for jq and scripts).
    Json,
}

impl Format {
    pub fn parse(s: &str) -> Option<Format> {
        match s {
            "pretty" => Some(Format::Pretty),
            "table" => Some(Format::Table),
            "json" | "jsonl" => Some(Format::Json),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Format::Pretty => "pretty",
            Format::Table => "table",
            Format::Json => "json",
        }
    }
}

pub struct Out {
    pub format: Format,
    pub timing: bool,
}

impl Out {
    /// A status line ("inserted 1 document"). Suppressed in JSON mode.
    pub fn info(&self, msg: impl AsRef<str>) {
        if self.format != Format::Json {
            println!("{}", msg.as_ref());
        }
    }

    pub fn summary(&self, msg: impl AsRef<str>, ms: u128) {
        if self.format != Format::Json {
            if self.timing {
                println!("{} ({ms} ms)", msg.as_ref());
            } else {
                println!("{}", msg.as_ref());
            }
        }
    }

    pub fn value(&self, v: &Value) {
        match self.format {
            Format::Json => println!("{v}"),
            _ => println!("{}", serde_json::to_string_pretty(v).unwrap_or_default()),
        }
    }

    pub fn docs(&self, docs: &[Value]) {
        match self.format {
            Format::Json => docs.iter().for_each(|d| println!("{d}")),
            Format::Pretty => docs.iter().for_each(|d| self.value(d)),
            Format::Table => print!("{}", table(docs)),
        }
    }

    /// Rows of objects, always shown as a table except in JSON mode.
    pub fn rows(&self, rows: &[Value]) {
        match self.format {
            Format::Json => rows.iter().for_each(|d| println!("{d}")),
            _ => print!("{}", table(rows)),
        }
    }
}

const MAX_CELL: usize = 40;

fn cell(v: Option<&Value>) -> String {
    let s = match v {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) => "null".into(),
        Some(other) => other.to_string(),
    };
    let s = s.replace('\n', " ");
    if s.chars().count() > MAX_CELL {
        format!("{}…", s.chars().take(MAX_CELL - 1).collect::<String>())
    } else {
        s
    }
}

/// Render objects as an aligned table; columns are top-level keys in first-seen order.
pub fn table(rows: &[Value]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut cols: Vec<String> = Vec::new();
    for r in rows {
        if let Some(o) = r.as_object() {
            for k in o.keys() {
                if !cols.contains(k) {
                    cols.push(k.clone());
                }
            }
        }
    }
    if cols.is_empty() {
        return rows.iter().map(|r| format!("{r}\n")).collect();
    }
    let cells: Vec<Vec<String>> = rows.iter().map(|r| cols.iter().map(|c| cell(r.get(c))).collect()).collect();
    let widths: Vec<usize> = cols
        .iter()
        .enumerate()
        .map(|(i, c)| cells.iter().map(|row| row[i].chars().count()).max().unwrap_or(0).max(c.chars().count()))
        .collect();
    let line = |vals: &[String]| -> String {
        let parts: Vec<String> = vals.iter().zip(&widths).map(|(v, w)| format!("{v:<w$}", w = *w)).collect();
        format!("{}\n", parts.join("  ").trim_end())
    };
    let mut out = line(&cols);
    out.push_str(&line(&widths.iter().map(|w| "-".repeat(*w)).collect::<Vec<_>>()));
    for row in &cells {
        out.push_str(&line(row));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn table_aligns_and_unions_columns() {
        let t = table(&[json!({"_id": "a", "age": 31}), json!({"_id": "bob", "city": "Leeds", "tags": ["x"]})]);
        assert_eq!(
            t,
            "_id  age  city   tags\n---  ---  -----  -----\na    31\nbob       Leeds  [\"x\"]\n"
        );
    }
}
