//! Statement parser for the cassango shell.
//!
//! Two kinds of statements:
//!
//! ```text
//! db.<collection>.<method>(<args>)[.<modifier>(<args>)]...    e.g. db.users.find({age: {$gt: 30}}).limit(5)
//! <shell command> [args]                                        e.g. show cluster, consistency one
//! ```
//!
//! Arguments are JSON5, so keys may be unquoted (`{age: {$gt: 30}}`), strings may use
//! single quotes, and trailing commas are fine. Dotted keys still need quotes:
//! `{"addr.city": "Leeds"}`.

use serde_json::Value;

#[derive(Debug, PartialEq)]
pub enum Command {
    Help(Option<String>),
    Exit,
    ShowCollections,
    ShowCluster,
    Connect(String),
    Consistency(Option<String>),
    Format(Option<String>),
    Timing(Option<bool>),
    Repair,
    Compact { major: bool },
    Cleanup,
    Db(DbCall),
}

#[derive(Debug, PartialEq)]
pub struct DbCall {
    pub collection: String,
    pub method: String,
    pub args: Vec<Value>,
    /// Chained calls such as `.sort({...})`, `.limit(10)`, `.explain()`.
    pub modifiers: Vec<(String, Vec<Value>)>,
}

/// (name, min args, max args)
const METHODS: &[(&str, usize, usize)] = &[
    ("insert", 1, 1),
    ("insertMany", 1, 1),
    ("get", 1, 1),
    ("find", 0, 1),
    ("findOne", 0, 1),
    ("count", 0, 1),
    ("update", 2, 2),
    ("updateMany", 2, 2),
    ("replace", 2, 2),
    ("delete", 1, 1),
    ("deleteMany", 1, 1),
    ("createIndex", 1, 1),
    ("dropIndex", 1, 1),
    ("indexes", 0, 0),
    ("owners", 1, 1),
];

const MODIFIERS: &[(&str, usize, usize)] = &[
    ("sort", 1, 1),
    ("limit", 1, 1),
    ("skip", 1, 1),
    ("explain", 0, 0),
    ("count", 0, 0),
    ("consistency", 1, 1),
];

/// Remove a trailing `// comment` that isn't inside a string.
pub fn strip_comment(line: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    let mut prev = '\0';
    for (i, ch) in line.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
        } else if ch == '"' || ch == '\'' {
            quote = Some(ch);
        } else if ch == '/' && prev == '/' {
            return line[..i - 1].trim_end();
        }
        prev = ch;
    }
    line
}

/// True when `input` has balanced brackets and no unterminated string, i.e. the
/// shell can stop reading continuation lines.
pub fn is_complete(input: &str) -> bool {
    let mut depth = 0i32;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for ch in input.chars() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => depth -= 1,
            _ => {}
        }
    }
    quote.is_none() && depth <= 0
}

pub fn parse(input: &str) -> Result<Command, String> {
    let s = input.trim().trim_end_matches(';').trim();
    if s.is_empty() {
        return Err("empty statement".into());
    }
    if let Some(rest) = s.strip_prefix("db.") {
        return parse_db(rest).map(Command::Db);
    }
    let words: Vec<&str> = s.split_whitespace().collect();
    let arg = words.get(1).map(|w| w.to_string());
    let cmd = match words[0].to_ascii_lowercase().as_str() {
        "help" | "?" | "\\h" => Command::Help(arg),
        "exit" | "quit" | "\\q" => Command::Exit,
        "show" => match words.get(1).map(|w| w.to_ascii_lowercase()).as_deref() {
            Some("collections") => Command::ShowCollections,
            Some("cluster") | Some("nodes") => Command::ShowCluster,
            _ => return Err("usage: show collections | show cluster".into()),
        },
        "status" => Command::ShowCluster,
        "connect" => Command::Connect(arg.ok_or("usage: connect <host:port>[,<host:port>...]")?),
        "consistency" => Command::Consistency(arg),
        "format" => Command::Format(arg),
        "timing" => Command::Timing(match arg.as_deref() {
            None => None,
            Some("on") => Some(true),
            Some("off") => Some(false),
            Some(_) => return Err("usage: timing [on|off]".into()),
        }),
        "repair" => Command::Repair,
        "compact" => Command::Compact { major: arg.as_deref() == Some("major") },
        "cleanup" => Command::Cleanup,
        other => return Err(format!("unknown command '{other}'. Type 'help' for a list of commands.")),
    };
    Ok(cmd)
}

fn ident(s: &str) -> (&str, &str) {
    let end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// Given text starting just after `(`, return (inside, text after the matching `)`).
fn call_args(s: &str) -> Result<(&str, &str), String> {
    let mut depth = 1i32;
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, ch) in s.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => {
                depth -= 1;
                if depth == 0 {
                    if ch != ')' {
                        return Err(format!("mismatched '{ch}'"));
                    }
                    return Ok((&s[..i], &s[i + 1..]));
                }
            }
            _ => {}
        }
    }
    Err("missing ')'".into())
}

/// Split call arguments at top-level commas.
fn split_args(s: &str) -> Vec<&str> {
    let (mut out, mut depth, mut quote, mut escaped, mut start) = (Vec::new(), 0i32, None::<char>, false, 0);
    for (i, ch) in s.char_indices() {
        if let Some(q) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == q {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' => quote = Some(ch),
            '(' | '{' | '[' => depth += 1,
            ')' | '}' | ']' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out.into_iter().map(str::trim).filter(|a| !a.is_empty()).collect()
}

fn parse_args(inside: &str, what: &str) -> Result<Vec<Value>, String> {
    split_args(inside)
        .into_iter()
        .enumerate()
        .map(|(i, a)| json5::from_str::<Value>(a).map_err(|e| format!("{what}: argument {} is not valid JSON: {e}", i + 1)))
        .collect()
}

fn check_arity(table: &[(&str, usize, usize)], name: &str, n: usize, kind: &str) -> Result<(), String> {
    let Some((_, min, max)) = table.iter().find(|(m, _, _)| *m == name) else {
        let names: Vec<&str> = table.iter().map(|(m, _, _)| *m).collect();
        return Err(format!("unknown {kind} '{name}'. Available: {}", names.join(", ")));
    };
    if n < *min || n > *max {
        let want = if min == max { format!("{min}") } else { format!("{min}-{max}") };
        return Err(format!("{name}() takes {want} argument(s), got {n}"));
    }
    Ok(())
}

fn parse_db(s: &str) -> Result<DbCall, String> {
    let (collection, rest) = ident(s);
    if collection.is_empty() {
        return Err("expected a collection name after 'db.'".into());
    }
    let rest = rest.strip_prefix('.').ok_or_else(|| format!("expected '.<method>(...)' after 'db.{collection}'"))?;
    let (method, rest) = ident(rest);
    let rest = rest
        .trim_start()
        .strip_prefix('(')
        .ok_or_else(|| format!("expected '(' after '{method}'"))?;
    let (inside, mut rest) = call_args(rest)?;
    let args = parse_args(inside, method)?;
    check_arity(METHODS, method, args.len(), "method")?;

    let mut modifiers = Vec::new();
    loop {
        rest = rest.trim_start();
        if rest.is_empty() {
            break;
        }
        let r = rest.strip_prefix('.').ok_or_else(|| format!("unexpected text: '{rest}'"))?;
        let (name, r) = ident(r);
        let r = r.trim_start().strip_prefix('(').ok_or_else(|| format!("expected '(' after '{name}'"))?;
        let (inside, r) = call_args(r)?;
        let margs = parse_args(inside, name)?;
        check_arity(MODIFIERS, name, margs.len(), "modifier")?;
        if name != "consistency" && method != "find" {
            return Err(format!(".{name}() can only follow find()"));
        }
        modifiers.push((name.to_string(), margs));
        rest = r;
    }
    Ok(DbCall { collection: collection.to_string(), method: method.to_string(), args, modifiers })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn db(s: &str) -> DbCall {
        match parse(s).unwrap() {
            Command::Db(c) => c,
            other => panic!("not a db call: {other:?}"),
        }
    }

    #[test]
    fn find_with_relaxed_json_and_modifiers() {
        let c = db(r#"db.users.find({age: {$gt: 30}, 'addr.city': "Leeds", tags: ['a',],}).sort({age: -1}).limit(5).explain();"#);
        assert_eq!(c.collection, "users");
        assert_eq!(c.method, "find");
        assert_eq!(c.args, vec![json!({"age": {"$gt": 30}, "addr.city": "Leeds", "tags": ["a"]})]);
        let mods: Vec<&str> = c.modifiers.iter().map(|(m, _)| m.as_str()).collect();
        assert_eq!(mods, vec!["sort", "limit", "explain"]);
        assert_eq!(c.modifiers[1].1, vec![json!(5)]);
    }

    #[test]
    fn multiple_args_with_nested_commas_and_strings() {
        let c = db(r#"db.users.update("a,b", {$set: {note: "x), y", n: [1, 2]}})"#);
        assert_eq!(c.args, vec![json!("a,b"), json!({"$set": {"note": "x), y", "n": [1, 2]}})]);
        assert_eq!(db("db.users.find()").args, Vec::<Value>::new());
        assert_eq!(db("db.my-coll_2.get(42)").args, vec![json!(42)]);
    }

    #[test]
    fn errors_are_helpful() {
        assert!(parse("db.users.fnd({})").unwrap_err().contains("Available"));
        assert!(parse("db.users.get()").unwrap_err().contains("takes 1"));
        assert!(parse("db.users.get(1).limit(2)").unwrap_err().contains("only follow find"));
        assert!(parse("db.users.find({a: })").unwrap_err().contains("not valid JSON"));
        assert!(parse("db.users.find({a: 1}").is_err());
        assert!(parse("selec * from x").unwrap_err().contains("help"));
    }

    #[test]
    fn shell_commands() {
        assert_eq!(parse("show cluster").unwrap(), Command::ShowCluster);
        assert_eq!(parse("consistency one").unwrap(), Command::Consistency(Some("one".into())));
        assert_eq!(parse("format").unwrap(), Command::Format(None));
        assert_eq!(parse("compact major").unwrap(), Command::Compact { major: true });
        assert_eq!(parse("timing off").unwrap(), Command::Timing(Some(false)));
        assert_eq!(parse("QUIT").unwrap(), Command::Exit);
    }

    #[test]
    fn trailing_comments() {
        assert_eq!(strip_comment("db.u.find({a: 1})   // note"), "db.u.find({a: 1})");
        assert_eq!(strip_comment(r#"db.u.find({url: "http://x"}) // c"#), r#"db.u.find({url: "http://x"})"#);
        assert_eq!(strip_comment("db.u.find({s: 'a//b'})"), "db.u.find({s: 'a//b'})");
        assert_eq!(strip_comment("// whole line"), "");
    }

    #[test]
    fn completeness_for_multiline_input() {
        assert!(!is_complete("db.users.find({"));
        assert!(!is_complete("db.users.insert({name: 'it''"));
        assert!(is_complete("db.users.find({\n  age: {$gt: 30}\n})"));
        assert!(is_complete("db.users.find({s: \"(\"})"));
        assert!(is_complete("show cluster"));
    }
}
