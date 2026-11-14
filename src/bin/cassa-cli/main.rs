//! cassa-cli: an interactive shell and scripting tool for cassa.
//!
//! ```text
//! cassa-cli                                   interactive shell (history, multi-line input)
//! cassa-cli -e 'db.users.find({age: 31})'     run statements and exit
//! cassa-cli -f setup.cassa                    run a script file
//! echo 'db.users.count()' | cassa-cli         read statements from stdin
//! ```

mod client;
mod output;
mod parse;

use clap::Parser;
use client::{enc, Client};
use output::{Format, Out};
use parse::{Command, DbCall};
use reqwest::Method;
use serde_json::{json, Map, Value};
use std::io::{BufRead, IsTerminal};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "cassa-cli", version, about = "Shell for the cassa distributed document database")]
struct Args {
    /// Node(s) to talk to, comma-separated. Later ones are used if earlier ones are down.
    #[arg(long, short = 'H', env = "CASSA_HOST", value_delimiter = ',', default_value = "127.0.0.1:7001")]
    host: Vec<String>,

    /// Statement to execute (repeatable). Exits afterwards.
    #[arg(long, short = 'e')]
    execute: Vec<String>,

    /// Script file to execute. Exits afterwards.
    #[arg(long, short = 'f')]
    file: Option<std::path::PathBuf>,

    /// Consistency level for reads and writes: one, quorum or all.
    #[arg(long, short = 'c', default_value = "quorum")]
    consistency: String,

    /// Output format: pretty, table or json (JSON Lines; nothing else is printed).
    #[arg(long, short = 'o')]
    format: Option<String>,

    /// Keep going after a failed statement in scripts.
    #[arg(long)]
    continue_on_error: bool,
}

/// Documents shown by an interactive find() without an explicit .limit().
const INTERACTIVE_LIMIT: usize = 20;

struct Shell {
    client: Client,
    out: Out,
    interactive: bool,
}

fn main() {
    let args = Args::parse();
    let interactive = args.execute.is_empty() && args.file.is_none() && std::io::stdin().is_terminal();
    let format = match args.format.as_deref().map(Format::parse) {
        Some(Some(f)) => f,
        Some(None) => die("--format must be pretty, table or json"),
        None => Format::Pretty,
    };
    if !["one", "quorum", "all"].contains(&args.consistency.as_str()) {
        die("--consistency must be one, quorum or all");
    }
    let mut shell = Shell {
        client: Client::new(args.host.clone(), args.consistency.clone()),
        out: Out { format, timing: interactive },
        interactive,
    };

    let ok = if !args.execute.is_empty() {
        shell.run_script(args.execute.iter().map(|s| s.to_string()), args.continue_on_error)
    } else if let Some(path) = &args.file {
        let text = std::fs::read_to_string(path).unwrap_or_else(|e| die(&format!("{}: {e}", path.display())));
        shell.run_script(text.lines().map(str::to_string), args.continue_on_error)
    } else if !interactive {
        let lines = std::io::stdin().lock().lines().map_while(Result::ok);
        shell.run_script(lines, args.continue_on_error)
    } else {
        shell.repl();
        true
    };
    std::process::exit(if ok { 0 } else { 1 });
}

fn die(msg: &str) -> ! {
    eprintln!("error: {msg}");
    std::process::exit(2);
}

impl Shell {
    // ---------------------------------------------------------------- input loops

    fn repl(&mut self) {
        let mut rl = rustyline::DefaultEditor::new().unwrap_or_else(|e| die(&e.to_string()));
        let history = std::env::var_os("HOME").map(|h| std::path::PathBuf::from(h).join(".cassa_history"));
        if let Some(h) = &history {
            let _ = rl.load_history(h);
        }
        println!("cassa shell — connected to {} (consistency {}). Type 'help' for examples, 'exit' to quit.", self.client.host(), self.client.consistency);
        let mut buf = String::new();
        loop {
            let prompt = if buf.is_empty() { format!("cassa {}> ", self.client.host()) } else { "   ...> ".to_string() };
            match rl.readline(&prompt) {
                Ok(raw) => {
                    if buf.is_empty() && (raw.trim().is_empty() || is_comment(&raw)) {
                        continue;
                    }
                    let line = parse::strip_comment(&raw);
                    buf.push_str(line);
                    buf.push('\n');
                    if !parse::is_complete(&buf) {
                        continue;
                    }
                    let stmt = std::mem::take(&mut buf);
                    let _ = rl.add_history_entry(stmt.trim());
                    if !self.run(stmt.trim()) {
                        break;
                    }
                }
                Err(rustyline::error::ReadlineError::Interrupted) => {
                    if !buf.is_empty() {
                        buf.clear();
                        println!("(statement cancelled)");
                    }
                }
                Err(rustyline::error::ReadlineError::Eof) => break,
                Err(e) => {
                    eprintln!("error: {e}");
                    break;
                }
            }
        }
        if let Some(h) = &history {
            let _ = rl.save_history(h);
        }
    }

    /// Run statements from lines; a statement may span lines until brackets balance.
    fn run_script(&mut self, lines: impl Iterator<Item = String>, keep_going: bool) -> bool {
        let (mut buf, mut ok, mut start_line) = (String::new(), true, 0);
        for (i, raw) in lines.enumerate() {
            let line = parse::strip_comment(&raw);
            if buf.is_empty() {
                if line.trim().is_empty() || is_comment(line) {
                    continue;
                }
                start_line = i + 1;
            }
            buf.push_str(line);
            buf.push('\n');
            if !parse::is_complete(&buf) {
                continue;
            }
            let stmt = std::mem::take(&mut buf);
            match self.exec(stmt.trim()) {
                Ok(true) => {}
                Ok(false) => return ok,
                Err(e) => {
                    eprintln!("error (statement at line {start_line}): {e}");
                    ok = false;
                    if !keep_going {
                        return false;
                    }
                }
            }
        }
        if !buf.trim().is_empty() {
            eprintln!("error (statement at line {start_line}): incomplete statement (unbalanced brackets or quotes)");
            return false;
        }
        ok
    }

    /// Run one statement interactively; returns false to exit.
    fn run(&mut self, stmt: &str) -> bool {
        match self.exec(stmt) {
            Ok(cont) => cont,
            Err(e) => {
                eprintln!("error: {e}");
                true
            }
        }
    }

    // ---------------------------------------------------------------- statements

    fn exec(&mut self, stmt: &str) -> Result<bool, String> {
        match parse::parse(stmt)? {
            Command::Exit => return Ok(false),
            Command::Help(topic) => print_help(topic.as_deref()),
            Command::Connect(hosts) => {
                self.client.set_hosts(hosts.split(',').map(|h| h.trim().to_string()).collect());
                let info = self.client.ok(Method::GET, "/", None, false)?;
                self.out.info(format!("connected to {} ({})", self.client.host(), info["node_id"].as_str().unwrap_or("?")));
            }
            Command::Consistency(None) => self.out.info(format!("consistency: {}", self.client.consistency)),
            Command::Consistency(Some(cl)) => {
                let cl = cl.to_ascii_lowercase();
                if !["one", "quorum", "all"].contains(&cl.as_str()) {
                    return Err("consistency must be one, quorum or all".into());
                }
                self.client.consistency = cl;
                self.out.info(format!("consistency: {}", self.client.consistency));
            }
            Command::Format(None) => self.out.info(format!("format: {}", self.out.format.name())),
            Command::Format(Some(f)) => {
                self.out.format = Format::parse(&f).ok_or("format must be pretty, table or json")?;
                self.out.info(format!("format: {}", self.out.format.name()));
            }
            Command::Timing(None) => self.out.info(format!("timing: {}", if self.out.timing { "on" } else { "off" })),
            Command::Timing(Some(on)) => self.out.timing = on,
            Command::ShowCluster => self.show_cluster()?,
            Command::ShowCollections => self.show_collections()?,
            Command::Repair => {
                let t = Instant::now();
                let r = self.client.ok(Method::POST, "/admin/repair", None, false)?;
                let streamed: u64 = r["streamed"].as_object().map(|m| m.values().filter_map(Value::as_u64).sum()).unwrap_or(0);
                if self.out.format == Format::Json {
                    self.out.value(&r);
                }
                self.out.summary(
                    format!(
                        "repair on {}: {} ranges, {} in sync, {} repaired, {} skipped, {} entries streamed{}",
                        r["node_id"].as_str().unwrap_or("?"),
                        r["ranges"],
                        r["ranges_in_sync"],
                        r["ranges_repaired"],
                        r["ranges_skipped"],
                        streamed,
                        errors_suffix(&r)
                    ),
                    t.elapsed().as_millis(),
                );
            }
            Command::Compact { major } => {
                let t = Instant::now();
                let path = if major { "/admin/compact?major=true" } else { "/admin/compact" };
                let r = self.client.ok(Method::POST, path, None, false)?;
                if self.out.format == Format::Json {
                    self.out.value(&r);
                }
                self.out.summary(
                    format!("compacted {} sstables; {} now on disk", r["sstables_compacted"], r["storage"]["sstable_count"]),
                    t.elapsed().as_millis(),
                );
            }
            Command::Cleanup => {
                let t = Instant::now();
                let r = self.client.ok(Method::POST, "/admin/cleanup", None, false)?;
                if self.out.format == Format::Json {
                    self.out.value(&r);
                }
                self.out.summary(format!("cleanup dropped {} entries", r["dropped_entries"]), t.elapsed().as_millis());
            }
            Command::Db(call) => self.db(call)?,
        }
        Ok(true)
    }

    fn show_cluster(&mut self) -> Result<(), String> {
        let c = self.client.ok(Method::GET, "/cluster", None, false)?;
        let rows: Vec<Value> = c["members"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|m| {
                json!({
                    "node": m["node_id"], "address": m["addr"], "status": m["status"], "state": m["state"],
                    "ownership_%": m["primary_ownership_pct"], "last_seen_s": m["last_seen_secs_ago"].as_f64().map(|s| (s * 10.0).round() / 10.0),
                })
            })
            .collect();
        self.out.rows(&rows);
        let hints: u64 = c["pending_hints"].as_object().map(|m| m.values().filter_map(Value::as_u64).sum()).unwrap_or(0);
        self.out.info(format!(
            "coordinator {} · RF {} · pending hints {} · range movements in progress: {}",
            c["coordinator"].as_str().unwrap_or("?"),
            c["replication_factor"],
            hints,
            c["pending_range_movements"]
        ));
        Ok(())
    }

    fn show_collections(&mut self) -> Result<(), String> {
        let c = self.client.ok(Method::GET, "/collections", None, false)?;
        let rows: Vec<Value> = c["local_document_counts"]
            .as_object()
            .into_iter()
            .flatten()
            .map(|(name, n)| json!({"collection": name, "documents_on_this_node": n}))
            .collect();
        self.out.rows(&rows);
        self.out.info(format!(
            "counts are local to {} (including replicas of other nodes' data); use db.<c>.count() for cluster-wide totals",
            c["node_id"].as_str().unwrap_or("?")
        ));
        Ok(())
    }

    // ---------------------------------------------------------------- db.<c>.<method>()

    fn db(&mut self, call: DbCall) -> Result<(), String> {
        let saved_cl = self.client.consistency.clone();
        for (m, a) in &call.modifiers {
            if m == "consistency" {
                let cl = a[0].as_str().ok_or(".consistency() takes \"one\", \"quorum\" or \"all\"")?;
                if !["one", "quorum", "all"].contains(&cl) {
                    return Err(".consistency() takes \"one\", \"quorum\" or \"all\"".into());
                }
                self.client.consistency = cl.to_string();
            }
        }
        let res = self.db_inner(&call);
        self.client.consistency = saved_cl;
        res
    }

    fn db_inner(&mut self, call: &DbCall) -> Result<(), String> {
        let c = enc(&call.collection);
        let t = Instant::now();
        let ms = || t.elapsed().as_millis();
        let arg = |i: usize| call.args.get(i).cloned().unwrap_or(Value::Null);
        match call.method.as_str() {
            "insert" => {
                let doc = expect_object(arg(0), "insert() takes a document")?;
                let r = self.client.ok(Method::POST, &format!("/collections/{c}/docs"), Some(&doc), true)?;
                if self.out.format == Format::Json {
                    self.out.value(&r);
                }
                self.out.summary(format!("inserted 1 document, _id: {}", r["_id"].as_str().unwrap_or("?")), ms());
            }
            "insertMany" => {
                let docs = arg(0);
                let docs = docs.as_array().ok_or("insertMany() takes an array of documents")?;
                let mut ids = Vec::new();
                for d in docs {
                    let d = expect_object(d.clone(), "insertMany() takes an array of documents")?;
                    let r = self.client.ok(Method::POST, &format!("/collections/{c}/docs"), Some(&d), true)?;
                    ids.push(r["_id"].clone());
                }
                if self.out.format == Format::Json {
                    self.out.value(&json!({ "inserted": ids }));
                }
                self.out.summary(format!("inserted {}", n_docs(ids.len())), ms());
            }
            "get" => {
                let id = id_arg(&arg(0))?;
                let r = self.client.call(Method::GET, &format!("/collections/{c}/docs/{}", enc(&id)), None, true)?;
                match r.status.as_u16() {
                    200 => self.out.docs(&[r.body]),
                    404 => self.out.info("not found"),
                    _ => return Err(server_error(&r)),
                }
            }
            "find" | "findOne" | "count" => self.find(call, &c, t)?,
            "update" => {
                let id = id_arg(&arg(0))?;
                let r = self.client.call(Method::PATCH, &format!("/collections/{c}/docs/{}", enc(&id)), Some(&arg(1)), true)?;
                match r.status.as_u16() {
                    200 => {
                        self.out.docs(&[r.body]);
                        self.out.summary("updated 1 document", ms());
                    }
                    404 => self.out.info("not found; nothing updated"),
                    _ => return Err(server_error(&r)),
                }
            }
            "updateMany" => {
                let ids = self.matching_ids(&c, arg(0))?;
                for id in &ids {
                    self.client.ok(Method::PATCH, &format!("/collections/{c}/docs/{}", enc(id)), Some(&arg(1)), true)?;
                }
                if self.out.format == Format::Json {
                    self.out.value(&json!({ "updated": ids.len() }));
                }
                self.out.summary(format!("updated {}", n_docs(ids.len())), ms());
            }
            "replace" => {
                let id = id_arg(&arg(0))?;
                let doc = expect_object(arg(1), "replace() takes an id and a document")?;
                let r = self.client.ok(Method::PUT, &format!("/collections/{c}/docs/{}", enc(&id)), Some(&doc), true)?;
                self.out.docs(&[r]);
                self.out.summary("replaced 1 document", ms());
            }
            "delete" => {
                let id = id_arg(&arg(0))?;
                self.client.ok(Method::DELETE, &format!("/collections/{c}/docs/{}", enc(&id)), None, true)?;
                if self.out.format == Format::Json {
                    self.out.value(&json!({ "deleted": id }));
                }
                self.out.summary(format!("deleted {id}"), ms());
            }
            "deleteMany" => {
                let ids = self.matching_ids(&c, arg(0))?;
                for id in &ids {
                    self.client.ok(Method::DELETE, &format!("/collections/{c}/docs/{}", enc(id)), None, true)?;
                }
                if self.out.format == Format::Json {
                    self.out.value(&json!({ "deleted": ids.len() }));
                }
                self.out.summary(format!("deleted {}", n_docs(ids.len())), ms());
            }
            "createIndex" => {
                let field = arg(0).as_str().ok_or("createIndex() takes a field name, e.g. createIndex(\"age\")")?.to_string();
                let r = self.client.call(Method::POST, &format!("/collections/{c}/indexes"), Some(&json!({ "field": field })), false)?;
                match r.status.as_u16() {
                    201 => self.out.summary(format!("index on {}.{field} created; nodes build it in the background (see db.{}.indexes())", call.collection, call.collection), ms()),
                    200 => self.out.info(format!("index on {}.{field} already exists", call.collection)),
                    _ => return Err(server_error(&r)),
                }
            }
            "dropIndex" => {
                let field = arg(0).as_str().ok_or("dropIndex() takes a field name")?.to_string();
                let r = self.client.call(Method::DELETE, &format!("/collections/{c}/indexes/{}", enc(&field)), None, false)?;
                match r.status.as_u16() {
                    204 => self.out.summary(format!("index on {}.{field} dropped", call.collection), ms()),
                    404 => self.out.info(format!("no index on {}.{field}", call.collection)),
                    _ => return Err(server_error(&r)),
                }
            }
            "indexes" => {
                let r = self.client.ok(Method::GET, &format!("/collections/{c}/indexes"), None, false)?;
                let rows: Vec<Value> = r["indexes"].as_array().cloned().unwrap_or_default();
                if rows.is_empty() {
                    self.out.info(format!("no indexes on {}", call.collection));
                } else {
                    self.out.rows(&rows);
                    self.out.info(format!("state is as seen by {}", r["node_id"].as_str().unwrap_or("?")));
                }
            }
            "owners" => {
                let id = id_arg(&arg(0))?;
                let r = self.client.ok(Method::GET, &format!("/cluster/owners/{c}/{}", enc(&id)), None, false)?;
                self.out.rows(r["replicas"].as_array().map(Vec::as_slice).unwrap_or(&[]));
            }
            other => return Err(format!("unsupported method {other}")),
        }
        Ok(())
    }

    fn find(&mut self, call: &DbCall, c: &str, t: Instant) -> Result<(), String> {
        let mut body = Map::new();
        body.insert("filter".into(), call.args.first().cloned().unwrap_or(json!({})));
        let mut explicit_limit = false;
        let mut explain = false;
        let mut count_only = call.method == "count";
        for (m, a) in &call.modifiers {
            match m.as_str() {
                "sort" => {
                    body.insert("sort".into(), a[0].clone());
                }
                "limit" | "skip" => {
                    let n = a[0].as_u64().ok_or_else(|| format!(".{m}() takes a non-negative integer"))?;
                    body.insert(m.clone(), json!(n));
                    explicit_limit |= m == "limit";
                }
                "explain" => explain = true,
                "count" => count_only = true,
                _ => {}
            }
        }
        if call.method == "findOne" {
            body.insert("limit".into(), json!(1));
            explicit_limit = true;
        }
        if count_only {
            body.insert("limit".into(), json!(0));
        } else if self.interactive && !explicit_limit && self.out.format != Format::Json {
            body.insert("limit".into(), json!(INTERACTIVE_LIMIT));
        }
        if explain {
            body.insert("explain".into(), json!(true));
        }

        let r = self.client.ok(Method::POST, &format!("/collections/{c}/find"), Some(&Value::Object(body.clone())), false)?;
        let docs = r["docs"].as_array().cloned().unwrap_or_default();
        let matched = r["matched"].as_u64().unwrap_or(0);
        let partial = if r["partial"].as_bool() == Some(true) { " — PARTIAL: some nodes were unreachable" } else { "" };
        let ms = t.elapsed().as_millis();

        if count_only {
            match self.out.format {
                Format::Json => println!("{matched}"),
                _ => self.out.summary(format!("{matched}{partial}"), ms),
            }
        } else if call.method == "findOne" {
            match docs.first() {
                Some(d) => self.out.docs(std::slice::from_ref(d)),
                None => self.out.info("no match"),
            }
        } else {
            self.out.docs(&docs);
            let skip = body.get("skip").and_then(Value::as_u64).unwrap_or(0);
            let more = matched.saturating_sub(skip).saturating_sub(docs.len() as u64);
            let hint = if more > 0 && !explicit_limit {
                format!(", {more} more (add .limit(n) to see more)")
            } else {
                String::new()
            };
            self.out.summary(format!("{} of {matched} matching {}{hint}{partial}", docs.len(), if matched == 1 { "document" } else { "documents" }), ms);
        }
        if explain {
            let rows: Vec<Value> = r["explain"]
                .as_object()
                .into_iter()
                .flatten()
                .map(|(node, p)| json!({"node": node, "plan": p["index"].as_str().map(|i| format!("index {i}")).unwrap_or("full scan".into()), "examined": p["examined"], "matched": p["matched"]}))
                .collect();
            if self.out.format == Format::Json {
                println!("{}", json!({ "explain": rows }));
            } else {
                println!("plan:");
                print!("{}", output::table(&rows));
            }
        }
        Ok(())
    }

    fn matching_ids(&mut self, c: &str, filter: Value) -> Result<Vec<String>, String> {
        let body = json!({ "filter": filter });
        let r = self.client.ok(Method::POST, &format!("/collections/{c}/find"), Some(&body), false)?;
        if r["partial"].as_bool() == Some(true) {
            return Err("some nodes are unreachable, so the set of matching documents may be incomplete; refusing to continue".into());
        }
        Ok(r["docs"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| d["_id"].as_str().map(str::to_string))
            .collect())
    }
}

fn n_docs(n: usize) -> String {
    format!("{n} document{}", if n == 1 { "" } else { "s" })
}

fn is_comment(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("//") || t.starts_with('#') || t.starts_with("--")
}

fn expect_object(v: Value, msg: &str) -> Result<Value, String> {
    if v.is_object() { Ok(v) } else { Err(msg.into()) }
}

fn id_arg(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err("expected a document id (string or number)".into()),
    }
}

fn server_error(r: &client::Response) -> String {
    let msg = r.body.get("error").and_then(Value::as_str).map(str::to_string).unwrap_or_else(|| r.body.to_string());
    format!("{} {msg}", r.status.as_u16())
}

fn errors_suffix(r: &Value) -> String {
    match r["errors"].as_array() {
        Some(e) if !e.is_empty() => format!("; {} problem(s): {}", e.len(), e.iter().filter_map(Value::as_str).collect::<Vec<_>>().join("; ")),
        _ => String::new(),
    }
}

fn print_help(topic: Option<&str>) {
    match topic {
        Some("query") | Some("queries") | Some("find") => println!("{HELP_QUERY}"),
        Some("update") | Some("updates") => println!("{HELP_UPDATE}"),
        Some("admin") | Some("cluster") => println!("{HELP_ADMIN}"),
        _ => println!("{HELP}"),
    }
}

const HELP: &str = r#"Documents                     (arguments are JSON5: unquoted keys and 'single quotes' are fine)
  db.users.insert({name: "ann", age: 31})
  db.users.insertMany([{name: "bob"}, {name: "cat"}])
  db.users.get("ann-id")
  db.users.find({age: {$gt: 30}}).sort({age: -1}).skip(10).limit(5)
  db.users.findOne({name: "ann"})
  db.users.count({age: {$gte: 18}})
  db.users.update("ann-id", {$set: {"addr.city": "Leeds"}, $inc: {visits: 1}})
  db.users.updateMany({city: "York"}, {$set: {region: "north"}})
  db.users.replace("ann-id", {name: "ann", age: 32})
  db.users.delete("ann-id")
  db.users.deleteMany({inactive: true})
  db.users.owners("ann-id")                 which nodes hold this document

Indexes
  db.users.createIndex("age")               built in the background on every node
  db.users.indexes()                        list indexes and their build state
  db.users.find({age: 31}).explain()        show each node's plan
  db.users.dropIndex("age")

Shell
  show cluster | status                     nodes, UP/DOWN, state, ownership
  show collections                          document counts on the connected node
  consistency [one|quorum|all]              default for reads and writes
  db.users.find({}).consistency("one")      per statement
  format [pretty|table|json]                output style
  timing [on|off]                           show elapsed time
  connect host:port[,host:port]             switch nodes (later hosts are fallbacks)
  repair | compact [major] | cleanup        maintenance on the connected node
  help query | help update | help admin     more examples
  exit

Statements can span lines; the shell waits until brackets balance. Ctrl-C cancels."#;

const HELP_QUERY: &str = r#"Filters (MongoDB syntax)
  {name: "ann"}                             equality (also matches inside arrays)
  {"addr.city": "Leeds"}                    dotted paths need quotes
  {"tags.0": "admin"}                       array element by position
  {age: {$gt: 30, $lte: 40}}                $gt $gte $lt $lte
  {age: {$ne: 31}}                          $eq $ne
  {city: {$in: ["Leeds", "York"]}}          $in $nin
  {phone: {$exists: false}}                 missing fields
  {tags: {$size: 2}}                        array length
  {age: {$not: {$lt: 18}}}                  negation
  {$or: [{age: {$lt: 18}}, {vip: true}]}    $and $or $nor
  {deleted: null}                           null or missing

Modifiers (after find)
  .sort({age: -1, name: 1})  .skip(20)  .limit(10)  .count()  .explain()

Interactive find() shows 20 documents unless you add .limit(n)."#;

const HELP_UPDATE: &str = r#"Update operators (for update() and updateMany())
  {$set: {name: "ann", "addr.zip": "LS1"}}  set fields (creates nested objects)
  {$unset: {nickname: ""}}                  remove fields
  {$inc: {visits: 1, balance: -2.5}}        add to numbers (missing counts as 0)
  {$push: {tags: "vip"}}                    append to an array (creates it)

updateMany()/deleteMany() find the matching ids, then update/delete each one;
they are not atomic, and they refuse to run if some nodes are unreachable."#;

const HELP_ADMIN: &str = r#"Cluster and maintenance (run against the connected node)
  show cluster                              membership as seen by this node
  db.users.owners("id")                     replicas for a document
  repair                                    anti-entropy repair of this node's primary ranges
  compact                                   flush memtable + size-tiered compaction
  compact major                             merge all SSTables into one
  cleanup                                   drop data this node no longer owns (after joins)

Adding/removing nodes is done with the server and HTTP API, e.g.
  curl -XPOST host:port/admin/decommission
  curl -XPOST host:port/admin/removenode/<node-id>"#;
