use std::{
    collections::HashMap,
    io::{self, Stdout},
    net::SocketAddr,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table},
    Frame, Terminal,
};
use thiserror::Error;

use crate::{
    client::{ClientError, RespClient},
    protocol::Frame as RespFrame,
};

type TuiTerminal = Terminal<CrosstermBackend<Stdout>>;

#[derive(Debug, Clone, Copy)]
pub struct DashboardConfig {
    pub addr: SocketAddr,
    pub refresh_interval: Duration,
    pub key_limit: usize,
}

#[derive(Debug, Error)]
pub enum DashboardError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("invalid stats response: {0}")]
    InvalidStats(String),
    #[error("invalid inspect response: {0}")]
    InvalidInspect(String),
}

#[derive(Debug, Default)]
struct DashboardApp {
    snapshot: Option<DashboardSnapshot>,
    last_refresh: Option<Instant>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct DashboardSnapshot {
    stats: DashboardStats,
    keys: Vec<KeyRow>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct DashboardStats {
    total_keys: usize,
    string_keys: usize,
    list_keys: usize,
    expiring_keys: usize,
    list_items: usize,
    key_bytes: usize,
    payload_bytes: usize,
    estimated_memory_bytes: usize,
    max_memory_bytes: Option<usize>,
    eviction_policy: String,
    expired_keys_cleaned: u64,
    evicted_keys: u64,
    rejected_writes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyRow {
    key: String,
    value_type: String,
    ttl_seconds: i64,
    payload_bytes: usize,
    estimated_memory_bytes: usize,
    list_items: usize,
}

pub async fn run(config: DashboardConfig) -> Result<(), DashboardError> {
    let mut terminal = setup_terminal()?;
    let run_result = run_loop(&mut terminal, config).await;
    let restore_result = restore_terminal(&mut terminal);

    match (run_result, restore_result) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error.into()),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn run_loop(
    terminal: &mut TuiTerminal,
    config: DashboardConfig,
) -> Result<(), DashboardError> {
    let mut app = DashboardApp::default();

    loop {
        if app
            .last_refresh
            .is_none_or(|last_refresh| last_refresh.elapsed() >= config.refresh_interval)
        {
            refresh_snapshot(&mut app, config).await;
        }

        terminal.draw(|frame| render(frame, &app, config))?;

        if event::poll(Duration::from_millis(50))? {
            let Event::Key(key) = event::read()? else {
                continue;
            };

            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break,
                KeyCode::Char('r') => refresh_snapshot(&mut app, config).await,
                _ => {}
            }
        }
    }

    Ok(())
}

async fn refresh_snapshot(app: &mut DashboardApp, config: DashboardConfig) {
    app.last_refresh = Some(Instant::now());

    match fetch_snapshot(config).await {
        Ok(snapshot) => {
            app.snapshot = Some(snapshot);
            app.error = None;
        }
        Err(error) => {
            app.error = Some(error.to_string());
        }
    }
}

async fn fetch_snapshot(config: DashboardConfig) -> Result<DashboardSnapshot, DashboardError> {
    let mut client = RespClient::connect(config.addr).await?;
    let stats = client.command(&[String::from("AERUGO.STATS")]).await?;
    let keys = client
        .command(&[String::from("AERUGO.INSPECT"), config.key_limit.to_string()])
        .await?;

    Ok(DashboardSnapshot {
        stats: parse_stats(stats)?,
        keys: parse_keys(keys)?,
    })
}

fn render(frame: &mut Frame<'_>, app: &DashboardApp, config: DashboardConfig) {
    let area = frame.area();
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(3),
        ])
        .split(area);

    render_header(frame, layout[0], app, config);
    render_body(frame, layout[1], app);
    render_footer(frame, layout[2], app);
}

fn render_header(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp, config: DashboardConfig) {
    let age = app
        .last_refresh
        .map(|last_refresh| format!("{:.1}s ago", last_refresh.elapsed().as_secs_f32()))
        .unwrap_or_else(|| "never".to_string());
    let title = Line::from(vec![
        Span::styled(
            "Aerugo Cache",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            config.addr.to_string(),
            Style::default()
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  refresh: {age}")),
    ]);

    frame.render_widget(
        Paragraph::new(title).block(Block::default().borders(Borders::ALL).title("Dashboard")),
        area,
    );
}

fn render_body(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Length(42), Constraint::Min(30)])
        .split(area);

    render_stats(frame, layout[0], app.snapshot.as_ref());
    render_keys(frame, layout[1], app.snapshot.as_ref());
}

fn render_stats(frame: &mut Frame<'_>, area: Rect, snapshot: Option<&DashboardSnapshot>) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(9),
            Constraint::Min(6),
        ])
        .split(area);

    let Some(snapshot) = snapshot else {
        frame.render_widget(
            Paragraph::new("Waiting for server data")
                .style(Style::default().fg(Color::Yellow))
                .block(Block::default().borders(Borders::ALL).title("Stats")),
            area,
        );
        return;
    };

    render_memory(frame, layout[0], &snapshot.stats);
    render_keyspace(frame, layout[1], &snapshot.stats);
    render_counters(frame, layout[2], &snapshot.stats);
}

fn render_memory(frame: &mut Frame<'_>, area: Rect, stats: &DashboardStats) {
    let (percent, label) = match stats.max_memory_bytes {
        Some(max_memory) if max_memory > 0 => {
            let percent = ((stats.estimated_memory_bytes as f64 / max_memory as f64) * 100.0)
                .clamp(0.0, 100.0);
            (
                percent.round() as u16,
                format!(
                    "{} / {} ({percent:.1}%)",
                    format_bytes(stats.estimated_memory_bytes),
                    format_bytes(max_memory),
                ),
            )
        }
        _ => (
            0,
            format!("{} / unbounded", format_bytes(stats.estimated_memory_bytes)),
        ),
    };

    frame.render_widget(
        Gauge::default()
            .block(Block::default().borders(Borders::ALL).title("Memory"))
            .gauge_style(Style::default().fg(Color::Green))
            .percent(percent)
            .label(label),
        area,
    );
}

fn render_keyspace(frame: &mut Frame<'_>, area: Rect, stats: &DashboardStats) {
    let rows = [
        Row::new(vec![
            Cell::from("total keys"),
            Cell::from(stats.total_keys.to_string()),
        ]),
        Row::new(vec![
            Cell::from("strings"),
            Cell::from(stats.string_keys.to_string()),
        ]),
        Row::new(vec![
            Cell::from("lists"),
            Cell::from(stats.list_keys.to_string()),
        ]),
        Row::new(vec![
            Cell::from("expiring"),
            Cell::from(stats.expiring_keys.to_string()),
        ]),
        Row::new(vec![
            Cell::from("list items"),
            Cell::from(stats.list_items.to_string()),
        ]),
        Row::new(vec![
            Cell::from("eviction"),
            Cell::from(stats.eviction_policy.clone()),
        ]),
    ];

    frame.render_widget(
        Table::new(rows, [Constraint::Length(14), Constraint::Min(12)])
            .block(Block::default().borders(Borders::ALL).title("Keyspace")),
        area,
    );
}

fn render_counters(frame: &mut Frame<'_>, area: Rect, stats: &DashboardStats) {
    let rows = [
        Row::new(vec![
            Cell::from("key bytes"),
            Cell::from(format_bytes(stats.key_bytes)),
        ]),
        Row::new(vec![
            Cell::from("payload bytes"),
            Cell::from(format_bytes(stats.payload_bytes)),
        ]),
        Row::new(vec![
            Cell::from("expired"),
            Cell::from(stats.expired_keys_cleaned.to_string()),
        ]),
        Row::new(vec![
            Cell::from("evicted"),
            Cell::from(stats.evicted_keys.to_string()),
        ]),
        Row::new(vec![
            Cell::from("rejected"),
            Cell::from(stats.rejected_writes.to_string()),
        ]),
    ];

    frame.render_widget(
        Table::new(rows, [Constraint::Length(14), Constraint::Min(12)])
            .block(Block::default().borders(Borders::ALL).title("Counters")),
        area,
    );
}

fn render_keys(frame: &mut Frame<'_>, area: Rect, snapshot: Option<&DashboardSnapshot>) {
    let Some(snapshot) = snapshot else {
        frame.render_widget(
            Paragraph::new("No key data yet")
                .style(Style::default().fg(Color::Yellow))
                .block(Block::default().borders(Borders::ALL).title("Keys")),
            area,
        );
        return;
    };

    let rows = if snapshot.keys.is_empty() {
        vec![Row::new([
            Cell::from("empty"),
            Cell::from("-"),
            Cell::from("-"),
            Cell::from("-"),
            Cell::from("-"),
            Cell::from("-"),
        ])]
    } else {
        snapshot
            .keys
            .iter()
            .map(|key| {
                Row::new([
                    Cell::from(key.key.clone()),
                    Cell::from(key.value_type.clone()),
                    Cell::from(format_ttl(key.ttl_seconds)),
                    Cell::from(format_bytes(key.payload_bytes)),
                    Cell::from(format_bytes(key.estimated_memory_bytes)),
                    Cell::from(key.list_items.to_string()),
                ])
            })
            .collect()
    };

    let header = Row::new(["key", "type", "ttl", "payload", "est mem", "items"]).style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    );

    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Percentage(34),
                Constraint::Length(8),
                Constraint::Length(12),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Length(7),
            ],
        )
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Stored Keys"))
        .column_spacing(1),
        area,
    );
}

fn render_footer(frame: &mut Frame<'_>, area: Rect, app: &DashboardApp) {
    let text = match &app.error {
        Some(error) => Line::from(vec![
            Span::styled("error: ", Style::default().fg(Color::Red)),
            Span::raw(error.clone()),
            Span::raw("  "),
            Span::styled("r", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" retry  "),
            Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" quit"),
        ]),
        None => Line::from(vec![
            Span::styled("r", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" refresh  "),
            Span::styled("q", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(" quit"),
        ]),
    };

    frame.render_widget(
        Paragraph::new(text).block(Block::default().borders(Borders::ALL).title("Controls")),
        area,
    );
}

fn setup_terminal() -> Result<TuiTerminal, DashboardError> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    Ok(Terminal::new(CrosstermBackend::new(stdout))?)
}

fn restore_terminal(terminal: &mut TuiTerminal) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()
}

fn parse_stats(frame: RespFrame) -> Result<DashboardStats, DashboardError> {
    let RespFrame::Bulk(value) = frame else {
        return Err(DashboardError::InvalidStats(
            "expected bulk stats payload".to_string(),
        ));
    };
    let text = String::from_utf8(value)
        .map_err(|_| DashboardError::InvalidStats("stats payload is not UTF-8".to_string()))?;
    let fields = text
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect::<HashMap<_, _>>();

    Ok(DashboardStats {
        total_keys: parse_stat_usize(&fields, "total_keys")?,
        string_keys: parse_stat_usize(&fields, "string_keys")?,
        list_keys: parse_stat_usize(&fields, "list_keys")?,
        expiring_keys: parse_stat_usize(&fields, "expiring_keys")?,
        list_items: parse_stat_usize(&fields, "list_items")?,
        key_bytes: parse_stat_usize(&fields, "key_bytes")?,
        payload_bytes: parse_stat_usize(&fields, "payload_bytes")?,
        estimated_memory_bytes: parse_stat_usize(&fields, "estimated_memory_bytes")?,
        max_memory_bytes: parse_stat_optional_usize(&fields, "max_memory_bytes")?,
        eviction_policy: parse_stat_string(&fields, "eviction_policy")?,
        expired_keys_cleaned: parse_stat_u64(&fields, "expired_keys_cleaned")?,
        evicted_keys: parse_stat_u64(&fields, "evicted_keys")?,
        rejected_writes: parse_stat_u64(&fields, "rejected_writes")?,
    })
}

fn parse_keys(frame: RespFrame) -> Result<Vec<KeyRow>, DashboardError> {
    let RespFrame::Array(rows) = frame else {
        return Err(DashboardError::InvalidInspect(
            "expected array inspect payload".to_string(),
        ));
    };

    rows.into_iter().map(parse_key_row).collect()
}

fn parse_key_row(frame: RespFrame) -> Result<KeyRow, DashboardError> {
    let RespFrame::Array(columns) = frame else {
        return Err(DashboardError::InvalidInspect(
            "expected key row array".to_string(),
        ));
    };

    let columns: [RespFrame; 6] = columns.try_into().map_err(|columns: Vec<RespFrame>| {
        DashboardError::InvalidInspect(format!("expected 6 key columns, got {}", columns.len()))
    })?;
    let [key, value_type, ttl_seconds, payload_bytes, estimated_memory_bytes, list_items] = columns;

    Ok(KeyRow {
        key: frame_to_string(key)?,
        value_type: frame_to_string(value_type)?,
        ttl_seconds: frame_to_i64(ttl_seconds)?,
        payload_bytes: frame_to_usize(payload_bytes)?,
        estimated_memory_bytes: frame_to_usize(estimated_memory_bytes)?,
        list_items: frame_to_usize(list_items)?,
    })
}

fn parse_stat_string(
    fields: &HashMap<String, String>,
    key: &'static str,
) -> Result<String, DashboardError> {
    fields
        .get(key)
        .cloned()
        .ok_or_else(|| DashboardError::InvalidStats(format!("missing field {key}")))
}

fn parse_stat_usize(
    fields: &HashMap<String, String>,
    key: &'static str,
) -> Result<usize, DashboardError> {
    parse_stat_string(fields, key)?
        .parse()
        .map_err(|_| DashboardError::InvalidStats(format!("invalid usize field {key}")))
}

fn parse_stat_optional_usize(
    fields: &HashMap<String, String>,
    key: &'static str,
) -> Result<Option<usize>, DashboardError> {
    let value = parse_stat_string(fields, key)?;

    if value == "none" {
        return Ok(None);
    }

    value
        .parse()
        .map(Some)
        .map_err(|_| DashboardError::InvalidStats(format!("invalid optional usize field {key}")))
}

fn parse_stat_u64(
    fields: &HashMap<String, String>,
    key: &'static str,
) -> Result<u64, DashboardError> {
    parse_stat_string(fields, key)?
        .parse()
        .map_err(|_| DashboardError::InvalidStats(format!("invalid u64 field {key}")))
}

fn frame_to_string(frame: RespFrame) -> Result<String, DashboardError> {
    let RespFrame::Bulk(value) = frame else {
        return Err(DashboardError::InvalidInspect(
            "expected bulk string column".to_string(),
        ));
    };

    String::from_utf8(value)
        .map_err(|_| DashboardError::InvalidInspect("column is not UTF-8".to_string()))
}

fn frame_to_i64(frame: RespFrame) -> Result<i64, DashboardError> {
    let RespFrame::Integer(value) = frame else {
        return Err(DashboardError::InvalidInspect(
            "expected integer column".to_string(),
        ));
    };

    Ok(value)
}

fn frame_to_usize(frame: RespFrame) -> Result<usize, DashboardError> {
    let value = frame_to_i64(frame)?;

    usize::try_from(value)
        .map_err(|_| DashboardError::InvalidInspect("negative size column".to_string()))
}

fn format_ttl(ttl_seconds: i64) -> String {
    match ttl_seconds {
        -1 => "persistent".to_string(),
        -2 => "missing".to_string(),
        seconds => format!("{seconds}s"),
    }
}

fn format_bytes(bytes: usize) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];

    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_stats_payload() {
        let stats = parse_stats(RespFrame::Bulk(
            b"# Keyspace\r\n\
total_keys:2\r\n\
string_keys:1\r\n\
list_keys:1\r\n\
expiring_keys:1\r\n\
list_items:3\r\n\
\r\n\
# Memory\r\n\
key_bytes:10\r\n\
payload_bytes:20\r\n\
estimated_memory_bytes:120\r\n\
max_memory_bytes:none\r\n\
eviction_policy:noeviction\r\n\
\r\n\
# Counters\r\n\
expired_keys_cleaned:0\r\n\
evicted_keys:0\r\n\
rejected_writes:0\r\n"
                .to_vec(),
        ))
        .unwrap();

        assert_eq!(stats.total_keys, 2);
        assert_eq!(stats.max_memory_bytes, None);
        assert_eq!(stats.eviction_policy, "noeviction");
    }

    #[test]
    fn parses_key_rows() {
        let keys = parse_keys(RespFrame::Array(vec![RespFrame::Array(vec![
            RespFrame::Bulk(b"events".to_vec()),
            RespFrame::Bulk(b"list".to_vec()),
            RespFrame::Integer(-1),
            RespFrame::Integer(12),
            RespFrame::Integer(120),
            RespFrame::Integer(3),
        ])]))
        .unwrap();

        assert_eq!(
            keys,
            vec![KeyRow {
                key: "events".to_string(),
                value_type: "list".to_string(),
                ttl_seconds: -1,
                payload_bytes: 12,
                estimated_memory_bytes: 120,
                list_items: 3,
            }]
        );
    }
}
