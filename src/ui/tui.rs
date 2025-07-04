use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Clear, List, ListItem, ListState, Paragraph, Row, 
        Table, TableState, Tabs, Wrap,
    },
    Frame, Terminal,
};
use std::{
    io,
    time::{Duration, Instant},
};

use crate::{Database, TableRows};

#[derive(Debug, Clone, PartialEq)]
pub enum AppMode {
    Tables,
    Query,
    Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InputMode {
    Normal,
    Editing,
}

#[derive(Debug, Clone)]
pub enum QueryResult {
    Text(String),
    Table {
        headers: Vec<String>,
        rows: Vec<Vec<String>>,
    },
}

#[derive(Debug, Clone)]
pub struct QueryHistory {
    pub query: String,
    pub result: QueryResult,
    pub timestamp: Instant,
}

pub struct App {
    pub database_path: String,
    pub database: Database,
    pub mode: AppMode,
    pub input_mode: InputMode,
    pub tables: Vec<String>,
    pub selected_table: Option<String>,
    pub table_data: Option<TableRows>,
    pub table_state: TableState,
    pub table_list_state: ListState,
    pub query_input: String,
    pub query_history: Vec<QueryHistory>,
    pub status_message: String,
    pub status_style: Style,
    pub vertical_scroll: usize,
    pub horizontal_scroll: usize,
    pub show_help: bool,
    pub query_cursor_position: usize,
    pub schema_content: String,
    pub query_scroll: usize,
    pub schema_scroll: usize,
}

impl App {
    pub fn new(database_path: String) -> Result<Self> {
        let mut database = Database::new(&database_path)?;
        let tables = database.get_table_names()?;
        
        let mut app = App {
            database_path,
            database,
            mode: AppMode::Tables,
            input_mode: InputMode::Normal,
            tables,
            selected_table: None,
            table_data: None,
            table_state: TableState::default(),
            table_list_state: ListState::default(),
            query_input: String::new(),
            query_history: Vec::new(),
            status_message: "Welcome to SQLite TUI! Use Tab to switch views, ? for help".to_string(),
            status_style: Style::default().fg(Color::Green),
            vertical_scroll: 0,
            horizontal_scroll: 0,
            show_help: false,
            query_cursor_position: 0,
            schema_content: String::new(),
            query_scroll: 0,
            schema_scroll: 0,
        };
        
        // Select first table if available
        if !app.tables.is_empty() {
            app.table_list_state.select(Some(0));
            app.selected_table = app.tables.first().cloned();
            app.load_table_data()?;
        }
        
        // Load schema content
        let _ = app.load_schema_content();
        
        Ok(app)
    }
    
    pub fn next_mode(&mut self) {
        self.mode = match self.mode {
            AppMode::Tables => AppMode::Query,
            AppMode::Query => AppMode::Schema,
            AppMode::Schema => AppMode::Tables,
        };
        self.set_status("Switched view", Style::default().fg(Color::Yellow));
    }
    
    pub fn previous_mode(&mut self) {
        self.mode = match self.mode {
            AppMode::Tables => AppMode::Schema,
            AppMode::Query => AppMode::Tables,
            AppMode::Schema => AppMode::Query,
        };
        self.set_status("Switched view", Style::default().fg(Color::Yellow));
    }
    
    pub fn next_table(&mut self) -> Result<()> {
        if self.tables.is_empty() {
            return Ok(());
        }
        
        let selected = self.table_list_state.selected().unwrap_or(0);
        let next = if selected >= self.tables.len() - 1 {
            0
        } else {
            selected + 1
        };
        
        self.table_list_state.select(Some(next));
        self.selected_table = self.tables.get(next).cloned();
        self.load_table_data()?;
        Ok(())
    }
    
    pub fn previous_table(&mut self) -> Result<()> {
        if self.tables.is_empty() {
            return Ok(());
        }
        
        let selected = self.table_list_state.selected().unwrap_or(0);
        let previous = if selected == 0 {
            self.tables.len() - 1
        } else {
            selected - 1
        };
        
        self.table_list_state.select(Some(previous));
        self.selected_table = self.tables.get(previous).cloned();
        self.load_table_data()?;
        Ok(())
    }
    
    pub fn select_table(&mut self) -> Result<()> {
        if let Some(selected_idx) = self.table_list_state.selected() {
            if let Some(table_name) = self.tables.get(selected_idx).cloned() {
                self.selected_table = Some(table_name.clone());
                self.load_table_data()?;
                self.set_status(&format!("Loaded table: {}", table_name), Style::default().fg(Color::Green));
            }
        }
        Ok(())
    }
    
    pub fn load_table_data(&mut self) -> Result<()> {
        if let Some(table_name) = &self.selected_table {
            match self.database.get_table_rows(table_name) {
                Ok(data) => {
                    self.table_data = Some(data);
                    self.table_state = TableState::default();
                    self.vertical_scroll = 0;
                    self.horizontal_scroll = 0;
                }
                Err(e) => {
                    self.set_status(&format!("Error loading table {}: {}", table_name, e), Style::default().fg(Color::Red));
                }
            }
        }
        Ok(())
    }
    
    pub fn execute_query(&mut self) -> Result<()> {
        if self.query_input.trim().is_empty() {
            self.set_status("Please enter a query", Style::default().fg(Color::Yellow));
            return Ok(());
        }
        
        let query = self.query_input.trim().to_string();
        let start_time = Instant::now();
        
        // Execute the query using the existing command system
        let result = match self.execute_sql_query(&query) {
            Ok(result) => {
                self.set_status(&format!("Query executed successfully in {:?}", start_time.elapsed()), Style::default().fg(Color::Green));
                result
            }
            Err(e) => {
                self.set_status(&format!("Query error: {}", e), Style::default().fg(Color::Red));
                QueryResult::Text(format!("Error: {}", e))
            }
        };
        
        // Add to history
        self.query_history.push(QueryHistory {
            query: query.clone(),
            result,
            timestamp: start_time,
        });
        
        // Keep only last 50 queries
        if self.query_history.len() > 50 {
            self.query_history.remove(0);
        }
        
        // Clear input
        self.query_input.clear();
        self.query_cursor_position = 0;
        
        Ok(())
    }
    
    fn execute_sql_query(&mut self, query: &str) -> Result<QueryResult> {
        // Handle different types of queries and capture their actual results
        let query_trimmed = query.trim();
        
        if query_trimmed.to_lowercase().starts_with("select count(*) from ") {
            // Handle COUNT queries specifically
            let parts: Vec<&str> = query_trimmed.split_whitespace().collect();
            if parts.len() >= 4 && parts[2].to_lowercase() == "from" {
                let table_name = parts[3];
                match self.database.count_table_rows(table_name) {
                    Ok(count) => Ok(QueryResult::Text(count.to_string())),
                    Err(e) => Err(e),
                }
            } else {
                Err(anyhow::anyhow!("Invalid COUNT query format"))
            }
        } else if query_trimmed == ".tables" {
            // Handle .tables command
            match self.database.get_table_names() {
                Ok(tables) => Ok(QueryResult::Text(tables.join("\n"))),
                Err(e) => Err(e),
            }
        } else if query_trimmed == ".dbinfo" {
            // Handle .dbinfo command
            let page_size = self.database.get_page_size();
            match self.database.get_num_tables() {
                Ok(num_tables) => Ok(QueryResult::Text(format!("database page size: {}\nnumber of tables: {}", page_size, num_tables))),
                Err(e) => Err(e),
            }
        } else if query_trimmed.starts_with(".schema") {
            // Handle .schema command
            match self.database.get_all_schema_objects() {
                Ok(objects) => {
                    let schema_info: Vec<String> = objects.iter().map(|obj| {
                        if let Some(sql) = &obj.sql {
                            sql.clone()
                        } else {
                            format!("{}: {} (table: {}, page: {})", obj.object_type, obj.name, obj.tbl_name, obj.rootpage)
                        }
                    }).collect();
                    Ok(QueryResult::Text(schema_info.join("\n")))
                },
                Err(e) => Err(e),
            }
        } else if query_trimmed.to_lowercase().starts_with("select") {
            // Handle other SELECT queries properly
            match self.execute_select_query_in_tui(query_trimmed) {
                Ok(result) => Ok(result),
                Err(e) => Err(e),
            }
        } else {
            // For other commands, use the original execute_command
            match crate::query::execute_command(&self.database_path, query) {
                Ok(_) => Ok(QueryResult::Text("Command executed successfully".to_string())),
                Err(e) => Err(e),
            }
        }
    }
    
    /// Execute SELECT queries in TUI and format results for display
    fn execute_select_query_in_tui(&mut self, query: &str) -> Result<QueryResult> {
        let query_lower = query.to_lowercase();
        
        // Check if this query has a WHERE clause - if so, use the CLI command system which handles it properly
        if query_lower.contains(" where ") {
            // Use the CLI command system for WHERE clause queries since it handles filtering correctly
            return self.execute_query_via_cli(query);
        }
        
        // Parse SELECT query to determine what to do (simple queries without WHERE)
        if query_lower.starts_with("select * from ") {
            // Handle SELECT * FROM table queries (without WHERE)
            let parts: Vec<&str> = query.split_whitespace().collect();
            if parts.len() >= 4 {
                let table_name = parts[3];
                match self.database.get_table_rows(table_name) {
                    Ok(table_data) => {
                        Ok(QueryResult::Table {
                            headers: table_data.columns.iter().map(|col| col.name.clone()).collect(),
                            rows: table_data.rows.iter().map(|row| {
                                row.values.iter().map(|val| val.to_display_string()).collect()
                            }).collect(),
                        })
                    }
                    Err(e) => Err(e),
                }
            } else {
                Err(anyhow::anyhow!("Invalid SELECT * FROM query format"))
            }
        } else if query_lower.contains(" from ") {
            // Handle SELECT specific_columns FROM table queries (without WHERE)
            let from_pos = query_lower.find(" from ").unwrap();
            let columns_part = query[..from_pos].trim();
            let from_part = query[from_pos + 6..].trim();
            
            // Extract column names
            let select_pos = if columns_part.to_lowercase().starts_with("select ") {
                7 // Length of "select "
            } else {
                return Err(anyhow::anyhow!("Query must start with SELECT"));
            };
            
            let columns_str = columns_part[select_pos..].trim();
            let column_names: Vec<&str> = columns_str
                .split(',')
                .map(|s| s.trim())
                .collect();
            
            // Extract table name (simple case, no WHERE)
            let table_name = from_part.split_whitespace().next().unwrap_or("");
            
            // Get the data
            match self.database.get_column_values(table_name, &column_names) {
                Ok(column_data) => {
                    Ok(QueryResult::Table {
                        headers: column_names.iter().map(|s| s.to_string()).collect(),
                        rows: column_data.iter().map(|row| {
                            row.iter().map(|val| val.to_display_string()).collect()
                        }).collect(),
                    })
                }
                Err(e) => Err(e),
            }
        } else {
            // For complex queries we don't handle yet, fall back to CLI
            self.execute_query_via_cli(query)
        }
    }
    
    fn execute_query_via_cli(&mut self, query: &str) -> Result<QueryResult> {
        // Execute the query through our command system that handles WHERE clauses
        self.execute_query_and_capture_result(query)
    }
    
    fn execute_query_and_capture_result(&mut self, query: &str) -> Result<QueryResult> {
        // Use the command module's handle_select_query logic directly
        let query_trimmed = query.trim();
        if query_trimmed.to_lowercase().starts_with("select") {
            // Parse and execute using our database methods with WHERE clause support
            self.execute_select_with_where_support(query_trimmed)
        } else {
            Err(anyhow::anyhow!("Unsupported query type in TUI"))
        }
    }
    
    fn execute_select_with_where_support(&mut self, query: &str) -> Result<QueryResult> {
        // Skip the "SELECT" keyword (case insensitive)
        let query_after_select = if query.to_lowercase().starts_with("select ") {
            &query[7..] // Skip "SELECT "
        } else {
            return Err(anyhow::anyhow!("Query must start with SELECT"));
        };
        
        let query_lower = query_after_select.to_lowercase();
        
        if query_lower.starts_with("*") {
            // Handle SELECT * FROM table [WHERE condition]
            let mut parts = query_after_select.split_whitespace();
            parts.next(); // skip "*"
            if let Some(from_keyword) = parts.next() {
                if from_keyword.to_lowercase() == "from" {
                    let remaining_parts: Vec<&str> = parts.collect();
                    let (table_name, where_condition) = self.parse_table_and_where(&remaining_parts)?;
                    
                    let table_data = self.database.get_table_rows(table_name)?;
                    
                    // Apply WHERE filter if present
                    let final_data = if let Some(condition) = where_condition {
                        self.apply_where_filter(&table_data, &condition)?
                    } else {
                        table_data
                    };
                    
                    Ok(QueryResult::Table {
                        headers: final_data.columns.iter().map(|col| col.name.clone()).collect(),
                        rows: final_data.rows.iter().map(|row| {
                            row.values.iter().map(|val| val.to_display_string()).collect()
                        }).collect(),
                    })
                } else {
                    Err(anyhow::anyhow!("Expected FROM after SELECT *"))
                }
            } else {
                Err(anyhow::anyhow!("Incomplete SELECT * query"))
            }
        } else if query_lower.contains(" from ") {
            // Handle SELECT column1, column2, ... FROM table [WHERE condition]
            let from_pos = query_lower.find(" from ").unwrap();
            let columns_part = query_after_select[..from_pos].trim();
            let from_part = query_after_select[from_pos + 6..].trim(); // Skip " from "
            
            // Parse column names (handle comma-separated)
            let column_names: Vec<&str> = columns_part
                .split(',')
                .map(|s| s.trim())
                .collect();
            
            // Parse table name and WHERE condition from the remaining part
            let from_parts: Vec<&str> = from_part.split_whitespace().collect();
            let (table_name, where_condition) = self.parse_table_and_where(&from_parts)?;
            
            // Get full table data first
            let table_data = self.database.get_table_rows(table_name)?;
            
            // Apply WHERE filter if present
            let filtered_data = if let Some(condition) = where_condition {
                self.apply_where_filter(&table_data, &condition)?
            } else {
                table_data
            };
            
            // Extract specific columns
            let result_data = self.extract_columns(&filtered_data, &column_names)?;
            
            Ok(QueryResult::Table {
                headers: column_names.iter().map(|s| s.to_string()).collect(),
                rows: result_data.iter().map(|row| {
                    row.iter().map(|val| val.to_display_string()).collect()
                }).collect(),
            })
        } else {
            Err(anyhow::anyhow!("Complex SELECT queries not yet supported in TUI"))
        }
    }
    
    fn parse_table_and_where<'a>(&self, parts: &'a [&str]) -> Result<(&'a str, Option<crate::query::WhereCondition>)> {
        if parts.is_empty() {
            return Err(anyhow::anyhow!("Missing table name in SELECT query"));
        }
        
        // Convert to lowercase for keyword matching but preserve original case
        let parts_lower: Vec<String> = parts.iter().map(|s| s.to_lowercase()).collect();
        
        // Check if there's a WHERE clause (case-insensitive)
        if let Some(where_pos) = parts_lower.iter().position(|part| part == "where") {
            if where_pos == 0 {
                return Err(anyhow::anyhow!("Missing table name before WHERE clause"));
            }
            let table_name = parts[0];
            let where_clause = parts[where_pos + 1..].join(" ");
            let condition = crate::query::WhereCondition::parse(&where_clause)?;
            Ok((table_name, Some(condition)))
        } else {
            Ok((parts[0], None))
        }
    }
    
    fn apply_where_filter(&self, table_data: &crate::TableRows, condition: &crate::query::WhereCondition) -> Result<crate::TableRows> {
        // Find the column index for the condition
        let column_index = table_data.columns.iter()
            .position(|col| col.name.eq_ignore_ascii_case(&condition.column_name))
            .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", condition.column_name))?;
        
        // Filter rows based on the condition
        let filtered_rows: Vec<crate::database::TableRow> = table_data.rows.iter()
            .filter(|row| {
                if let Some(value) = row.values.get(column_index) {
                    condition.matches(value)
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        
        Ok(crate::TableRows {
            columns: table_data.columns.clone(),
            rows: filtered_rows,
        })
    }
    
    /// Extract specific columns from table data
    fn extract_columns(&self, table_data: &crate::TableRows, column_names: &[&str]) -> Result<Vec<Vec<crate::RecordValue>>> {
        let mut column_indices = Vec::new();
        
        // Find indices for the requested columns
        for col_name in column_names {
            let index = table_data.columns.iter()
                .position(|col| col.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table", col_name))?;
            column_indices.push(index);
        }
        
        // Extract values for the specified columns
        let mut results = Vec::new();
        for row in &table_data.rows {
            let mut row_values = Vec::new();
            for &col_index in &column_indices {
                if let Some(value) = row.values.get(col_index) {
                    row_values.push(value.clone());
                } else {
                    row_values.push(crate::RecordValue::Null);
                }
            }
            results.push(row_values);
        }
        
        Ok(results)
    }
    
    pub fn add_char_to_query(&mut self, c: char) {
        self.query_input.insert(self.query_cursor_position, c);
        self.query_cursor_position += 1;
    }
    
    pub fn delete_char_from_query(&mut self) {
        if self.query_cursor_position > 0 {
            self.query_cursor_position -= 1;
            self.query_input.remove(self.query_cursor_position);
        }
    }
    
    pub fn move_cursor_left(&mut self) {
        if self.query_cursor_position > 0 {
            self.query_cursor_position -= 1;
        }
    }
    
    pub fn move_cursor_right(&mut self) {
        if self.query_cursor_position < self.query_input.len() {
            self.query_cursor_position += 1;
        }
    }
    
    pub fn scroll_up(&mut self) {
        match self.mode {
            AppMode::Tables => {
                if self.vertical_scroll > 0 {
                    self.vertical_scroll -= 1;
                }
            }
            AppMode::Query => {
                if self.query_scroll > 0 {
                    self.query_scroll -= 1;
                }
            }
            AppMode::Schema => {
                if self.schema_scroll > 0 {
                    self.schema_scroll -= 1;
                }
            }
        }
    }
    
    pub fn scroll_down(&mut self) {
        match self.mode {
            AppMode::Tables => {
                // Add bounds checking for table data scrolling
                if let Some(table_data) = &self.table_data {
                    let max_scroll = table_data.rows.len().saturating_sub(1);
                    if self.vertical_scroll < max_scroll {
                        self.vertical_scroll += 1;
                    }
                }
            }
            AppMode::Query => {
                self.query_scroll += 1;
            }
            AppMode::Schema => {
                self.schema_scroll += 1;
            }
        }
    }
    
    pub fn scroll_left(&mut self) {
        if self.horizontal_scroll > 0 {
            self.horizontal_scroll -= 1;
        }
    }
    
    pub fn scroll_right(&mut self) {
        match self.mode {
            AppMode::Tables => {
                // Add bounds checking for horizontal scrolling in tables
                if let Some(table_data) = &self.table_data {
                    let max_horizontal_scroll = table_data.columns.len().saturating_sub(1);
                    if self.horizontal_scroll < max_horizontal_scroll {
                        self.horizontal_scroll += 1;
                    }
                }
            }
            _ => {
                self.horizontal_scroll += 1;
            }
        }
    }
    
    // Additional scrolling methods for better control
    pub fn scroll_table_down_fast(&mut self) {
        if let Some(table_data) = &self.table_data {
            let max_scroll = table_data.rows.len().saturating_sub(1);
            self.vertical_scroll = (self.vertical_scroll + 10).min(max_scroll);
        }
    }
    
    pub fn scroll_table_up_fast(&mut self) {
        self.vertical_scroll = self.vertical_scroll.saturating_sub(10);
    }
    
    pub fn scroll_to_table_top(&mut self) {
        self.vertical_scroll = 0;
    }
    
    pub fn scroll_to_table_bottom(&mut self) {
        if let Some(table_data) = &self.table_data {
            self.vertical_scroll = table_data.rows.len().saturating_sub(1);
        }
    }
    
    pub fn toggle_help(&mut self) {
        self.show_help = !self.show_help;
    }
    
    pub fn set_status(&mut self, message: &str, style: Style) {
        self.status_message = message.to_string();
        self.status_style = style;
    }
    
    pub fn update(&mut self) {
        // No longer need cursor blinking logic since we use native cursor
    }
    
    pub fn load_schema_content(&mut self) -> Result<()> {
        match self.database.get_all_schema_objects() {
            Ok(objects) => {
                let mut content = String::new();
                for obj in objects {
                    if let Some(sql) = &obj.sql {
                        content.push_str(&format!("-- {}: {}\n", obj.object_type.to_uppercase(), obj.name));
                        content.push_str(sql);
                        content.push_str("\n\n");
                    }
                }
                if content.is_empty() {
                    content = "No schema information available".to_string();
                }
                self.schema_content = content;
                self.schema_scroll = 0; // Reset scroll when loading new content
                Ok(())
            }
            Err(e) => {
                self.schema_content = format!("Error loading schema: {}", e);
                Err(e)
            }
        }
    }
}

pub fn run_tui(database_path: String) -> Result<()> {
    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    
    // Create app
    let mut app = App::new(database_path)?;
    
    // Run the app
    let result = run_app(&mut terminal, &mut app);
    
    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
    
    result
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> Result<()> {
    loop {
        // Update app state
        app.update();
        
        terminal.draw(|f| ui(f, app))?;
        
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if handle_key_event(app, key)? {
                    return Ok(());
                }
            }
        }
    }
}

fn handle_key_event(app: &mut App, key: KeyEvent) -> Result<bool> {
    if app.show_help {
        match key.code {
            KeyCode::Char('?') | KeyCode::Esc => {
                app.toggle_help();
            }
            _ => {}
        }
        return Ok(false);
    }
    
    match key.code {
        KeyCode::Char('q') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            return Ok(true);
        }
        KeyCode::Char('?') => {
            app.toggle_help();
        }
        KeyCode::Tab => {
            // Exit editing mode when switching tabs
            app.input_mode = InputMode::Normal;
            app.next_mode();
        }
        KeyCode::BackTab => {
            // Exit editing mode when switching tabs  
            app.input_mode = InputMode::Normal;
            app.previous_mode();
        }
        _ => {
            match app.mode {
                AppMode::Tables => handle_tables_key(app, key)?,
                AppMode::Query => handle_query_key(app, key)?,
                AppMode::Schema => handle_schema_key(app, key)?,
            }
        }
    }
    
    Ok(false)
}

fn handle_tables_key(app: &mut App, key: KeyEvent) -> Result<()> {
    match key.code {
        // Table list navigation (left panel)
        KeyCode::Up | KeyCode::Char('k') => {
            app.previous_table()?;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.next_table()?;
        }
        KeyCode::Enter => {
            app.select_table()?;
        }
        
        // Table data refresh
        KeyCode::Char('r') => {
            app.load_table_data()?;
            app.set_status("Table data refreshed", Style::default().fg(Color::Green));
        }
        
        // Table data scrolling (right panel)
        KeyCode::Char('w') => {
            app.scroll_up();
        }
        KeyCode::Char('s') => {
            app.scroll_down();
        }
        KeyCode::Char('a') => {
            app.scroll_left();
        }
        KeyCode::Char('d') => {
            app.scroll_right();
        }
        
        // Fast scrolling
        KeyCode::PageUp | KeyCode::Char('W') => {
            app.scroll_table_up_fast();
        }
        KeyCode::PageDown | KeyCode::Char('S') => {
            app.scroll_table_down_fast();
        }
        
        // Jump to top/bottom
        KeyCode::Home | KeyCode::Char('g') => {
            app.scroll_to_table_top();
        }
        KeyCode::End | KeyCode::Char('G') => {
            app.scroll_to_table_bottom();
        }
        
        // Arrow keys for data scrolling (more intuitive)
        KeyCode::Left => {
            app.scroll_left();
        }
        KeyCode::Right => {
            app.scroll_right();
        }
        
        _ => {}
    }
    Ok(())
}

fn handle_query_key(app: &mut App, key: KeyEvent) -> Result<()> {
    match app.input_mode {
        InputMode::Normal => {
            match key.code {
                KeyCode::Enter => {
                    // Start editing mode
                    app.input_mode = InputMode::Editing;
                }
                KeyCode::Up => {
                    app.scroll_up();
                }
                KeyCode::Down => {
                    app.scroll_down();
                }
                _ => {}
            }
        }
        InputMode::Editing => {
            match key.code {
                KeyCode::Esc => {
                    // Exit editing mode
                    app.input_mode = InputMode::Normal;
                }
                KeyCode::Enter => {
                    // Execute query and exit editing mode
                    app.execute_query()?;
                    app.input_mode = InputMode::Normal;
                }
                KeyCode::Backspace => {
                    app.delete_char_from_query();
                }
                KeyCode::Left => {
                    app.move_cursor_left();
                }
                KeyCode::Right => {
                    app.move_cursor_right();
                }
                KeyCode::Char(c) => {
                    app.add_char_to_query(c);
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn handle_schema_key(app: &mut App, key: KeyEvent) -> Result<()> {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            app.scroll_up();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.scroll_down();
        }
        KeyCode::Left | KeyCode::Char('h') => {
            app.scroll_left();
        }
        KeyCode::Right | KeyCode::Char('l') => {
            app.scroll_right();
        }
        KeyCode::Char('r') => {
            // Refresh schema content
            match app.load_schema_content() {
                Ok(_) => app.set_status("Schema refreshed", Style::default().fg(Color::Green)),
                Err(e) => app.set_status(&format!("Error refreshing schema: {}", e), Style::default().fg(Color::Red)),
            }
        }
        KeyCode::PageUp => {
            for _ in 0..10 {
                app.scroll_up();
            }
        }
        KeyCode::PageDown => {
            for _ in 0..10 {
                app.scroll_down();
            }
        }
        _ => {}
    }
    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    if app.show_help {
        render_help(f);
        return;
    }
    
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Header
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Status bar
        ])
        .split(f.size());
    
    render_header(f, chunks[0], app);
    render_main_content(f, chunks[1], app);
    render_status_bar(f, chunks[2], app);
    
    // Set cursor position only when editing in Query mode (following user input example)
    if let (AppMode::Query, InputMode::Editing) = (&app.mode, &app.input_mode) {
        // Calculate the input area the same way as render_query_view
        let query_area = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(0)])
            .split(chunks[1])[0];
        
        // Position cursor inside the input box borders
        let cursor_x = query_area.x + 1 + app.query_cursor_position as u16;
        let cursor_y = query_area.y + 1;
        
        // Ensure cursor doesn't go beyond input area bounds
        if cursor_x < query_area.x + query_area.width - 1 {
            f.set_cursor(cursor_x, cursor_y);
        }
    }
}

fn render_header(f: &mut Frame, area: Rect, app: &App) {
    let titles = vec!["Tables", "Query", "Schema"];
    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("SQLite TUI"))
        .style(Style::default().fg(Color::White))
        .highlight_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .select(match app.mode {
            AppMode::Tables => 0,
            AppMode::Query => 1,
            AppMode::Schema => 2,
        });
    f.render_widget(tabs, area);
}

fn render_main_content(f: &mut Frame, area: Rect, app: &App) {
    match app.mode {
        AppMode::Tables => render_tables_view(f, area, app),
        AppMode::Query => render_query_view(f, area, app),
        AppMode::Schema => render_schema_view(f, area, app),
    }
}

fn render_tables_view(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(25), Constraint::Percentage(75)])
        .split(area);
    
    // Tables list
    let table_items: Vec<ListItem> = app
        .tables
        .iter()
        .map(|table| {
            ListItem::new(table.as_str()).style(Style::default().fg(Color::White))
        })
        .collect();
    
    let tables_list = List::new(table_items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Tables")
                .title_style(Style::default().fg(Color::Cyan))
        )
        .highlight_style(Style::default().bg(Color::DarkGray).add_modifier(Modifier::BOLD))
        .highlight_symbol(">> ");
    
    f.render_stateful_widget(tables_list, chunks[0], &mut app.table_list_state.clone());
    
    // Table data
    render_table_data(f, chunks[1], app);
}

fn render_table_data(f: &mut Frame, area: Rect, app: &App) {
    if let Some(table_data) = &app.table_data {
        if table_data.rows.is_empty() {
            let paragraph = Paragraph::new("No data in table")
                .block(Block::default().borders(Borders::ALL).title("Table Data"))
                .style(Style::default().fg(Color::Yellow))
                .alignment(Alignment::Center);
            f.render_widget(paragraph, area);
            return;
        }
        
        // Calculate visible columns with horizontal scrolling
        let total_columns = table_data.columns.len();
        let visible_columns_count = ((area.width.saturating_sub(4)) / 22) as usize; // Account for borders and column spacing
        let start_col = app.horizontal_scroll.min(total_columns.saturating_sub(1));
        let end_col = (start_col + visible_columns_count).min(total_columns);
        
        // Prepare headers with horizontal scrolling
        let headers: Vec<&str> = table_data.columns
            .iter()
            .skip(start_col)
            .take(end_col - start_col)
            .map(|col| col.name.as_str())
            .collect();
        
        // Calculate visible rows bounds
        let visible_height = area.height.saturating_sub(3) as usize; // Account for borders and header
        let max_vertical_scroll = table_data.rows.len().saturating_sub(visible_height);
        let actual_vertical_scroll = app.vertical_scroll.min(max_vertical_scroll);
        
        // Prepare rows with both vertical and horizontal scrolling
        let visible_rows: Vec<Row> = table_data.rows
            .iter()
            .skip(actual_vertical_scroll)
            .take(visible_height)
            .map(|row| {
                let cells: Vec<String> = row.values
                    .iter()
                    .skip(start_col)
                    .take(end_col - start_col)
                    .map(|val| {
                        let display = val.to_display_string();
                        if display.len() > 20 {
                            format!("{}...", &display[..17])
                        } else {
                            display
                        }
                    })
                    .collect();
                Row::new(cells)
            })
            .collect();
        
        let widths = headers.iter()
            .map(|_| Constraint::Length(20))
            .collect::<Vec<_>>();
        
        // Create scroll indicators
        let vertical_indicator = if table_data.rows.len() > visible_height {
            format!(" │ Rows: {}-{}/{}", 
                actual_vertical_scroll + 1, 
                (actual_vertical_scroll + visible_rows.len()).min(table_data.rows.len()),
                table_data.rows.len())
        } else {
            format!(" │ Rows: {}", table_data.rows.len())
        };
        
        let horizontal_indicator = if total_columns > visible_columns_count {
            format!(" │ Cols: {}-{}/{}", 
                start_col + 1, 
                end_col,
                total_columns)
        } else {
            format!(" │ Cols: {}", total_columns)
        };
        
        let scroll_info = format!("{}{}", vertical_indicator, horizontal_indicator);
        
        let table = Table::new(visible_rows, widths)
            .header(
                Row::new(headers)
                    .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
                    .bottom_margin(1)
            )
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(format!("Table Data: {}{}", 
                        app.selected_table.as_deref().unwrap_or("None"),
                        scroll_info))
                    .title_style(Style::default().fg(Color::Green))
            )
            .highlight_style(Style::default().bg(Color::DarkGray))
            .highlight_symbol(">> ");
        
        f.render_stateful_widget(table, area, &mut app.table_state.clone());
    } else {
        let paragraph = Paragraph::new("Select a table to view its data")
            .block(Block::default().borders(Borders::ALL).title("Table Data"))
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        f.render_widget(paragraph, area);
    }
}

fn render_query_view(f: &mut Frame, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0)])
        .split(area);
    
    // Query input with different styles based on input mode
    let (query_text, input_style) = if app.query_input.is_empty() {
        // Show placeholder text
        let text = if app.input_mode == InputMode::Editing {
            "Type your SQL query here..."
        } else {
            "Press Enter to start editing..."
        };
        (Line::from(Span::styled(text, Style::default().fg(Color::DarkGray))), Style::default())
    } else {
        // Show actual query text with style based on mode
        let style = match app.input_mode {
            InputMode::Normal => Style::default().fg(Color::White),
            InputMode::Editing => Style::default().fg(Color::Yellow),
        };
        (Line::from(Span::styled(app.query_input.clone(), style)), style)
    };
    
    let query_input = Paragraph::new(Text::from(vec![query_text]))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("SQL Query (Enter: edit/execute, Esc: stop editing, ↑↓: scroll results)")
                .title_style(Style::default().fg(Color::Cyan))
        )
        .style(input_style)
        .wrap(Wrap { trim: true });
    
    f.render_widget(query_input, chunks[0]);
    
    // Cursor positioning is now handled centrally in ui() function
    
    // Query results with better formatting and scrolling
    if app.query_history.is_empty() {
        let help_text = vec![
            Line::from("No query history yet."),
            Line::from(""),
            Line::from("Try some sample queries:"),
            Line::from(""),
            Line::from("• SELECT COUNT(*) FROM superheroes"),
            Line::from("• SELECT * FROM superheroes LIMIT 5"),
            Line::from("• .tables"),
            Line::from("• .schema"),
            Line::from("• .dbinfo"),
            Line::from(""),
            Line::from("Navigation:"),
            Line::from("• Type your query and press Enter"),
            Line::from("• Use ↑↓ to scroll through results"),
            Line::from("• Use Tab to switch views"),
        ];
        
        let paragraph = Paragraph::new(help_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Query Results & Help")
                    .title_style(Style::default().fg(Color::Green))
            )
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
        f.render_widget(paragraph, chunks[1]);
    } else {
        // Show query history with improved formatting
        let mut all_lines = Vec::new();
        
        for (i, h) in app.query_history.iter().rev().enumerate() {
            if i > 0 {
                all_lines.push(Line::from(""));
                all_lines.push(Line::from("─".repeat(60)));
                all_lines.push(Line::from(""));
            }
            
            // Query line
            all_lines.push(Line::from(vec![
                Span::styled("Query: ", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                Span::styled(h.query.clone(), Style::default().fg(Color::White)),
            ]));
            
            all_lines.push(Line::from(""));
            
            // Handle different result types
            match &h.result {
                QueryResult::Text(text) => {
                    // Text results - split into multiple lines
                    let result_lines: Vec<&str> = text.lines().collect();
                    for (line_idx, line) in result_lines.iter().enumerate() {
                        if line_idx == 0 {
                            all_lines.push(Line::from(vec![
                                Span::styled("Result: ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                                Span::styled((*line).to_string(), Style::default().fg(Color::Gray)),
                            ]));
                        } else {
                            all_lines.push(Line::from(vec![
                                Span::styled("        ", Style::default()), // Indent continuation lines
                                Span::styled((*line).to_string(), Style::default().fg(Color::Gray)),
                            ]));
                        }
                    }
                }
                QueryResult::Table { headers, rows } => {
                    // Table results - render as structured table
                    all_lines.push(Line::from(vec![
                        Span::styled("Result: ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                        Span::styled("Table data", Style::default().fg(Color::Gray)),
                    ]));
                    
                    // Add table headers
                    let header_line = headers.join(" | ");
                    all_lines.push(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled(header_line, Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
                    ]));
                    
                    // Add separator line
                    let separator = headers.iter()
                        .map(|h| "-".repeat(h.len().max(10)))
                        .collect::<Vec<_>>()
                        .join("-+-");
                    all_lines.push(Line::from(vec![
                        Span::styled("        ", Style::default()),
                        Span::styled(separator, Style::default().fg(Color::DarkGray)),
                    ]));
                    
                    // Add table rows (limit to first 20 for readability)
                    for (row_idx, row) in rows.iter().enumerate() {
                        if row_idx >= 20 {
                            all_lines.push(Line::from(vec![
                                Span::styled("        ", Style::default()),
                                Span::styled(format!("... and {} more rows", rows.len() - 20), Style::default().fg(Color::DarkGray)),
                            ]));
                            break;
                        }
                        let row_line = row.join(" | ");
                        all_lines.push(Line::from(vec![
                            Span::styled("        ", Style::default()),
                            Span::styled(row_line, Style::default().fg(Color::White)),
                        ]));
                    }
                }
            }
        }
        
        let total_lines = all_lines.len();
        let visible_area_height = chunks[1].height.saturating_sub(2) as usize; // Account for borders
        let max_scroll = total_lines.saturating_sub(visible_area_height);
        let scroll_pos = app.query_scroll.min(max_scroll);
        
        let title = if total_lines > visible_area_height {
            format!("Query History (line {}/{}, ↑↓ to scroll)", 
                scroll_pos + 1, 
                total_lines)
        } else {
            "Query History".to_string()
        };
        
        let paragraph = Paragraph::new(all_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_style(Style::default().fg(Color::Green))
            )
            .style(Style::default())
            .wrap(Wrap { trim: true })
            .scroll((scroll_pos as u16, 0));
        
        f.render_widget(paragraph, chunks[1]);
    }
}

fn render_schema_view(f: &mut Frame, area: Rect, app: &App) {
    if app.schema_content.is_empty() {
        let help_text = vec![
            Line::from("Loading schema information..."),
            Line::from(""),
            Line::from("This view shows:"),
            Line::from("• CREATE TABLE statements"),
            Line::from("• CREATE INDEX statements"),
            Line::from("• Other database objects"),
            Line::from(""),
            Line::from("Navigation:"),
            Line::from("• Use ↑↓ to scroll through schema"),
            Line::from("• Use Tab to switch views"),
            Line::from("• Press 'r' to refresh schema"),
        ];
        
        let paragraph = Paragraph::new(help_text)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Database Schema")
                    .title_style(Style::default().fg(Color::Magenta))
            )
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
        f.render_widget(paragraph, area);
    } else {
        // Parse schema content into lines for better display
        let schema_lines: Vec<Line> = app.schema_content
            .lines()
            .map(|line| {
                if line.starts_with("--") {
                    // Comment lines in cyan
                    Line::from(Span::styled(line.to_string(), Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)))
                } else if line.trim().to_uppercase().starts_with("CREATE") {
                    // CREATE statements in yellow
                    Line::from(Span::styled(line.to_string(), Style::default().fg(Color::Yellow)))
                } else if line.trim().is_empty() {
                    // Empty lines
                    Line::from("")
                } else {
                    // Regular SQL content in white
                    Line::from(Span::styled(line.to_string(), Style::default().fg(Color::White)))
                }
            })
            .collect();
        
        let total_lines = schema_lines.len();
        let visible_area_height = area.height.saturating_sub(2) as usize; // Account for borders
        let max_scroll = total_lines.saturating_sub(visible_area_height);
        let scroll_pos = app.schema_scroll.min(max_scroll);
        
        let title = if total_lines > visible_area_height {
            format!("Database Schema (line {}/{}, ↑↓ to scroll, r to refresh)", 
                scroll_pos + 1, 
                total_lines)
        } else {
            "Database Schema (r to refresh)".to_string()
        };
        
        let paragraph = Paragraph::new(schema_lines)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(title)
                    .title_style(Style::default().fg(Color::Magenta))
            )
            .style(Style::default().fg(Color::White))
            .wrap(Wrap { trim: true })
            .scroll((scroll_pos as u16, 0));
        
        f.render_widget(paragraph, area);
    }
}

fn render_status_bar(f: &mut Frame, area: Rect, app: &App) {
    let mode_specific_help = match app.mode {
        AppMode::Tables => "k/j: select table | Enter: load | wasd: scroll data | PageUp/Down: fast scroll | g/G: top/bottom",
        AppMode::Query => "Enter: execute | ↑↓: scroll results | ←→: move cursor",
        AppMode::Schema => "↑↓: scroll | r: refresh | Page Up/Down: fast scroll",
    };
    
    let status_text = vec![
        Line::from(vec![
            Span::styled("Status: ", Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
            Span::styled(app.status_message.clone(), app.status_style),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled(mode_specific_help, Style::default().fg(Color::DarkGray)),
            Span::styled(" | ", Style::default().fg(Color::DarkGray)),
            Span::styled("Tab: Switch | Ctrl+Q: Quit | ?: Help", Style::default().fg(Color::DarkGray)),
        ])
    ];
    
    let paragraph = Paragraph::new(status_text)
        .block(Block::default().borders(Borders::ALL))
        .style(Style::default());
    
    f.render_widget(paragraph, area);
}

fn render_help(f: &mut Frame) {
    let area = centered_rect(80, 80, f.size());
    
    f.render_widget(Clear, area);
    
    let help_text = vec![
        Line::from(vec![Span::styled("SQLite TUI - Help", Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))]),
        Line::from(""),
        Line::from(vec![Span::styled("Global Keys:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))]),
        Line::from("  Tab / Shift+Tab    - Switch between views"),
        Line::from("  Ctrl+Q             - Quit application"),
        Line::from("  ?                  - Toggle this help"),
        Line::from(""),
        Line::from(vec![Span::styled("Tables View:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))]),
        Line::from("  ↑/↓ or j/k         - Navigate table list"),
        Line::from("  Enter              - Load selected table"),
        Line::from("  r                  - Refresh table data"),
        Line::from("  w/s                - Scroll table data up/down"),
        Line::from("  a/d                - Scroll table data left/right"),
        Line::from("  ←/→                - Scroll table data left/right"),
        Line::from("  Page Up/Down       - Fast scroll (10 rows)"),
        Line::from("  g/G                - Jump to top/bottom"),
        Line::from("  Home/End           - Jump to top/bottom"),
        Line::from(""),
        Line::from(vec![Span::styled("Query View:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))]),
        Line::from("  Type               - Enter SQL query"),
        Line::from("  Enter              - Execute query"),
        Line::from("  ←/→                - Move cursor in input"),
        Line::from("  ↑/↓                - Scroll query results"),
        Line::from("  Page Up/Down       - Fast scroll results"),
        Line::from("  Backspace          - Delete character"),
        Line::from(""),
        Line::from(vec![Span::styled("Schema View:", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD))]),
        Line::from("  ↑/↓ or j/k         - Scroll vertically"),
        Line::from("  Page Up/Down       - Fast scroll"),
        Line::from("  r                  - Refresh schema"),
        Line::from("  ←/→ or h/l         - Scroll horizontally"),
        Line::from(""),
        Line::from(vec![Span::styled("Press ? or Esc to close help", Style::default().fg(Color::Yellow))])
    ];
    
    let paragraph = Paragraph::new(help_text)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Help")
                .title_style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        )
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });
    
    f.render_widget(paragraph, area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
} 