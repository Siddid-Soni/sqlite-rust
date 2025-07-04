use anyhow::{bail, Result};
use std::fs::File;
use std::io::prelude::*;

use crate::{DB_HEADER_SIZE, BTREE_HEADER_SIZE};
use crate::cell::Cell;
use crate::schema::{TableSchema, ColumnInfo};
use crate::record::RecordValue;

// B-tree page types
const INTERIOR_INDEX_PAGE: u8 = 2;
const INTERIOR_TABLE_PAGE: u8 = 5;
const LEAF_INDEX_PAGE: u8 = 10;
const LEAF_TABLE_PAGE: u8 = 13;

#[derive(Debug, Clone)]
pub struct IndexCell {
    pub key: String,
    pub row_id: u64,
}

#[derive(Debug, Clone)]
pub struct SchemaObject {
    pub object_type: String,  // "table", "index", "view", etc.
    pub name: String,         // name of the object
    pub tbl_name: String,     // table this object belongs to
    pub rootpage: usize,      // page number where object data starts
    pub sql: Option<String>,  // CREATE statement
}

impl SchemaObject {
    pub fn from_record(record: &crate::record::Record) -> Option<Self> {
        // Schema records have: type, name, tbl_name, rootpage, sql
        if record.body.len() >= 4 {
            let object_type = match record.body.get(0) {
                Some(RecordValue::Text(t)) => t.clone(),
                _ => return None,
            };
            
            let name = match record.body.get(1) {
                Some(RecordValue::Text(n)) => n.clone(),
                _ => return None,
            };
            
            let tbl_name = match record.body.get(2) {
                Some(RecordValue::Text(t)) => t.clone(),
                _ => return None,
            };
            
            let rootpage = match record.body.get(3) {
                Some(RecordValue::Int(p)) => *p as usize,
                _ => return None,
            };
            
            let sql = record.body.get(4).and_then(|v| match v {
                RecordValue::Text(s) => Some(s.clone()),
                _ => None,
            });
            
            Some(SchemaObject {
                object_type,
                name,
                tbl_name,
                rootpage,
                sql,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableRow {
    pub row_id: u64,
    pub values: Vec<RecordValue>,
}

#[derive(Debug, Clone)]
pub struct TableRows {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<TableRow>,
}

pub struct Database {
    file: File,
    page_size: usize,
}

impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let page_size = Self::read_page_size(&mut file)?;
        Ok(Self { file, page_size })
    }

    fn read_page_size(file: &mut File) -> Result<usize> {
        file.seek(std::io::SeekFrom::Start(0))?;
        let mut header = [0; DB_HEADER_SIZE];
        file.read_exact(&mut header)?;

        let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;
        Ok(page_size)
    }

    pub fn get_page_size(&self) -> usize {
        self.page_size
    }

    pub fn read_page(&mut self, page_number: usize) -> Result<Vec<Cell>> {
        let page_data = self.read_page_data(page_number)?;
        
        let cell_offsets = self.get_cell_offsets(&page_data, page_number)?;
        let mut cells = Vec::new();

        for cell_offset in cell_offsets {
            let cell = Cell::from_bytes(&page_data, cell_offset)?;
            cells.push(cell);
        }
        Ok(cells)
    }

    fn read_page_data(&mut self, page_number: usize) -> Result<Vec<u8>> {
        let offset = (page_number - 1) * self.page_size;
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;

        let mut page_data = vec![0; self.page_size];
        self.file.read_exact(&mut page_data)?;
        Ok(page_data)
    }

    fn get_cell_offsets(&self, page_data: &[u8], page_number: usize) -> Result<Vec<usize>> {
        let cell_count = self.get_cell_count(page_data, page_number)?;
        let dbheader_offset = self.get_dbheader_offset(page_number);
        let ptr_start = dbheader_offset + BTREE_HEADER_SIZE;
        let ptr_end = ptr_start + cell_count * 2;

        if ptr_end > page_data.len() {
            bail!("Page data too small to contain all cell pointers");
        }

        let pointer_array = &page_data[ptr_start..ptr_end];
        let cell_offsets: Vec<usize> = pointer_array
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes(chunk.try_into().unwrap()) as usize)
            .collect();

        Ok(cell_offsets)
    }

    fn get_cell_count(&self, page_data: &[u8], page_number: usize) -> Result<usize> {
        let dbheader_offset = self.get_dbheader_offset(page_number);

        if dbheader_offset + 5 > page_data.len() {
            bail!("Page data too small to contain B-tree header");
        }

        let cell_count = u16::from_be_bytes([
            page_data[dbheader_offset + 3],
            page_data[dbheader_offset + 4],
        ]) as usize;

        Ok(cell_count)
    }

    /// Calculate the database header offset for a given page number
    fn get_dbheader_offset(&self, page_number: usize) -> usize {
        if page_number == 1 { DB_HEADER_SIZE } else { 0 }
    }

    pub fn get_col_names(&mut self, table_name: &str) -> Result<Vec<ColumnInfo>> {
        let table_info = self.find_table_info(table_name)?;
        
        let sql = table_info
            .record
            .get_sql_schema()
            .ok_or_else(|| anyhow::anyhow!("No SQL schema found for table {}", table_name))?;

        let schema = TableSchema::from_create_sql(sql)?;
        Ok(schema.columns)
    }

    pub fn find_table_info(&mut self, table_name: &str) -> Result<Cell> {
        let cells = self.read_page(1)?;
        
        for cell in cells {
            if let Some(name) = cell.record.get_table_name() {
                if name == table_name {
                    return Ok(cell);
                }
            }
        }
        
        bail!("Table {} not found", table_name)
    }

    pub fn count_table_rows(&mut self, table_name: &str) -> Result<usize> {
        let table_info = self.find_table_info(table_name)?;
        let page_num = table_info.record.get_page_number()?;
        let rows = self.read_page(page_num)?;
        Ok(rows.len())
    }

    pub fn get_table_names(&mut self) -> Result<Vec<String>> {
        let cells = self.read_page(1)?;
        let mut tables = Vec::new();
        
        for cell in cells {
            if let Some(table_name) = cell.record.get_table_name() {
                tables.push(table_name.to_string());
            }
        }
        
        Ok(tables)
    }

    /// Get all schema objects (tables, indexes, etc.) from sqlite_master
    pub fn get_all_schema_objects(&mut self) -> Result<Vec<SchemaObject>> {
        let cells = self.read_page(1)?;
        let mut objects = Vec::new();
        
        for cell in cells {
            if let Some(schema_obj) = SchemaObject::from_record(&cell.record) {
                objects.push(schema_obj);
            }
        }
        
        Ok(objects)
    }

    /// Find an index that can be used for the given table and column
    pub fn find_index_for_column(&mut self, table_name: &str, column_name: &str) -> Result<Option<SchemaObject>> {
        let objects = self.get_all_schema_objects()?;
        
        for obj in objects {
            if obj.object_type == "index" && obj.tbl_name == table_name {
                if let Some(sql) = &obj.sql {
                    if sql.to_lowercase().contains(&format!("on {} ({})", table_name, column_name)) {
                        return Ok(Some(obj));
                    }
                }
            }
        }
        
        Ok(None)
    }

    /// Search an index for entries matching the given value and return row IDs
    pub fn search_index(&mut self, index: &SchemaObject, search_value: &str) -> Result<Vec<u64>> {
        let mut row_ids = Vec::new();
        self.traverse_index_for_value(index.rootpage, search_value, &mut row_ids)?;
        Ok(row_ids)
    }
    
    fn traverse_index_for_value(&mut self, page_num: usize, search_value: &str, row_ids: &mut Vec<u64>) -> Result<()> {
        let page_data = self.read_page_data(page_num)?;
        let dbheader_offset = self.get_dbheader_offset(page_num);
        
        if dbheader_offset >= page_data.len() {
            bail!("Page data too small for page header");
        }
        
        let page_type = page_data[dbheader_offset];
        
        match page_type {
            LEAF_INDEX_PAGE => {
                // This is a leaf index page - search for matching entries
                self.search_index_leaf_page_stack(&page_data, page_num, search_value, row_ids)?;
            }
            INTERIOR_INDEX_PAGE => {
                // This is an interior index page - traverse all children that might contain our value
                let child_pages = self.get_index_child_pages_proper(&page_data, page_num, search_value)?;
                
                for child_page in child_pages {
                    self.traverse_index_for_value(child_page, search_value, row_ids)?;
                }
            }
            _ => {
                bail!("Unsupported page type {} for index search", page_type);
            }
        }
        
        Ok(())
    }

    /// Search a leaf index page for matching entries
    fn search_index_leaf_page_stack(&self, page_data: &[u8], page_num: usize, search_value: &str, row_ids: &mut Vec<u64>) -> Result<()> {
        let cell_offsets = self.get_cell_offsets(page_data, page_num)?;
        
        for cell_offset in cell_offsets.iter() {
            if let Ok(cell) = self.read_index_cell(page_data, *cell_offset) {
                if cell.key == search_value {
                    row_ids.push(cell.row_id);
                }
            }
        }
        
        Ok(())
    }

    /// Get child pages from an interior index page with smart key-based navigation
    fn get_index_child_pages_proper(&self, page_data: &[u8], page_num: usize, search_value: &str) -> Result<Vec<usize>> {
        let dbheader_offset = self.get_dbheader_offset(page_num);
        let cell_count = self.get_cell_count(page_data, page_num)?;
        
        let rightmost_page = self.read_rightmost_page(page_data, dbheader_offset)?;
        
        let ptr_start = dbheader_offset + 12;
        let ptr_end = ptr_start + cell_count * 2;

        if ptr_end > page_data.len() {
            bail!("Page data too small to contain all cell pointers");
        }
        
        let pointer_array = &page_data[ptr_start..ptr_end];
        let cell_offsets: Vec<usize> = pointer_array
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes(chunk.try_into().unwrap()) as usize)
            .collect();
        
        let mut child_pages = Vec::new();
        let mut found_target_range = false;
        
        for cell_offset in cell_offsets.iter() {
            if cell_offset + 4 > page_data.len() {
                continue;
            }
            
            let child_page = self.read_page_number_from_cell(page_data, *cell_offset)?;
            
            if let Ok(index_cell) = self.read_index_cell(page_data, *cell_offset) {
                if search_value <= index_cell.key.as_str() {
                    child_pages.push(child_page);
                    found_target_range = true;
                    break;
                }
            } else {
                child_pages.push(child_page);
            }
        }
        
        if !found_target_range {
            child_pages.push(rightmost_page);
        }
        
        if child_pages.is_empty() {
            for cell_offset in cell_offsets.iter() {
                if let Ok(page_number) = self.read_page_number_from_cell(page_data, *cell_offset) {
                    child_pages.push(page_number);
                }
            }
            child_pages.push(rightmost_page);
        }
        
        Ok(child_pages)
    }

    /// Read an index cell from the page data
    fn read_index_cell(&self, page_data: &[u8], offset: usize) -> Result<IndexCell> {
        let mut pos = offset;
        
        let (payload_size, bytes_read) = crate::varint::read_varint(page_data, pos)?;
        pos += bytes_read;
        
        let payload_end = pos + payload_size as usize;
        if payload_end > page_data.len() {
            bail!("Index cell payload extends beyond page");
        }
        
        let payload_data = &page_data[pos..payload_end];
        let record = crate::record::Record::from_bytes(payload_data)?;
        
        if record.body.len() < 2 {
            bail!("Index cell has fewer than 2 fields: {}", record.body.len());
        }
        
        let key = match record.body.get(0) {
            Some(RecordValue::Text(s)) => s.clone(),
            Some(other) => other.to_display_string(),
            None => bail!("Index cell missing key value"),
        };
        
        let row_id = match record.body.get(1) {
            Some(RecordValue::Int(id)) => *id as u64,
            _ => bail!("Index cell missing or invalid row ID"),
        };
        
        Ok(IndexCell { key, row_id })
    }

    pub fn get_num_tables(&mut self) -> Result<u16> {
        let page_data = self.read_page_data(1)?;
        
        let bheader_start = DB_HEADER_SIZE;
        if bheader_start + 5 > page_data.len() {
            bail!("Page data too small to contain B-tree header");
        }
        
        Ok(u16::from_be_bytes([
            page_data[bheader_start + 3], 
            page_data[bheader_start + 4]
        ]))
    }

    /// Read all rows from a table and return them with column information
    pub fn get_table_rows(&mut self, table_name: &str) -> Result<TableRows> {
        let table_info = self.find_table_info(table_name)?;
        let columns = self.get_col_names(table_name)?;
        let page_num = table_info.record.get_page_number()?;
        
        let all_cells = self.collect_all_table_cells(page_num)?;

        let mut rows = Vec::new();
        for cell in all_cells {
            rows.push(self.create_table_row(cell, &columns));
        }

        Ok(TableRows {
            columns,
            rows,
        })
    }

    /// Create a TableRow from a cell and column information
    fn create_table_row(&self, cell: Cell, columns: &[ColumnInfo]) -> TableRow {
        let mut row = Vec::new();
        
        for (i, column) in columns.iter().enumerate() {
            if column.is_primary_key {
                row.push(RecordValue::Int(cell.row_id as i64));
            } else {
                if let Some(value) = cell.record.body.get(i) {
                    row.push(value.clone());
                } else {
                    row.push(RecordValue::Null);
                }
            }
        }
        
        TableRow {
            row_id: cell.row_id,
            values: row,
        }
    }

    /// Collect all cells from a table using B-tree traversal
    fn collect_all_table_cells(&mut self, page_num: usize) -> Result<Vec<Cell>> {
        let page_data = self.read_page_data(page_num)?;
        let dbheader_offset = self.get_dbheader_offset(page_num);
        
        if dbheader_offset >= page_data.len() {
            bail!("Page data too small for page header");
        }
        
        let page_type = page_data[dbheader_offset];
        
        match page_type {
            LEAF_TABLE_PAGE => {
                let cell_offsets = self.get_cell_offsets(&page_data, page_num)?;
                let mut cells = Vec::new();

                for cell_offset in cell_offsets {
                    let cell = Cell::from_bytes(&page_data, cell_offset)?;
                    cells.push(cell);
                }
                
                Ok(cells)
            }
            INTERIOR_TABLE_PAGE => {
                let child_pages = self.get_child_page_numbers(&page_data, page_num)?;
                let mut all_cells = Vec::new();
                
                for child_page in child_pages {
                    let mut child_cells = self.collect_all_table_cells(child_page)?;
                    all_cells.append(&mut child_cells);
                }
                
                Ok(all_cells)
            }
            _ => {
                bail!("Unsupported page type {} for table data", page_type);
            }
        }
    }



    /// Get child page numbers from an interior page
    fn get_child_page_numbers(&self, page_data: &[u8], page_num: usize) -> Result<Vec<usize>> {
        let dbheader_offset = self.get_dbheader_offset(page_num);
        let cell_count = self.get_cell_count(page_data, page_num)?;
        
        let ptr_start = dbheader_offset + 12;
        let ptr_end = ptr_start + cell_count * 2;

        if ptr_end > page_data.len() {
            bail!("Page data too small to contain all cell pointers");
        }
        
        let rightmost_page = self.read_rightmost_page(page_data, dbheader_offset)?;
        
        let mut child_pages = Vec::new();
        
        let pointer_array = &page_data[ptr_start..ptr_end];
        let cell_offsets: Vec<usize> = pointer_array
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes(chunk.try_into().unwrap()) as usize)
            .collect();
        
        for cell_offset in cell_offsets.iter() {
            if let Ok(page_number) = self.read_page_number_from_cell(page_data, *cell_offset) {
                child_pages.push(page_number);
            }
        }
        
        child_pages.push(rightmost_page);
        
        Ok(child_pages)
    }

    /// Get specific column values from a table
    pub fn get_column_values(&mut self, table_name: &str, column_names: &[&str]) -> Result<Vec<Vec<RecordValue>>> {
        let table_rows = self.get_table_rows(table_name)?;
        let mut results = Vec::new();

        let mut column_indices = Vec::new();
        for col_name in column_names {
            let index = table_rows.columns.iter()
                .position(|col| col.name.eq_ignore_ascii_case(col_name))
                .ok_or_else(|| anyhow::anyhow!("Column '{}' not found in table '{}'", col_name, table_name))?;
            column_indices.push(index);
        }

        for row in table_rows.rows {
            let mut row_values = Vec::new();
            for &col_index in &column_indices {
                if let Some(value) = row.values.get(col_index) {
                    row_values.push(value.clone());
                } else {
                    row_values.push(RecordValue::Null);
                }
            }
            results.push(row_values);
        }

        Ok(results)
    }

    /// Fetch specific rows from a table by their row IDs (optimized version)
    pub fn get_table_rows_by_ids(&mut self, table_name: &str, row_ids: &[u64]) -> Result<TableRows> {
        let columns = self.get_col_names(table_name)?;
        
        let mut rows = Vec::new();
        
        for &row_id in row_ids {
            if let Some(row) = self.get_table_row_by_id(table_name, row_id)? {
                rows.push(row);
            }
        }

        Ok(TableRows {
            columns,
            rows,
        })
    }

    /// Fetch a single row by its row ID using B-tree navigation (much faster than loading all data)
    pub fn get_table_row_by_id(&mut self, table_name: &str, row_id: u64) -> Result<Option<TableRow>> {
        let table_info = self.find_table_info(table_name)?;
        let columns = self.get_col_names(table_name)?;
        let page_num = table_info.record.get_page_number()?;
        
        // Navigate directly to the row using B-tree search
        if let Some(cell) = self.search_table_for_row_id(page_num, row_id)? {
            Ok(Some(self.create_table_row(cell, &columns)))
        } else {
            Ok(None)
        }
    }
    
    /// Search for a specific row ID in the table B-tree
    fn search_table_for_row_id(&mut self, page_num: usize, target_row_id: u64) -> Result<Option<Cell>> {
        let page_data = self.read_page_data(page_num)?;
        let dbheader_offset = self.get_dbheader_offset(page_num);
        
        if dbheader_offset >= page_data.len() {
            bail!("Page data too small for page header");
        }
        
        let page_type = page_data[dbheader_offset];
        
        match page_type {
            LEAF_TABLE_PAGE => {
                let cell_offsets = self.get_cell_offsets(&page_data, page_num)?;
                
                for cell_offset in cell_offsets {
                    let cell = Cell::from_bytes(&page_data, cell_offset)?;
                    if cell.row_id == target_row_id {
                        return Ok(Some(cell));
                    }
                }
                Ok(None)
            }
            INTERIOR_TABLE_PAGE => {
                let child_pages = self.get_child_page_numbers(&page_data, page_num)?;
                
                for child_page in child_pages {
                    if let Some(cell) = self.search_table_for_row_id(child_page, target_row_id)? {
                        return Ok(Some(cell));
                    }
                }
                Ok(None)
            }
            _ => {
                bail!("Unsupported page type {} for table search", page_type);
            }
        }
    }

    /// Read a 4-byte page number from cell data at the given offset
    fn read_page_number_from_cell(&self, page_data: &[u8], offset: usize) -> Result<usize> {
        if offset + 4 > page_data.len() {
            bail!("Not enough data to read page number");
        }
        
        let page_number = u32::from_be_bytes([
            page_data[offset],
            page_data[offset + 1],
            page_data[offset + 2],
            page_data[offset + 3],
        ]) as usize;
        
        Ok(page_number)
    }

    /// Read the rightmost page pointer from an interior page header
    fn read_rightmost_page(&self, page_data: &[u8], dbheader_offset: usize) -> Result<usize> {
        if dbheader_offset + 12 > page_data.len() {
            bail!("Page data too small for interior page header");
        }
        
        let rightmost_page = u32::from_be_bytes([
            page_data[dbheader_offset + 8],
            page_data[dbheader_offset + 9],
            page_data[dbheader_offset + 10],
            page_data[dbheader_offset + 11],
        ]) as usize;
        
        Ok(rightmost_page)
    }
} 