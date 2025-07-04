use anyhow::{bail, Result};
use crate::varint::read_varint;

#[derive(Debug, Clone)]
pub enum RecordValue {
    Null,
    Int(i64),      // 1-6
    Float(f64),    // 7
    Zero,          // 8
    One,           // 9
    Blob(Vec<u8>), // N >=12 and even
    Text(String),  // N >=13 and odd
    Reserved(u64), // 10 or 11
}

impl RecordValue {
    pub fn from_type_and_data(col_type: u64, data: &[u8], offset: usize) -> Result<(Self, usize)> {
        match col_type {
            0 => Ok((RecordValue::Null, 0)),
            1 => Self::read_int(data, offset, 1), // 8-bit twos-complement
            2 => Self::read_int(data, offset, 2), // 16-bit twos-complement
            3 => Self::read_int(data, offset, 3), // 24-bit twos-complement
            4 => Self::read_int(data, offset, 4), // 32-bit twos-complement
            5 => Self::read_int(data, offset, 6), // 48-bit twos-complement
            6 => Self::read_int(data, offset, 8), // 64-bit twos-complement
            7 => {
                if offset + 8 > data.len() {
                    bail!("Not enough data for float");
                }
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&data[offset..offset + 8]);
                let value = f64::from_be_bytes(bytes);
                Ok((RecordValue::Float(value), 8))
            }
            8 => Ok((RecordValue::Zero, 0)),
            9 => Ok((RecordValue::One, 0)),
            10 | 11 => Ok((RecordValue::Reserved(col_type), 0)),
            n if n >= 12 && n % 2 == 0 => {
                let blob_len = ((n - 12) / 2) as usize;
                if offset + blob_len > data.len() {
                    bail!("Not enough data for blob");
                }
                let blob_data = data[offset..offset + blob_len].to_vec();
                Ok((RecordValue::Blob(blob_data), blob_len))
            }
            n if n >= 13 && n % 2 == 1 => {
                let text_len = ((n - 13) / 2) as usize;
                if offset + text_len > data.len() {
                    bail!("Not enough data for text");
                }
                let text = String::from_utf8_lossy(&data[offset..offset + text_len]);
                Ok((RecordValue::Text(text.to_string()), text_len))
            }
            _ => bail!("Invalid column type: {}", col_type),
        }
    }

    fn read_int(data: &[u8], offset: usize, size: usize) -> Result<(Self, usize)> {
        if offset + size > data.len() {
            bail!("Not enough data for integer of size {}", size);
        }
        
        let bytes = &data[offset..offset + size];
        let mut padded = [0; 8];

        padded[8 - size..].copy_from_slice(bytes);

        if !bytes.is_empty() && bytes[0] & 0x80 != 0 {
            for i in 0..(8 - size) {
                padded[i] = 0xFF;
            }
        }

        let value = i64::from_be_bytes(padded);
        Ok((RecordValue::Int(value), size))
    }
}

impl RecordValue {
    /// Format the record value for display
    pub fn to_display_string(&self) -> String {
        match self {
            RecordValue::Null => "NULL".to_string(),
            RecordValue::Int(i) => i.to_string(),
            RecordValue::Float(f) => f.to_string(),
            RecordValue::Zero => "0".to_string(),
            RecordValue::One => "1".to_string(),
            RecordValue::Text(s) => s.clone(),
            RecordValue::Blob(b) => format!("<BLOB {} bytes>", b.len()),
            RecordValue::Reserved(r) => format!("<RESERVED {}>", r),
        }
    }
}

#[derive(Debug)]
pub struct RecordHeader {
    pub size: u64,
    pub column_types: Vec<u64>,
}

impl RecordHeader {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut pos = 0;
        let (header_size, bytes_read) = read_varint(data, pos)?;
        pos += bytes_read;

        let mut column_types = Vec::new();

        while pos < header_size as usize {
            let (col_type, bytes_read) = read_varint(data, pos)?;
            pos += bytes_read;
            column_types.push(col_type);
        }

        Ok(RecordHeader {
            size: header_size,
            column_types,
        })
    }
}

#[derive(Debug)]
pub struct Record {
    pub header: RecordHeader,
    pub body: Vec<RecordValue>,
}

impl Record {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let header = RecordHeader::from_bytes(data)?;
        let mut body = Vec::new();

        let mut data_offset = header.size as usize;

        for &col_type in &header.column_types {
            let (value, bytes_read) = RecordValue::from_type_and_data(col_type, data, data_offset)?;
            body.push(value);
            data_offset += bytes_read;
        }

        Ok(Record { header, body })
    }

    /// Get the table name from this record if it represents a table entry
    pub fn get_table_name(&self) -> Option<&str> {
        if let Some(RecordValue::Text(t)) = self.body.get(0) {
            if t == "table" {
                if let Some(RecordValue::Text(tbl)) = self.body.get(2) {
                    return Some(tbl);
                }
            }
        }
        None
    }

    /// Get the SQL schema from this record if it represents a table entry
    pub fn get_sql_schema(&self) -> Option<&str> {
        if let Some(RecordValue::Text(sql)) = self.body.get(4) {
            Some(sql)
        } else {
            None
        }
    }

    /// Get the page number where this table's data is stored
    pub fn get_page_number(&self) -> Result<usize> {
        let page_num = self.body.get(3)
            .and_then(|v| match v {
                RecordValue::Int(num) => Some(*num as usize),
                _ => None,
            })
            .ok_or_else(|| anyhow::anyhow!("Invalid or missing page number in record"))?;
        Ok(page_num)
    }
} 