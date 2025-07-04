use anyhow::Result;
use super::varint::read_varint;
use super::record::Record;

#[derive(Debug)]
pub struct Cell {
    pub record_size: u64,
    pub row_id: u64,
    pub record: Record,
}

impl Cell {
    pub fn from_bytes(data: &[u8], offset: usize) -> Result<Self> {
        let mut pos = offset;
        let (record_size, bytes_read) = read_varint(data, pos)?;
        pos += bytes_read;
        let (row_id, bytes_read) = read_varint(data, pos)?;
        pos += bytes_read;

        let record_data = &data[pos..pos + record_size as usize];
        let record = Record::from_bytes(record_data)?;

        Ok(Cell {
            record_size,
            row_id,
            record,
        })
    }
} 