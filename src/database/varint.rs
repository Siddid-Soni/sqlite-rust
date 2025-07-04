use anyhow::{bail, Result};

/// Read a variable-length integer from the given data at the specified offset
/// Returns (value, bytes_read)
pub fn read_varint(data: &[u8], offset: usize) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut bytes_read = 0;

    // Varints are at most 9 bytes
    for i in 0..9 {
        if offset + bytes_read >= data.len() {
            bail!("Not enough data to read varint at offset {}", offset);
        }
        let byte = data[offset + bytes_read];
        bytes_read += 1;
        
        if i == 8 {
            // 9th byte: take all 8 bits
            value = (value << 8) | byte as u64;
            break;
        } else {
            // High-order group comes first → shift before OR'ing
            value = (value << 7) | (byte & 0x7F) as u64;
            // msb 0 → this was the last byte
            if (byte & 0x80) == 0 {
                break;
            }
        }
    }
    
    Ok((value, bytes_read))
} 