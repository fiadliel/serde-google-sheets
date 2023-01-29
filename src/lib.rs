mod de;
mod error;

pub use de::{from_grid_data, from_spreadsheet, Deserializer};
pub use error::{Error, Result};
