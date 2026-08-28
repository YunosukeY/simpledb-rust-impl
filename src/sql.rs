use crate::util::Result;

#[derive(Clone)]
pub enum ColumnType {
    Integer,
    Double,
    VarBit,
    VarChar,
    Boolean,
    Date,
    Time,
    DateTime,
    Json,
}

impl TryFrom<i32> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Integer),
            1 => Ok(Self::Double),
            2 => Ok(Self::VarBit),
            3 => Ok(Self::VarChar),
            4 => Ok(Self::Boolean),
            5 => Ok(Self::Date),
            6 => Ok(Self::Time),
            7 => Ok(Self::DateTime),
            8 => Ok(Self::Json),
            _ => Err(format!("invalid column type: {}", value).into()),
        }
    }
}
