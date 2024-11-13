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
