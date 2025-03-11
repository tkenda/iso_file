use thiserror::Error;

/// The result type for all methods that can return an error.
pub type Result<T> = std::result::Result<T, IsoFileError>;

#[derive(Error, Debug)]
pub enum IsoFileError {
    #[error("Invalid date.")]
    InvalidDate,
    #[error("Invalid time.")]
    InvalidTime,
    #[error("File not found.")]
    FileNotFound,
    #[error("Entry is current directory.")]
    EntryCurrentDirectory,
    #[error("Entry is parent directory.")]
    EntryParentDirectory,
    #[error("Entry is directory.")]
    EntryDirectory,
    #[error("Std. IO: {0}.")]
    StdIo(#[from] std::io::Error),
}
