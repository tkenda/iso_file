use std::{io::Cursor, ptr::read};

use chrono::Utc;
use tokio::fs::File;

use crate::{IsoFileReader, IsoFileWriter, core::IsoHeader};

#[tokio::test]
async fn main() {
    let mut buffer = Cursor::new(Vec::new());

    let mut writer = IsoFileWriter::new(&mut buffer, IsoHeader::default())
        .await
        .unwrap();

    writer.append_file("/folder1/test3", b"file3", Utc::now());
    writer.append_file("/folder2/test5", b"file5", Utc::now());
    writer.append_file("/test1", b"file1", Utc::now());
    writer.append_file("/folder1/test4", b"file4", Utc::now());
    writer.append_file("/test2", b"file2", Utc::now());
    writer.append_file("/folder2/test6", b"file6", Utc::now());
    writer.append_file("/bibux", b"file2", Utc::now());
    writer.append_file("/folder/folder/folder", b"file2", Utc::now());
    writer.append_file("/bibux", b"file2", Utc::now());
    writer.append_file("/folder8/meme", b"file2", Utc::now());
    writer.append_file("/folder2/folder3/test6", b"file6", Utc::now());
    writer.append_file("/folder2/folder0/test7", b"file6", Utc::now());


    writer.close().await.unwrap();

    /*
    let mut file = File::open("image.iso").await.unwrap();
    let mut reader = IsoFileReader::read(&mut file).await.unwrap();

    for entry in reader.entries().inner() {
        println!("{:?} {:?} => {:?}", entry.1.record().location(None), entry.1.file_id(), entry.0)
    }


    let value = reader.read_file("/ONE/HELLO2.TXT").await.unwrap();
    panic!("{:?} = {:?}", value, String::from_utf8_lossy(&value));
    */
}
