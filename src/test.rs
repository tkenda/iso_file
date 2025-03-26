use chrono::Utc;
use tokio::fs::File;

use crate::{IsoFileReader, IsoFileWriter, core::IsoHeader};

#[tokio::test]
async fn main() {
    let mut buffer1 = File::create("image2.iso").await.unwrap();

    let mut header = IsoHeader::default();
    header.set_volumen_id("DICOM");

    let mut writer = IsoFileWriter::new(&mut buffer1, header)
        .await
        .unwrap();

    writer.append_file("/hello.txt", b"Hello, World!", Utc::now());

    writer.append_file("/one/hello2.txt", b"Hello, World!", Utc::now());
    writer.append_file("/one/hello3.txt", b"Hello, World!", Utc::now());

    writer.append_file("/one/three/hello8.txt", b"Hello, World!", Utc::now());
    writer.append_file("/one/three/hello9.txt", b"Hello, World!", Utc::now());

    writer.append_file("/two/hello4.txt", b"Hello, World!", Utc::now());
    writer.append_file("/two/hellowaka.txt", b"Hello, Worldx!", Utc::now());

    writer.close().await.unwrap();

    let mut buffer2 = File::open("image2.iso").await.unwrap();

    let reader = IsoFileReader::read(&mut buffer2).await.unwrap();
}
