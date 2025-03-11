use tokio::fs::File;

use crate::IsoFileReader;

#[tokio::test]
async fn main() {
    let mut file = File::open("image.iso").await.unwrap();
    let mut reader = IsoFileReader::read(&mut file).await.unwrap();

    panic!("{:?}", reader.entries());

    let value = reader.read_file("/ONE/HELLO2.TXT").await.unwrap();

    panic!("{:?}", String::from_utf8_lossy(&value));
}
