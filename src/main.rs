use std::{fs::{File, self}, io::{Write, self, BufRead}, time};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};
use scylla::*;
use uuid::Uuid;

//https://www.scylladb.com/


#[tokio::main]
async fn main() {
    download_database().await;

    let wallet = Wallet::new();

    println!("{}", wallet.to_string());

    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await
        .expect("Failed to establish connection to the Database");

    println!("{}", search_in_db(wallet.address, &session).await);
}

async fn download_database() {
    File::open("database.txt").is_ok().then(|| {
        fs::remove_file("database.txt").expect("Failed to remove old database file");
    });
    File::open("database.txt.gz").is_ok().then(|| {
        fs::remove_file("database.txt.gz").expect("Failed to remove old database file");
    });
    fs::remove_dir_all("database").is_err().then(|| {
        fs::create_dir("database").unwrap();
    });
    println!("Downloading database...");
    let res = Client::new()
        .get("http://addresses.loyce.club/Bitcoin_addresses_LATEST.txt.gz")
        .send()
        .await
        .expect("Failed to download database, Network error");
    println!("Downloaded database. Size: {:?} Bytes", res.content_length().unwrap().file_size(file_size_opts::BINARY).unwrap());
    
    File::open("database.txt.gz").is_ok().then(|| {
        fs::remove_file("database.txt.gz").expect("Failed to remove old database file");
    });

    let mut file = File::create("database.txt.gz").expect("Failed to create database file");

    let mut stream = res.bytes_stream();

    println!("Writing database...");

    while let Some(item) = stream.next().await {
        let chunk = item.expect("Failed to download database, Network error");
        file.write_all(&chunk).expect("Failed to write to database file");
    }
    println!("Database is downloaded and written to a file");

    
    let file = File::open("database.txt.gz").expect("Failed to open database file");
    let mut decoder = GzDecoder::new(file);
    io::copy(&mut decoder, &mut File::create("database.txt").expect("Failed to create database file")).expect("Failed to extract database file");
    fs::remove_file("database.txt.gz").expect("Failed to remove old database file");
    println!("Database is extracted");

    let file = File::open("database.txt").expect("Failed to open txt file");
    let reader = io::BufReader::new(file);

    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await
        .expect("Failed to establish connection to the Database");

    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};",
            &[],
        )
        .await
        .expect("Failed to create Table");

    session
        .query(
            "DROP TABLE IF EXISTS my_keyspace.bit;",
            &[],
        )
        .await
        .expect("Failed to create Table");


    session
        .query(
            "CREATE TABLE IF NOT EXISTS my_keyspace.bit (uuid uuid PRIMARY KEY, address text)",
            &[],
        )
        .await
        .expect("Failed to create Table");

    for line in reader.lines() {
        session.query(
            "INSERT INTO my_keyspace.bit (uuid, address) VALUES (?, ?);", 
            (Uuid::new_v4(), line.unwrap())
        )
        .await
        .expect("Failed to insert a value");
    }
    println!("Data were inserted into the Database");
}

async fn search_in_db(address: String, session: &Session) -> bool {
    let time = time::Instant::now();
    
    let result = session.query(
        "SELECT * FROM my_keyspace.bit WHERE address = ?;", 
        (address,)
    ).await
    .expect("Error reading database");
    
    if result.rows_num().unwrap() > 0 {
        println!("Time: {:?}", time.elapsed());
        return true;
    }

    println!("Time: {:?}", time.elapsed());
    false
}

#[derive(Debug, Clone)]
struct Wallet {
    private_key: String,
    public_key: String,
    address: String,
}

impl Wallet {
    fn new() -> Self {
        let secp = Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let address = Address::p2pkh(&PublicKey::new(public_key), Network::Bitcoin);
        Wallet {
            private_key: PrivateKey::new(secret_key, Network::Bitcoin).to_string(),
            public_key: PublicKey::new(public_key).to_string(),
            address: address.to_string(),
        }
    }
}

impl ToString for Wallet {
    fn to_string(&self) -> String {
        //convert to json
        format!("{{\n\t\"private_key\": \"{}\",\n\t\"public_key\": \"{}\",\n\t\"address\": \"{}\"\n}}", self.private_key, self.public_key, self.address)
    }
}