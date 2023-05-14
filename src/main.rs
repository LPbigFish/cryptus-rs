use std::{fs::{File, self}, io::{Write, self, BufRead}, time, collections::HashMap};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};

//https://www.scylladb.com/


#[tokio::main]
async fn main() {
    let database = download_database().await;

    loop {
        let wallet = Wallet::new();

        print!("{}", wallet.address);
        let result = search_in_db(&wallet.address, &database).await;
        println!(" - {:?}", &result);

        if result {
            let mut file = File::create("found.txt").expect("Failed to create found file");
            file.write_all(wallet.to_string().as_bytes()).expect("Failed to write to found file");
            break;
        }
    }
}

async fn download_database() -> HashMap<String, u8> {
    File::open("database.txt").is_ok().then(|| {
        fs::remove_file("database.txt").expect("Failed to remove old database file");
    });
    File::open("database.txt.gz").is_ok().then(|| {
        fs::remove_file("database.txt.gz").expect("Failed to remove old database file");
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

    let mut map: HashMap<String, u8> = HashMap::new();

    let mut i: u8 = 0;

    for line in reader.lines() {
        map.entry(line.unwrap()).or_insert(i);
        i += 1;
        if i % 255 == 0 {
            i = 0;
        }
    }

    println!("Data were inserted into the Database");

    map
}

async fn search_in_db(address: &String, map: &HashMap<String, u8>) -> bool {
    let time = time::Instant::now();

    let result = map.get(address).is_some().then(|| {
        true
    }).unwrap_or(false);

    return result;
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