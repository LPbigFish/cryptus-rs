use std::{fs::{File, self}, io::{Write, self, BufRead}, time::Instant};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey, base58};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use num::BigUint;
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};


#[tokio::main]
async fn main() {
    let wallet = Wallet::new();

    //download_database().await;

    println!("{}", wallet.to_string());

    search_in_db(wallet.address);
}

async fn download_database() {
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

}

fn search_in_db(address: String) -> bool {
    let current_time = Instant::now();
    let searched_address = BigUint::from_bytes_be(&base58::decode(address.to_string().as_str()).unwrap());
    let file = File::open("database.txt").unwrap();
    let reader = io::BufReader::new(file);
    for line in reader.lines() {
        let line = line.unwrap();
        let line = BigUint::from_bytes_be(&base58::decode(line.as_str()).unwrap());
        if line == searched_address {
            println!("Found address in database");
            return true;
        } else if line > searched_address {
            println!("Address not found in database");
            println!("Time taken: {}ms", current_time.elapsed().as_millis());
            return false;
        }
    }
    println!("Address not found in database");
    println!("Time taken: {}ms", current_time.elapsed().as_millis());
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
        println!("{:?}", base58::decode(address.to_string().as_str()).unwrap());
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