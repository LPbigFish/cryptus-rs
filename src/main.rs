use std::{fs::{File, self}, io::{Write, self, BufRead}, time, error::Error};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey, base58};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use num::BigUint;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};
use scylla::*;


#[tokio::main]
async fn main() {
    download_database().await;

    let wallet = Wallet::new();

    let file_list : Vec<String> = fs::read_dir("database").unwrap().map(|res| res.unwrap().file_name().to_str().unwrap().to_string()).collect();
    //delete .txt from all Strings in file_list
    let file_names = file_list.iter().map(|s| s.replace(".txt", "")).collect::<Vec<_>>();
    let file_nums = file_names.iter().map(|s| BigUint::from_bytes_be(&base58::decode(s.as_str()).unwrap().as_slice())).collect::<Vec<_>>();

    //split_the_db();

    println!("{}", wallet.to_string());

    println!("{}", search_in_db(wallet.address, file_list, file_nums));
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

    split_the_db();
}

fn split_the_db() {
    println!("Splitting the database...");
    fs::create_dir("database").is_err().then(|| {
        fs::remove_dir_all("database").unwrap();
        fs::create_dir("database").unwrap();
    });

    let file = File::open("database.txt").unwrap();
    let reader = io::BufReader::new(file);
    let local_lines = reader.lines().filter(|line| line.as_ref().unwrap().len() < 35).collect::<Vec<_>>();

    for i in 0..100 {
        let u = &local_lines[(i * (local_lines.len() / 100))..((i + 1) * (local_lines.len() / 100))];
        let mut new_file = File::create(format!("database/{}.txt", u[0].as_deref().unwrap())).unwrap();

        for line in u.iter() {
            new_file.write_all(format!("{}\n", line.as_ref().unwrap()).as_bytes()).unwrap();
        }
    }
}

fn search_in_db(address: String, file_list: Vec<String>, file_nums: Vec<BigUint>) -> bool {
    let time = time::Instant::now();
    
    let address_num = BigUint::from_bytes_be(&base58::decode(&address.as_str()).unwrap().as_slice());
    
    for i in 1..file_nums.len() {
        if file_nums[i - 1] < address_num && file_nums[i] > address_num {
            let file = File::open(format!("database\\{}", file_list[i - 1])).unwrap();
            let reader = io::BufReader::new(file);
            for line in reader.lines() {
                if line.as_ref().unwrap() == &address {
                    println!("Time: {:?}", time.elapsed());
                    return true;
                }
            }
        }
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

async fn test() -> Result<(), Box<dyn Error>> {
    // Create a new Session which connects to node at 127.0.0.1:9042
    // (or SCYLLA_URI if specified)
    let uri = std::env::var("SCYLLA_URI")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new()
        .known_node(uri)
        .build()
        .await?;

    // Create an example keyspace and table
    session
        .query(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = \
            {'class' : 'SimpleStrategy', 'replication_factor' : 1}",
            &[],
        )
        .await?;

    session
        .query(
            "CREATE TABLE IF NOT EXISTS ks.extab (a int primary key)",
            &[],
        )
        .await?;

    // Insert a value into the table
    let to_insert: i32 = 12345;
    session
        .query("INSERT INTO ks.extab (a) VALUES(?)", (to_insert,))
        .await?;

    // Query rows from the table and print them
    if let Some(rows) = session.query("SELECT a FROM ks.extab", &[]).await?.rows {
        // Parse each row as a tuple containing single i32
        for row in rows.into_typed::<(i32,)>() {
            let read_row: (i32,) = row?;
            println!("Read a value from row: {}", read_row.0);
        }
    }

    Ok(())
}