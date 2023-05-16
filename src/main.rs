use std::{fs::{File, self}, io::{Write, self, BufRead}, collections::HashMap, sync::{Arc, Mutex}, thread};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use num::BigUint;
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};

//https://www.scylladb.com/


fn main() {

    let database;

    if File::open("database.txt").is_err() {
        println!("Database is not downloaded");
        database = tokio::runtime::Runtime::new().unwrap().block_on(download_database());
    } else {
        println!("Database is already downloaded");
        let file = File::open("database.txt").expect("Failed to open txt file");
        let reader = io::BufReader::new(file);

        let mut map: HashMap<String, bool> = HashMap::new();
        println!("Reading database...");
        for line in reader.lines() {
            map.entry(line.unwrap()).or_insert(true);
        }
        map.shrink_to_fit();
        database = map;
    }
    println!("Database is read");

    //.is_ok().then(|| {
    //    let file = File::open("database.txt").expect("Failed to open txt file");
    //    let reader = io::BufReader::new(file);
    //    let mut map: HashMap<String, bool> = HashMap::new();
    //    for line in reader.lines() {
    //        map.entry(line.unwrap()).or_insert(true);
    //    }
    //    println!("Data were inserted into the Database");
    //    map
    //});

    let database = Arc::new(Mutex::new(database));
    let cycle = Arc::new(Mutex::new(BigUint::from(0u32)));
    let mut hnadles = Vec::new();
    for i in 0..num_cpus::get() {
        let database = Arc::clone(&database);
        let mut cycle = Arc::clone(&cycle);
        let handle = thread::spawn(move || {
            finder(database, i, &mut cycle);
        });
        hnadles.push(handle);
    }
    for handle in hnadles {
        handle.join().unwrap();
    }
}

fn finder(database: Arc<Mutex<HashMap<String, bool>>>, i: usize, cycle: &mut Arc<Mutex<BigUint>>) -> Option<String> {
    {
        println!("thread started: {}", i);
        let mut count: u32 = 0;
        let time = std::time::Instant::now();
        let mut prev_time = time.elapsed();
        loop {
            let wallet = Wallet::new();
            let result = database.lock().unwrap().get(&wallet.address).is_some().then(|| {
                true
            }).unwrap_or(false);
            if result {
                println!("Found a match: \n {}", wallet.to_string());
                let mut file = File::create(format!("{}", &wallet.address)).unwrap();
                file.write_all(wallet.to_string().as_bytes()).unwrap();
                return Some(wallet.address);
            }
            
            *cycle.lock().unwrap() += BigUint::from(1u8);

            count += 1;
            if count == 100000 {
                println!("cycle: {} | time: {:?}", cycle.lock().unwrap(), time.elapsed() - prev_time);
                count = 0;
                prev_time = time.elapsed();
            }
        }
    }
}

async fn download_database() -> HashMap<String, bool> {
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

    let mut map: HashMap<String, bool> = HashMap::new();

    for line in reader.lines() {
        map.entry(line.unwrap()).or_insert(true);
    }

    println!("Data were inserted into the Database");

    map
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