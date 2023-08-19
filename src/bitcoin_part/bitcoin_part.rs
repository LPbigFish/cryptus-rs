use std::{sync::{Arc, Mutex}, collections::HashMap, fs::{File, self}, io::{Write, self}, str::FromStr};

use bitcoin::{Address, Network, secp256k1::{Secp256k1, rand}, PublicKey, PrivateKey};
use flate2::write::GzDecoder;
use futures_util::StreamExt;
use humansize::{file_size_opts, FileSize};
use memmap::Mmap;
use num::BigUint;
use rayon::prelude::*;
use reqwest::Client;

fn finder(database: Arc<Mutex<HashMap<String, bool>>>, i: usize, cycle: &mut Arc<Mutex<BigUint>>) -> Option<String> {
    {
        println!("thread started: {}", i);
        let mut count: u32 = 0;
        let time = std::time::Instant::now();
        let mut prev_time = time.elapsed();
        loop {
            let wallet = Wallet::new();
            let result = database.lock().unwrap().get(&wallet.address_p2pkh).is_some().then(|| {
                true
            }).unwrap_or(false);
            if result {
                println!("Found a match: \n {}", wallet.to_string());
                let mut file = File::create(format!("{}", &wallet.address_p2pkh)).unwrap();
                file.write_all(wallet.to_string().as_bytes()).unwrap();
                return Some(wallet.address_p2pkh);
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

fn prepair_with_balance() {
    let file = File::open("database.tsv").expect("Failed to open txt file");
    let mapping = unsafe { Mmap::map(&file).expect("Failed to map file") };
    let map: Arc<Mutex<HashMap<String, Vec<(String, u64)>>>> = Arc::new(Mutex::new(HashMap::new()));

    let lines: Vec<&str> = mapping.split(|&byte| byte == b'\n').into_iter().par_bridge().map(|line| std::str::from_utf8(line).unwrap()).collect();

    lines.par_iter().enumerate().for_each(|(index, line)| {
        let line: Vec<&str> = line.split('\t').collect();
        let address = line[0];
        let balance = line[1].parse::<u64>().unwrap_or(0);

        Address::from_str(address).is_ok().then(|| {
            let address = Address::from_str(address).unwrap().require_network(Network::Bitcoin).unwrap();
            address.address_type().is_some().then( || {
                let address_type = address.address_type().unwrap().to_string();
                map.lock().unwrap().entry(address_type).or_insert(Vec::new()).push((address.to_string(), balance));
            });
        }).unwrap_or(());

        if index % 100000 == 0 {
            println!("{} / {}", index, lines.len());
        }
    });

    for (key, value) in map.lock().unwrap().iter_mut() {
        value.sort_by(|a, b| b.1.cmp(&a.1));
        let mut file = File::create(format!("{}.tsv", key)).expect("Failed to create txt file");
        for address in value {
            file.write_all(format!("{}\t{}\n", address.0, address.1).as_bytes()).expect("Failed to write to txt file");
        }
    }
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
    println!("Downloaded database. Size: {} Bytes", res.content_length().unwrap().file_size(file_size_opts::BINARY).unwrap());
    
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

    println!("Data were inserted into the Database");

    sort_gpt();
}

fn sort_gpt() {
    let file = File::open("database.txt").expect("Failed to open txt file");
    let mapping = unsafe { Mmap::map(&file).expect("Failed to map file") };

    let lines: Vec<&str> = mapping.split(|&byte| byte == b'\n').into_iter().par_bridge().map(|line| std::str::from_utf8(line).unwrap()).collect();

    let map: Arc<Mutex<HashMap<String, Vec<&str>>>> = Arc::new(Mutex::new(HashMap::new()));

    lines.par_iter().enumerate().for_each(|(index, line)| {
        Address::from_str(line).is_ok().then(|| {
            let address = Address::from_str(line).unwrap().require_network(Network::Bitcoin).unwrap();
            address.address_type().is_some().then( || {
                let address_type = address.address_type().unwrap().to_string();
                map.lock().unwrap().entry(address_type).or_insert(Vec::new()).push(line);
            });
        }).unwrap_or(());
        if index % 100000 == 0 {
            println!("{} / {}", index, lines.len());
        }
    });

    for (key, value) in map.lock().unwrap().iter() {
        let mut file = File::create(format!("{}.txt", key)).expect("Failed to create txt file");
        for address in value {
            file.write_all(format!("{}\n", address).as_bytes()).expect("Failed to write to txt file");
        }
    }
} 

async fn download_database_with_balance() {
    File::open("database.txt").is_ok().then(|| {
        fs::remove_file("database.txt").expect("Failed to remove old database file");
    });
    File::open("database.txt.gz").is_ok().then(|| {
        fs::remove_file("database.txt.gz").expect("Failed to remove old database file");
    });
    println!("Downloading database with balances...");
    let res = Client::new()
        .get("http://addresses.loyce.club/blockchair_bitcoin_addresses_and_balance_LATEST.tsv.gz")
        .send()
        .await
        .expect("Failed to download database, Network error");
    println!("Downloaded database. Size: {:?} Bytes", res.content_length().unwrap().file_size(file_size_opts::BINARY).unwrap());
    
    File::open("database.tsv.gz").is_ok().then(|| {
        fs::remove_file("database.tsv.gz").expect("Failed to remove old database file");
    });

    let mut file = File::create("database.tsv.gz").expect("Failed to create database file");

    let mut stream = res.bytes_stream();

    println!("Writing database...");

    while let Some(item) = stream.next().await {
        let chunk = item.expect("Failed to download database, Network error");
        file.write_all(&chunk).expect("Failed to write to database file");
    }
    println!("Database is downloaded and written to a file");

    
    let file = File::open("database.tsv.gz").expect("Failed to open database file");
    let mut decoder = GzDecoder::new(file);
    io::copy(&mut decoder, &mut File::create("database.tsv").expect("Failed to create database file")).expect("Failed to extract database file");
    fs::remove_file("database.tsv.gz").expect("Failed to remove old database file");
    println!("Database is extracted");

    println!("Data were inserted into the Database");

    prepair_with_balance();
}

#[derive(Debug, Clone)]
struct Wallet {
    private_key: String,
    address_p2pkh: String,
    address_p2wpkh: String,
    address_p2shwpkh: String
}

impl Wallet {
    fn new() -> Self {
        let secp = Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut rand::thread_rng());
        let address = Address::p2pkh(&PublicKey::new(public_key), Network::Bitcoin);
        let address_1 = Address::p2wpkh(&PublicKey::new(public_key), Network::Bitcoin).unwrap();
        let address_2 = Address::p2shwpkh(&PublicKey::new(public_key), Network::Bitcoin).unwrap();
        
        Wallet {
            private_key: PrivateKey::new_uncompressed(secret_key, Network::Bitcoin).to_wif(),
            address_p2pkh: address.to_string(),
            address_p2wpkh: address_1.to_string(),
            address_p2shwpkh: address_2.to_string()
        }
    }
}

impl ToString for Wallet {
    fn to_string(&self) -> String {
        //convert to json
        format!("{{\n\t\"private_key\": \"{}\",\n\t\"address_p2pkh\": \"{}\",\n\t\"address_p2wpkh\": \"{}\",\n\t\"address_p2shwpkh\": \"{}\"\n}}", self.private_key, self.address_p2pkh, self.address_p2wpkh, self.address_p2shwpkh)
    }
}