use std::{fs::{File, self}, io::{Write, self, BufRead}, time, thread, collections::HashMap};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey, base58};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use num::BigUint;
use rayon::prelude::{ParallelBridge, ParallelIterator, IntoParallelIterator};
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};

//https://www.scylladb.com/


#[tokio::main]
async fn main() {
    //download_database().await;

    let files = fs::read_dir("database").unwrap().map(|res| res.unwrap().path()).collect::<Vec<_>>();
    let files = files.iter().map(|file| File::open(file).unwrap()).collect::<Vec<_>>();
    let names = fs::read_dir("database").unwrap().map(|res| res.unwrap().file_name().into_string().unwrap()).collect::<Vec<_>>();
    let num_of_lines = files.iter().map(|file| io::BufReader::new(file).lines().count()).collect::<Vec<_>>();

    let wallet = Wallet::new();

    println!("{}", wallet.to_string());

    println!("{}", search_in_db(wallet.address, &files, names, num_of_lines).await);
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
    let lines = reader.lines().into_iter().collect::<Vec<_>>();
    let lines = lines.into_par_iter().filter(|line| line.as_ref().unwrap().len() < 35).collect::<Vec<_>>();

    let count = lines.len();
    let address_per_file = count / 100;

    println!("Inserting data into the Database...");

    fs::remove_dir_all("database").is_err().then(|| {
        fs::create_dir("database").unwrap();
    });

    for i in 0..100 {
        let name = lines[i * address_per_file].as_ref().unwrap();

        let mut file = File::create(format!("database\\{}", name)).expect("Failed to create database file");

        for line in lines[i * address_per_file..(i + 1) * address_per_file].iter() {
            file.write_all(format!("{}\n", line.as_ref().unwrap()).as_bytes()).expect("Failed to write to database file");
        }
    }

    println!("Data were inserted into the Database");
}

async fn search_in_db(address: String, files: &Vec<File>, names: Vec<String>, num_of_lines: Vec<usize>) -> bool {
    let time = time::Instant::now();

    let address_num = BigUint::from_bytes_be(&base58::decode(address.as_str()).unwrap());
    println!("Time: {:?}", time.elapsed());
    for i in 0..(files.len() - 1) {
        let file = &files[i];
        let num_of_lines = num_of_lines[i];
        let name = BigUint::from_bytes_be(&base58::decode(names[i].as_str()).unwrap());
        let next_name = BigUint::from_bytes_be(&base58::decode(names[i+1].as_str()).unwrap());
        if address_num >= name && address_num < next_name {
            

            println!("Time: {:?}", time.elapsed());
            if result.len() > 0 {
                println!("Time: {:?}", time.elapsed());
                return true;
            } else {
                println!("Time: {:?}", time.elapsed());
                return false;
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