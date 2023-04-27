use std::{fs::{File, self}, io::{Write, self, BufRead}, time};

use bitcoin::{secp256k1::{Secp256k1, rand}, Address, Network, PublicKey, PrivateKey, base58};
use flate2::read::GzDecoder;
use futures_util::StreamExt;
use num::BigUint;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use reqwest::{self, Client};
use humansize::{FileSize, file_size_opts};


#[tokio::main]
async fn main() {
    let wallet = Wallet::new();

    let file_list : Vec<String> = fs::read_dir("database").unwrap().map(|res| res.unwrap().file_name().to_str().unwrap().to_string()).collect();
    //delete .txt from all Strings in file_list
    let file_names = file_list.iter().map(|s| s.replace(".txt", "")).collect::<Vec<_>>();
    let file_nums = file_names.iter().map(|s| BigUint::from_bytes_be(&base58::decode(s.as_str()).unwrap().as_slice())).collect::<Vec<_>>();

    //download_database().await;

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
    let local_lines = local_lines.par_iter().map(|line| BigUint::from_bytes_be(&base58::decode(line.as_ref().unwrap()).unwrap().as_slice())).collect::<Vec<_>>();

    for i in 0..100 {
        let u = &local_lines[(i * (local_lines.len() / 100))..((i + 1) * (local_lines.len() / 100))];
        let j = &u[0].to_str_radix(10);
        let mut new_file = File::create(format!("database/{}.txt", j)).unwrap();

        for line in u.iter() {
            let line = line.to_str_radix(10);
            new_file.write_all(format!("{}\n", line.to_owned()).as_bytes()).unwrap();
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