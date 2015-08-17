#![feature(plugin)]
#![plugin(docopt_macros)]
#![feature(ip_addr)]
#![feature(test)]
#![feature(duration)]
#![feature(socket_timeout)]
#![allow(dead_code)]
#![feature(path_ext)]
#![feature(result_expect)]

extern crate rustc_serialize;
extern crate docopt;
extern crate toml;
extern crate msgpack;
extern crate byteorder;
#[macro_use] extern crate log;
extern crate env_logger;
extern crate rand;
extern crate time;
//extern crate nix;
extern crate rocksdb;

use std::net::TcpListener;
use std::thread;
use std::io::prelude::*;
use std::fs::File;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Receiver, Sender};
use std::net::{TcpStream, SocketAddr};
use std::sync::{Arc, Mutex};
use std::process;
use std::path::Path;
use std::fs;

use docopt::Docopt;
use toml::Value;
//use nix::sys::signal;

#[macro_use] mod util;
mod encd;
mod cluster;
mod internode;
mod node;
mod crc32;
mod db;
mod api;
mod dbclient;

use api::ApiService;
//use internode::MeState;
use internode::InternodeService;
//use cluster;




docopt!(Args derive Debug, "
Zufar

Usage:
  zufar serve <host> <port>
  zufar serve <configfile>
  zufar --version

Options:
  -h --help             Show this screen.
  --version             Show version.
", arg_port: Option<i32>);


fn main() {

    env_logger::init().unwrap();
    
    

    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    //println!("{:?}", args);

    if (&args).flag_version{
        println!("version 0.1.0");
        return;
    }

    let mut api_address = format!("{}:{}", args.arg_host, args.arg_port.unwrap_or(9123));
    let mut node_address:String = String::new();
    let mut seeds:Vec<String> = Vec::new();
    let mut data_dir:String = "data/node0".to_string();

    if (&args.arg_configfile).len() > 0 {
        match File::open(&args.arg_configfile) {
            Ok(mut f) => {
                let mut s = String::new();
                let _ = f.read_to_string(&mut s);

                //println!("cfg content: {}", s);

                let cfg = toml::Parser::new(&*s).parse().unwrap();
                //println!("cfg: {:?}", cfg);

                match cfg.get("zufar"){
                    Some(&Value::Table(ref section)) => {
                        match section.get("listen_address"){
                            Some(&Value::String(ref hnp)) => api_address = hnp.clone(),
                            _ => err!(2, "No `listen_address` in configuration.")
                        }
                        match section.get("node_address"){
                            Some(&Value::String(ref _node_address)) => {
                                println!("node address: {}", _node_address);
                                node_address = _node_address.clone();
                            },
                            _ => err!(5, "No `node_address` in configuration.")
                        }
                        match section.get("seeds"){
                            Some(&Value::Array(ref _seeds)) => {
                                println!("seeds: ");
                                for seed in _seeds {
                                    println!(" + {}", seed);
                                    match seed {
                                        &Value::String(ref seed) => seeds.push(seed.clone()),
                                        _ => ()
                                    }
                                    
                                }
                            },
                            _ => err!(5, "No `seeds` in configuration.")
                        }
                        match section.get("data_dir"){
                            Some(&Value::String(ref _data_dir)) => {
                                data_dir = _data_dir.clone()
                            },
                            _ => err!(2, "No `data_dir` in configuration.")
                        }

                    },
                    _ => err!(3, "No [zufar] section")
                }

            },
            Err(e) => {
                err!(4, "error: {}", e);
            }
        }
    }
    
    // // check for existing data dir
    // let path = Path::new(&data_dir);
    // if !path.exists(){
    //     info!("data dir `{}` not exists, create first.", &data_dir);
    //     fs::create_dir_all(path).unwrap_or_else(|why| {
    //         panic!("{}", why);
    //     });
    // }
    
    let info = Arc::new(Mutex::new(cluster::Info::new(&node_address, 
        &api_address, seeds,
        &data_dir)));

    let inode = InternodeService::new(info.clone());

    let api_service = ApiService::new(inode.clone(), info.clone());

    InternodeService::start(inode);
    
    // extern fn handle_sigint(_:i32){
    //     println!("Interrupted!");
    //     api_service.flush();
    //     process::exit(1);
    // }
    // 
    // let sig_action = signal::SigAction::new(handle_sigint,
    //                                           signal::SockFlag::empty(),
    //                                           signal::SigSet::empty());
    // unsafe {
    //     signal::sigaction(signal::SIGINT, &sig_action);
    // }


    if args.cmd_serve {
        api_service.start(&api_address);
    }

}








