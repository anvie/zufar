#![feature(plugin)]
#![plugin(docopt_macros)]

extern crate rustc_serialize;
extern crate docopt;
extern crate toml;
extern crate msgpack;
extern crate byteorder;


use std::net::TcpListener;
use std::thread;
use std::io::prelude::*;
use std::fs::File;

use docopt::Docopt;

use toml::Value;

#[macro_use] mod util;
mod encd;
mod internode;




docopt!(Args derive Debug, "
Zufar

Usage:
  zufar serve <host> <port>
  zufar serve --cfg <CONFIGFILE>
  zufar --version

Options:
  -h --help             Show this screen.
  --version             Show version.
  --cfg CONFIGFILE      file config to init from.
", arg_port: Option<i32>);


fn main() {

    let args: Args = Args::docopt().decode().unwrap_or_else(|e| e.exit());
    println!("{:?}", args);

    if (&args).flag_version{
        println!("version 0.1.0");
        return;
    }

    let mut host_n_port = format!("{}:{}", args.arg_host, args.arg_port.unwrap_or(9123));
    let mut node_address:String = String::new();

    if (&args.flag_cfg).len() > 0 {
        match File::open(&args.flag_cfg) {
            Ok(mut f) => {
                let mut s = String::new();
                let _ = f.read_to_string(&mut s);

                //println!("cfg content: {}", s);

                let cfg = toml::Parser::new(&*s).parse().unwrap();
                println!("cfg: {:?}", cfg);

                match cfg.get("zufar"){
                    Some(&Value::Table(ref section)) => {
                        match section.get("listen_address"){
                            Some(&Value::String(ref hnp)) => host_n_port = hnp.clone(),
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
                            Some(&Value::Array(ref seeds)) => {
                                println!("seeds: ");
                                for seed in seeds {
                                    println!(" + {}", seed);
                                }
                            },
                            _ => err!(5, "No `seeds` in configuration.")
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

    if node_address.len() > 0 {
        let inode = internode::InternodeService::new();
        inode.setup_internode_communicator(&node_address);
    }

    if args.cmd_serve {
        serve(&host_n_port);
    }
}

fn serve(host_n_port:&String){
    let listener = TcpListener::bind(&**host_n_port).unwrap();
    println!("client comm listening at {} ...", host_n_port);
    for stream in listener.incoming() {
        thread::spawn(move || {
            let mut stream = stream.unwrap();
            stream.write(b"Hello World\r\n").unwrap();
        });
    }
}
