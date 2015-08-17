

#[derive(Debug, Clone)]
pub struct RoutingTable {
    guid: u32,
    node_address: String,
    api_address: String,
//    port: u16,
//    socket_address: String
}

// impl Clone for RoutingTable {
//     fn clone(&self) -> RoutingTable {
//         RoutingTable {
//             guid: self.guid,
//             node_address: self.node_address.clone(),
//             api_address: self.api_address.clone()
//         }
//     }
// }

impl RoutingTable {
    pub fn new(guid: u32, node_address: String, api_address: String) -> RoutingTable {
        RoutingTable {
            guid: guid,
            node_address: node_address,
            api_address: api_address,
//            port: port,
//            socket_address: socket_address
        }
    }

    pub fn guid(&self) -> u32 {
        self.guid
    }

    pub fn node_address(&self) -> &String {
        &self.node_address
    }
    
    pub fn api_address(&self) -> &String {
        &self.api_address
    }
}

pub struct Info {
    pub my_guid: u32,
    pub my_node_address: String,
    pub my_api_address: String,
    pub routing_tables: Vec<RoutingTable>,
    pub seeds:Vec<String>,
    pub data_dir:String
}

impl Info {
    pub fn new(node_address:&String, api_address:&String, seeds:Vec<String>, data_dir:&String) -> Info {
        Info {
            my_guid: 0u32,
            my_node_address: node_address.clone(),
            my_api_address: api_address.clone(),
            routing_tables: Vec::new(),
            seeds: seeds,
            data_dir: data_dir.clone()
        }
    }
    
    // pub fn my_node_address(&self) -> &String {
    //     &self.my_node_address
    // }
    // 
    // pub fn my_api_address(&self) -> &String {
    //     &self.my_api_address
    // }
}


