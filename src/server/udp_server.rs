use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::net::{IpAddr, UdpSocket};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};

use crate::server::client::ClientSignalingInfo;
use crate::server::log::LogManager;

pub struct UdpServer {
    host: String,
    log_manager: Arc<Mutex<LogManager>>,
    running: Arc<Mutex<bool>>,
    signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
}

impl UdpServer {
    fn log(log_manager: &Arc<Mutex<LogManager>>, s: &str) {
        log_manager.lock().write(&format!("UdpServer: {}", s));
    }

    pub fn new(s_host: &str, log_manager: Arc<Mutex<LogManager>>, signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>) -> UdpServer {
        UdpServer {
            host: String::from(s_host),
            log_manager,
            running: Arc::new(Mutex::new(false)),
            signaling_infos,
        }
    }

    pub fn start(&self) -> bool {
        let bind_addr = self.host.clone() + ":3657";
        let server_sock = UdpSocket::bind(&bind_addr);
        if let Err(e) = server_sock {
            UdpServer::log(&self.log_manager, &format!("Error binding udp to <{}>: {}", &bind_addr, e));
            return false;
        }

        let socket = server_sock.unwrap();
        if let Err(e) = socket.set_read_timeout(Some(Duration::from_millis(1))) {
            UdpServer::log(&self.log_manager, &format!("Error setting timeout to 1ms: {}", e));
            return false;
        }

        let log_manager = self.log_manager.clone();
        let running = self.running.clone();
        let signaling_infos = self.signaling_infos.clone();

        *self.running.lock() = true;

        thread::spawn(|| {
            UdpServer::server_proc(log_manager, socket, running, signaling_infos);
        });

        UdpServer::log(&self.log_manager, &format!("Now waiting for packets on <{}:3657>", &self.host));

        true
    }

    pub fn stop(&self) {
        *self.running.lock() = false;
    }

    fn server_proc(log: Arc<Mutex<LogManager>>, sock: UdpSocket, running: Arc<Mutex<bool>>, signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>) {
        let mut recv_buf = [0; 65535];
        let mut send_buf = [0; 65535];

        loop {
            if *running.lock() == false {
                break;
            }

            let res = sock.recv_from(&mut recv_buf);

            if let Err(e) = res {
                let err_kind = e.kind();
                if err_kind == io::ErrorKind::WouldBlock || err_kind == io::ErrorKind::TimedOut {
                    continue;
                } else {
                    UdpServer::log(&log, &format!("Error recv_from: {}", e));
                    break;
                }
            }

            // Parse packet
            let (amt, src) = res.unwrap();

            if amt != 9 || recv_buf[0] != 1 {
                UdpServer::log(&log, &format!("Received invalid packet from {}", src));
                continue;
            }

            let user_id = i64::from_le_bytes((&recv_buf[1..9]).try_into().unwrap());

            let ip_addr;
            match src.ip() {
                IpAddr::V4(ip) => {
                    ip_addr = ip.octets();
                }
                IpAddr::V6(_) => {
                    UdpServer::log(&log, &format!("Received packet from IPv6 IP"));
                    continue;
                }
            }
            let ip_port = src.port();

            let mut need_update = false;
            // Get a read lock to check if an udpate is needed
            {
                let si = signaling_infos.read();
                let user_si = si.get(&user_id);

                match user_si {
                    None => continue,
                    Some(user_si) => {
                        if user_si.port_p2p != ip_port || user_si.addr_p2p != ip_addr {
                            need_update = true;
                        }
                    }
                }
            }

            if need_update {
                let mut si = signaling_infos.write();
                let user_si = si.get_mut(&user_id);

                if let None = user_si {
                    continue;
                }

                let user_si = user_si.unwrap();
                user_si.port_p2p = ip_port;
                user_si.addr_p2p = ip_addr;
            }

            send_buf[0..2].clone_from_slice(&(0 as u16).to_le_bytes()); // VPort 0
            send_buf[2..6].clone_from_slice(&ip_addr);
            send_buf[6..8].clone_from_slice(&src.port().to_be_bytes());

            let res = sock.send_to(&send_buf[0..8], src);
            if let Err(e) = res {
                UdpServer::log(&log, &format!("Error send_to: {}", e));
                break;
            }
        }

        *running.lock() = false;

        UdpServer::log(&log, "UdpServer::server_proc terminating");
    }
}
