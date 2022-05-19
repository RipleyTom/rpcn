use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::net::{IpAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{error, info, warn};

use crate::server::client::ClientSignalingInfo;

pub struct UdpServer {
	host: String,
	signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
	join_handle: Option<thread::JoinHandle<()>>,
}

struct UdpServerInstance {
	socket: UdpSocket,
	signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>,
}

static UDP_SERVER_RUNNING: AtomicBool = AtomicBool::new(false);

impl UdpServer {
	pub fn new(s_host: &str, signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>) -> UdpServer {
		UdpServer {
			host: String::from(s_host),
			signaling_infos,
			join_handle: None,
		}
	}

	pub fn start(&mut self) -> io::Result<()> {
		let bind_addr = self.host.clone() + ":3657";
		let socket = UdpSocket::bind(&bind_addr).map_err(|e| io::Error::new(e.kind(), format!("Error binding udp server to <{}>", &bind_addr)))?;
		socket
			.set_read_timeout(Some(Duration::from_millis(1)))
			.map_err(|e| io::Error::new(e.kind(), "Error setting udp server timeout to 1ms"))?;

		UDP_SERVER_RUNNING.store(true, Ordering::SeqCst);

		let mut udp_serv_inst = UdpServerInstance::new(socket, self.signaling_infos.clone());
		self.join_handle = Some(thread::spawn(move || udp_serv_inst.server_proc()));

		info!("Now waiting for packets on <{}:3657>", &self.host);

		Ok(())
	}

	pub fn stop(&mut self) {
		UDP_SERVER_RUNNING.store(false, Ordering::SeqCst);
		if self.join_handle.is_some() {
			let j = self.join_handle.take().unwrap();
			let _ = j.join();
		}
		info!("Server Stopped");
	}
}
impl UdpServerInstance {
	fn new(socket: UdpSocket, signaling_infos: Arc<RwLock<HashMap<i64, ClientSignalingInfo>>>) -> UdpServerInstance {
		UdpServerInstance { socket, signaling_infos }
	}

	fn server_proc(&mut self) {
		let mut recv_buf = [0; 65535];
		let mut send_buf = [0; 65535];

		loop {
			if !UDP_SERVER_RUNNING.load(Ordering::SeqCst) {
				break;
			}

			let res = self.socket.recv_from(&mut recv_buf);

			if let Err(e) = res {
				let err_kind = e.kind();
				if err_kind == io::ErrorKind::WouldBlock || err_kind == io::ErrorKind::TimedOut {
					continue;
				} else {
					warn!("Error recv_from: {}", e);
					break;
				}
			}

			// Parse packet
			let (amt, src) = res.unwrap();

			if amt != (1 + 8 + 4) || recv_buf[0] != 1 {
				warn!("Received invalid packet from {}", src);
				continue;
			}

			let user_id = i64::from_le_bytes((&recv_buf[1..9]).try_into().unwrap());
			let local_addr: [u8; 4] = recv_buf[9..13].try_into().unwrap();

			let ip_addr;
			match src.ip() {
				IpAddr::V4(ip) => {
					ip_addr = ip.octets();
				}
				IpAddr::V6(_) => {
					error!("Received packet from IPv6 IP");
					continue;
				}
			}
			let ip_port = src.port();

			let mut need_update = false;
			// Get a read lock to check if an udpate is needed
			{
				let si = self.signaling_infos.read();
				let user_si = si.get(&user_id);

				match user_si {
					None => continue,
					Some(user_si) => {
						if user_si.port_p2p != ip_port || user_si.addr_p2p != ip_addr || user_si.local_addr_p2p != local_addr {
							need_update = true;
						}
					}
				}
			}

			if need_update {
				let mut si = self.signaling_infos.write();
				let user_si = si.get_mut(&user_id);

				if user_si.is_none() {
					continue;
				}

				let user_si = user_si.unwrap();
				user_si.port_p2p = ip_port;
				user_si.addr_p2p = ip_addr;
				user_si.local_addr_p2p = local_addr;
			}

			send_buf[0..2].clone_from_slice(&0u16.to_le_bytes()); // VPort 0
			send_buf[2] = 0; // Subset 0
			send_buf[3..7].clone_from_slice(&ip_addr);
			send_buf[7..9].clone_from_slice(&src.port().to_be_bytes());

			let res = self.socket.send_to(&send_buf[0..9], src);
			if let Err(e) = res {
				warn!("Error send_to: {}", e);
				break;
			}
		}

		UDP_SERVER_RUNNING.store(false, Ordering::SeqCst);

		info!("UdpServerInstance::server_proc terminating");
	}
}
