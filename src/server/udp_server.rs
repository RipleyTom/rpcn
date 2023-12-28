use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::UdpSocket;
use tracing::{error, info, warn};

use crate::server::client::{ClientSharedInfo, TerminateWatch};
use crate::server::Server;

pub struct UdpServer {
	host: String,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
	term_watch: TerminateWatch,
	socket: Option<UdpSocket>,
}

impl Server {
	pub async fn start_udp_server(&self, term_watch: TerminateWatch) -> io::Result<()> {
		// Starts udp signaling helper
		let mut udp_serv = UdpServer::new(self.config.read().get_host(), self.client_infos.clone(), term_watch);
		udp_serv.start().await?;

		tokio::task::spawn(async move {
			udp_serv.server_proc().await;
		});

		Ok(())
	}
}

impl UdpServer {
	pub fn new(s_host: &str, client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>, term_watch: TerminateWatch) -> UdpServer {
		UdpServer {
			host: String::from(s_host),
			client_infos,
			term_watch,
			socket: None,
		}
	}

	pub async fn start(&mut self) -> io::Result<()> {
		let bind_addr = self.host.clone() + ":3657";
		self.socket = Some(
			UdpSocket::bind(&bind_addr)
				.await
				.map_err(|e| io::Error::new(e.kind(), format!("Error binding udp server to <{}>", &bind_addr)))?,
		);
		info!("Udp server now waiting for packets on <{}:3657>", &self.host);

		Ok(())
	}

	async fn server_proc(&mut self) {
		let mut recv_buf = [0; 65535];
		let mut send_buf = [0; 65535];

		let socket = self.socket.take().unwrap();

		'udp_server_loop: loop {
			tokio::select! {
				recv_result = socket.recv_from(&mut recv_buf) => {
					if let Err(e) = recv_result {
						let err_kind = e.kind();
						if err_kind == io::ErrorKind::WouldBlock || err_kind == io::ErrorKind::TimedOut {
							continue;
						} else {
							error!("Error recv_from: {}", e);
							break;
						}
					}

					// Parse packet
					let (amt, src) = recv_result.unwrap();

					if amt != (1 + 8 + 4) || recv_buf[0] != 1 {
						warn!("Received invalid packet from {}", src);
						continue;
					}

					let user_id = i64::from_le_bytes((&recv_buf[1..9]).try_into().unwrap());
					let local_addr: [u8; 4] = recv_buf[9..13].try_into().unwrap();

					let ip_addr = match src.ip() {
						IpAddr::V4(ip) => ip.octets(),
						IpAddr::V6(_) => {
							error!("Received packet from IPv6 IP");
							continue;
						}
					};
					let ip_port = src.port();

					{
						let client_infos = self.client_infos.read();
						let client_info = client_infos.get(&user_id);

						match client_info {
							None => continue,
							Some(client_info) => {
								let need_update = {
									let client_si = client_info.signaling_info.read();
									client_si.port_p2p != ip_port || client_si.addr_p2p != ip_addr || client_si.local_addr_p2p != local_addr
								};

								if need_update {
									let mut client_si = client_info.signaling_info.write();
									client_si.port_p2p = ip_port;
									client_si.addr_p2p = ip_addr;
									client_si.local_addr_p2p = local_addr;
								}
							}
						}
					}

					send_buf[0..2].clone_from_slice(&0u16.to_le_bytes()); // VPort 0
					send_buf[2] = 0; // Subset 0
					send_buf[3..7].clone_from_slice(&ip_addr);
					send_buf[7..9].clone_from_slice(&src.port().to_be_bytes());

					let send_result = socket.send_to(&send_buf[0..9], src).await;
					if let Err(e) = send_result {
						error!("Error send_to: {}", e);
						break;
					}
				}
				_ = self.term_watch.recv.changed() => {
					break 'udp_server_loop;
				}
			}
		}
		info!("UdpServer::server_proc terminating");
	}
}
