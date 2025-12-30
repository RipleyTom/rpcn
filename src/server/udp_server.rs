use std::collections::HashMap;
use std::convert::TryInto;
use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::UdpSocket;
use tracing::{error, info, warn};

use crate::server::Server;
use crate::server::client::{ClientSharedInfo, TerminateWatch};

pub struct UdpServer {
	host_ipv4: String,
	host_ipv6: String,
	client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>,
	term_watch: TerminateWatch,
	socket: Option<UdpSocket>,
}

impl Server {
	pub async fn start_udp_server(&self, term_watch: TerminateWatch) -> io::Result<()> {
		// Starts udp signaling helper
		let (addr_ipv4, addr_ipv6);
		{
			let config = self.config.read();
			addr_ipv4 = config.get_host_ipv4().clone();
			addr_ipv6 = config.get_host_ipv6().clone();
		}

		let mut udp_serv = UdpServer::new(addr_ipv4, addr_ipv6, self.client_infos.clone(), term_watch);
		udp_serv.start().await?;

		tokio::task::spawn(async move {
			udp_serv.server_proc().await;
		});

		Ok(())
	}
}

impl UdpServer {
	pub fn new(host_ipv4: String, host_ipv6: String, client_infos: Arc<RwLock<HashMap<i64, ClientSharedInfo>>>, term_watch: TerminateWatch) -> UdpServer {
		UdpServer {
			host_ipv4,
			host_ipv6,
			client_infos,
			term_watch,
			socket: None,
		}
	}

	fn try_to_bind_ipv6(str_addr: &str) -> Result<UdpSocket, io::Error> {
		// We need to use socket2's Socket to force IPv6 only off for windows
		let socket = socket2::Socket::new(socket2::Domain::IPV6, socket2::Type::DGRAM, None)?;
		socket.set_only_v6(false)?;
		socket.set_nonblocking(true)?;
		let address: std::net::SocketAddr = str_addr
			.parse()
			.map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Address is not a valid IPv6 address".to_string()))?;
		socket.bind(&address.into())?;
		UdpSocket::from_std(std::net::UdpSocket::from(socket))
	}

	pub async fn start(&mut self) -> io::Result<()> {
		// We try to bind to IPv6 and if it fails to IPv4
		let bind_addr_ipv6 = format!("[{}]:3657", self.host_ipv6.clone());
		let bind_addr_ipv4 = format!("{}:3657", self.host_ipv4.clone());
		self.socket = Some({
			let sock_res = UdpServer::try_to_bind_ipv6(&bind_addr_ipv6);
			if let Err(e) = sock_res {
				warn!("Failed to bind to IPv6({}): {}", bind_addr_ipv6, e);
				UdpSocket::bind(&bind_addr_ipv4)
					.await
					.map_err(|e| io::Error::new(e.kind(), format!("Error binding udp server to IPv4({}): {}", &bind_addr_ipv4, e)))?
			} else {
				sock_res.unwrap()
			}
		});

		info!("Udp server now waiting for packets on <{}>", self.socket.as_ref().unwrap().local_addr().unwrap());
		Ok(())
	}

	fn update_signaling_information(&self, user_id: i64, local_addr: [u8; 4], ip_addr: IpAddr, ip_port: u16) {
		let client_infos = self.client_infos.read();
		let client_info = client_infos.get(&user_id);

		match client_info {
			None => {}
			Some(client_info) => {
				let need_update = {
					let client_si = client_info.signaling_info.read();

					match ip_addr {
						IpAddr::V4(addr) => client_si.addr_p2p_ipv4.0 != addr.octets() || client_si.addr_p2p_ipv4.1 != ip_port,
						IpAddr::V6(addr) => {
							// If IPv6 is bound we can receive both IPv6 and IPv4 packets
							if let Some(actually_ipv4) = addr.to_ipv4() {
								client_si.addr_p2p_ipv4.0 != actually_ipv4.octets() || client_si.addr_p2p_ipv4.1 != ip_port
							} else {
								client_si.addr_p2p_ipv6.as_ref().is_none_or(|si_addr| si_addr.0 != addr.octets() || si_addr.1 != ip_port)
							}
						}
					}
				};

				if need_update {
					let mut client_si = client_info.signaling_info.write();
					client_si.local_addr_p2p = local_addr;

					match ip_addr {
						IpAddr::V4(addr) => {
							client_si.addr_p2p_ipv4 = (addr.octets(), ip_port);
						}
						IpAddr::V6(addr) => {
							if let Some(actually_ipv4) = addr.to_ipv4() {
								client_si.addr_p2p_ipv4 = (actually_ipv4.octets(), ip_port);
							} else {
								client_si.addr_p2p_ipv6 = Some((addr.octets(), ip_port));
							}
						}
					}
				}
			}
		}
	}

	async fn handle_socket_input(&self, socket: &UdpSocket, recv_result: io::Result<(usize, core::net::SocketAddr)>, recv_buf: &[u8], send_buf: &mut [u8]) {
		if let Err(e) = recv_result {
			let err_kind = e.kind();
			if err_kind == io::ErrorKind::WouldBlock || err_kind == io::ErrorKind::TimedOut {
				return;
			} else {
				error!("Error recv_from: {}", e);
				return;
			}
		}

		// Parse packet
		let (amt, src) = recv_result.unwrap();

		if amt != (1 + 8 + 4) || recv_buf[0] != 1 {
			warn!("Received invalid packet from {}", src);
			return;
		}

		let user_id = i64::from_le_bytes((&recv_buf[1..9]).try_into().unwrap());
		let local_addr: [u8; 4] = recv_buf[9..13].try_into().unwrap();

		let ip_addr = src.ip();
		let ip_port = src.port();

		self.update_signaling_information(user_id, local_addr, ip_addr, ip_port);

		send_buf[0..2].clone_from_slice(&0u16.to_le_bytes()); // VPort 0
		send_buf[2] = 0; // Subset 0

		let send_result = match ip_addr {
			IpAddr::V4(addr) => {
				send_buf[3..7].clone_from_slice(&addr.octets());
				send_buf[7..9].clone_from_slice(&src.port().to_be_bytes());
				socket.send_to(&send_buf[0..9], src).await
			}
			IpAddr::V6(addr) => {
				if let Some(actually_ipv4) = addr.to_ipv4() {
					send_buf[3..7].clone_from_slice(&actually_ipv4.octets());
					send_buf[7..9].clone_from_slice(&src.port().to_be_bytes());
					socket.send_to(&send_buf[0..9], src).await
				} else {
					send_buf[3..19].clone_from_slice(&addr.octets());
					send_buf[19..21].clone_from_slice(&src.port().to_be_bytes());
					socket.send_to(&send_buf[0..21], src).await
				}
			}
		};

		if let Err(e) = send_result {
			error!("Error send_to: {}", e);
		}
	}

	async fn server_proc(&mut self) {
		let mut recv_buf = Box::new([0u8; 65535]);
		let mut send_buf = Box::new([0u8; 65535]);

		let socket = self.socket.take().unwrap();

		if *self.term_watch.recv.borrow_and_update() {
			return;
		}

		'udp_server_loop: loop {
			tokio::select! {
				recv_result = socket.recv_from(recv_buf.as_mut_slice()) => {
					self.handle_socket_input(&socket, recv_result, recv_buf.as_slice(), send_buf.as_mut_slice()).await;
				}
				_ = self.term_watch.recv.changed() => {
					break 'udp_server_loop;
				}
			}
		}
		info!("UdpServer::server_proc terminating");
	}
}
