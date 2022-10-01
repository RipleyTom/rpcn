use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private};
use openssl::sign::Signer;

use tracing::error;

enum TicketData {
	Empty(),
	U32(u32),
	U64(u64),
	Time(u64),
	Binary(Vec<u8>),
	BString(Vec<u8>),
	Blob(u8, Vec<TicketData>),
}

impl TicketData {
	fn id(&self) -> u16 {
		match self {
			TicketData::Empty() => 0,
			TicketData::U32(_) => 1,
			TicketData::U64(_) => 2,
			TicketData::BString(_) => 4,
			TicketData::Time(_) => 7,
			TicketData::Binary(_) => 8,
			TicketData::Blob(id, _) => 0x3000 | (*id as u16),
		}
	}

	fn len(&self) -> u16 {
		match self {
			TicketData::Empty() => 0,
			TicketData::U32(_) => 4,
			TicketData::U64(_) => 8,
			TicketData::BString(string_data) => string_data.len() as u16,
			TicketData::Time(_) => 8,
			TicketData::Binary(binary_data) => binary_data.len() as u16,
			TicketData::Blob(_, sdata) => sdata.iter().map(|x| x.len() + 4).sum(),
		}
	}

	pub fn write(&self, dest: &mut Vec<u8>) {
		dest.extend(&self.id().to_be_bytes());
		dest.extend(&self.len().to_be_bytes());
		match self {
			TicketData::Empty() => {}
			TicketData::U32(value) => dest.extend(&value.to_be_bytes()),
			TicketData::U64(value) => dest.extend(&value.to_be_bytes()),
			TicketData::BString(string_data) => dest.extend(string_data),
			TicketData::Time(time) => dest.extend(&time.to_be_bytes()),
			TicketData::Binary(binary_data) => dest.extend(binary_data),
			TicketData::Blob(_, sdata) => {
				for sub in sdata {
					sub.write(dest);
				}
			}
		}
	}
}

pub struct Ticket {
	data: Vec<TicketData>,
}

static TICKET_ID_DISPENSER: AtomicU64 = AtomicU64::new(1);

// TODO: get saved value from DB or 1 to initialze dispenser (to ensure continuity after restart)
// impl Server {
// 	pub fn initialize_ticket_dispenser() -> Result<(), String> {
// 		Ok(())
// 	}
// }

impl Ticket {
	fn sign_ticket(user_blob: &TicketData, ec_key: &Option<PKey<Private>>) -> TicketData {
		let signature = TicketData::Blob(2, vec![TicketData::Binary(vec![0, 0, 0, 0]), TicketData::Binary([0; 56].to_vec())]);

		if ec_key.is_none() {
			return signature;
		}

		let signer = Signer::new(MessageDigest::sha224(), ec_key.as_ref().unwrap());
		if let Err(e) = signer {
			error!("Failed to create Signer for ticket data: {}", e);
			return signature;
		}
		let mut signer = signer.unwrap();

		let signature_size = signer.len();
		if let Err(e) = signature_size {
			error!("Failed to get signature size: {}", e);
			return signature;
		}
		let signature_size = signature_size.unwrap();

		if signature_size != 56 {
			error!("Signature size({}) isn't what is expected(56)!", signature_size);
			return signature;
		}

		let mut vec_sign = [0; 56].to_vec();

		let mut user_rawdata = Vec::new();
		user_blob.write(&mut user_rawdata);

		let sign_res = signer.sign_oneshot(&mut vec_sign, &user_rawdata);
		if let Err(e) = sign_res {
			error!("Failed to sign ticket data: {}", e);
			return signature;
		}
		let sign_res = sign_res.unwrap();
		if sign_res != 56 {
			error!("Final signature size ({}) is not what was expected(56)!", sign_res);
			return signature;
		}

		TicketData::Blob(2, vec![TicketData::Binary(vec![b'R', b'P', b'C', b'N']), TicketData::Binary(vec_sign)])
	}

	pub fn new(user_id: u64, npid: &str, service_id: &str, cookie: Vec<u8>, ec_key: &Option<PKey<Private>>) -> Ticket {
		let ticket_id = TICKET_ID_DISPENSER.fetch_add(1, Ordering::SeqCst);

		let serial_str = format!("{}", ticket_id);
		let mut serial_vec = serial_str.as_bytes().to_vec();
		serial_vec.resize(0x14, 0);

		let issuer_id: u32 = 0x33333333;
		let issued_date: u64 = (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64) - (60 * 1000);
		let expire_date = issued_date + (60 * 1000 * 15);

		let mut online_id = npid.as_bytes().to_vec();
		online_id.resize(0x20, 0);

		let mut service_id: Vec<u8> = service_id.as_bytes().to_vec();
		service_id.resize(0x18, 0);

		let mut user_data = vec![
			TicketData::Binary(serial_vec),
			TicketData::U32(issuer_id),
			TicketData::Time(issued_date),
			TicketData::Time(expire_date),
			TicketData::U64(user_id),
			TicketData::BString(online_id),
			TicketData::Binary(vec![b'b', b'r', 0, 0]),  // region (yes you're going to brazil)
			TicketData::BString(vec![b'u', b'n', 0, 0]), // domain
			TicketData::Binary(service_id),
			TicketData::U32(0), // status
		];

		if !cookie.is_empty() {
			user_data.push(TicketData::Binary(cookie));
		}

		user_data.push(TicketData::Empty());
		user_data.push(TicketData::Empty());

		let user_blob = TicketData::Blob(0, user_data);
		let signature = Ticket::sign_ticket(&user_blob, ec_key);

		Ticket { data: vec![user_blob, signature] }
	}

	pub fn generate_blob(&self) -> Vec<u8> {
		let mut ticket_blob: Vec<u8> = Vec::new();

		// Version
		ticket_blob.extend(&(0x21010000u32).to_be_bytes());

		let size: u32 = self.data.iter().map(|x| (x.len() + 4) as u32).sum::<u32>();
		ticket_blob.extend(&size.to_be_bytes());

		for data in &self.data {
			data.write(&mut ticket_blob);
		}

		ticket_blob
	}
}
