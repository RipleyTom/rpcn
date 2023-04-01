use crate::server::client::*;

impl Client {
	pub fn get_network_time(&self, reply: &mut Vec<u8>) -> Result<ErrorType, ErrorType> {
		reply.extend(Client::get_psn_timestamp().to_le_bytes());
		Ok(ErrorType::NoError)
	}
}
