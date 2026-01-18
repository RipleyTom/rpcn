// Session Commands

use crate::server::client::*;
use crate::server::room_manager::RoomBinAttr;

const SCE_NP_MATCHING2_USER_BIN_ATTR_1_ID: u16 = 0x5F;

pub struct ClientSharedSessionInfo {
	bin_attr: RoomBinAttr<128>,
}

impl ClientSharedSessionInfo {
	pub fn new() -> ClientSharedSessionInfo {
		ClientSharedSessionInfo {
			bin_attr: RoomBinAttr::<128>::with_id(SCE_NP_MATCHING2_USER_BIN_ATTR_1_ID),
		}
	}
}

impl Client {
	pub async fn req_set_userinfo(&mut self, data: &mut StreamExtractor) -> Result<ErrorType, ErrorType> {
		let (_com_id, setuser_req) = self.get_com_and_pb::<SetUserInfo>(data)?;

		if setuser_req.user_bin_attr.is_empty() {
			return Ok(ErrorType::NoError);
		}
		let binattrs = &setuser_req.user_bin_attr;
		if binattrs.len() > 1 {
			return Err(ErrorType::Malformed);
		}

		// Take the first valid one?
		let mut binattr = None;
		'search_loop: for attr in binattrs {
			if attr.id.as_ref().map(|v| v.value).unwrap_or(0) == SCE_NP_MATCHING2_USER_BIN_ATTR_1_ID as u32 {
				binattr = Some(attr);
				break 'search_loop;
			}
		}
		if binattr.is_none() {
			return Err(ErrorType::Malformed);
		}
		let binattr = binattr.unwrap();

		let client_info = self.shared.client_infos.read();
		let mut session_info = client_info.get(&self.client_info.user_id).unwrap().session_info.write();
		session_info.bin_attr = RoomBinAttr::from_protobuf(binattr)?;

		Ok(ErrorType::NoError)
	}
}
