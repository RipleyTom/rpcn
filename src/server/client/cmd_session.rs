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
		let (_com_id, setuser_req) = self.get_com_and_fb::<SetUserInfo>(data)?;

		if setuser_req.userBinAttr().is_none() || setuser_req.userBinAttr().as_ref().unwrap().is_empty() {
			return Ok(ErrorType::NoError);
		}
		let binattrs = setuser_req.userBinAttr().unwrap();
		if binattrs.len() > 1 {
			error!("SetUserInfo request sent more than one binattr!");
		}

		// Take the first valid one?
		let mut binattr = None;
		'search_loop: for i in 0..binattrs.len() {
			if binattrs.get(i).id() == SCE_NP_MATCHING2_USER_BIN_ATTR_1_ID {
				binattr = Some(binattrs.get(i));
				break 'search_loop;
			}
		}
		if binattr.is_none() {
			error!("SetUserInfo request didn't contain a valid binattr id!");
			return Ok(ErrorType::NoError);
		}
		let binattr = binattr.unwrap();

		let client_info = self.shared.client_infos.read();
		let mut session_info = client_info.get(&self.client_info.user_id).unwrap().session_info.write();
		session_info.bin_attr = RoomBinAttr::from_flatbuffer(&binattr);

		Ok(ErrorType::NoError)
	}
}
