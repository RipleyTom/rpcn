use tracing::warn;

use crate::server::{
	client::ErrorType,
	stream_extractor::np2_structs::{Uint8, Uint16},
};

pub trait ProtobufVerifier<T> {
	fn get_verified(&self) -> Result<T, ErrorType>;
}

pub trait ProtobufMaker<T> {
	fn new_from_value(value: T) -> Option<Self>
	where
		Self: Sized;
}

impl ProtobufVerifier<u16> for Uint16 {
	fn get_verified(&self) -> Result<u16, ErrorType> {
		if self.value > u16::MAX as u32 {
			warn!("Protobuf Uint16 cast overflowed!");
			return Err(ErrorType::Malformed);
		}

		return Ok(self.value as u16);
	}
}

impl ProtobufMaker<u16> for Uint16 {
	fn new_from_value(value: u16) -> Option<Uint16> {
		Some(Uint16 { value: value as u32 })
	}
}

impl ProtobufVerifier<u16> for Option<Uint16> {
	fn get_verified(&self) -> Result<u16, ErrorType> {
		match self {
			None => {
				warn!("Protobuf Uint16 is none!");
				Err(ErrorType::Malformed)
			}
			Some(uint16) => uint16.get_verified(),
		}
	}
}

impl ProtobufVerifier<u8> for Uint8 {
	fn get_verified(&self) -> Result<u8, ErrorType> {
		if self.value > u8::MAX as u32 {
			warn!("Protobuf Uint8 cast overflowed!");
			return Err(ErrorType::Malformed);
		}

		return Ok(self.value as u8);
	}
}

impl ProtobufMaker<u8> for Uint8 {
	fn new_from_value(value: u8) -> Option<Uint8> {
		Some(Uint8 { value: value as u32 })
	}
}

impl ProtobufVerifier<u8> for Option<Uint8> {
	fn get_verified(&self) -> Result<u8, ErrorType> {
		match self {
			None => {
				warn!("Protobuf Uint8 is none!");
				Err(ErrorType::Malformed)
			}
			Some(uint8) => uint8.get_verified(),
		}
	}
}
