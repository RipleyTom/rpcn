#[allow(clippy::all)]
#[allow(unused_imports)]
#[rustfmt::skip]
pub mod np2_structs {
	include!(concat!(env!("OUT_DIR"), "/np2_structs.rs"));
}

pub mod protobuf_helpers;

use crate::server::client::{COMMUNICATION_ID_SIZE, ComId};
use num_traits::*;
use std::cell::Cell;
use std::io::Cursor;
use std::mem;

use prost::Message;

pub struct StreamExtractor {
	vec: Vec<u8>,
	i: Cell<usize>,
	error: Cell<bool>,
}

impl StreamExtractor {
	pub fn new(vec: Vec<u8>) -> StreamExtractor {
		StreamExtractor {
			vec,
			i: Cell::new(0),
			error: Cell::new(false),
		}
	}
	pub fn error(&self) -> bool {
		self.error.get()
	}

	pub fn get<T: PrimInt>(&self) -> T {
		let size = mem::size_of::<T>();

		if (self.i.get() + size) > self.vec.len() {
			self.error.set(true);
			return T::zero();
		}

		let value = unsafe { std::ptr::read_unaligned(self.vec[self.i.get()..(self.i.get() + size)].as_ptr() as *const T) };

		let value = T::from_le(value);

		self.i.set(self.i.get() + size);
		value
	}
	pub fn get_string(&self, empty: bool) -> String {
		let mut res_s = String::new();

		while self.i.get() < self.vec.len() && self.vec[self.i.get()] != 0 {
			res_s.push(self.vec[self.i.get()] as char);
			self.i.set(self.i.get() + 1);
		}

		self.i.set(self.i.get() + 1);

		if self.i.get() > self.vec.len() || (!empty && res_s.is_empty()) {
			self.error.set(true);
		}

		res_s
	}
	pub fn get_rawdata(&self) -> Vec<u8> {
		let mut res_vec = Vec::new();

		let size = self.get::<u32>() as usize;
		if (size + self.i.get()) > self.vec.len() {
			self.error.set(true);
			return res_vec;
		}

		let cur_i = self.i.get();
		res_vec.extend_from_slice(&self.vec[cur_i..cur_i + size]);
		self.i.set(cur_i + size);

		res_vec
	}
	pub fn get_com_id(&self) -> ComId {
		let mut com_id: ComId = [0; COMMUNICATION_ID_SIZE];

		if self.i.get() + com_id.len() > self.vec.len() {
			self.error.set(true);
		} else {
			for c in &mut com_id {
				*c = self.get::<u8>();
			}
		}

		if !com_id[0..9].iter().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()) {
			self.error.set(true);
		}

		if com_id[9] != b'_' {
			self.error.set(true);
		}

		if !com_id[10..12].iter().all(|c| c.is_ascii_digit()) {
			self.error.set(true);
		}

		com_id
	}
	pub fn get_protobuf<T: Message + Default>(&self) -> Result<T, ()> {
		let size = self.get::<u32>();
		if (size as usize + self.i.get()) > self.vec.len() {
			self.error.set(true);
			return Err(());
		}

		let ret = T::decode(&mut Cursor::new(&self.vec[self.i.get()..(self.i.get() + size as usize)]));
		self.i.set(self.i.get() + size as usize);

		ret.map_err(|_| ())
	}
}
