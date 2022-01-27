#[allow(unused_imports)]
pub mod np2_structs_generated;

pub mod fb_helpers;

use crate::server::client::ComId;
use num_traits::*;
use std::cell::Cell;
use std::mem;

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
		return self.error.get();
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

		if self.i.get() > self.vec.len() || (!empty && res_s.len() == 0) {
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
		res_vec.clone_from_slice(&self.vec[cur_i..cur_i + size]);
		self.i.set(cur_i + size);

		res_vec
	}
	pub fn get_com_id(&self) -> ComId {
		let mut com_id: ComId = [0; 9];

		if self.i.get() + com_id.len() > self.vec.len() {
			self.error.set(true);
		} else {
			for c in &mut com_id {
				*c = self.get::<u8>();
			}
		}

		com_id
	}
	pub fn get_flatbuffer<'a, T: flatbuffers::Follow<'a> + flatbuffers::Verifiable + 'a>(&'a self) -> Result<T::Inner, ()> {
		let size = self.get::<u32>();
		if (size as usize + self.i.get()) > self.vec.len() {
			self.error.set(true);
			return Err(());
		}

		let ret = flatbuffers::root::<T>(&self.vec[self.i.get()..]);
		self.i.set(self.i.get() + size as usize);

		ret.map_err(|_| ())
	}
}
