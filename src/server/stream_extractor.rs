use num_traits::*;
use std::mem;

pub mod np2_structs_generated;
use np2_structs_generated::*;

pub struct StreamExtractor {
    vec: Vec<u8>,
    i: usize,
    error: bool,
}

unsafe fn read_slice<T>(bytes: &[u8]) -> T {
    std::ptr::read_unaligned(bytes.as_ptr() as *const T)
}

impl StreamExtractor {
    pub fn new(vec: Vec<u8>) -> StreamExtractor {
        StreamExtractor { vec, i: 0, error: false }
    }
    pub fn error(&self) -> bool {
        return self.error;
    }

    pub fn get<T: PrimInt>(&mut self) -> T {
        let size = mem::size_of::<T>();

        if self.i + size > self.vec.len() {
            self.error = true;
            return T::zero();
        }

        let value = unsafe { std::ptr::read_unaligned(self.vec[self.i..self.i + size].as_ptr() as *const T) };

        let value = T::from_le(value);

        self.i += size;
        value
    }
    pub fn get_string(&mut self, empty: bool) -> String {
        let mut res_s = String::new();

        while self.i < self.vec.len() && self.vec[self.i] != 0 {
            res_s.push(self.vec[self.i] as char);
            self.i += 1;
        }

        self.i += 1;

        if self.i > self.vec.len() || (!empty && res_s.len() == 0) {
            self.error = true;
        }

        res_s
    }

    pub fn get_flatbuffer<'a, T: flatbuffers::Follow<'a> + 'a>(&'a mut self) -> Result<T::Inner, bool> {
        let size = self.get::<u32>();
        if size as usize + self.i > self.vec.len() {
            return Err(self.error);
        }

        let ret = flatbuffers::get_root::<T>(&self.vec[self.i..]);
        self.i += size as usize;

        if self.i > self.vec.len() {
            self.error = true;
        }

        if self.error {
            Err(self.error)
        } else {
            Ok(ret)
        }
    }
}
