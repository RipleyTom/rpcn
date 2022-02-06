use crate::server::stream_extractor::np2_structs_generated::*;

// Flatbuffers doesn't provide deep copy functionality so we have to make our own
pub fn dc_opt_data<'a>(builder: &mut flatbuffers::FlatBufferBuilder<'a>, opt_data: Option<&PresenceOptionData>) -> flatbuffers::WIPOffset<PresenceOptionData<'a>> {
	let mut opt_data_vec: Vec<u8> = Vec::new();
	let mut opt_data_vec_len: u32 = 0;
	if let Some(opt_data) = opt_data {
		for i in 0..16 {
			opt_data_vec.push(*opt_data.data().unwrap().get(i).unwrap());
		}
		opt_data_vec_len = opt_data.len();
	} else {
		opt_data_vec.resize(16, 0u8);
	}
	let opt_data_vec = builder.create_vector(&opt_data_vec);

	PresenceOptionData::create(
		builder,
		&PresenceOptionDataArgs {
			len: opt_data_vec_len,
			data: Some(opt_data_vec),
		},
	)
}
