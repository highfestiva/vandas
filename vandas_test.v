module vandas

fn test_maxlen() {
	assert maxlen(['a', 'ab', 'b']) == 2
}

fn test_int_to_str() {
	assert int_to_str([7, 33, -2]) == ['7', '33', '-2']
}

fn test_f64_to_str() {
	assert f64_to_str([7.0, 33.0, -2.0]) == ['7.0000', '33.0000', '-2.0000']
}

fn test_rolling_mean() {
	ser := create_series('rull', [1.0, 11.0, 111.0])
	t := ser.rolling(2).mean().str()
	assert t.contains('NaN\n')
	assert t.contains('6.0')
	assert t.contains('rull')
}

fn test_data_frame_mul() {
	df := create_data_frame(map{
		'A': [1.0, 2.0]
		'B': [2.0, 3.0]
		'C': [7.0, 0.0]
	})
	t := df.mul(2).str()
	assert t.contains('14.0000\n')
	assert df.columns() == ['A', 'B', 'C']
	assert df.len() == 2
}
