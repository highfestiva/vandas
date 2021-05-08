module vandas

fn test_read_csv_str() ? {
	df := read_csv_str('A,B\n3,2') ?
	assert df.str().contains(' 3 ')
}
