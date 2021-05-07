module vandas

fn test_parse_time() ? {
	times := [
		'2021-05-07 23:50:00'
		'2021-05-07 23:50:00+01:00'
		'2021-05-07T23:50:15.123456Z'
		'2021-05-07T23:50:15.123456+02:00'
		'Fri 7 May 2021 23:51:45 -0300'
	]
	ttypes := [
		TimeType.datetime_iso8601,
		TimeType.datetime_iso8601
		TimeType.datetime_iso8601
		TimeType.datetime_iso8601
		TimeType.datetime_rfc2822
	]
	for i, ts in times {
		mut tt := TimeType.undefined
		parse_time(ts, mut tt)?
		assert tt == ttypes[i]
		parse_time(ts, mut tt)? // parse with forced time type
	}
}

fn test_ser_to_time() ? {
	s := Series{
		name: 'time'
		dtype: .dstr
		data: Data{
			data_str: ['2021-05-07T23:55:00Z', '2021-05-07T23:56:00Z']
		}
	}
	t := s.to_time()?
	assert t.get_u64() == [u64(1620431700000), u64(1620431760000)]
	assert t.str().contains('2021-05-07 23:55')
}
