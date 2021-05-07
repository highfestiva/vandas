module vandas

import time

enum TimeType {
	undefined
	datetime
	datetime_iso8601
	datetime_rfc2822
}

fn parse_time(s string, mut ttype &TimeType) ?u64 {
	return match *ttype {
		.undefined {
			ttype = .datetime_iso8601
			t := time.parse_iso8601(s) or {
				ttype = .datetime
				time.parse(s) or {
					ttype = .datetime_rfc2822
					time.parse_rfc2822(s)?
				}
			}
			t.unix_time_milli()
		}
		.datetime {
			t := time.parse(s)?
			t.unix_time_milli()
		}
		.datetime_iso8601 {
			t := time.parse_iso8601(s)?
			t.unix_time_milli()
		}
		.datetime_rfc2822 {
			t := time.parse_rfc2822(s)?
			t.unix_time_milli()
		}
	}
}

pub fn (ser Series) to_time() ?Series {
	mut time_ser := []u64{len: ser.len()}
	mut ttype := TimeType.undefined
	astr := ser.as_str().get_str()
	for i, a in astr {
		t := parse_time(a, mut &ttype) or { return error('Invalid time format: $a') }
		time_ser[i] = t
	}
	return Series{
		name: ser.name
		dtype: .dunix_ms
		data: Data{
			data_u64: time_ser
		}
	}
}

pub fn unix_to_str(data []u64) []string {
	mut a := []string{len: data.len}
	for i, v in data {
		a[i] = time.unix2(int(v/1000), int(v%1000)).str()
	}
	return a
}
