module vandas

import math
import rand
import strconv

const (
	nan = math.nan()
)

pub union Data {
	data_int []int
	data_u64 []u64
	data_f64 []f64
	data_str []string
}

pub enum DType {
	dint
	du64
	df64
	dunix_ms
	dstr
}

struct Roller {
	window int
mut:
	ser Series
}

pub struct Series {
mut:
	name  string
	dtype DType
	data  Data
}

pub struct DataFrame {
mut:
	index Series
	cols  []Series
}

fn rnd(i int) []f64 {
	mut a := []f64{len: i, cap: i}
	for j in 0 .. i {
		a[j] = rand.f64()
	}
	return a
}

fn maxlen(astr []string) int {
	mut l := 0
	for s in astr {
		if s.len > l {
			l = s.len
		}
	}
	return l
}

fn int_to_str(data []int) []string {
	mut a := []string{len: data.len}
	for i, v in data {
		a[i] = '${v:d}'
	}
	return a
}

fn u64_to_str(data []u64) []string {
	mut a := []string{len: data.len}
	for i, v in data {
		a[i] = '${int(v):d}'
	}
	return a
}

fn f64_to_str(data []f64) []string {
	mut a := []string{len: data.len}
	for i, v in data {
		if !math.is_nan(v) {
			a[i] = '${v:.4f}'
		} else {
			a[i] = 'NaN'
		}
	}
	return a
}

fn str_to_int(data []string) ?[]int {
	mut d := []int{len: data.len}
	for i, s in data {
		d[i] = strconv.atoi(s) ?
	}
	return d
}

fn str_to_f64(data []string) ?[]f64 {
	mut d := []f64{len: data.len}
	for i, s in data {
		d[i] = strconv.atof64(s)
	}
	return d
}

pub fn (ser Series) rolling(window int) Roller {
	return Roller{
		ser: ser
		window: window
	}
}

pub fn (r Roller) mean() Series {
	arr := r.ser.get_f64()
	assert r.window > 0
	assert r.window < arr.len
	mut out := []f64{len: arr.len}

	// start with NaNs
	for i in 0 .. r.window - 1 {
		out[i] = vandas.nan
	}

	// rolling window mean
	iwin := 1.0 / f64(r.window)
	for i in 0 .. arr.len - r.window + 1 {
		mut m := 0.0
		for j in i .. i + r.window {
			m += arr[j]
		}
		out[i + r.window - 1] = m * iwin
	}
	return Series{
		name: r.ser.name
		dtype: .df64
		data: Data{
			data_f64: out
		}
	}
}

pub fn create_series(name string, values []f64) Series {
	return Series{
		name: name
		dtype: .df64
		data: Data{
			data_f64: values
		}
	}
}

fn create_series_guess_dtype(name string, values []string) Series {
	ints := str_to_int(values) or {
		floats := str_to_f64(values) or {
			return Series{
				name: name
				dtype: .dstr
				data: Data{
					data_str: values
				}
			}
		}
		return Series{
			name: name
			dtype: .df64
			data: Data{
				data_f64: floats
			}
		}
	}
	return Series{
		name: name
		dtype: .dint
		data: Data{
			data_int: ints
		}
	}
}

pub fn (ser Series) get_int() []int {
	assert ser.dtype == .dint
	unsafe {
		return ser.data.data_int
	}
}

pub fn (ser Series) get_u64() []u64 {
	assert ser.dtype == .du64 || ser.dtype == .dunix_ms
	unsafe {
		return ser.data.data_u64
	}
}

pub fn (ser Series) get_f64() []f64 {
	assert ser.dtype == .df64
	unsafe {
		return ser.data.data_f64
	}
}

pub fn (ser Series) get_str() []string {
	assert ser.dtype == .dstr
	unsafe {
		return ser.data.data_str
	}
}

pub fn (a Series) mul(b f64) Series {
	assert a.dtype == .df64
	mut c := a.get_f64().clone()
	for i in 0 .. c.len {
		c[i] *= b
	}
	return Series{
		name: a.name
		dtype: .df64
		data: Data{
			data_f64: c
		}
	}
}

pub fn (ser Series) len() int {
	unsafe {
		return match ser.dtype {
			.dint { ser.data.data_int.len }
			.du64 { ser.data.data_u64.len }
			.df64 { ser.data.data_f64.len }
			.dunix_ms { ser.data.data_u64.len }
			.dstr { ser.data.data_str.len }
		}
	}
}

pub fn (ser Series) str() string {
	// measure width
	astr := ser.as_str().get_str()
	width := maxlen(astr)
	pad := '                                        '
	mut s := ''

	// values
	for v in astr {
		s += pad[0..width - v.len] + v + '\n'
	}

	// name and length
	s += 'name: $ser.name, length: $astr.len'
	return s
}

pub fn (ser Series) as_str() Series {
	return match ser.dtype {
		.dint {
			a := int_to_str(ser.get_int())
			Series{
				name: ser.name
				dtype: .dstr
				data: Data{
					data_str: a
				}
			}
		}
		.du64 {
			a := u64_to_str(ser.get_u64())
			Series{
				name: ser.name
				dtype: .dstr
				data: Data{
					data_str: a
				}
			}
		}
		.df64 {
			a := f64_to_str(ser.get_f64())
			Series{
				name: ser.name
				dtype: .dstr
				data: Data{
					data_str: a
				}
			}
		}
		.dunix_ms {
			a := unix_to_str(ser.get_u64())
			Series{
				name: ser.name
				dtype: .dstr
				data: Data{
					data_str: a
				}
			}
		}
		.dstr {
			ser
		}
	}
}

pub fn create_data_frame(m map[string][]f64) DataFrame {
	mut df := DataFrame{
		cols: []Series{len: m.len}
	}
	mut i := 0
	mut l := 0
	for k, v in m {
		assert i == 0 || l == v.len
		l = v.len
		s := create_series(k, v)
		df.cols[i] = s
		i++
	}
	df.reset_index()
	return df
}

pub fn (mut df DataFrame) reset_index() {
	l := match df.cols.len {
		0 { 0 }
		else { df.cols[0].len() }
	}
	mut idx := []int{len: l}
	for j in 0 .. l {
		idx[j] = j
	}
	df.index = Series{
		name: ''
		dtype: .dint
		data: Data{
			data_int: idx
		}
	}
}

pub fn (a DataFrame) mul(b f64) DataFrame {
	mut cols := []Series{len: a.cols.len}
	for i, ser in a.cols {
		cols[i] = ser.mul(b)
	}
	return DataFrame{
		cols: cols
		index: a.index
	}
}

pub fn (df DataFrame) columns() []string {
	mut cols := []string{len: df.cols.len}
	for i, ser in df.cols {
		cols[i] = ser.name
	}
	return cols
}

pub fn (df DataFrame) len() int {
	return df.index.len()
}

pub fn (df DataFrame) str() string {
	// measure column widths
	mut width := []int{len: 1 + df.cols.len}
	mut str_sers := []Series{len: 1 + df.cols.len}
	mut sers := [df.index]
	sers << df.cols
	for i, ser in sers {
		str_sers[i] = ser.as_str()
		mut row_strs := str_sers[i].get_str().clone()
		row_strs << [ser.name]
		width[i] = maxlen(row_strs)
	}

	// columns
	pad := '                                        '
	mut row_strs := []string{len: sers.len}
	for i, ser in sers {
		w := width[i]
		row_strs[i] = pad[0..(w - ser.name.len)] + ser.name
	}
	mut s := row_strs.join('  ')

	// cell data
	l := df.len()
	if l == 0 {
		s += '\n[empty DataFrame]'
	}
	for r in 0 .. l {
		for i, ser in str_sers {
			w := width[i]
			row_strs[i] = pad[0..(w - ser.get_str()[r].len)] + ser.get_str()[r]
		}
		s += '\n' + row_strs.join('  ')
	}
	return s
}
