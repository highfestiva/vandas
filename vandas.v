module vandas

import math
import rand

union Data {
	data_int []int
	data_f64 []f64
	data_str []string
}

enum DType {
	dint
	df64
	dstr
}

struct Roller {
	window int
mut:
	ser Series
}

struct Series {
mut:
	name  string
	dtype DType
	data  Data
}

struct DataFrame {
mut:
	index Series
	cols  []Series
}

const (
	nan = math.nan()
)

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
	mut i := 0
	for v in data {
		a[i] = '${v:d}'
		i++
	}
	return a
}

fn f64_to_str(data []f64) []string {
	mut a := []string{len: data.len}
	mut i := 0
	for v in data {
		if !math.is_nan(v) {
			a[i] = '${v:.4f}'
		} else {
			a[i] = 'NaN'
		}
		i++
	}
	return a
}

fn (ser Series) rolling(window int) Roller {
	return Roller{
		ser: ser
		window: window
	}
}

fn (r Roller) mean() Series {
	arr := r.ser.get_f64()
	assert r.window > 0
	assert r.window < arr.len
	mut out := []f64{len: arr.len}

	// start with NaNs
	for i in 0 .. r.window - 1 {
		out[i] = nan
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

fn create_ser(name string, values []f64) Series {
	return Series{
		name: name
		dtype: .df64
		data: Data{
			data_f64: values
		}
	}
}

fn (ser Series) get_int() []int {
	assert ser.dtype == .dint
	unsafe {
		return ser.data.data_int
	}
}

fn (ser Series) get_f64() []f64 {
	assert ser.dtype == .df64
	unsafe {
		return ser.data.data_f64
	}
}

fn (ser Series) get_str() []string {
	assert ser.dtype == .dstr
	unsafe {
		return ser.data.data_str
	}
}

fn (a Series) mul(b f64) Series {
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

fn (ser Series) len() int {
	unsafe {
		return match ser.dtype {
			.dint { ser.data.data_int.len }
			.df64 { ser.data.data_f64.len }
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

fn (ser Series) as_str() Series {
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
		.dstr {
			ser
		}
	}
}

fn create_data_frame(m map[string][]f64) DataFrame {
	mut df := DataFrame{
		cols: []Series{len: m.len}
	}
	mut i := 0
	mut l := 0
	for k, v in m {
		assert i == 0 || l == v.len
		l = v.len
		s := create_ser(k, v)
		df.cols[i] = s
		i++
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
	return df
}

fn (a DataFrame) mul(b f64) DataFrame {
	mut cols := []Series{len: a.cols.len}
	mut i := 0
	for ser in a.cols {
		cols[i] = ser.mul(b)
		i++
	}
	return DataFrame{
		cols: cols
		index: a.index
	}
}

fn (df DataFrame) columns() []string {
	mut cols := []string{len: df.cols.len}
	mut i := 0
	for ser in df.cols {
		cols[i] = ser.name
		i++
	}
	return cols
}

fn (df DataFrame) len() int {
	return df.index.len()
}

pub fn (df DataFrame) str() string {
	// measure column widths
	mut width := []int{len: 1 + df.cols.len}
	mut str_sers := []Series{len: 1 + df.cols.len}
	mut sers := [df.index]
	sers << df.cols
	mut i := 0
	for ser in sers {
		str_sers[i] = ser.as_str()
		mut row_strs := str_sers[i].get_str().clone()
		row_strs << [ser.name]
		width[i] = maxlen(row_strs)
		i++
	}

	// columns
	pad := '                                        '
	mut row_strs := []string{len: sers.len}
	i = 0
	for ser in sers {
		w := width[i]
		row_strs[i] = pad[0..(w - ser.name.len)] + ser.name
		i++
	}
	mut s := row_strs.join('  ')

	// cell data
	l := df.len()
	if l == 0 {
		s += '\n[empty DataFrame]'
	}
	for r in 0 .. l {
		i = 0
		for ser in str_sers {
			w := width[i]
			row_strs[i] = pad[0..(w - ser.get_str()[r].len)] + ser.get_str()[r]
			i++
		}
		s += '\n' + row_strs.join('  ')
	}
	return s
}
