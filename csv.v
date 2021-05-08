module vandas

import os

struct StringColumn {
mut:
	cells []string
}

pub fn read_csv(filename string) ?DataFrame {
	data := os.read_file(filename) ?
	return read_csv_str(data)
}

pub fn read_csv_str(data string) ?DataFrame {
	mut columns := []string{}
	mut cells := []StringColumn{}
	mut separator := ','
	for i, line in data.split_into_lines() {
		if i == 0 {
			if line.contains('\t') {
				separator = '\t'
			} else if line.contains(';') {
				separator = ';'
			}
			columns = line.split(separator)
			cells = []StringColumn{len: columns.len}
			for j in 0 .. cells.len {
				cells[j] = StringColumn{
					cells: []string{}
				}
			}
		} else {
			for j, word in line.split(separator) {
				cells[j].cells << word
			}
		}
	}
	mut series := []Series{len: columns.len}
	for i, column in columns {
		series[i] = create_series_guess_dtype(column, cells[i].cells)
	}
	mut df := DataFrame{
		cols: series
	}
	df.reset_index()
	return df
}
