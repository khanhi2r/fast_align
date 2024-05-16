#!/usr/bin/env bash
parallel_data_path="data/data.en-bm"

./build/fast_align -i "${parallel_data_path}" -d -o -v > "${parallel_data_path}.forwardalign"
./build/fast_align -i "${parallel_data_path}" -d -o -v -r > "${parallel_data_path}.reversealign"
./build/atools -i "${parallel_data_path}.forwardalign" -j "${parallel_data_path}.reversealign" -c grow-diag-final-and > "${parallel_data_path}.symmetricalign"