import os
from local.util import read_table
from local.clean_text import clean_line
from tqdm import tqdm


bm_text_path_list = [
    "/home/khanh/ws/rw_10_2_56_217/scripts/db/english_to_malay_text_mesolitica",
]

en_text_path_list = [
    "/home/khanh/ws/rw_10_2_56_217/scripts/db/english_text"
]

data_out_path = "data/data.en-bm"

with open(data_out_path, "w") as f:

    bm_dict = {}

    for bm_text_path in bm_text_path_list:
        for utt, text in tqdm(list(read_table(open(bm_text_path), ts=(str, str))), desc=f"loading {bm_text_path} ..."):
            text = clean_line(text)
            if len(text) == 0:
                continue
            bm_dict[utt] = text

    for en_text_path in en_text_path_list:
        for utt, text in tqdm(list(read_table(open(en_text_path), ts=(str, str))), desc=f"loading {en_text_path} ..."):
            en_text = clean_line(text)
            if len(en_text) == 0:
                continue
            if utt not in bm_dict:
                continue
            bm_text = bm_dict[utt]

            f.write(f"{en_text} ||| {bm_text}\n")


            

