#!/usr/bin/env python
from typing import Iterable, Tuple, List, Set, Callable
import sys
import os
import shutil
from tqdm import tqdm
from local.util import read_table



def to_upper() -> Callable[[str,], str]:
    def helper(text: str) -> str:
        return text.upper()
    return helper

def limit_alphabet(alphabet: str="ABCDEFGHIJKLMNOPQRSTUVWXYZ '-") -> Callable[[str,], str]:
    def helper(text: str) -> str:
        char_list = list(text)
        filtered_char_list = list(filter(lambda char: char in alphabet, char_list))
        filtered_text = "".join(filtered_char_list)
        return filtered_text
    return helper

def remove_tag() -> Callable[[str,], str]:
    def helper(text: str) -> str:
        word_list = text.split()
        filtered_word_list = []
        for word in word_list:
            is_tag = False
            for pair in ["**", "<>", "()", "[]", "{}"]:
                open_char = pair[0]
                close_char = pair[1]
                if word.startswith(open_char) and word.endswith(close_char):
                    is_tag = True
                    break
            if is_tag:
                continue
            filtered_word_list.append(word)
        return " ".join(filtered_word_list)
    return helper

def make_alphabet() -> str:
    vietnamese_vowel_alphabet = {
        "A", "Ă", "Â", "E", "Ê", "I", "Y", "O", "Ô", "Ơ", "U", "Ư",
        "Á", "Ắ", "Ấ", "É", "Ế", "Í", "Ý", "Ó", "Ố", "Ớ", "Ú", "Ứ",
        "À", "Ằ", "Ầ", "È", "Ề", "Ì", "Ỳ", "Ò", "Ồ", "Ờ", "Ù", "Ừ",
        "Ả", "Ẳ", "Ẩ", "Ẻ", "Ể", "Ỉ", "Ỷ", "Ỏ", "Ổ", "Ở", "Ủ", "Ử",
        "Ã", "Ẵ", "Ẫ", "Ẽ", "Ễ", "Ĩ", "Ỹ", "Õ", "Ỗ", "Ỡ", "Ũ", "Ữ",
        "Ạ", "Ặ", "Ậ", "Ẹ", "Ệ", "Ị", "Ỵ", "Ọ", "Ộ", "Ợ", "Ụ", "Ự",
    }
    vietnamese_consonant_alphabet = "DĐ"

    english_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ'-"

    tamil_alphabet = "£×áéëìôšŸஃஅஆஇஈஉஊஎஏஐஒஓஔகஙசஜஞடணதநனபமயரறலளழவஷஸஹாிீுூெேைொோௌ்ௗ௭௶ഥ\u200c"

    number_alphabet = "0123456789"

    alphabet = {
        " ",
        *vietnamese_vowel_alphabet,
        *vietnamese_consonant_alphabet,
        *english_alphabet,
        *number_alphabet,
        *tamil_alphabet,
    }

    return "".join(alphabet).upper()

PIPELINE = [
    to_upper(),
    remove_tag(),
    limit_alphabet(make_alphabet()),
]

class TextCounter:
    def __init__(self) -> str:
        self.char_count = 0
        self.word_count = 0
        self.line_count = 0
    
    def __str__(self):
        return str({
            "char_count": self.char_count,
            "word_count": self.word_count,
            "line_count": self.line_count,
        })
    def __repr__(self) -> str:
        return self.__str__()

    def count(self, line: str):
        self.line_count += 1
        self.word_count += len(line.split())
        self.char_count += len("".join(line.split()))

def clean_line(line: str) -> str:
    for mapping in PIPELINE:
        line = mapping(line)
    return line

def main(data_dir: str):
    # BACKUP TEXT
    if not os.path.exists(f"{data_dir}/text_uncleaned"):
        shutil.copyfile(f"{data_dir}/text", f"{data_dir}/text_uncleaned")

    # CLEAN TEXT
    before_text_counter = TextCounter()
    after_text_counter = TextCounter()

    text_list = []

    with open(f"{data_dir}/text_uncleaned") as f:
        for utt, line in read_table(
            it=tqdm(list(f), desc="reading text_uncleaned ..."),
            ts=(str, str),
        ):
            line = line.strip()
            before_text_counter.count(line)

            # clean
            line = clean_line(line)
            # end clean

            after_text_counter.count(line)

            #
            text_list.append((utt, line))

    # LOG
    print(f"before_text_counter: {before_text_counter}")
    print(f"after_text_counter: {after_text_counter}")

    # WRITE
    with open(f"{data_dir}/text", "w") as f:
        for utt, line in text_list:
            f.write(f"{utt} {line}\n")


if __name__ == "__main__":
    main(sys.argv[1].rstrip("/"))