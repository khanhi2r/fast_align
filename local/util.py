from __future__ import annotations
from typing import *
import os
import sys
import concurrent.futures
from tqdm import tqdm
from audio_toolkit import AudioStatsV4
import statistics
import itertools
import duckdb


def fs_walk(root_dir: str, skip_links: bool=False, skip_mounts: bool=False) -> Iterable[str]:
    """
    iterate over all files in a directory
    """
    for name in os.listdir(root_dir):
        path = os.path.join(root_dir, name)
        if skip_links and os.path.islink(path):
            continue
        if skip_mounts and os.path.ismount(path):
            continue
        

        if os.path.isfile(path):
            yield path
        elif os.path.isdir(path):
            yield from fs_walk(
                root_dir=path,
                skip_links=skip_links,
                skip_mounts=skip_mounts,
            )
        else:
            print(f"skipped {path} due to unknown file type")

def read_table(it: Iterable[str], ts: Iterable[type], sep: Optional[str] = None) -> Iterator:
    ts = tuple(ts)
    for l in it:
        l = l.rstrip("\n")
        if len(l) == 0:
            continue
        if l.startswith("#"): # comments
            continue
        splits = l.split(sep=sep, maxsplit=len(ts)-1)
        while len(splits) < len(ts):
            splits.append("")

        yield tuple(ts[i](s) for i, s in enumerate(splits))

def read_csv(filename: str, sep: str | None = None, fetch_batch_size: int = 1000000) -> Iterator:
    with tqdm(desc=f"loading {filename} ...") as pbar:
        handle = duckdb.read_csv(filename, sep=sep, header=False)
        while True:
            row_list = handle.fetchmany(fetch_batch_size)
            if len(row_list) == 0:
                break
            yield from row_list
            pbar.update(len(row_list))

class Counter:
    def __init__(self):
        self.count_dict = {}
    
    def add(self, key: Hashable, addon: Union[int, float] = 1):
        self.count_dict[key] = addon + self.count_dict.get(key, 0)
    
    def update(self, i: Iterable[Hashable]):
        for key in i:
            self.add(key)

    def __dict__(self) -> dict:
        return self.count_dict
    
    def __iter__(self) -> Iterator[Tuple[Hashable, int]]:
        count_list = [(k, v) for k, v in self.count_dict.items()]
        count_list.sort(key=lambda pair: pair[1], reverse=True)
        return iter(count_list)

    def __repr__(self) -> str:
        return self.count_dict.__repr__()

def data_write(
    data_dst_dir: str,
    text_list: List[Tuple[str, str]], utt2spk_list: List[Tuple[str, str]], wavscp_list: List[Tuple[str, str]], segments_list: Optional[List[Tuple[str, str, float, float]]] = None,
    utt2dur_list: Optional[List[Tuple[str, float]]] = None ,
    sanitize_utt: bool = True, check_wav_exist: bool = False,
):
    data_name = os.path.basename(data_dst_dir)
    def sanitize(s: str) -> str:
        if not sanitize_utt:
            return s
        if not s.startswith(f"{data_name}-"):
            s = f"{data_name}-{s}"
        return s

    if not os.path.exists(data_dst_dir):
        os.makedirs(data_dst_dir)

    with open(f"{data_dst_dir}/text", "w") as f:
        for utt, text in text_list:
            f.write(f"{sanitize(utt)} {text}\n")
    print(f"write {len(text_list)} utterances to {data_dst_dir}/text")
            
    with open(f"{data_dst_dir}/utt2spk", "w") as f:
        for utt, spk in utt2spk_list:
            f.write(f"{sanitize(utt)} {spk}\n")
    print(f"write {len(utt2spk_list)} utterances to {data_dst_dir}/utt2spk")

    with open(f"{data_dst_dir}/wav.scp", "w") as f:
        for rec, wav in wavscp_list:
            if check_wav_exist and (not os.path.exists(wav)):
                raise RuntimeError(f"wav not exist \"{wav}\"")
            f.write(f"{sanitize(rec)} {wav}\n")
    print(f"write {len(wavscp_list)} recordings to {data_dst_dir}/wav.scp")

    if segments_list is not None:
        with open(f"{data_dst_dir}/segments", "w") as f:
            for utt, rec, beg, end in segments_list:
                f.write(f"{sanitize(utt)} {sanitize(rec)} {beg} {end}\n")
        print(f"write {len(segments_list)} utterances to {data_dst_dir}/segments")
    
    if utt2dur_list is not None:
        with open(f"{data_dst_dir}/utt2dur", "w") as f:
            for utt, dur in utt2dur_list:
                f.write(f"{sanitize(utt)} {dur}\n")
        print(f"write {len(utt2dur_list)} utterances to {data_dst_dir}/utt2dur")


def data_read(data_src_dir: str, text_encoding: str = "utf-8"):
    data_src_dir = data_src_dir.rstrip("/")

    text_list = []
    for utt, text in read_table(it=open(f"{data_src_dir}/text", encoding=text_encoding), ts=(str, str)):
        text_list.append((utt, text))
    
    utt2spk_list = []
    if os.path.exists(f"{data_src_dir}/utt2spk"):
        for utt, spk in read_table(it=open(f"{data_src_dir}/utt2spk"), ts=(str, str)):
            utt2spk_list.append((utt, spk))
    else:
        for utt, _ in text_list:
            utt2spk_list.append((utt, utt))
    
    wavscp_list = []
    for rec, wav in read_table(it=open(f"{data_src_dir}/wav.scp"), ts=(str, str)):
        wavscp_list.append((rec, wav))
    
    segments_list = None
    if os.path.exists(f"{data_src_dir}/segments"):
        segments_list = []
        for utt, rec, beg, end in read_table(it=open(f"{data_src_dir}/segments"), ts=(str, str, float, float)):
            segments_list.append((utt, rec, beg, end))
    return text_list, utt2spk_list, wavscp_list, segments_list

def make_segments_list(wavscp_list: List[Tuple[str, str]], stats: AudioStats) -> List[Tuple[str, str, float, float]]:
    segments_list = []
    for rec, wav in tqdm(wavscp_list, desc="making segments from wavscp"):
        o = stats.get(wav)
        dur = o["frame_count"] / o["sample_rate"]
        segments_list.append((rec, rec, 0, dur))
    return segments_list

def data_copy(
    data_src_dir: str, data_dst_dir: str,
    wav_map: Optional[Callable[[str], str]] = None,
    text_encoding: str = "utf-8",
    sanitize_utt: bool = True,
    check_wav_exist: bool = False,
    remove_utt2spk: bool = True,
):
    data_src_dir = data_src_dir.rstrip("/")
    
    # READ
    text_list, utt2spk_list, wavscp_list, segments_list = data_read(data_src_dir, text_encoding=text_encoding)

    if wav_map is not None:
        wavscp_list = [(utt, wav_map(wav)) for utt, wav in wavscp_list]

    # WRITE
    data_write(
        data_dst_dir=data_dst_dir,
        text_list=text_list,
        utt2spk_list=utt2spk_list,
        wavscp_list=wavscp_list,
        segments_list=segments_list,
        sanitize_utt=sanitize_utt,
        check_wav_exist=check_wav_exist,
    )


class Executor:
    def __init__(self, f: Callable, max_workers: int | None = None):
        self.f = f
        self.max_workers = max_workers

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

    def thread_iter_map(self, kvs: List[Dict]) -> Iterator:
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as e:
            future_list = [e.submit(self.f, **kv) for kv in kvs]
            for future in concurrent.futures.as_completed(future_list):
                yield future.result()

    def process_iter_map(self, kvs: List[Dict]) -> Iterator:
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers) as e:
            future_list = [e.submit(self.f, **kv) for kv in kvs]
            for future in concurrent.futures.as_completed(future_list):
                yield future.result()


def executor(max_workers: int | None = None):
    def transform(f: Callable):
        return Executor(f, max_workers=max_workers)

    return transform


def update_scp_path(root_dir: str, from_path: str, to_path: str):
    for path in fs_walk(root_dir=root_dir):
        if not path.endswith(".scp"):
            continue
        text = open(path, "r").read()
        text = text.replace(from_path, to_path)
        open(path, "w").write(text)
        print(f"updated {path}")

flat_map = lambda f, xs: (y for ys in xs for y in f(ys))

def print_duration_stats(duration_list: List[float]):
    if len(duration_list) > 0:
        print(f" total duration: {round(sum(duration_list) / 3600, 2)} hours or {sum(duration_list)} seconds")
        print(f" quantiles duration in hours: {[round(min(duration_list) / 3600, 2), ] + [round(q / 3600, 2) for q in statistics.quantiles(duration_list, n=10)] + [round(max(duration_list) / 3600, 2), ]}")
        print(f" quantiles duration in seconds: {[round(min(duration_list), 2), ] + [round(q, 2) for q in statistics.quantiles(duration_list, n=10)] + [round(max(duration_list), 2), ]}")


def read_rttm(rttm_path: str) -> Tuple[int, str, float, float, str]:
    for line_type, rec, channel, onset, duration, orthography, s_type, s_name, confidence, lookahead in read_table(
        it=open(rttm_path, "r"),
        ts=(str, str, int, float, float, str, str, str, str, str),
    ):
        # sanity
        assert line_type == "SPEAKER"
        assert orthography == "<NA>"
        assert s_type == "<NA>"
        assert confidence == "<NA>"
        assert lookahead == "<NA>"

        yield channel, rec, onset, duration, s_name

def write_rttm(rttm_path: str, rttm: Iterable[Tuple[int, str, float, float, str]]):
    with open(rttm_path, "w") as f:
        for channel, rec, onset, duration, s_name in rttm:
                f.write(f"SPEAKER {rec} {channel} {onset:.3f} {duration:.3f} <NA> <NA> {s_name} <NA> <NA>\n")

def fs_walk_utt(root_dir: str, ext: str, skip_links: bool=False, skip_mounts: bool=False) -> Dict[str]:
    path_dict = {}
    for path in fs_walk(root_dir=root_dir, skip_links=skip_links, skip_mounts=skip_mounts):
        if not path.endswith(f"{ext}"):
            continue
        utt = os.path.basename(path)[:-len(ext)]
        assert utt in path, print(utt, path)
        path_dict[utt] = path
    return path_dict

def match_list_exact(l: list[str], s: list[str]) -> bool:
    if len(l) != len(s):
        return False
    for c1, c2 in zip(l, s):
        if c1 != c2:
            return False
    return True

def match_sublist(l: list[str], s: list[str]) -> int | None:
    if len(l) < len(s):
        return
    for i in range(len(l)-len(s)+1):
        if match_list_exact(l[i:i+len(s)], s):
            yield i

def iter_subset(n: int, m: int | None = None) -> Iterator[int]:
    if m is None:
        for m in range(0, n+1, 1):
            yield from iter_subset(n=n, m=m)
    else:
        yield from itertools.combinations(range(n), m)
