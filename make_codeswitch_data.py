from build.force_align import Aligner

parallel_data_path = "data/data.en-bm"

aligner = Aligner()

with open(parallel_data_path) as f:
    for line in f:
