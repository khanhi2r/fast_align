from tqdm import tqdm
import random

data_path = "data/data.en-bm"
alignment_path = f"{data_path}.reversealign"
codeswitch_path = f"{data_path}.reversecodeswitch"

line1_list = []
line2_list = []

for line in open(data_path):
    line1, line2 = line.split("|||")
    
    line1 = line1.strip().split()
    line2 = line2.strip().split()

    line1_list.append(line1)
    line2_list.append(line2)

alignment_list = []

for line in open(alignment_path):
    alignment = []
    for pair in line.split():
        i_s, j_s = pair.split("-")
        i, j = int(i_s), int(j_s)
        alignment.append((i, j))
    alignment_list.append(alignment)

with open(codeswitch_path, "w") as f:
    count = 0
    for line1, line2, alignment in tqdm(zip(line1_list, line2_list, alignment_list), desc="sampling ...", total=len(line1_list)):
        if len(line1) < 4 or len(line2) < 4:
            continue

        line2_edge_list = [None for _ in line2]
        line2_degree_list = [0 for _ in line2]
        for i, j in alignment:
            line2_degree_list[j] += 1
            line2_edge_list[j] = i

        line2_degree1_index_list = []
        for index in range(len(line2_degree_list)):
            if line2_degree_list[index] == 1:
                line2_degree1_index_list.append(index)
        
        
        random.shuffle(line2_degree1_index_list)
        line2_degree1_index_list = line2_degree1_index_list[:int(len(line2) * 0.2)] # take at most 20% of line2

        line_out = [*line2] # copy line2
        for j in line2_degree1_index_list:
            i = line2_edge_list[j]
            line_out[j] = line1[i]
        
        f.write(" ".join(line_out) + "\n")
        count += 1
    
    print(f"{count} lines written")





