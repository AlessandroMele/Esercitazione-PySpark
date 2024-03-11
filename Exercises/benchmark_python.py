from time import time

i_time = time()
to_remove = [',',':',';','.','!','?']
file_path = '/home/user/Downloads/romeo_and_juliet.txt'

with open(file_path,'r') as file:
    text = file.read()
    words = text.split(' ')

    filtered_words = []
    for word in words:
        if len(word) != 0 and word[-1] not in to_remove:
            filtered_words.append(word)

    counter = {}
    for f_word in filtered_words:
        if f_word in counter.keys():
            counter[f_word] += 1
        else:
            counter[f_word] = 1

    f_counter = {}
    for key, occ in f_counter.items():
        if occ > 5:
            f_counter[key] = occ

f_time = time()
print(f'Done with Python in {f_time-i_time} s.')