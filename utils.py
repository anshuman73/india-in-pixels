def divide_list(l, n=BATCH_SIZE):
    for i in range(0, len(l), n):  
        yield l[i:i + n]


