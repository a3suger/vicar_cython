import vicar

if __name__ == "__main__":
    import random
    import math
    import sys

    v = vicar.Vicar()
    v.not_make_pattern.append(30)
    line_item_set = bytearray(vicar.Vicar.SIZE_OF_ITEM * vicar.Vicar.N)
    for ii in range(0xFFFF):
        for i in range(vicar.Vicar.N):
            rand_val: int = int(math.log2(random.randint(1, 1023)) * 10.0) + 100
            byte_val = rand_val.to_bytes(vicar.Vicar.SIZE_OF_ITEM, sys.byteorder)
            line_item_set[i * vicar.Vicar.SIZE_OF_ITEM:(i + 1) * vicar.Vicar.SIZE_OF_ITEM] = byte_val
        v.insert_line_item_set(line_item_set, ii)
