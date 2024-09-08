import hashlib
import sys

from typing import List


def output_at_count_up(counter, index, item_set, stored_data, start, end, count):
    buf: str = ''
    for i in range(Vicar.N):
        raw_bytes = stored_data[i * Vicar.SIZE_OF_ITEM:(i + 1) * Vicar.SIZE_OF_ITEM]
        if Vicar.is_nth_item_a_wildcard(item_set, i):
            value = int.from_bytes(raw_bytes, byteorder=sys.byteorder)
            buf = buf + ' {:s}:{:8d}'.format("C", value)
        else:
            buf = buf + ' {:s}:{:8s}'.format("I", raw_bytes.hex())
    print('OUT:{:10d} {:10d} {:10d} {:10d} {:5d} {}'.format(counter, index, start, end - start, count, buf))


def output_at_rewrite(counter, index, item_set, stored_date, start, end, count):
    print('REW:{:10d} {:10d} {:10d} {:10d} {:5d}'.format(counter, index, start, end - start, count))


def my_hash(rawdata :bytearray, value:int) -> int:
    target :bytearray = rawdata.copy()
    target.append(value)
    return int.from_bytes(hashlib.md5(target).digest(), byteorder=sys.byteorder)


class Vicar:
    N: int = 5
    SIZE_OF_ITEM: int = 4
    SIZE_OF_TABLE: int = 0xFFFFFF
    STORED_WILDCARD = bytes(b'\xff\xff\xff\xff')
    WILDCARD = int.from_bytes(STORED_WILDCARD, sys.byteorder)
    THRESHOLD: int = 128
    MAX_RETRY: int = 4

    def __init__(self):
        self.hash_function = my_hash
        self.output_at_count_up_function = output_at_count_up
        self.output_at_rewrite_function = output_at_rewrite
        self.ITME_SET_UNUSED = bytearray([255] * Vicar.SIZE_OF_ITEM * Vicar.N)
        self.item_set_table_data: List[bytearray] = []
        for i in range(Vicar.SIZE_OF_TABLE):
            self.item_set_table_data.append(self.ITME_SET_UNUSED.copy())
        self.item_set_table_counter: List[int] = [0] * Vicar.SIZE_OF_TABLE
        self.item_set_table_start: List[int] = [0] * Vicar.SIZE_OF_TABLE
        self.item_set_table_end: List[int] = [0] * Vicar.SIZE_OF_TABLE
        self.line_counter: int = 0
        self.not_make_pattern: List[int] = [2 ** Vicar.N - 1]  # all bit are WILDCARD
        self.used_item_set :int = 0

    @classmethod
    def set_nth_item_a_wildcard(cls, item_set: bytearray, n: int) -> bytearray:
        byte_length : int = Vicar.SIZE_OF_ITEM
        ret_val = bytearray(byte_length)
        ret_val[:] = item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM]
        item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM] = bytes([255] * Vicar.SIZE_OF_ITEM)
        return ret_val

    @classmethod
    def restore_nth_item(cls, item_set: bytearray, n: int, value: bytearray) -> None:
        item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM] = value

    @classmethod
    def is_nth_item_a_wildcard(cls, item_set: bytearray, n: int) -> bool:
        comp_value :bytes = bytes([255] * Vicar.SIZE_OF_ITEM)
        target_value :bytearray = item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM]
        return comp_value == target_value

    @classmethod
    def reset_nth_cardinality_counter(cls, item_set: bytearray, n: int) -> None:
        count: int = 1
        item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM] = count.to_bytes(Vicar.SIZE_OF_ITEM,
                                                                                       sys.byteorder)

    @classmethod
    def count_up_nth_cardinality_counter(cls, item_set: bytearray, n: int) -> None:
        count: int = int.from_bytes(item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM], sys.byteorder)
        count += 1
        item_set[n * Vicar.SIZE_OF_ITEM:(n + 1) * Vicar.SIZE_OF_ITEM] = count.to_bytes(Vicar.SIZE_OF_ITEM,
                                                                                       sys.byteorder)

    def insert_line_item_set(self, line_item_set: bytearray, time_data: int):
        # A function that checks whether the item set satisfies the constraints.
        # constraints: When item values are expressed as bits, all bits are not allowed to be 1.
        def is_satisfied_a_constraint(item_set: bytearray) -> bool:
            for i  in range(Vicar.N):
                if Vicar.is_nth_item_a_wildcard(item_set, i):
                    return False
            return True

        def calculate_cardinality(base_index: int, compair_index: int, n: int) -> None:
            if base_index >= self.SIZE_OF_TABLE or compair_index >= self.SIZE_OF_TABLE:
                return
            if self.item_set_table_counter[compair_index] == 1 and self.item_set_table_counter[base_index] != 1:
                self.count_up_nth_cardinality_counter(self.item_set_table_data[base_index], n)

        def table_store(item_set: bytearray, time_data: int) -> int:
            # The return value of this function is greater than or equal to 0 and less than or equal to SIZE_OF_TABLE.
            def hash_for_index(raw_data: bytearray, value: int) -> int:
                val : int = self.hash_function(raw_data, value)
                return val & Vicar.SIZE_OF_TABLE

            def set_entry_into_table(index: int, item_set: bytearray, time_data: int) -> int:
                store_data: bytearray = self.item_set_table_data[index]
                store_data[:] = item_set
                for i in range(Vicar.N):
                    if Vicar.is_nth_item_a_wildcard(item_set, i):
                        Vicar.reset_nth_cardinality_counter(store_data, i)
                self.item_set_table_counter[index] = 1
                self.item_set_table_start[index] = time_data
                self.item_set_table_end[index] = time_data
                self.used_item_set += 1
                return index

            def update_counter_for_table(index: int, item_set: bytearray, time_data: int) -> int:
                self.item_set_table_counter[index] += 1
                self.item_set_table_end[index] = time_data
                if self.item_set_table_counter[index] >= Vicar.THRESHOLD:
                    # start print out for over threshold
                    self.output_at_count_up_function(self.line_counter, index, item_set,
                                                     self.item_set_table_data[index],
                                                     self.item_set_table_start[index], self.item_set_table_end[index],
                                                     self.item_set_table_counter[index])
                    # end print for over threshold
                    set_entry_into_table(index, item_set, time_data)
                return index

            def rewrite_entry_into_table(index: int, item_set: bytearray, time_data: int) -> int:
                # start print out_for rewrite
                self.output_at_rewrite_function(self.line_counter, index, item_set, self.item_set_table_data[index],
                                                self.item_set_table_start[index], self.item_set_table_end[index],
                                                self.item_set_table_counter[index])
                # end print out for rewrite
                self.used_item_set -= 1
                return set_entry_into_table(index, item_set, time_data)

            def item_set_compair(a: bytearray, b: bytearray) -> bool:
                for i in range(Vicar.N):
                    if Vicar.is_nth_item_a_wildcard(b, i):
                        continue
                    if (a[i * Vicar.SIZE_OF_ITEM:(i + 1) * Vicar.SIZE_OF_ITEM] !=
                            b[i * Vicar.SIZE_OF_ITEM:(i + 1) * Vicar.SIZE_OF_ITEM]):
                        return False
                return True

            def is_none_item_set(item_set: bytearray) -> bool:
                return item_set == self.ITME_SET_UNUSED

            indexes: List[int] = [Vicar.SIZE_OF_TABLE] * Vicar.MAX_RETRY
            value: int = 0
            counter: int = 0
            while True:
                indexes[counter] = hash_for_index(item_set, value)
                if indexes[counter] == Vicar.SIZE_OF_TABLE:
                    value += 1
                    continue
                stored_data: bytearray = self.item_set_table_data[indexes[counter]]
                if is_none_item_set(stored_data):
                    # This item set is not entered in the table.
                    return set_entry_into_table(indexes[counter], item_set, time_data)
                if item_set_compair(stored_data, item_set):
                    # This item set is already entered in the table.
                    return update_counter_for_table(indexes[counter], item_set, time_data)
                counter += 1
                value += 1
                if counter == Vicar.MAX_RETRY:
                    break
            min_sub_index :int = 0
            min_counter :int = self.item_set_table_counter[min_sub_index]
            min_last :int = self.item_set_table_end[min_sub_index]
            for counter in range(1, Vicar.MAX_RETRY):
                if min_counter > self.item_set_table_counter[counter]:
                    min_sub_index = counter
                elif min_counter == self.item_set_table_counter[min_sub_index]:
                    if min_last < self.item_set_table_end[min_sub_index]:
                        min_sub_index = counter
            return rewrite_entry_into_table(indexes[min_sub_index], item_set, time_data)

        def recursion(item_set: bytearray, n: int, count: int, index_list: List[int], time_data: int) -> int:
            if n == Vicar.N - 1:
                if count not in self.not_make_pattern:
                    index_list[count] = table_store(item_set, time_data)
                else:
                    index_list[count] = self.SIZE_OF_TABLE
                count += 1
                if count not in self.not_make_pattern:
                    tmp = Vicar.set_nth_item_a_wildcard(item_set, n)
                    index_list[count] = table_store(item_set, time_data)
                    Vicar.restore_nth_item(item_set, n, tmp)
                    calculate_cardinality(index_list[count], index_list[count - 1], n)
                else:
                    index_list[count] = self.SIZE_OF_TABLE
                count += 1
                return count
            count_nega :int = recursion(item_set, n + 1, count, index_list, time_data)
            tmp : bytearray = Vicar.set_nth_item_a_wildcard(item_set, n)
            count_next :int = recursion(item_set, n + 1, count_nega, index_list, time_data)
            Vicar.restore_nth_item(item_set, n, tmp)
            for i in range(count_next - count_nega):
                calculate_cardinality(index_list[count_nega + i], index_list[count + i], n)
            return count_next

        if is_satisfied_a_constraint(line_item_set):
            recursion(line_item_set, 0, 0, [Vicar.SIZE_OF_TABLE] * 2 ** Vicar.N, time_data)
        else:
            sys.stderr.write("Warning: not satisfied a constraint of input data( line no = {} )\n".format(self.line_counter))
        self.line_counter += 1


if __name__ == "__main__":
    import random
    import math

    v = Vicar()
    v.not_make_pattern.append(30)
    line_item_set = bytearray(Vicar.SIZE_OF_ITEM * Vicar.N)
    for ii in range(0xFFFF):
        for i in range(Vicar.N):
            rand_val: int = int(math.log2(random.randint(1, 1023)) * 10.0) + 100
            byte_val = rand_val.to_bytes(Vicar.SIZE_OF_ITEM, sys.byteorder)
            line_item_set[i * Vicar.SIZE_OF_ITEM:(i + 1) * Vicar.SIZE_OF_ITEM] = byte_val
        v.insert_line_item_set(line_item_set, ii)
