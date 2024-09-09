import random
import math
import os
import subprocess
import argparse
import struct
import sys
import array
import datetime
import ipaddress

import vicar
from wheel.macosx_libfile import read_data

vicar.Vicar.N = 3
vicar.Vicar.THRESHOLD = 4096

time_size: int = 4
line_size: int = time_size + vicar.Vicar.SIZE_OF_ITEM * vicar.Vicar.N


def make_string(ba: bytearray) -> str:
    offset = 0
    buf = ''
    for i in range(vicar.Vicar.N):
        val = int.from_bytes(ba[offset: offset + vicar.Vicar.SIZE_OF_ITEM], byteorder='big')
        offset += vicar.Vicar.SIZE_OF_ITEM
        buf += ' {}'.format(val)
    return buf


def writer_from_palo_log():
    def make_ip_packed(ip_str: str) -> bytes:
        ip = ipaddress.ip_address(ip_str)
        return ip.packed

    def make_ex_port_packed(proto_str: str, port_str: str) -> bytes:
        byte_data = bytearray(4)
        proto_num = 0
        if proto_str == 'tcp':
            proto_num = 6
        elif proto_str == 'udp':
            proto_num = 17
        byte_data[0:2] = proto_num.to_bytes(2, byteorder='big')
        port_num = int(port_str)
        byte_data[2:4] = port_num.to_bytes(2, byteorder='big')
        return byte_data

    def make_timestamp(time_str) -> bytes:
        s_format = '%Y/%m/%d %H:%M:%S'
        dt = datetime.datetime.strptime(time_str, s_format)
        num = int(dt.timestamp()) & 0xFFFFFFFF
        return num.to_bytes(4, byteorder='big')

    line_item_set = bytearray(line_size)
    while True:
        read_data = sys.stdin.readline()
        if read_data == '':
            break
        items = read_data.split(',')
        offset = 0
        line_item_set[offset:offset + 4] = make_timestamp(items[1])
        offset += 4
        line_item_set[offset:offset + 4] = make_ip_packed(items[7])
        offset += 4
        line_item_set[offset:offset + 4] = make_ip_packed(items[8])
        offset += 4
        line_item_set[offset:offset + 4] = make_ex_port_packed(items[29], items[25])
        sys.stdout.buffer.write(line_item_set)


def writer():
    line_item_set = bytearray(line_size)
    offset: int = 0
    for ii in range(0xFFFF):
        offset = 0
        line_item_set[0:time_size] = ii.to_bytes(time_size, byteorder='big')
        offset += time_size
        for i in range(vicar.Vicar.N):
            rand_val: int = int(math.log2(random.randint(1, 1023)) * 10.0) + 100
            byte_val: bytes = rand_val.to_bytes(vicar.Vicar.SIZE_OF_ITEM, sys.byteorder)
            line_item_set[offset:offset + vicar.Vicar.SIZE_OF_ITEM] = byte_val
            offset += vicar.Vicar.SIZE_OF_ITEM
        sys.stdout.buffer.write(line_item_set)
        sys.stdout.flush()
        sys.stderr.write(buf + ' ' + make_string(line_item_set[time_size:]) + '\n')


def reader():
    v = vicar.Vicar()
    v.not_make_pattern.append(6) # 0b110
    line_item_set = bytearray(vicar.Vicar.SIZE_OF_ITEM * vicar.Vicar.N)
    while True:
        read_data = sys.stdin.buffer.read(line_size)
        if len(read_data) == 0:
            break
        counter = int.from_bytes(read_data[0:time_size], byteorder='big')
        line_item_set[:] = read_data[time_size:]
        v.insert_line_item_set(line_item_set, counter)

def default_main():
    vicar.Vicar.N = 5
    vicar.Vicar.THRESHOLD = 1024
    v = vicar.Vicar()
    v.not_make_pattern.append(30)
    line_item_set = bytearray(vicar.Vicar.SIZE_OF_ITEM * vicar.Vicar.N)
    for ii in range(0xFFFF):
        for i in range(vicar.Vicar.N):
            rand_val: int = int(math.log2(random.randint(1, 1023)) * 10.0) + 100
            byte_val = rand_val.to_bytes(vicar.Vicar.SIZE_OF_ITEM, sys.byteorder)
            line_item_set[i * vicar.Vicar.SIZE_OF_ITEM:(i + 1) * vicar.Vicar.SIZE_OF_ITEM] = byte_val
        v.insert_line_item_set(line_item_set, ii)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='Proc',
        description='Jikken you')
    parser.add_argument('mode', help='Specify one of the following: rand | main | write | vicar | write2')
    args = parser.parse_args()

    if args.mode == 'rand':
        default_main()
    elif args.mode == 'main':
        abs_path = os.path.abspath(__file__)
        command_w = [sys.executable, abs_path, "write"]
        command_v = [sys.executable, abs_path, "vicar"]
        p1 = subprocess.Popen(command_w, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(command_v, stdin=p1.stdout)
        p1.stdout.close()  # SIGPIPE if p2 exits.
        output = p2.communicate()[0]
    elif args.mode == 'write':
        writer_from_palo_log()
    elif args.mode == 'vicar':
        reader()
    elif args.mode == 'write2':
        writer_from_palo_log()
    else:
        default_main()
