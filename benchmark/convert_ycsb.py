import re
import sys
import struct
import codecs

INSERT_RE = re.compile(r'^(INSERT|UPDATE) usertable user(\d+) \[ field0=(.{200}) .*$')
READ_RE = re.compile(r'^READ usertable user(\d+) .*$')
BIN_UINT64_T_FORMAT = '<Q'
BIN_UINT32_T_FORMAT = '<I'

OPS = { 'INSERT': 0, 'GET': 1, 'UPDATE': 2 }

def convert_file(in_file, out_file):
    with open(in_file) as in_f, open(out_file, 'wb') as out_f:
        for line_num, line in enumerate(in_f):
            matched = False
            match = INSERT_RE.match(line)
            if match is not None:
                op = match.group(1)
                key = int(match.group(2))
                value = match.group(3)
                matched = True

            match = READ_RE.match(line)
            if match is not None:
                op = 'GET'
                key = int(match.group(1))
                value = "a" * 200
                matched = True

            if not matched:
                continue

            # print(f"op: {op} | key: {key} | value: {value}")
            bytes = struct.pack(BIN_UINT32_T_FORMAT, OPS[op]) + \
                    struct.pack(BIN_UINT64_T_FORMAT, key) + \
                    value.encode('UTF-8')
            out_f.write(bytes)

            # if line_num % 1000000 == 0:
            #     print(f"Converted {line_num} records")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Need two args, infile outfile")
        sys.exit(1)
    convert_file(sys.argv[1], sys.argv[2])
