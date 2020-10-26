from collections import defaultdict
import json
from matplotlib import rcParams
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import re

rcParams.update(json.loads(open("matplot_conf.json").read()))

MILLION = 1_000_000

class Style:
    def __init__(self, color, marker, marker_size, hatch):
        self.color = color
        self.marker = marker
        self.marker_size = marker_size
        self.hatch = hatch


ROCKS = ('PmemRocksDb', "RocksDB")
PMEMKV = ('PmemKV', "PmemKV")
VIPER = ('Viper', 'Viper')
DRAM_MAP = ('DramMap', 'TBB Map')
HYBRID_FASTER = ('PmemHybridFaster', 'FASTER')
NVM_FASTER = ('NvmFaster', 'FASTER-NVM')
CCEH = ('Cceh', 'CCEH')

ALL_FIXTURES = [VIPER, ROCKS, PMEMKV, DRAM_MAP, HYBRID_FASTER, NVM_FASTER, CCEH]
ALL_BM_TYPES = [
    'insert', 'get', 'update', 'delete',
    '5050_uniform', '1090_uniform', '5050_zipf', '1090_zipf',
]

#           red        blue       green      grey       purple     turquoise
COLORS =  ['#990000', '#000099', '#006600', '#404040', '#990099', '#666600']
MARKERS = [('X', 12), ("s", 8),  ("d", 10), ("o", 10), ("^", 10), ('v', 10)]

STYLES = {
    VIPER[0]:         Style('#990000', 'X', 12, 'x'),
    DRAM_MAP[0]:      Style('#404040', 'o', 10, '//'),
    CCEH[0]:          Style('#000099', 's',  8, '//'),
    PMEMKV[0]:        Style('#006600', 'd', 10, ''),
    NVM_FASTER[0]:    Style('#666600', 'v', 10, '\\\\'),
    HYBRID_FASTER[0]: Style('#990099', '^', 10, '-'),
    # ROCKS[0]:         Style('#000099', 's',  8, ''),
}

def get_bm_type(bm_type_str):
    for t in ALL_BM_TYPES:
        if t in bm_type_str:
            return t
    raise RuntimeError(f"Unknown bm_type: {bm_type_str}")

def get_benchmarks(bms, fixtures=ALL_FIXTURES, types=ALL_BM_TYPES):
    runs = defaultdict(list)
    for bm in bms:
        for (fixture, _) in fixtures:
            bm_type = get_bm_type(bm['name'])
            if fixture in bm['name']:
                runs[(fixture, bm_type)].append(bm)
                break

    for fixture, _ in fixtures:
        for bm_type in types:
            runs[(fixture, bm_type)].sort(key=lambda x: x['threads'])

    return runs


def get_all_runs(result_json_file):
    with open(result_json_file) as rj_f:
        results_raw = json.loads(rj_f.read())
        return results_raw["benchmarks"]
