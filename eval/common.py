from collections import defaultdict
import json
from matplotlib import rcParams
import numpy as np
import matplotlib.pyplot as plt
from pprint import pprint
import re

# rcParams.update(json.loads(open("matplot_conf.json").read()))

MILLION = 1_000_000

class Style:
    def __init__(self, color, marker, marker_size, hatch):
        self.color = color
        self.marker = marker
        self.marker_size = marker_size
        self.hatch = hatch


ROCKS = ('PmemRocksDb', "PMem-RocksDB")
PMEMKV = ('PmemKV', "pmemkv")
VIPER = ('Viper', 'Viper')
DRAM_MAP = ('DramMap', 'TBB Map')
DASH = ('Dash', 'Dash')
UTREE = ('UTree', 'µTree')
CRL_STORE = ('Crl', 'CrlStore')
HYBRID_FASTER = ('PmemHybridFaster', 'FASTER')
NVM_FASTER = ('NvmFaster', 'FASTER-PMem')
CCEH = ('Cceh', 'CCEH')
VIPER_PMEM = ('PmemVip', 'PMem')
VIPER_DRAM = ('DramVip', 'DRAM')
VIPER_UNALIGNED = ('UnalignedVip', 'Unaligned')

ALL_FIXTURES = [
    VIPER, ROCKS, PMEMKV, HYBRID_FASTER, DASH,
    # VIPER_PMEM,
    # DRAM_MAP, CCEH, NVM_FASTER,
]

ALL_BM_TYPES = [
    'insert', 'get', 'update', 'delete',
    '5050_uniform', '1090_uniform', '5050_zipf', '1090_zipf',
]

#           red        blue       green      grey       purple     turquoise
COLORS =  ['#990000', '#000099', '#006600', '#404040', '#990099', '#666600']
MARKERS = [('X', 12), ("s", 8),  ("d", 10), ("o", 10), ("^", 10), ('v', 10)]

# THREAD_TO_MARKER = {
#     1: 'x', 2: '>', 4: 's', 6: '<', 8: 'o', 16: '^', 18: 'v', 24: '.', 32: 'P', 36: 'D'
# }
# THREAD_TO_COLOR = {
#     1: '#000000', 2: '#009999', 4: '#4C0099', 6: '#999900',
#     8: '#990000', 16: '#CC6600', 18: '#0080FF', 24: '#009900',
#     32: '#CCCC00', 36: '#404040',
# }
DRAM_COLOR = '#0080FF'
PMEM_COLOR = '#303030'

STYLES = {
    VIPER[0]:         Style('#990000',  '.', 12, 'x'),
    PMEMKV[0]:        Style('#009900',  'D', 10, ''),
    HYBRID_FASTER[0]: Style('#990099',  '^', 10, '\\\\'),
    DASH[0]:          Style(PMEM_COLOR, 'o', 10, '//'),
    ROCKS[0]:         Style('#000099',  'v',  8, ''),
    UTREE[0]:         Style('#999900',  '>',  8, ''),
    CRL_STORE[0]:     Style(DRAM_COLOR, 's',  8, ''),

    VIPER_PMEM[0]:      Style(PMEM_COLOR, 'o', 10, 'o'),
    VIPER_DRAM[0]:      Style(DRAM_COLOR, 's',  8, '//'),
    VIPER_UNALIGNED[0]: Style('#990000',  's',  8, '//'),
    CCEH[0]:          Style('#000099', 's',  8, '//'),
    # NVM_FASTER[0]:    Style('#666600', 'v', 10, '-'),
    # DRAM_MAP[0]:      Style('#404040', 'o', 10, '//'),
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


def hide_border(ax, show_left=False):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(True)
    ax.spines['left'].set_visible(show_left)
