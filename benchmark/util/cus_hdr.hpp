#pragma once

#include <hdr_histogram.h>

namespace viper {
    namespace cus_hdr{
        int64_t hdr_total(hdr_histogram * h){
            struct hdr_iter iter;
            int64_t total = 0;
            hdr_iter_init(&iter, h);
            while (hdr_iter_next(&iter))
            {
                if (0 != iter.count)
                {
                    total += iter.count * hdr_median_equivalent_value(h, iter.value);
                }
            }
            return total;
        }
        int64_t hdr_count(hdr_histogram * h){
            return h->total_count;
        }
    }
}