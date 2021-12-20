//
// Created by bysoulwarden on 2021/12/9.
//

#ifndef WORM_RE_HPP
#define WORM_RE_HPP

#include "../common_index.hpp"
#include "lib.h"
#include "wh.h"

namespace viper::index {
    template<typename K>
    class WormCare :public BaseIndex<K>{
    public:
        struct wormhole * wh;
        struct wormref * ref;
        WormCare(){
            wh = wh_create();
            ref = wh_ref(wh);
        }
        ~WormCare(){
            wh_unref(ref);
            wh_destroy(wh);
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            uint64_t offset=o.get_offset();
            wh_put(ref, &k, 8, &offset, 8);
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            uint64_t offset;
            uint32_t len_out;
            bool r = wh_get(ref, &k, 8, &offset, 8, &len_out);
            return KeyValueOffset((uint64_t)offset);
        }
    };
}


#endif //WORM_CARE_HPP
