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
        int threads;
        std::vector<struct wormref *> refs;
        WormCare(){
            wh = wh_create();
            ref = wh_ref(wh);
        }
        ~WormCare(){
            wh_unref(ref);
            for(int i=0;i<threads;i++){
                wh_unref(refs[i]);
            }
            wh_destroy(wh);
        }
        bool SupportBulk(int threads){
            this->threads=threads;
            for(int i=0;i<threads;i++){
                struct wormref * temp=wh_ref(wh);
                refs.push_back(temp);
            }
            return false;
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
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o,uint32_t thread_id) {
            uint64_t offset=o.get_offset();
            wh_put(refs[thread_id], &k, 8, &offset, 8);
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k,uint32_t thread_id) {
            uint64_t offset;
            uint32_t len_out;
            bool r = wh_get(refs[thread_id], &k, 8, &offset, 8, &len_out);
            return KeyValueOffset((uint64_t)offset);
        }
    };
}


#endif //WORM_CARE_HPP
