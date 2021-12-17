//
// Created by bysoulwarden on 2021/12/9.
//

#ifndef VIPER_MASS_CARE_HPP
#define VIPER_MASS_CARE_HPP

#include "../common_index.hpp"
#include "masstree.h"

namespace viper::index {
    template<typename K>
    class MassCare :public BaseIndex<K>{
    public:
        masstree_t *mt;
        MassCare(){
            mt = masstree_create(NULL);
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            masstree_put(mt, &k, 8, (void*)o.get_offset());
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            void* tmp = masstree_get(mt, &k, 8);
            return KeyValueOffset((uint64_t)tmp);
        }
    };
}


#endif //VIPER_MASS_CARE_HPP
