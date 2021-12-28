//
// Created by bysoulwarden on 2021/12/9.
//

#ifndef VIPER_B_CARE_HPP
#define VIPER_B_CARE_HPP

#include "../common_index.hpp"
#include "../FITing-tree/btree.h"

namespace viper::index {
    template<typename K>
    class BTreeCare :public BaseIndex<K>{
    public:
        stx::btree<K, uint64_t> btree;
        BTreeCare(){
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            btree.insert(std::pair<K,uint64_t>(k,o.get_offset()));
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            typename stx::btree<K, uint64_t>::iterator i=btree.find(k);
            return KeyValueOffset((uint64_t)*i);
        }
    };
}


#endif //VIPER_B_CARE_HPP
