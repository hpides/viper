//
// Created by bysoulwarden on 2021/12/7.
//

#ifndef VIPER_CUCKOO_CARE_HPP
#define VIPER_CUCKOO_CARE_HPP

#include "../common_index.hpp"
#include "cuckoohash_map.hh"


namespace viper::index {
    template<typename K>
    class CuckooCare :public BaseIndex<K>{
    public:
        libcuckoo::cuckoohash_map<K,KeyValueOffset> map;
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            map.insert_or_assign(k,o);
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            KeyValueOffset o;
            map.template find(k,o);
            return o;
        }
        using storage_value_type = std::pair<K, KeyValueOffset>;
        typedef typename std::aligned_storage<sizeof(storage_value_type),alignof(storage_value_type)>::type t;
        uint64_t GetIndexSize(){
            return map.capacity()*sizeof(t);
        }
    };
}

#endif //VIPER_CUCKOO_CARE_HPP
