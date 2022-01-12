//
// Created by bysoulwarden on 2021/12/10.
//

#ifndef VIPER_SKIPLIST_CARE_HPP
#define VIPER_SKIPLIST_CARE_HPP
#include "../common_index.hpp"
#include "skiplist.h"

namespace viper::index {

    template<typename K>
    class SkipListCare :public BaseIndex<K>{
        struct SkipListKeyComparator {
            int operator()(const SkipListKey a, const SkipListKey b)const{
                if (a.u < b.u)
                    return -1;
                if (a.u == b.u)
                    return 0;
                return 1;
            }
        }cmp;
    public:
        leveldb::SkipList<SkipListKey,SkipListKeyComparator> *map;
        leveldb::Arena * arena;
        std::mutex m;
        int threads;
        SkipListCare(){
            arena = new leveldb::Arena();
            map = new leveldb::SkipList<SkipListKey,SkipListKeyComparator>(cmp,arena);
        }
        ~SkipListCare(){
            delete map;
            delete arena;
        }
        bool SupportBulk(int threads){
            this->threads=threads;
            return false;
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            if(threads>1){
                m.lock();
            }
            SkipListKey key(k,o);
            map->Insert(key);
            if(threads>1){
                m.unlock();
            }
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            SkipListKey key(k,KeyValueOffset());
            typename leveldb::SkipList<SkipListKey,SkipListKeyComparator>::Iterator i(map);
            i.Seek(key);
            return i.key().o;
        }
    };
}
#endif //VIPER_SKIPLIST_CARE_HPP
