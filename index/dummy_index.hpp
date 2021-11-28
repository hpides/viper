//
// Created by bysoulwarden on 2021/10/25.
//

#ifndef VIPER_DUMMY_INDEX_H
#define VIPER_DUMMY_INDEX_H

#include <hdr_histogram.h>
#include <set>
#include "common_index.hpp"
#include "FITing-tree/buffer_index.h"
#include "FITing-tree/inplace_index.h"

namespace viper::index {
    template<typename KeyType>
    class DummyIndex : public BaseIndex<KeyType>{
    public:
        int index_type;
        DummyIndex(int type){
            index_type=type;
        }
        bool SupportBulk(){
            return true;
        }
        BaseIndex<KeyType>* bulk_load(std::vector<std::pair<uint64_t, KeyValueOffset>> * vector,hdr_histogram * bulk_hdr){
            if(index_type==4){
                uint64_t * ks=new uint64_t[vector->size()];
                KeyValueOffset * vs=new KeyValueOffset[vector->size()];
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks[x] = (*vector)[x].first;
                    vs[x] = (*vector)[x].second;
                }
                auto p=new BufferIndex<uint64_t, KeyValueOffset>(ks,vs,vector->size());
                delete[] ks;
                delete[] vs;
                return p;
            } else if(index_type==5){
                uint64_t * ks=new uint64_t[vector->size()];
                KeyValueOffset * vs=new KeyValueOffset[vector->size()];
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks[x] = (*vector)[x].first;
                    vs[x] = (*vector)[x].second;
                }
                auto p=new BufferIndex<uint64_t, KeyValueOffset>(ks,vs,vector->size());
                delete[] ks;
                delete[] vs;
                return p;
            }
            return nullptr;
        }
        KeyValueOffset CoreInsert(const KeyType &, KeyValueOffset) {
            return KeyValueOffset::NONE();
        }

    };
}
#endif //VIPER_DUMMY_INDEX_H