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
#include "XIndex-R/xindex.h"
#include "XIndex-R/xindex_impl.h"

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
        BaseIndex<KeyType>* bulk_load(std::vector<std::pair<uint64_t, KeyValueOffset>> * vector,hdr_histogram * bulk_hdr,int threads){
            if(index_type==4){
                uint64_t * ks=new uint64_t[vector->size()];
                KeyValueOffset * vs=new KeyValueOffset[vector->size()];
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks[x] = (*vector)[x].first;
                    vs[x] = (*vector)[x].second;
                }
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                auto p=new BufferIndex<uint64_t, KeyValueOffset>(ks,vs,vector->size());
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
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
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                auto p=new InplaceIndex<uint64_t, KeyValueOffset>(ks,vs,vector->size());
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
                delete[] ks;
                delete[] vs;
                return p;
            }else if(index_type==6){

                std::vector<uint64_t> ks;
                std::vector<KeyValueOffset> vs;
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks.push_back((*vector)[x].first);
                    vs.push_back((*vector)[x].second);
                }
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                auto p=new xindex::XIndex<uint64_t,KeyValueOffset>(ks, vs, threads, 1);
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
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