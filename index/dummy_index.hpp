//
// Created by bysoulwarden on 2021/10/25.
//

#ifndef VIPER_DUMMY_INDEX_H
#define VIPER_DUMMY_INDEX_H

#include <hdr_histogram.h>
#include <set>
#include <utility>
#include "common_index.hpp"
#include "FITing-tree/buffer_index.h"
#include "FITing-tree/inplace_index.h"
#include "XIndex-R/xindex.h"
#include "XIndex-R/xindex_impl.h"
#include "XIndex-H/xindex.h"
#include "XIndex-H/xindex_impl.h"
#include "pgm/pgm_index_dynamic.hpp"
#include  "rs/radix_spline.h"
#include "rs/builder.h"

namespace viper::index {
    template<typename K>
    class RsCare: public BaseIndex<K>{
    public:
        rs::RadixSpline<uint64_t> rs;
        std::vector<uint64_t> ks;
        std::vector<KeyValueOffset> vs;

        KeyValueOffset CoreInsert(const K & k, viper::index::KeyValueOffset offset) {
            throw std::runtime_error("RadixSpline dont support CoreInsert");
        }
        KeyValueOffset CoreGet(const K & k) {
            rs::SearchBound bound = rs.GetSearchBound(k);
            auto start = std::begin(ks) + bound.begin, last = std::begin(ks) + bound.end;
            auto position = std::lower_bound(start, last, k)-std::begin(ks);
            return vs[position];
        }


        RsCare(rs::RadixSpline<uint64_t> rs,std::vector<uint64_t> ks,std::vector<KeyValueOffset> vs):
                rs(std::move(rs)),
                ks(std::move(ks)),
                vs(std::move(vs)){
        }
    };
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
            if(index_type==3){
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                auto p= new pgm::DynamicPGMIndex<uint64_t,viper::index::KeyValueOffset>(vector->begin(),vector->end());
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
                return p;
            }else if(index_type==4){
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
                auto p=new xindex::XIndexR<uint64_t,KeyValueOffset>(ks, vs, threads, 0);
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
                return p;
            }else if(index_type==7){
                std::vector<uint64_t> ks;
                std::vector<KeyValueOffset> vs;
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks.push_back((*vector)[x].first);
                    vs.push_back((*vector)[x].second);
                }
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                auto p=new xindexh::XIndexH<uint64_t,KeyValueOffset>(ks, vs, threads, 0);
                const auto end = std::chrono::high_resolution_clock::now();
                const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                hdr_record_value_atomic(bulk_hdr, duration.count());
                return p;
            }else if(index_type==8){

                std::vector<uint64_t> ks;
                std::vector<KeyValueOffset> vs;
                for(int x = 0; x < vector->size(); ++x)
                {
                    ks.push_back((*vector)[x].first);
                    vs.push_back((*vector)[x].second);
                }
                std::chrono::high_resolution_clock::time_point start= std::chrono::high_resolution_clock::now();
                uint64_t min = ks.front();
                uint64_t max = ks.back();
                rs::Builder<uint64_t> rsb(min, max);
                for (const auto& key : ks) rsb.AddKey(key);
                rs::RadixSpline<uint64_t> rs = rsb.Finalize();
                auto p=new RsCare<uint64_t>(rs,ks, vs);
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