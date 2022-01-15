//
// Created by bysoulwarden on 2021/12/9.
//

#ifndef VIPER_A_CARE_HPP
#define VIPER_A_CARE_HPP

#include "../common_index.hpp"
#include "Tree.h"
#include "Key.h"
#include <map>

namespace viper::index {


    static std::unordered_map<uint64_t,uint64_t> * umap=nullptr;
    static std::unordered_map<uint64_t,uint64_t> * v=nullptr;

    template<typename K>
    class ArtCare:public BaseIndex<K>{
    public:
        static void loadKey(TID tid, Key &key) {
            // Store the key of the tuple into the key vector
            // Implementation is database specific
            auto k=(umap->find(tid))->second;
            key.setKeyLen(sizeof(k));
            reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(k);
        }

        ART_OLC::Tree *tree;
        std::vector<ART::ThreadInfo> ts;
        ArtCare(){
            umap=new std::unordered_map<uint64_t,uint64_t>;
            tree=new ART_OLC::Tree(loadKey);
        }
        ~ArtCare(){
            delete tree;
            delete umap;
        }
        bool SupportBulk(int threads){
            for(int i=0;i<threads;i++){
                ts.push_back(tree->getThreadInfo());
            }
            return false;
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            auto t = tree->getThreadInfo();
            Key key;
            (*umap)[o.get_offset()]=k;
            loadKey(o.get_offset(),key);
            tree->insert(key, o.get_offset(),t);
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            auto t = tree->getThreadInfo();
            Key key;
            key.setKeyLen(sizeof(k));
            reinterpret_cast<uint64_t *>(&key[0])[0] = __builtin_bswap64(k);
            auto val = tree->lookup(key,t);
            return KeyValueOffset((uint64_t)val);
        }
    };
    //template<> std::unordered_map<uint64_t,uint64_t> ArtCare<uint64_t>::map;
}


#endif //VIPER_A_CARE_HPP
