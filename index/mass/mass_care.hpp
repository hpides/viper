//
// Created by bysoulwarden on 2021/12/9.
//

#ifndef VIPER_MASS_CARE_HPP
#define VIPER_MASS_CARE_HPP

#include "../common_index.hpp"
#include "mass_tree.h"

namespace viper::index {
    template<typename K>
    class MassCare :public BaseIndex<K>{
    public:
        mass_tree *mt;
        MassCare(){
            mt = new_mass_tree();
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            std::cout<<k<<std::endl;
            std::cout<<o.get_offset()<<std::endl;
            mass_tree_put(mt, &k, 8, (void*)o.get_offset());
            void* tmp = mass_tree_get(mt, &k, 8);
            uint64_t a=(uint64_t)tmp;
            std::cout<<std::to_string(a)<<std::endl;
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            std::cout<<k<<std::endl;
            void* tmp = mass_tree_get(mt, &k, 8);
            uint64_t a=(uint64_t)tmp;
            std::cout<<std::to_string(a)<<std::endl;
            return KeyValueOffset((uint64_t)tmp);
        }
    };
}


#endif //VIPER_MASS_CARE_HPP
