//
// Created by bysoulwarden on 2021/12/10.
//

#ifndef VIPER_BW_CARE_HPP
#define VIPER_BW_CARE_HPP
#include "../common_index.hpp"
#include "bwtree.h"

namespace viper::index {
    /*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
    class BwKeyComparator {
    public:
        inline bool operator()(const uint64_t k1, const uint64_t k2) const {
            return k1 < k2;
        }

        BwKeyComparator(int dummy) {
            (void)dummy;
            return;
        }

        BwKeyComparator() = delete;
        //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
    };

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
    class BwKeyEqualityChecker {
    public:
        inline bool operator()(const uint64_t k1, const uint64_t k2) const {
            return k1 == k2;
        }

        BwKeyEqualityChecker(int dummy) {
            (void)dummy;

            return;
        }

        BwKeyEqualityChecker() = delete;
        //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
    };

    using TreeType = wangziqi2013::bwtree::BwTree<uint64_t,
            uint64_t,
            BwKeyComparator,
            BwKeyEqualityChecker>;
    template<typename K>
    class BwTreeCare :public BaseIndex<K>{
    public:
        TreeType *map;
        BwTreeCare(){
            map = new TreeType(true,
                               BwKeyComparator(1),
                               BwKeyEqualityChecker(1));
        }
        ~BwTreeCare(){
            delete map;
        }
        bool SupportBulk(int threads){
            map->UpdateThreadLocal(threads);
            return false;
        }
        BaseIndex<K>* bulk_load(std::vector<std::pair<uint64_t, KeyValueOffset>> * vector,hdr_histogram * bulk_hdr,int threads) {
            delete map;
            wangziqi2013::bwtree::BwTreeBase::total_thread_num=threads;
            map = new TreeType(true,
                               BwKeyComparator(1),
                               BwKeyEqualityChecker(1));
            for(auto o:*vector){
                map->Insert(o.first,o.second.get_offset());
            }
            return this;
        }
        KeyValueOffset CoreInsert(const K & k, KeyValueOffset o) {
            map->Insert(k,o.get_offset());
            return KeyValueOffset();
        }
        KeyValueOffset CoreGet(const K & k) {
            auto set=map->GetValue(k);
            for (auto it = set.begin(); it != set.end(); ++it)
                return KeyValueOffset(*it);
        }
    };
}
#endif //VIPER_BW_CARE_HPP
