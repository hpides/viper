//
// Created by tawnysky on 2020/8/30.
//

#pragma once

#include "btree.h"
#include "buffer_node.h"
#include "piecewise_linear_model.h"
#include "../common_index.hpp"
#include <hdr_histogram.h>

template<typename K, typename P>
class BufferIndex : public viper::index::BaseIndex<K>{
protected:

    using NODE = BufferNode<K, P>;
    using KEY_NODE_PAIR = std::pair<K, NODE*>;

    struct Segment;     // For storing models, to building nodes
    struct Buffer;

    K min_key;
    K max_key;
    stx::btree<K, NODE*> btree;     // A pointer to the root node

    Buffer left_buffer;         // A buffer for storing records less than the min key
    Buffer right_buffer;        // A buffer for storing records greater than the max key


    using KeyValueOffset=viper::index::KeyValueOffset;
    uint64_t GetIndexSize() { return index_size(); }

    uint64_t GetIndexSizeWithoutData() { return index_model_size(); }

    KeyValueOffset CoreInsert(const K & k, viper::index::KeyValueOffset offset) {
        upsert(k,offset);
        return KeyValueOffset::NONE();
    }
    KeyValueOffset CoreGet(const K & k) {
        auto p=find(k);
        return p;
    }
    // For double comparison
    inline bool equal(K key1, K key2) const {
        //return (key1 - key2 <= EPS && key1 - key2 >= -EPS);
        return (key1 == key2);
    }

    inline bool greater_than(K key1, K key2) const {
        //return (key1 - key2 > EPS);
        return (key1 > key2);
    }

    inline bool less_than(K key1, K key2) const {
        //return (key1 - key2 < -EPS);
        return (key1 < key2);
    }

    // Build index
    void build(K* keys, P* payloads, size_t number) {

        // Make segmentation for leaves
        std::vector<Segment> segments;
        auto in_fun = [keys](auto i) {
            return std::pair<K, size_t>(keys[i],i);
        };
        auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
        size_t last_n = make_segmentation(number, EPSILON, in_fun, out_fun);
        std::cout<<"Number of leaf nodes: "<<last_n<<std::endl;

        // Build leaf nodes
        size_t start_pos = 0;
        auto key_node_pairs = new KEY_NODE_PAIR[last_n];
        for(size_t i = 0; i < last_n; i++){
            key_node_pairs[i] = KEY_NODE_PAIR(segments[i].first_key, new NODE(segments[i].number, segments[i].first_key, segments[i].slope, segments[i].intercept, keys, payloads, start_pos));
            start_pos += segments[i].number;
        }
        btree.bulk_load(key_node_pairs, key_node_pairs + last_n);
    }


public:

    BufferIndex(K* keys, P* payloads, size_t number)
            :   min_key(*keys),
                max_key(*(keys + number - 1)) {
        build(keys, payloads, number);
    }

    // Point query
    P find(K key) {
        if (!less_than(key, min_key) && !greater_than(key, max_key)) {
            this->LogHdr1Start();
            auto * p1=(--btree.upper_bound(key))->second;
            this->LogHdr1End();
            this->LogHdr2Start();
            auto res=p1->find(key);
            this->LogHdr2End();
            return res;
        }
        else if (less_than(key, min_key)) {
            this->LogHdr2Start();
            auto res=left_buffer.find(key);
            this->LogHdr2End();
            return res;
        }
        else {
            this->LogHdr2Start();
            auto res=right_buffer.find(key);
            this->LogHdr2End();
            return res;
        }
    }

    // Range query
    void range_query(K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        if (!less_than(lower_bound, min_key) && !greater_than(lower_bound, max_key)) {
            auto it = --btree.upper_bound(lower_bound);
            if (it->second->range_query(false, lower_bound, upper_bound, answers)) {
                if (++it == btree.end()) {
                    right_buffer.range_query(lower_bound, upper_bound, answers);
                }
                else {
                    while (it->second->range_query(true, lower_bound, upper_bound, answers)) {
                        if (++it == btree.end()) {
                            right_buffer.range_query(lower_bound, upper_bound, answers);
                            break;
                        }
                    }
                }
            }
        }
        else if (less_than(lower_bound, min_key)) {
            left_buffer.range_query(lower_bound, upper_bound, answers);
            if (!less_than(lower_bound, max_key)) {
                auto it = btree.begin();
                while (it->second->range_query(true, lower_bound, upper_bound, answers)) {
                    if (++it == btree.end()) {
                        right_buffer.range_query(lower_bound, upper_bound, answers);
                        break;
                    }
                }
            }
        }
        else {
            right_buffer.range_query(lower_bound, upper_bound, answers);
        }
    }

    // Upsert records
    void upsert(K key, P payload) {
        if (!less_than(key, min_key) && !greater_than(key, max_key)) {
            auto it = --btree.upper_bound(key);
            NODE* leaf = it->second;
            this->LogHdr3Start();
            size_t ref = leaf->upsert(key, payload);
            this->LogHdr3End();
            if (ref < RESERVE) {
                return;
            }
            // Split leaf
            else {
                this->LogRetrainStart();
                btree.erase(it);
                leaf->merge_buffer();
                size_t old_number = leaf->get_number();
                K* old_keys = leaf->get_keys();
                P* old_payloads = leaf->get_payloads();
                std::vector<Segment> segments;
                auto in_fun = [old_keys](auto i) {
                    return std::pair<K, size_t>(old_keys[i],i);
                };
                auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
                size_t last_n = make_segmentation(old_number, EPSILON, in_fun, out_fun);
                KEY_NODE_PAIR* key_node_pairs = new KEY_NODE_PAIR[last_n];
                size_t start_pos = 0;
                for(size_t i = 0; i < last_n; i++){
                    key_node_pairs[i] = KEY_NODE_PAIR(segments[i].first_key, new NODE(segments[i].number, segments[i].first_key, segments[i].slope, segments[i].intercept, old_keys, old_payloads, start_pos));
                    start_pos += segments[i].number;
                }
                delete leaf;
                btree.insert(key_node_pairs, key_node_pairs + last_n);
                this->LogRetrainEnd();
            }
        }
        else if (less_than(key, min_key)) {
            this->LogHdr3Start();
            size_t ref = left_buffer.upsert(key, payload);
            this->LogHdr3End();
            if (ref < RESERVE) {
                return;
            }
            else {
                this->LogRetrainStart();
                K* old_keys = left_buffer.keys;
                P* old_payloads = left_buffer.payloads;
                std::vector<Segment> segments;
                auto in_fun = [old_keys](auto i) {
                    return std::pair<K, size_t>(old_keys[i],i);
                };
                auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
                size_t last_n = make_segmentation(RESERVE, EPSILON, in_fun, out_fun);
                KEY_NODE_PAIR* key_node_pairs = new KEY_NODE_PAIR[last_n];
                size_t start_pos = 0;
                for(size_t i = 0; i < last_n; i++){
                    key_node_pairs[i] = KEY_NODE_PAIR(segments[i].first_key, new NODE(segments[i].number, segments[i].first_key, segments[i].slope, segments[i].intercept, old_keys, old_payloads, start_pos));
                    start_pos += segments[i].number;
                }
                btree.insert(key_node_pairs, key_node_pairs + last_n);
                min_key = left_buffer.keys[0];
                left_buffer.init();
                this->LogRetrainEnd();
            }
        }
        else {
            this->LogHdr3Start();
            size_t ref = right_buffer.upsert(key, payload);
            this->LogHdr3End();
            if (ref < RESERVE) {
                return;
            }
            else {
                this->LogRetrainStart();
                K* old_keys = right_buffer.keys;
                P* old_payloads = right_buffer.payloads;
                std::vector<Segment> segments;
                auto in_fun = [old_keys](auto i) {
                    return std::pair<K, size_t>(old_keys[i], i);
                };
                auto out_fun = [&segments](auto cs) { segments.emplace_back(cs); };
                size_t last_n = make_segmentation(RESERVE, EPSILON, in_fun, out_fun);
                KEY_NODE_PAIR *key_node_pairs = new KEY_NODE_PAIR[last_n];
                size_t start_pos = 0;
                for (size_t i = 0; i < last_n; i++) {
                    key_node_pairs[i] = KEY_NODE_PAIR(segments[i].first_key, new NODE(segments[i].number, segments[i].first_key, segments[i].slope, segments[i].intercept, old_keys, old_payloads, start_pos));
                    start_pos += segments[i].number;
                }
                btree.insert(key_node_pairs, key_node_pairs + last_n);
                max_key = right_buffer.keys[right_buffer.number];
                right_buffer.init();
                this->LogRetrainEnd();
            }
        }
    }


    // Print index+data info
    size_t index_size() const {
        size_t model_size = 0;
        size_t slot_size = 0;
        size_t data_size = 0;
        model_size += sizeof(*this) - 2 * RESERVE * (sizeof(K) + sizeof(P)) + (btree.get_stats().innernodes * btree.get_stats().innerslots + btree.get_stats().leaves * btree.get_stats().leafslots) * (sizeof(K) + sizeof(void*)) + btree.get_stats().leaves * sizeof(void*) * 2;
        slot_size += 2 * RESERVE * (sizeof(K) + sizeof(P));
        data_size += (left_buffer.number + right_buffer.number) * (sizeof(K) + sizeof(P));
        auto it = btree.begin();
        while (it != btree.end()) {
            it->second->node_size(model_size, slot_size, data_size);
            it++;
        }

        std::cout << "Total size: " << (double)(model_size+slot_size)/(1024*1024) << " MB" << std::endl;
        std::cout << "Model size: " << (double)model_size/(1024*1024) << " MB" << std::endl;
        std::cout << "Fill rate: " << (double)100*data_size/slot_size << "%" << std::endl;
        return model_size+slot_size;
    }
    // Print index info
    size_t index_model_size() const {
        size_t model_size = 0;
        size_t slot_size = 0;
        size_t data_size = 0;
        model_size += sizeof(*this) - 2 * RESERVE * (sizeof(K) + sizeof(P)) + (btree.get_stats().innernodes * btree.get_stats().innerslots + btree.get_stats().leaves * btree.get_stats().leafslots) * (sizeof(K) + sizeof(void*)) + btree.get_stats().leaves * sizeof(void*) * 2;
        slot_size += 2 * RESERVE * (sizeof(K) + sizeof(P));
        data_size += (left_buffer.number + right_buffer.number) * (sizeof(K) + sizeof(P));
        auto it = btree.begin();
        while (it != btree.end()) {
            it->second->node_size(model_size, slot_size, data_size);
            it++;
        }

        std::cout << "Total size: " << (double)(model_size+slot_size)/(1024*1024) << " MB" << std::endl;
        std::cout << "Model size: " << (double)model_size/(1024*1024) << " MB" << std::endl;
        std::cout << "Fill rate: " << (double)100*data_size/slot_size << "%" << std::endl;
        return model_size;
    }

};

template<typename K, typename P>
struct BufferIndex<K, P>::Buffer {

    size_t number = 0;
    K keys[RESERVE];
    P payloads[RESERVE];

    Buffer() = default;

    // For double comparison
    inline bool equal(K key1, K key2) const {
        //return (key1 - key2 <= EPS && key1 - key2 >= -EPS);
        return (key1 == key2);
    }

    inline bool greater_than(K key1, K key2) const {
        //return (key1 - key2 > EPS);
        return (key1 > key2);
    }

    inline bool less_than(K key1, K key2) const {
        //return (key1 - key2 < -EPS);
        return (key1 < key2);
    }

    void init() {
        number = 0;
    }

    P find(K key) const {
        for(size_t i = 0; i < number; i++){
            if(!less_than(keys[i], key)){
                if(equal(keys[i], key))
                    return payloads[i];
                else
                    return viper::index::KeyValueOffset::NONE();
            }
        }
        return viper::index::KeyValueOffset::NONE();
    }

    void range_query(K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        for(size_t i = 0; i < number; i++) {
            if (!less_than(keys[i], lower_bound)) {
                while (i < number && !greater_than(keys[i], upper_bound)) {
                    answers.emplace_back(keys[i], payloads[i]);
                    i++;
                }
                return;
            }
        }
    }

    size_t upsert(K key, P payload) {
        size_t pos = number;
        for(size_t i = 0; i < number; i++) {
            if (!less_than(keys[i], key)) {
                if (equal(keys[i], key)) {
                    payloads[i] = payload;
                    return number;
                }
                else {
                    memmove(keys+i+1, keys+i, (number-i)*sizeof(K));
                    memmove(payloads+i+1, payloads+i, (number-i)*sizeof(P));
                    pos = i;
                    break;
                }
            }
        }
        keys[pos] = key;
        payloads[pos] = payload;
        number++;
        return number;
    }

};

// For storing models, to build nodes
template<typename K, typename P>
struct BufferIndex<K, P>::Segment {
    K first_key;
    double slope;
    double intercept;
    size_t number;
    explicit Segment(const typename OptimalPiecewiseLinearModel<K, size_t>::CanonicalSegment &cs)
            : first_key(cs.get_first_x()),
              number(cs.get_number()) {
        auto [cs_slope, cs_intercept] = cs.get_floating_point_segment(first_key);
        slope = cs_slope;
        intercept = cs_intercept;
    }
};
