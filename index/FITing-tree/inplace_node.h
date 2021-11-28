//
// Created by tawnysky on 2020/8/31.
//

#pragma once

#include "parameters.h"

template<typename K, typename P>
class InPlaceNode {
protected:

    size_t size;
    size_t epsilon;       // Epsilon of node
    size_t number;      // The number of records in node
    double slope;       // Slope of the linear model
    double intercept;       // Intercept of the linear model

    K* keys;        // For storing keys
    P* payloads;        // For storing payloads, only take effects in leaf node

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

    inline K get_key (size_t position) const {
        return keys[position];
    }

    inline P get_payload (size_t position) const {
        return payloads[position];
    }

    // Predict position
    inline size_t predict_pos (K key) const {
        int pos = static_cast<int>(key * slope + intercept + 0.5);
        if(pos < 0)
            return 0;
        else if (pos >= number)
            return number - 1;
        return pos;
    }

    // Init the step length for binary search
    // Direction: 0 means left and 1 means right
    [[nodiscard]] inline size_t init_step_len (size_t position, bool direction) const {
        size_t range = epsilon;
        if(!direction && position < epsilon)
            range = position;
        if(direction && position + epsilon >= number)
            range = number - position - 1;
        return (range + 2) / 2;
    }

    // Find precise position
    size_t find_precise_position(K key) const {
        size_t position = predict_pos(key);
        K current_key = get_key(position);
        size_t step_len = init_step_len(position, !greater_than(current_key, key));
        while(!equal(current_key, key) && step_len > 0){
            if(greater_than(current_key, key)){
                if(position < step_len)
                    step_len = position;
                position -= step_len;
            }
            else{
                if(position + step_len >= number)
                    step_len = number - position - 1;
                position += step_len;
            }
            current_key = get_key(position);
            step_len = step_len > 1 ? (step_len + 1) / 2 : 0;
        }
        return position;
    }

public:

    InPlaceNode(size_t number, K first_key, double slope, double intercept, K* keys, P* payloads, size_t start_pos)
        :   size(number + RESERVE),
            epsilon(EPSILON),
            number(number),
            slope(slope),
            // Original intercept is for all of data, we need it for the data in the segment
            intercept(intercept - start_pos - first_key * slope) {
        this->keys = new K[size];
        memcpy(this->keys, keys + start_pos, number * sizeof(K));
        this->payloads = new P[size];
        memcpy(this->payloads, payloads + start_pos, number * sizeof(P));
    }

    ~InPlaceNode() {
        delete [] keys;
        delete [] payloads;
    }

    [[nodiscard]] size_t get_number() {
        return number;
    }

    K* get_keys() {
        return keys;
    }

    P* get_payloads() {
        return payloads;
    }

    // Find payload in leaf node
    P find(K key) const {
        size_t position = find_precise_position(key);
        K current_key = get_key(position);
        if(equal(current_key, key))
            return get_payload(position);
        else
            return viper::index::KeyValueOffset::NONE();
    }

    bool range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        size_t it = 0;
        if (!type) {
            it = find_precise_position(lower_bound);
            while (it < number && less_than(get_key(it), lower_bound))
                ++it;
        }
        while (it < number && !greater_than(get_key(it), upper_bound)) {
            answers.emplace_back(get_key(it), get_payload(it));
            ++it;
        }
        if (it < number)
            return false;
        else
            return true;
    }

    // Upsert record into leaf node
    size_t upsert(K key, P payload) {
        size_t position = find_precise_position(key);
        K current_key = get_key(position);
        if(equal(current_key, key)){
            payloads[position] = payload;
            return epsilon;
        }
        if(less_than(current_key, key))
            ++position;
        if(position < number){
            memmove(keys+position+1, keys+position, (number-position)*sizeof(K));
            memmove(payloads+position+1, payloads+position, (number-position)*sizeof(P));
        }
        keys[position] = key;
        payloads[position] = payload;
        ++number;
        ++epsilon;
        return epsilon;
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size) const {
        model_size += sizeof(*this);
        slot_size += size * (sizeof(K) + sizeof(P));
        data_size += number * (sizeof(K) + sizeof(P));
    }

};
