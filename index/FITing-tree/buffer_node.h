//
// Created by tawnysky on 2020/8/31.
//

#pragma once

#include "parameters.h"

template<typename K, typename P>
class BufferNode {
protected:

    struct Buffer;

    size_t size;        // The size of node
    double slope;       // Slope of the linear model
    double intercept;       // Intercept of the linear model

    K* keys;        // For storing keys
    P* payloads;        // For storing payloads, only take effects in leaf node

    Buffer buffer;

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
        else if (pos >= size)
            return size - 1;
        return pos;
    }

    // Init the step length for binary search
    // Direction: 0 means left and 1 means right
    [[nodiscard]] inline size_t init_step_len (size_t position, bool direction) const {
        size_t range = EPSILON;
        if(!direction && position < EPSILON)
            range = position;
        if(direction && position + EPSILON >= size)
            range = size - position - 1;
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
                if(position + step_len >= size)
                    step_len = size - position - 1;
                position += step_len;
            }
            current_key = get_key(position);
            step_len = step_len > 1 ? (step_len + 1) / 2 : 0;
        }
        return position;
    }

public:

    BufferNode(size_t number, K first_key, double slope, double intercept, K* keys, P* payloads, size_t start_pos)
            :   size(number),
                slope(slope),
            // Original intercept is for all of data, we need it for the data in the segment
                intercept(intercept - start_pos - first_key * slope) {
        this->keys = new K[size];
        memcpy(this->keys, keys + start_pos, number * sizeof(K));
        this->payloads = new P[size];
        memcpy(this->payloads, payloads + start_pos, number * sizeof(P));
    }

    ~BufferNode() {
        delete [] keys;
        delete [] payloads;
    }

    [[nodiscard]] size_t get_number() {
        return size;
    }

    K* get_keys() {
        return keys;
    }

    P* get_payloads() {
        return payloads;
    }

    void merge_buffer() {
        K* old_keys = keys;
        P* old_payloads = payloads;
        keys = new K[size + buffer.number];
        payloads = new P[size + buffer.number];
        size_t ito = 0;
        size_t itb = 0;
        while (ito < size || itb < buffer.number) {
            if (itb == buffer.number || ito < size && less_than(old_keys[ito], buffer.keys[itb])) {
                keys[ito + itb] = old_keys[ito];
                payloads[ito + itb] = old_payloads[ito];
                ito++;
            }
            else {
                keys[ito + itb] = buffer.keys[itb];
                payloads[ito + itb] = buffer.payloads[itb];
                itb++;
            }
        }
        size += buffer.number;
        buffer.init();
        delete [] old_keys;
        delete [] old_payloads;
    }

    // Find payload in leaf node
    P find(K key) const {
        size_t position = find_precise_position(key);
        K current_key = get_key(position);
        if(equal(current_key, key))
            return get_payload(position);
        else
            return buffer.find(key);
    }

    bool range_query(bool type, K lower_bound, K upper_bound, std::vector<std::pair<K, P>>& answers) const {
        buffer.range_query(lower_bound, upper_bound, answers);
        size_t it = 0;
        if (!type) {
            it = find_precise_position(lower_bound);
            while (it < size && less_than(get_key(it), lower_bound))
                ++it;
        }
        while (it < size && !greater_than(get_key(it), upper_bound)) {
            answers.emplace_back(get_key(it), get_payload(it));
            ++it;
        }
        if (it < size)
            return false;
        else
            return true;
    }

    // Upsert record into leaf node
    size_t upsert(K key, P payload) {
        size_t position = find_precise_position(key);
        K current_key = get_key(position);
        if(equal(current_key, key)) {
            payloads[position] = payload;
            return buffer.number;
        }
        else
            return buffer.upsert(key, payload);
    }

    void node_size(size_t& model_size, size_t& slot_size, size_t& data_size) const {
        model_size += sizeof(*this) - RESERVE * (sizeof(K) + sizeof(P));
        slot_size += (size + RESERVE) * (sizeof(K) + sizeof(P));
        data_size += (size + buffer.number) * (sizeof(K) + sizeof(P));
    }
};

template<typename K, typename P>
struct BufferNode<K, P>::Buffer {

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
                    return NULL;
            }
        }
        return NULL;
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
