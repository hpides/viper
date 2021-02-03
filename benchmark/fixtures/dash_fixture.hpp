#pragma once

#include "ex_finger.h"
#include "common_fixture.hpp"
#include "../benchmark.hpp"
#include <libpmemobj++/allocator.hpp>
#include <libpmemobj++/transaction.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/container/vector.hpp>

namespace viper::kv_bm {

template <typename KeyT>
struct DashVarKey {
    string_key str_k;
    KeyT key;
};

template <typename KeyT = KeyType8, typename ValueT = ValueType8>
class DashFixture : public BaseFixture {
    static constexpr size_t KeyTSize = sizeof(KeyT);
    using EntryKeyT = typename std::conditional<KeyTSize == 8, KeyT, DashVarKey<KeyT>>::type;
    using DashKeyT = typename std::conditional<KeyTSize == 8, KeyT, string_key*>::type;
    using Entry = std::pair<EntryKeyT, ValueT>;
    using EntryVector = pmem::obj::vector<pmem::obj::persistent_ptr<Entry>>;

    struct DashPool {
        pmem::obj::persistent_ptr<EntryVector> ptrs;
    };

  public:
    void InitMap(const uint64_t num_prefill_inserts, const bool re_init) final;
    void DeInitMap() final;
    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds);
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes);
    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data,
                      hdr_histogram* hdr) final;
    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;
    void prefill_ycsb(const std::vector<ycsb::Record>& data) override;

  protected:
    Hash<DashKeyT>* dram_map_;
    pmem::obj::pool<DashPool> pmem_pool_;
    std::string dash_pool_name_;
    EntryVector* ptrs_;
    std::atomic<size_t> pool_vector_pos_;
    std::string pmem_pool_name_;
    bool map_initialized_ = false;

    bool insert_internal(const KeyT& key, const ValueT& value);
};

template <typename KeyT, typename ValueT>
void DashFixture<KeyT, ValueT>::InitMap(const uint64_t num_prefill_inserts, const bool re_init) {
    if (map_initialized_ && !re_init) {
        return;
    }

    pmem_pool_name_ = random_file(DB_PMEM_DIR);
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
    pmem_pool_ = pmem::obj::pool<DashPool>::create(pmem_pool_name_, "", 80ul * ONE_GB, S_IRWXU);
    if (pmem_pool_.handle() == nullptr) {
        throw std::runtime_error("Could not create pool");
    }
    pmem::obj::transaction::run(pmem_pool_, [&] {
        pmem_pool_.root()->ptrs = pmem::obj::make_persistent<EntryVector>();
        ptrs_ = pmem_pool_.root()->ptrs.get();
        ptrs_->resize(num_prefill_inserts * 2);
    });

    size_t segment_number = 64;
    dash_pool_name_ = random_file(DB_PMEM_DIR);
    Allocator::Initialize(dash_pool_name_.c_str(), ONE_GB * 10);
    dram_map_ = reinterpret_cast<Hash<DashKeyT> *>(Allocator::GetRoot(sizeof(extendible::Finger_EH<DashKeyT>)));
    new (dram_map_) extendible::Finger_EH<DashKeyT>(segment_number, Allocator::Get()->pm_pool_);

    pool_vector_pos_ = 0;
    prefill(num_prefill_inserts);
    map_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void DashFixture<KeyT, ValueT>::DeInitMap() {
    pmem_pool_.close();
    pmempool_rm(pmem_pool_name_.c_str(), 0);
    pmemobj_close(Allocator::Get()->pm_pool_);
    std::filesystem::remove(dash_pool_name_);
    dram_map_ = nullptr;
    map_initialized_ = false;
}

template <typename KeyT, typename ValueT>
bool DashFixture<KeyT, ValueT>::insert_internal(const KeyT& key, const ValueT& value) {
    block_size_t ptrs_pos;
    pmem::obj::persistent_ptr<Entry> offset_ptr;
    pmem::obj::transaction::run(pmem_pool_, [&] {
        if constexpr (sizeof(KeyT) == 8) {
            offset_ptr = pmem::obj::make_persistent<Entry>(key, value);
        } else {
            string_key str_k{};
            str_k.length = sizeof(KeyT);
            DashVarKey<KeyT> var_key{ .str_k = str_k, .key = key };
            offset_ptr = pmem::obj::make_persistent<Entry>(var_key, value);
        }
        ptrs_pos = pool_vector_pos_.fetch_add(1);
        (*ptrs_)[ptrs_pos] = offset_ptr;
    });
    if constexpr (sizeof(KeyT) == 8) {
        return dram_map_->Insert(key, (char *)ptrs_pos) == 0;
    } else {
        return dram_map_->Insert(&(offset_ptr->first.str_k), (char *)ptrs_pos) == 0;
    }
}

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    for (uint64_t pos = start_idx; pos < end_idx; ++pos) {
        const KeyT db_key{pos};
        const ValueT value{pos};
        insert_counter += insert_internal(db_key, value);
    }
    return insert_counter;
}

//template <>
//uint64_t DashFixture<std::string, std::string>::insert(uint64_t start_idx, uint64_t end_idx) {
//    constexpr size_t num_per_epoch = 1000;
//    uint64_t insert_counter = 0;
//    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
//    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);
//    for (uint64_t pos = start_idx; pos < end_idx; pos += num_per_epoch) {
//        auto epoch_guard = Allocator::AquireEpochGuard();
//        for (uint64_t ep_count = 0; ep_count < num_per_epoch; ++ep_count) {
//            const std::string& db_key = keys[pos + ep_count];
//            const std::string& value = values[pos + ep_count];
//            insert_counter += insert_internal(db_key, value);
//        }
//    }
//    return insert_counter;
//}

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    uint64_t found_counter = 0;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        bool found;
        block_size_t entry_ptr_pos;
        if constexpr (sizeof(KeyT) == 8) {
            const char* val = dram_map_->Get(key);
            found = val != NONE;
            entry_ptr_pos = (block_size_t) val;
        } else {
            DashVarKey<KeyT> var_key{ .str_k = string_key{ .length = sizeof(KeyT) }, .key = key };
            const char* val = dram_map_->Get(&var_key.str_k);
            found = val != NONE;
            entry_ptr_pos = (block_size_t) val;
        }
        if (found) {
            pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
            ValueT found_val = entry_ptr->second;
            found_counter += (found_val.data[0] == key);
        }
    }
    return found_counter;
}

//template <>
//uint64_t DashFixture<std::string, std::string>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
//    std::random_device rnd{};
//    auto rnd_engine = std::default_random_engine(rnd());
//    std::uniform_int_distribution<> distrib(start_idx, end_idx);
//
//    auto key_check_fn = [this](const std::string& key, IndexV offset) {
//        block_size_t entry_ptr_pos = offset.block_number;
//        pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//        return key == entry_ptr->first;
//    };
//
//    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
//    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);
//
//    uint64_t found_counter = 0;
//    for (uint64_t i = 0; i < num_finds; ++i) {
//        const uint64_t key = distrib(rnd_engine);
//        Dash::DashAccessor accessor{};
//        const std::string& db_key = keys[key];
//        const std::string& value = values[key];
//        const bool found = dram_map_->Get(db_key, accessor, key_check_fn);
//        if (found) {
//            block_size_t entry_ptr_pos = accessor->block_number;
//            pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//            found_counter += (entry_ptr->second == value);
//        }
//    }
//    return found_counter;
//}

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    return 0;
//    std::random_device rnd{};
//    auto rnd_engine = std::default_random_engine(rnd());
//    std::uniform_int_distribution<> distrib(start_idx, end_idx);
//
//    auto key_check_fn = [this](const KeyT& key, IndexV offset) {
//        block_size_t entry_ptr_pos = offset.block_number;
//        pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//        return key == entry_ptr->first;
//    };
//
//    uint64_t update_counter = 0;
//    for (uint64_t i = 0; i < num_updates; ++i) {
//        const uint64_t key = distrib(rnd_engine);
//        const KeyT db_key{key};
//        Dash::DashAccessor accessor{};
//        const bool found = dram_map_->Get(db_key, accessor, key_check_fn);
//        if (found) {
//            block_size_t entry_ptr_pos = accessor->block_number;
//            pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//            ValueT& value = entry_ptr->second;
//            value.update_value();
//            pmem_persist(&value, sizeof(uint64_t));
//            update_counter++;
//        }
//    }
//    return update_counter;
}

template <>
uint64_t DashFixture<std::string, std::string>::setup_and_update(uint64_t, uint64_t, uint64_t) { return 0; }

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    return 0;
//    std::random_device rnd{};
//    auto rnd_engine = std::default_random_engine(rnd());
//    std::uniform_int_distribution<> distrib(start_idx, end_idx);
//
//    auto key_check_fn = [this](const KeyT& key, IndexV offset) {
//        block_size_t entry_ptr_pos = offset.block_number;
//        const pmem::obj::persistent_ptr<Entry>& entry_ptr = (*ptrs_)[entry_ptr_pos];
//        return !!entry_ptr && key == entry_ptr->first;
//    };
//
//    uint64_t delete_counter = 0;
//    for (uint64_t i = 0; i < num_deletes; ++i) {
//        Dash::DashAccessor accessor{};
//        const uint64_t key = distrib(rnd_engine);
//        const KeyT db_key{key};
//        const bool found = dram_map_->Get(db_key, accessor, key_check_fn);
//        if (found) {
//            block_size_t entry_ptr_pos = accessor->block_number;
//            pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//            pmem::obj::transaction::run(pmem_pool_, [&] {
//                pmem::obj::delete_persistent<Entry>(entry_ptr);
//            });
//            (*ptrs_)[entry_ptr_pos] = pmem::obj::persistent_ptr<Entry>();
//            dram_map_->Remove(accessor.offset);
//            delete_counter++;
//        }
//    }
//    return delete_counter;
}

template <>
uint64_t DashFixture<std::string, std::string>::setup_and_delete(uint64_t, uint64_t, uint64_t) { return 0; }

template <typename KeyT, typename ValueT>
uint64_t DashFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t DashFixture<KeyType8, ValueType200>::run_ycsb(uint64_t start_idx,
    uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    throw std::runtime_error("Not supported");
//    uint64_t op_count = 0;
//    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
//        const ycsb::Record& record = data[op_num];
//
//        const auto start = std::chrono::high_resolution_clock::now();
//
//        switch (record.op) {
//            case ycsb::Record::Op::INSERT: {
//                insert_internal(record.key, record.value);
//                op_count++;
//                break;
//            }
//            case ycsb::Record::Op::GET: {
//                Dash::DashAccessor accessor{};
//                const bool found = dram_map_->Get(record.key, accessor);
//                if (found) {
//                    block_size_t entry_ptr_pos = accessor->block_number;
//                    pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
//                    op_count += (entry_ptr->second == record.value);
//                }
//                break;
//            }
//            case ycsb::Record::Op::UPDATE: {
////                Dash::DashAccessor accessor{};
////                const bool found = dram_map_->Get(record.key, accessor);
////                if (found) {
////                    block_size_t entry_ptr_pos = accessor->block_number;
////                    pmem::obj::persistent_ptr<Entry> entry_ptr = (*ptrs_)[entry_ptr_pos];
////                    entry_ptr->second.update_value();
////                    pmem_persist(&entry_ptr->second, sizeof(uint64_t));
////                    op_count++;
////                }
//                insert_internal(record.key, record.value);
//                op_count++;
//                break;
//            }
//            default: {
//                throw std::runtime_error("Unknown operation: " + std::to_string(record.op));
//            }
//        }
//
//        if (hdr == nullptr) {
//            continue;
//        }
//
//        const auto end = std::chrono::high_resolution_clock::now();
//        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
//        hdr_record_value(hdr, duration.count());
//    }
//
//    return op_count;
}

template <typename KeyT, typename ValueT>
void DashFixture<KeyT, ValueT>::prefill_ycsb(const std::vector<ycsb::Record>& data) {
    ptrs_->resize(data.size() * 2);
    BaseFixture::prefill_ycsb(data);
}

}  // namespace
