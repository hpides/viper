#pragma once

#include "../benchmark.hpp"
#include "common_fixture.hpp"
#include "viper/viper.hpp"


namespace viper {
namespace kv_bm {

template <typename KeyT = KeyType8, typename ValueT = ValueType200>
class ViperFixture : public BaseFixture {
  public:
    typedef KeyT KeyType;
    using ViperT = Viper<KeyT, ValueT>;

    hdr_histogram* GetRetrainHdr();

    void BulkLoadIndex(hdr_histogram * bulk_hdr,int threads);

    //index+data size
    uint64_t GetIndexSize();
    //indexsize
    uint64_t GetIndexSizeWithoutData();

    std::string GetIndexType();

    hdr_histogram* GetOpHdr();

    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true) override;
    void InitMap(const uint64_t num_prefill_inserts, ViperConfig v_config);

    void DeInitMap() override;

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;

    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    uint64_t setup_and_get_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates);

    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
        const std::vector<ycsb::Record>& data, hdr_histogram* hdr) final;
    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
                      const std::vector<ycsb::Record>& data, hdr_histogram* hdr,uint32_t thread_id) final;

    ViperT* getViper() {
        return viper_.get();
    }


  protected:
    std::unique_ptr<ViperT> viper_;
    std::string index_type;
    bool viper_initialized_ = false;
    std::string pool_file_;
};
    template <typename KeyT, typename ValueT>
    void  ViperFixture<KeyT, ValueT>::BulkLoadIndex(hdr_histogram * bulk_hdr,int threads){
        viper_->bulkload_index(bulk_hdr,threads);
    }
    template <typename KeyT, typename ValueT>
    hdr_histogram* ViperFixture<KeyT, ValueT>::GetRetrainHdr(){
        return viper_->GetRetrainHdr();
    }
    //index+data size
    template <typename KeyT, typename ValueT>
    uint64_t ViperFixture<KeyT, ValueT>::GetIndexSize(){
        return viper_->get_index_size();
    }
    //index size
    template <typename KeyT, typename ValueT>
    uint64_t ViperFixture<KeyT, ValueT>::GetIndexSizeWithoutData(){
        return viper_->getindexsizewithoutdata();
    }

    template <typename KeyT, typename ValueT>
    hdr_histogram* ViperFixture<KeyT, ValueT>::GetOpHdr(){
        return viper_->GetOpHdr();
    }
    template <typename KeyT, typename ValueT>
    std::string ViperFixture<KeyT, ValueT>::GetIndexType(){
        return index_type;
    }
template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (viper_initialized_ && !re_init) {
        return;
    }

    return InitMap(num_prefill_inserts, ViperConfig{});
}

template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, ViperConfig v_config) {
#ifdef CCEH_PERSISTENT
    PMemAllocator::get().initialize();
#endif

    pool_file_ = VIPER_POOL_FILE;
//    pool_file_ = random_file(DB_PMEM_DIR);
//    pool_file_ = DB_PMEM_DIR + std::string("/viper");

//    viper_ = ViperT::open(pool_file_, v_config);
//todo
// 1 cceh
// 2 alex
// 3 pgm

    int index_num=12;

    viper_ = ViperT::create(pool_file_, BM_POOL_SIZE,index_num, v_config);
    if(index_num==1){
        index_type="cceh";
    }else if(index_num==2){
        index_type="alex";
    }else if(index_num==3){
        index_type="pgm";
    }else if(index_num==4){
        index_type="FITing-tree BufferIndex";
    }else if(index_num==5){
        index_type="FITing-tree InplaceIndex";
    }else if(index_num==6){
        index_type="R-Xindex";
    }else if(index_num==7){
        index_type="H-Xindex";
    }else if(index_num==8){
        index_type="rs";
    }else if(index_num==9){
        index_type="cuckoo";
    }else if(index_num==10){
        index_type="mass tree";
    }else if(index_num==11){
        index_type="skiplist";
    }else if(index_num==12){
        index_type="bwtree";
    }
    this->prefill(num_prefill_inserts);
    viper_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void ViperFixture<KeyT, ValueT>::DeInitMap() {
    BaseFixture::DeInitMap();
    viper_ = nullptr;
    viper_initialized_ = false;
    if (pool_file_.find("/dev/dax") == std::string::npos) {
        std::filesystem::remove_all(pool_file_);
    }
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    auto v_client = viper_->get_client();
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const KeyT db_key{key};
        const ValueT value{key};
        insert_counter += v_client.put(db_key, value);
    }
    return insert_counter;
}

/*template <>
uint64_t ViperFixture<std::string, std::string>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    auto v_client = viper_->get_client();
    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);
    for (uint64_t key = start_idx; key < end_idx; ++key) {
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        insert_counter += v_client.put(db_key, value);
    }
    return insert_counter;
}*/

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const auto v_client = viper_->get_read_only_client();
    uint64_t found_counter = 0;
    ValueT value;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        const bool found = v_client.get(db_key, &value);
        found_counter += found && (value == ValueT{key});
    }
    return found_counter;
}

/*template <>
uint64_t ViperFixture<std::string, std::string>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);
    const std::vector<std::string>& values = std::get<1>(var_size_kvs_);

    auto v_client = viper_->get_read_only_client();
    uint64_t found_counter = 0;
    std::string result;
    for (uint64_t i = 0; i < num_finds; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const std::string& db_key = keys[key];
        const std::string& value = values[key];
        const bool found = v_client.get(db_key, &result);
        found_counter += found && (result == value);
    }
    return found_counter;
}*/

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();
    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        delete_counter += v_client.remove(db_key);
    }
    return delete_counter;
}

/*template <>
uint64_t ViperFixture<std::string, std::string>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);
    const std::vector<std::string>& keys = std::get<0>(var_size_kvs_);

    auto v_client = viper_->get_client();
    uint64_t delete_counter = 0;
    for (uint64_t i = 0; i < num_deletes; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const std::string& db_key = keys[key];
        delete_counter += v_client.remove(db_key);
    }
    return delete_counter;
}*/

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();
    uint64_t update_counter = 0;

    auto update_fn = [](ValueT* value) {
        value->update_value();
        internal::pmem_persist(value, sizeof(uint64_t));
    };

    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        update_counter += v_client.update(db_key, update_fn);
    }
    return update_counter;
}

template <>
uint64_t ViperFixture<std::string, std::string>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    throw std::runtime_error("Not supported");
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::setup_and_get_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    auto v_client = viper_->get_client();

    ValueT new_v{};
    for (uint64_t i = 0; i < num_updates; ++i) {
        const uint64_t key = distrib(rnd_engine);
        const KeyT db_key{key};
        v_client.get(db_key, &new_v);
        new_v.update_value();
        v_client.put(db_key, new_v);
    }

    return num_updates;
}

template <>
uint64_t ViperFixture<std::string, std::string>::setup_and_get_update(uint64_t, uint64_t, uint64_t) {
    throw std::runtime_error("Not supported");
}

template <typename KeyT, typename ValueT>
uint64_t ViperFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t, const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t ViperFixture<KeyType8, ValueType200>::run_ycsb(
    uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    uint64_t op_count = 0;
    auto v_client = viper_->get_client();
    ValueType200 value;
    const ValueType200 null_value{0ul};
    std::chrono::high_resolution_clock::time_point start;
    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        const ycsb::Record& record = data[op_num];

        if (hdr != nullptr) {
            start = std::chrono::high_resolution_clock::now();
        }

        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                v_client.put(record.key, record.value);
                op_count++;
                break;
            }
            case ycsb::Record::Op::GET: {
                const bool found = v_client.get(record.key, &value);
                op_count += found && (value != null_value);
                break;
            }
            case ycsb::Record::Op::UPDATE: {
                auto update_fn = [&](ValueType200* value) {
                    value->data[0] = record.value.data[0];
                    internal::pmem_persist(value->data.data(), sizeof(uint64_t));
                };
                op_count += v_client.update(record.key, update_fn);
                break;
            }
            default: {
                throw std::runtime_error("Unknown operation: " + std::to_string(record.op));
            }
        }


        if (hdr == nullptr) {
            continue;
        }

        const auto end = std::chrono::high_resolution_clock::now();
        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
        hdr_record_value_atomic(hdr, duration.count());
    }

    return op_count;
}
    template <>
    uint64_t ViperFixture<KeyType8, ValueType200>::run_ycsb(
            uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr,uint32_t thread_id){
        uint64_t op_count = 0;
        auto v_client = viper_->get_client();
        ValueType200 value;
        const ValueType200 null_value{0ul};

        std::chrono::high_resolution_clock::time_point start;
        for (int op_num = start_idx; op_num < end_idx; ++op_num) {
            const ycsb::Record& record = data[op_num];

            if (hdr != nullptr) {
                start = std::chrono::high_resolution_clock::now();
            }

            switch (record.op) {
                case ycsb::Record::Op::INSERT: {
                    v_client.put(record.key, record.value,thread_id);
                    op_count++;
                    break;
                }
                case ycsb::Record::Op::GET: {
                    const bool found = v_client.get(record.key, &value,thread_id);
                    op_count += found && (value != null_value);
                    break;
                }
                case ycsb::Record::Op::UPDATE: {
                    auto update_fn = [&](ValueType200* value) {
                        value->data[0] = record.value.data[0];
                        internal::pmem_persist(value->data.data(), sizeof(uint64_t));
                    };
                    op_count += v_client.update(record.key, update_fn,thread_id);
                    break;
                }
                default: {
                    throw std::runtime_error("Unknown operation: " + std::to_string(record.op));
                }
            }


            if (hdr == nullptr) {
                continue;
            }

            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value_atomic(hdr, duration.count());
        }

        return op_count;
    }
}  // namespace kv_bm
}  // namespace viper
