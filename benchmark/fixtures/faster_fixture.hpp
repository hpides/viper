#pragma once

#include "common_fixture.hpp"
#include "core/faster.h"
#include "environment/file.h"
#include "device/file_system_disk.h"
#include "device/null_disk.h"
#include "libpmem.h"

namespace viper {
namespace kv_bm {

template <typename KeyT, typename ValueT>
class FasterFixture : public BaseFixture {
    static constexpr size_t LOG_FILE_SEGMENT_SIZE = 1 * ONE_GB;
    static constexpr size_t LOG_MEMORY_SIZE = 6 * ONE_GB;
    static constexpr size_t NVM_LOG_SIZE = LOG_MEMORY_SIZE;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true);
    void DeInitMap();

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) final;

    uint64_t run_ycsb(uint64_t start_idx, uint64_t end_idx,
                     const std::vector<ycsb::Record>& data, hdr_histogram* hdr) final;

    virtual std::string get_base_dir() = 0;
    virtual bool is_nvm_log() { return false; };

  protected:
    template <typename FK, typename FV>
    class ReadContext;
    template <typename FK, typename FV>
    class UpsertContext;

    class FasterKey {
        template <typename FK, typename FV>
        friend class ReadContext;
        template <typename FK, typename FV>
        friend class UpsertContext;
      public:
        explicit FasterKey(uint64_t key)
            : key_{ key } {
        }
        explicit FasterKey(const KeyT& key)
            : key_{ key } {
        }
        explicit FasterKey(const FasterKey& other)
            : key_{other.key_} {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterKey));
        }
        inline FASTER::core::KeyHash GetHash() const {
            return FASTER::core::KeyHash{ FASTER::core::Utility::HashBytes((uint16_t*) key_.data.data(), key_.data.size() * 2) };
        }

        inline bool operator==(const FasterKey& other) const {
            return key_ == other.key_;
        }
        inline bool operator!=(const FasterKey& other) const {
            return !(key_ == other.key_);
        }

        inline const KeyT& key() const { return key_; }

      private:
        KeyT key_;
    };

    class FasterKeyVar {
        template <typename FK, typename FV>
        friend class ReadContext;
        template <typename FK, typename FV>
        friend class UpsertContext;
      public:
        explicit FasterKeyVar(uint64_t key_idx) : key_{static_cast<uint32_t>(key_idx)} {
            const std::string& key_ref = var_size_kvs_.first[key_idx];
            key_buf_ = key_ref.data();
            key_length_ = key_ref.size();
        }

        explicit FasterKeyVar(const FasterKeyVar& other)
            : key_length_{other.key_length_}, key_{other.key_} {
            key_buf_ = nullptr;
            if (other.key_buf_ == nullptr) {
                memcpy(buffer(), other.buffer(), key_length_);
            } else {
                memcpy(buffer(), other.key_buf_, key_length_);
            }
        }

        uint32_t size() const {
            return static_cast<uint32_t>(sizeof(FasterKeyVar) + key_length_);

        }
        inline FASTER::core::KeyHash GetHash() const {
            if (key_buf_ != nullptr) {
                return FASTER::core::KeyHash(FASTER::core::Utility::HashBytes((uint16_t*) key_buf_, key_length_ / 2));
            }
            return FASTER::core::KeyHash(FASTER::core::Utility::HashBytes((uint16_t*) buffer(), key_length_ / 2));
        }

        inline bool operator==(const FasterKeyVar& other) const {
            return key_ == other.key_;
            if (key_length_ != other.key_length_) return false;
            if (key_buf_ != nullptr) {
                return memcmp(key_buf_, other.buffer(), key_length_) == 0;
            }
            return memcmp(buffer(), other.buffer(), key_length_) == 0;
        }

        inline bool operator!=(const FasterKeyVar& other) const {
            return !(*this == other);
        }

        inline const uint32_t key_idx() const { return key_; }

      private:
        const uint32_t key_;
        uint32_t key_length_;
        const char* key_buf_;

        inline const char* buffer() const {
            return reinterpret_cast<const char*>(this + 1);
        }
        inline char* buffer() {
            return reinterpret_cast<char*>(this + 1);
        }
    };

    class FasterValue {
        template <typename FK, typename FV>
        friend class ReadContext;
        template <typename FK, typename FV>
        friend class UpsertContext;
      public:
        FasterValue()
            : value_{} {
        }
        FasterValue(const FasterValue& other)
            : value_{other.value_} {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterValue));
        }

        ValueT value_;
    };

    class FasterValueVar {
        template <typename FK, typename FV>
        friend class ReadContext;
        template <typename FK, typename FV>
        friend class UpsertContext;
      public:
        FasterValueVar()
            : length_{0} {
        }

        uint32_t size() const {
            return length_;
        }

        const char* buffer() const {
            return reinterpret_cast<const char*>(this + 1);
        }
        char* buffer() {
            return reinterpret_cast<char*>(this + 1);
        }

        uint32_t length_;
    };

    struct BaseContext {
        using NanoTime = std::chrono::time_point<std::chrono::high_resolution_clock>;

        BaseContext(bool is_nvm, uint64_t* success_counter, hdr_histogram* hdr, const NanoTime& start)
            : is_nvm(is_nvm)
            , success_counter(success_counter)
            , hdr{ hdr }
            , start{ start } {}

        BaseContext(bool is_nvm, uint64_t* success_counter)
            : BaseContext{ is_nvm, success_counter, nullptr, NanoTime{} } {}

        explicit BaseContext(bool is_nvm) : BaseContext{ is_nvm, nullptr } {}

        bool is_nvm;
        uint64_t* success_counter;
        hdr_histogram* hdr;
        NanoTime start;
    };

    template <typename FK, typename FV>
    class UpsertContext : public FASTER::core::IAsyncContext {
      public:
        typedef FK key_t;
        typedef FV value_t;

        UpsertContext(const FK& key, uint64_t input)
            : UpsertContext{ key, input, BaseContext{false} } {
        }

        /// Copy (and deep-copy) constructor.
        UpsertContext(const UpsertContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ }
            , value_{ other.value_ }
            , base_{ other.base_ } {
        }

        UpsertContext(const FK& key, uint64_t input, const BaseContext& base)
            : key_{ key.key_ }
            , input_{ input }
            , value_{ nullptr }
            , base_{ base } {
            if constexpr (std::is_same_v<FV, FasterValueVar>) {
                value_ = &var_size_kvs_.second[input_];
            }
        }

        UpsertContext(const FK& key, const ValueT* value, const BaseContext& base)
            : key_{ key.key_ }
            , input_{ 0 }
            , value_{ value }
            , base_{ base } {
        }

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FK& key() const {
            return key_;
        }

        uint32_t value_size() const {
            if constexpr (std::is_same_v<FV, FasterValueVar>) {
                return sizeof(FasterValueVar) + value_->size();
            } else {
                return sizeof(value_t);
            }
        }

        inline void Put(value_t& value) {
            if constexpr (std::is_same_v<FV, FasterValueVar>) {
                value.length_ = value_->size();
                std::memcpy(value.buffer(), value_->data(), value_->size());

                if (base_.is_nvm) {
                    pmem_persist(value.buffer(), sizeof(value_->size()));
                }
            } else {
                if (value_ != nullptr) {
                    value.value_ = *value_;
                } else {
                    value.value_ = input_;
                }
                if (base_.is_nvm) {
                    pmem_persist(&value.value_, sizeof(value.value_));
                }
            }
        }

        inline bool PutAtomic(value_t& value) {
            return false;
        }

        inline const BaseContext& getBaseContext() const {
            return base_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        const FK key_;
        const uint64_t input_;
        const ValueT* value_;
        const BaseContext base_;
    };

    class RmwContext : public FASTER::core::IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        RmwContext(const FasterKey& key, const BaseContext& base)
            : key_{ key}
            , base_{ base } {}

        RmwContext(const RmwContext& other)
            : key_{ other.key_ }
            , base_{ other.base_ } {}

        RmwContext(const FasterKey& key, const ValueT* value, const BaseContext& base)
            : key_{ key }
            , new_value_{ value }
            , base_{ base } {}

        inline const FasterKey& key() const {
            return key_;
        }

        inline static constexpr uint32_t value_size() {
            return sizeof(value_t);
        }
        inline static constexpr uint32_t value_size(const FasterValue& old_value) {
            return sizeof(value_t);
        }

        inline void RmwInitial(FasterValue& value) {
            value.value_ = *new_value_;
            if (base_.is_nvm) {
                pmem_persist(&value.value_, sizeof(value.value_));
            }
        }
        inline void RmwCopy(const FasterValue& old_value, FasterValue& value) {
            value.value_ = old_value.value_;
            RmwAtomic(value);
        }

        inline bool RmwAtomic(FasterValue& value) {
            if constexpr (std::is_same_v<ValueT, std::string>) {
                return false;
            } else {
                if (new_value_ != nullptr) {
                    value.value_ = *new_value_;
                } else {
                    value.value_.update_value();
                }
                if (base_.is_nvm) {
                    pmem_persist(&value.value_, sizeof(value.value_));
                }
                return true;
            }
        }

        inline const BaseContext& getBaseContext() const {
            return base_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        const BaseContext base_;
        const ValueT* new_value_ = nullptr;
        uint64_t* success_counter_;
    };

    template <typename FK, typename FV>
    class ReadContext : public FASTER::core::IAsyncContext {
      public:
        typedef FK key_t;
        typedef FV value_t;

        ReadContext(const FK& key, ValueT* result)
            : ReadContext{key, result, BaseContext{false}} {}

        ReadContext(const FK& key, ValueT* result, const BaseContext& base)
            : key_{ key.key_ }
            , result_{ result }
            , base_{ base } {
        }

        /// Copy (and deep-copy) constructor.
        ReadContext(const ReadContext& other)
            : key_{ other.key_ }
            , result_{ other.result_ }
            , base_{ other.base_ } {
        }

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FK& key() const {
            return key_;
        }

        inline void Get(const value_t& value) {
            if constexpr (std::is_same_v<FV, FasterValueVar>) {
                result_->resize(value.size());
                result_->assign(value.buffer(), value.size());
            } else {
                *result_ = value.value_;
            }
        }

        inline void GetAtomic(const value_t& value) {
            return Get(value);
        }

        inline const ValueT* getResult() const {
            return result_;
        }

        inline const BaseContext& getBaseContext() const {
            return base_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FK key_;
        ValueT* result_;
        const BaseContext base_;
    };

    class DeleteContext : public FASTER::core::IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        DeleteContext(const FasterKey& key) : DeleteContext{ key, nullptr } {}

        DeleteContext(const DeleteContext& other)
        : key_{other.key_}
        , success_counter_{ other.success_counter_ } {}

        DeleteContext(const FasterKey& key, uint64_t* success_counter)
        : key_{key}
        , success_counter_{ success_counter } {}

        inline const FasterKey& key() const {
            return key_;
        }

        inline uint32_t value_size() const {
            return sizeof(value_t);
        }

        inline uint64_t* getSuccessCounter() const {
            return success_counter_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        FASTER::core::Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        ValueT* result_;
        uint64_t* success_counter_;
    };

    typedef typename std::conditional<std::is_same_v<KeyT, std::string>, FasterKeyVar, FasterKey>::type FK_T;
    typedef typename std::conditional<std::is_same_v<ValueT, std::string>, FasterValueVar, FasterValue>::type FV_T;
    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemDisk<handler_t, LOG_FILE_SEGMENT_SIZE> disk_t;
//    typedef FASTER::device::NullDisk disk_t;
    typedef FASTER::core::FasterKv<FK_T, FV_T, disk_t> faster_t;

    std::unique_ptr<faster_t> db_;
    std::filesystem::path base_dir_;
    std::filesystem::path db_dir_;
    bool faster_initialized_;
    const uint64_t kRefreshInterval = 64;
    const uint64_t kCompletePendingInterval = 1000;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class DiskHybridFasterFixture : public FasterFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class PmemHybridFasterFixture : public FasterFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class NvmFasterFixture : public FasterFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
    bool is_nvm_log() override;
};

template <typename KeyT, typename ValueT>
void FasterFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (faster_initialized_ && !re_init) {
        return;
    }

    const bool is_nvm = is_nvm_log();
    base_dir_ = get_base_dir();
    db_dir_ = random_file(base_dir_);
    std::filesystem::create_directory(db_dir_);


    // Make sure this is a power of two
    size_t initial_map_size = 1UL << ((size_t) std::log2(num_prefill_inserts) - 1);
    if (num_prefill_inserts == 0) {
        // Default to 33 mio. buckets
        initial_map_size = 1UL << 25;
    }

    // Make sure this is a multiple of 32 MiB
    const size_t page_size = FASTER::core::PersistentMemoryMalloc<disk_t>::kPageSize;
    size_t log_memory_size = (size_t)((LOG_MEMORY_SIZE) / page_size) * page_size;

    if (is_nvm) {
        log_memory_size = (size_t)(NVM_LOG_SIZE / page_size) * page_size;
    }

    DEBUG_LOG("Creating FASTER with " << initial_map_size << " buckets and a "
              << (log_memory_size / ONE_GB) << " GiB log in " << (is_nvm ? "NVM" : "DRAM")
              << " mode." << std::endl);
    db_ = std::make_unique<faster_t>(initial_map_size, log_memory_size, db_dir_);
//    db_ = std::make_unique<faster_t>(initial_map_size, log_memory_size, "");

    prefill(num_prefill_inserts);
    faster_initialized_ = true;
}

template <typename KeyT, typename ValueT>
void FasterFixture<KeyT, ValueT>::DeInitMap() {
    db_ = nullptr;
    faster_initialized_ = false;
    std::filesystem::remove_all(db_dir_);
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::insert(uint64_t start_idx, uint64_t end_idx) {
    uint64_t insert_counter = 0;
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        if (result != FASTER::core::Status::Ok) {
            throw new std::runtime_error("Bad insert");
        }
    };

    db_->StartSession();

    const bool is_nvm = is_nvm_log();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
        }
        if (key % kCompletePendingInterval == 0) {
            db_->CompletePending(false);
        }

        BaseContext base{is_nvm, &insert_counter};
        if constexpr (std::is_same_v<KeyT, std::string>) {
            UpsertContext<FasterKeyVar, FasterValueVar> context{FasterKeyVar{key}, key, base};
            insert_counter += db_->Upsert(context, callback, 1) == FASTER::core::Status::Ok;
        } else {
            UpsertContext<FasterKey, FasterValue> context{FasterKey{key}, key, base};
            insert_counter += db_->Upsert(context, callback, 1) == FASTER::core::Status::Ok;
        }
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();
    return insert_counter;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_insert(uint64_t start_idx, uint64_t end_idx) {
    return insert(start_idx, end_idx);
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx, uint64_t num_finds) {
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        if constexpr (std::is_same_v<KeyT, std::string>) {
            FASTER::core::CallbackContext<ReadContext<FasterKeyVar, FasterValueVar>> context{ctxt};
            if (result != FASTER::core::Status::Ok) { return; }
            const size_t key_idx = context->key().key_idx();
            const std::string& value = var_size_kvs_.second[key_idx];
            const bool success = *context->getResult() == value;
            *context->getBaseContext().success_counter += success;
        } else {
            if (result != FASTER::core::Status::Ok) { return; }
            FASTER::core::CallbackContext<ReadContext<FasterKey, FasterValue>> context{ctxt};
            const auto key = context->key().key().data[0];
            ValueT value{key};
            const bool success = *context->getResult() == value;
            *context->getBaseContext().success_counter += success;
        }
    };

    uint64_t found_counter = 0;
    db_->StartSession();

    const bool is_nvm = is_nvm_log();

    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    std::vector<ValueT> results{};
    results.resize(num_finds);
    for (uint64_t i = 0; i < num_finds; ++i) {
        if (i % kRefreshInterval == 0) {
            db_->Refresh();
        }
        if (i % kCompletePendingInterval == 0) {
            db_->CompletePending(false);
        }

        const uint64_t key = distrib(rnd_engine);
        results[i] = ValueT();
        ValueT& result = results[i];
        ValueT value{};
        if constexpr (std::is_same_v<KeyT, std::string>) {
            value = var_size_kvs_.second[key];
        } else {
            value = ValueT{key};
        }

        BaseContext base{is_nvm, &found_counter};
        bool found;
        if constexpr (std::is_same_v<KeyT, std::string>) {
            ReadContext<FasterKeyVar, FasterValueVar> context{FasterKeyVar{key}, &result, base};
            found = db_->Read(context, callback, i) == FASTER::core::Status::Ok;
        } else {
            ReadContext<FasterKey, FasterValue> context{FasterKey{key}, &result, base};
            found = db_->Read(context, callback, i) == FASTER::core::Status::Ok;
        }
        found_counter += found && (result == value);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx, uint64_t num_deletes) {
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        FASTER::core::CallbackContext<DeleteContext> context{ctxt};
        *context->getSuccessCounter() += result == FASTER::core::Status::Ok;
    };

    uint64_t delete_counter = 0;
    db_->StartSession();

    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    for (uint64_t i = 0; i < num_deletes; ++i) {
        if (i % kRefreshInterval == 0) {
            db_->Refresh();
        }
        if (i % kCompletePendingInterval == 0) {
            db_->CompletePending(false);
        }

        const uint64_t key = distrib(rnd_engine);
        DeleteContext context{FasterKey{key}};
        delete_counter += db_->Delete(context, callback, i) == FASTER::core::Status::Ok;
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return delete_counter;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx, uint64_t num_updates) {
    auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
        FASTER::core::CallbackContext<RmwContext> context{ ctxt };
        *context->getBaseContext().success_counter += result == FASTER::core::Status::Ok;
    };

    uint64_t update_counter = 0;
    db_->StartSession();

    const bool is_nvm = is_nvm_log();

    std::random_device rnd{};
    auto rnd_engine = std::default_random_engine(rnd());
    std::uniform_int_distribution<> distrib(start_idx, end_idx);

    for (uint64_t i = 0; i < num_updates; ++i) {
        if (i % kRefreshInterval == 0) {
            db_->Refresh();
        }
        if (i % kCompletePendingInterval == 0) {
            db_->CompletePending(false);
        }

        const uint64_t key = distrib(rnd_engine);
        BaseContext base{is_nvm, &update_counter};
        RmwContext context{FasterKey{key}, base};
        update_counter += db_->Rmw(context, callback, i) == FASTER::core::Status::Ok;
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return update_counter;
}

template <>
uint64_t FasterFixture<std::string, std::string>::setup_and_update(uint64_t, uint64_t, uint64_t) {
    return 0;
}

template <>
uint64_t FasterFixture<std::string, std::string>::setup_and_delete(uint64_t, uint64_t, uint64_t) {
    return 0;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::run_ycsb(uint64_t, uint64_t,
    const std::vector<ycsb::Record>&, hdr_histogram*) {
    throw std::runtime_error{"YCSB not implemented for non-ycsb key/value types."};
}

template <>
uint64_t FasterFixture<KeyType8, ValueType200>::run_ycsb(
    uint64_t start_idx, uint64_t end_idx, const std::vector<ycsb::Record>& data, hdr_histogram* hdr) {
    uint64_t op_count = 0;

    db_->StartSession();
    const bool is_nvm = is_nvm_log();
    ValueType200 value;
    const ValueType200 null_value{0ul};
    std::chrono::high_resolution_clock::time_point start;

    for (int op_num = start_idx; op_num < end_idx; ++op_num) {
        if (op_num % kRefreshInterval == 0) {
            db_->Refresh();
        }
        if (op_num % kCompletePendingInterval == 0) {
            db_->CompletePending(false);
        }

        if (hdr != nullptr) {
            start = std::chrono::high_resolution_clock::now();
        }

        const ycsb::Record& record = data[op_num];
        switch (record.op) {
            case ycsb::Record::Op::INSERT: {
                auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
                    FASTER::core::CallbackContext<UpsertContext<FasterKey, FasterValue>> context{ctxt};
                    const BaseContext& base = context->getBaseContext();
                    *base.success_counter += result == FASTER::core::Status::Ok;
                    if (base.hdr != nullptr) {
                        const auto end = std::chrono::high_resolution_clock::now();
                        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - base.start);
                        hdr_record_value(base.hdr, duration.count());
                    }
                };

                BaseContext base{is_nvm, &op_count, hdr, start};
                UpsertContext<FasterKey, FasterValue> context{FasterKey{record.key}, &record.value, base};
                const bool success = db_->Upsert(context, callback, op_num) == FASTER::core::Status::Ok;
                if (success) {
                    op_count++;
                    break;
                }
                continue;
            }
            case ycsb::Record::Op::GET: {
                auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
                    FASTER::core::CallbackContext<ReadContext<FasterKey, FasterValue>> context{ctxt};
                    const BaseContext& base = context->getBaseContext();
                    const bool success = result == FASTER::core::Status::Ok && (context->getResult()->data[0] != 0);
                    *base.success_counter += success;
                    if (base.hdr != nullptr) {
                        const auto end = std::chrono::high_resolution_clock::now();
                        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - base.start);
                        hdr_record_value(base.hdr, duration.count());
                    }
                };

                BaseContext base{is_nvm, &op_count, hdr, start};
                ReadContext<FasterKey, FasterValue> context{FasterKey{record.key}, &value, base};
                const bool found = db_->Read(context, callback, op_num) == FASTER::core::Status::Ok;
                const bool success = found && (value != null_value);
                if (success) {
                    op_count++;
                    break;
                }
                continue;
            }
            case ycsb::Record::Op::UPDATE: {
                auto callback = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result) {
                    FASTER::core::CallbackContext<RmwContext> context{ ctxt };
                    const BaseContext& base = context->getBaseContext();
                    *base.success_counter += result == FASTER::core::Status::Ok;
                    if (base.hdr != nullptr) {
                        const auto end = std::chrono::high_resolution_clock::now();
                        const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - base.start);
                        hdr_record_value(base.hdr, duration.count());
                    }
                };
                BaseContext base{is_nvm, &op_count, hdr, start};
                RmwContext context{FasterKey{record.key}, &record.value, base};
                const bool success = db_->Rmw(context, callback, op_num) == FASTER::core::Status::Ok;
                if (success) {
                    op_count++;
                    break;
                }
                continue;
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
        hdr_record_value(hdr, duration.count());
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return op_count;
}

template <typename KeyT, typename ValueT>
std::string DiskHybridFasterFixture<KeyT, ValueT>::get_base_dir() {
    return DB_FILE_DIR + std::string{"/faster"};
}

template <typename KeyT, typename ValueT>
std::string PmemHybridFasterFixture<KeyT, ValueT>::get_base_dir() {
    return DB_PMEM_DIR + std::string{"/faster"};
}

template <typename KeyT, typename ValueT>
std::string NvmFasterFixture<KeyT, ValueT>::get_base_dir() {
    return DB_PMEM_DIR + std::string{"/faster"};
}

template <typename KeyT, typename ValueT>
bool NvmFasterFixture<KeyT, ValueT>::is_nvm_log() {
    return true;
}

}  // namespace kv_bm
}  // namespace viper
