#pragma once

#include "common_fixture.hpp"
#include "core/faster.h"
#include "environment/file.h"
#include "device/file_system_disk.h"

namespace viper {
namespace kv_bm {

template <typename KeyT, typename ValueT>
class FasterFixture : public BaseFixture {
    static constexpr size_t LOG_FILE_SEGMENT_SIZE = (1024l * 1024 * 1024) * 1;  // 1 GiB;
    static constexpr size_t LOG_MEMORY_SIZE = (1024l * 1024 * 1024) * 4;  // 4 GiB;
    static constexpr size_t INITIAL_MAP_SIZE = 1L << 22;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true);
    void DeInitMap();

    uint64_t insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_update(uint64_t start_idx, uint64_t end_idx) final;

    virtual std::string get_base_dir() = 0;

  protected:
    class FasterKey {
      public:
        FasterKey(uint64_t key)
            : key_{ key } {
        }
        FasterKey(const KeyT key)
            : key_{ key } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterKey));
        }
        inline KeyHash GetHash() const {
            return KeyHash{ Utility::GetHashCode(key_.data[0]) };
        }

        inline bool operator==(const FasterKey& other) const {
            return key_ == other.key_;
        }
        inline bool operator!=(const FasterKey& other) const {
            return !(key_ == other.key_);
        }

        inline const KeyT key() const { return key_; }

      private:
        KeyT key_;
    };

    class FasterValue {
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

    class UpsertContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        UpsertContext(const FasterKey& key, uint64_t input)
            : key_{ key }
            , input_{ input } {
        }

        /// Copy (and deep-copy) constructor.
        UpsertContext(const UpsertContext& other)
            : key_{ other.key_ }
            , input_{ other.input_ }
            , success_counter_{ other.success_counter_ } {
        }

        UpsertContext(const FasterKey& key, uint64_t input, uint64_t* success_counter)
            : key_{ key }
            , input_{ input }
            , success_counter_{ success_counter } {}

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FasterKey& key() const {
            return key_;
        }

        inline static constexpr uint32_t value_size() {
            return sizeof(value_t);
        }
        inline static constexpr uint32_t value_size(const FasterValue& old_value) {
            return sizeof(value_t);
        }

        inline void Put(value_t& value) {
            value.value_ = input_;
        }
        inline bool PutAtomic(value_t& value) {
            value.value_ = input_;
            return true;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        uint64_t input_;
        uint64_t* success_counter_;
    };

    class RmwContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        RmwContext(const FasterKey& key) : RmwContext{ key, nullptr } {}

        /// Copy (and deep-copy) constructor.
        RmwContext(const RmwContext& other)
        : key_{ other.key_ }
        , success_counter_{ other.success_counter_ } {}

        RmwContext(const FasterKey& key, uint64_t* success_counter)
        : key_{ key }
        , success_counter_{ success_counter } {}

        /// The implicit and explicit interfaces require a key() accessor.
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
            value.value_ = key_.key().data[0];
        }
        inline void RmwCopy(const FasterValue& old_value, FasterValue& value) {
            value.value_ = old_value.value_;
            value.value_.update_value();
        }
        inline bool RmwAtomic(FasterValue& value) {
            value.value_.update_value();
            return true;
        }

        inline uint64_t* getSuccessCounter() const {
            return success_counter_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        uint64_t* success_counter_;
    };

/// Context to read the store (after recovery).
    class ReadContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        ReadContext(const FasterKey& key, value_t* result) : ReadContext{key, result, nullptr} {}

        /// Copy (and deep-copy) constructor.
        ReadContext(const ReadContext& other)
            : key_{ other.key_ }
            , result_{ other.result_ }
            , success_counter_{ other.success_counter_ } {
        }

        ReadContext(const FasterKey& key, value_t* result, uint64_t* success_counter)
            : key_{ key }
            , result_{ &result->value_ }
            , success_counter_{ success_counter } {
        }

        /// The implicit and explicit interfaces require a key() accessor.
        inline const FasterKey& key() const {
            return key_;
        }

        inline void Get(const value_t& value) {
            *result_ = value.value_;
        }
        inline void GetAtomic(const value_t& value) {
            *result_ = value.value_;
        }

        inline const ValueT* getResult() const {
            return result_;
        }

        inline uint64_t* getSuccessCounter() const {
            return success_counter_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        ValueT* result_;
        std::uint64_t* success_counter_;
    };

    class DeleteContext : public IAsyncContext {
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
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        ValueT* result_;
        uint64_t* success_counter_;
    };

    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemDisk<handler_t, LOG_FILE_SEGMENT_SIZE> disk_t;
    typedef FASTER::core::FasterKv<FasterKey, FasterValue, disk_t> faster_t;

    std::unique_ptr<faster_t> db_;
    std::filesystem::path base_dir_;
    std::filesystem::path db_dir_;
    bool faster_initialized_;
    const uint64_t kRefreshInterval = 64;
    const uint64_t kCompletePendingInterval = 1600;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class DiskFasterFixture : public FasterFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT = KeyType16, typename ValueT = ValueType200>
class PmemFasterFixture : public FasterFixture<KeyT, ValueT> {
  public:
    std::string get_base_dir() override;
};

template <typename KeyT, typename ValueT>
void FasterFixture<KeyT, ValueT>::InitMap(uint64_t num_prefill_inserts, const bool re_init) {
    if (faster_initialized_ && !re_init) {
        return;
    }

    base_dir_ = DB_NVM_DIR;
    db_dir_ = random_file(base_dir_);
    std::filesystem::create_directory(db_dir_);

    // Make sure this is a power of two
//    const size_t initial_map_size = 1UL << ((size_t) std::log2(INITIAL_MAP_SIZE * SCALE_FACTOR - 1) + 1);
    const size_t initial_map_size = INITIAL_MAP_SIZE;

    // Make sure this is a multiple of 32 MiB
    const size_t page_size = PersistentMemoryMalloc<disk_t>::kPageSize;
//    const size_t log_memory_size = (size_t)((LOG_MEMORY_SIZE * SCALE_FACTOR) / page_size) * page_size;
    const size_t log_memory_size = LOG_MEMORY_SIZE;

    db_ = std::make_unique<faster_t>(initial_map_size, log_memory_size, db_dir_);

    db_->StartSession();
    prefill(num_prefill_inserts);
    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

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
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
        if (result != Status::Ok) {
            throw new std::runtime_error("Bad insert");
        }
    };

    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        UpsertContext context{key, key};
        insert_counter += db_->Upsert(context, callback, 1) == Status::Ok;
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
uint64_t FasterFixture<KeyT, ValueT>::setup_and_find(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
        const uint64_t key = context->key().key().data[0];
        const bool success = result == Status::Ok && (context->getResult()->data[0] == key);
        *context->getSuccessCounter() += success;
    };

    uint64_t found_counter = 0;
    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        FasterValue result;
        ReadContext context{key, &result};
        const bool found = db_->Read(context, callback, key) == FASTER::core::Status::Ok;
        found_counter += found && (result.value_.data[0] == key);
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return found_counter;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_delete(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<DeleteContext> context{ctxt};
        *context->getSuccessCounter() += result == Status::Ok;
    };

    uint64_t delete_counter = 0;
    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        DeleteContext context{key};
        delete_counter += db_->Delete(context, callback, key) == FASTER::core::Status::Ok;
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return delete_counter;
}

template <typename KeyT, typename ValueT>
uint64_t FasterFixture<KeyT, ValueT>::setup_and_update(uint64_t start_idx, uint64_t end_idx) {
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallbackContext<RmwContext> context{ ctxt };
        *context->getSuccessCounter() += result == Status::Ok;
    };

    uint64_t update_counter = 0;
    db_->StartSession();

    for (uint64_t key = start_idx; key < end_idx; ++key) {
        if (key % kRefreshInterval == 0) {
            db_->Refresh();
            if (key % kCompletePendingInterval == 0) {
                db_->CompletePending(false);
            }
        }

        RmwContext context{key, &update_counter};
        update_counter += db_->Rmw(context, callback, key) == Status::Ok;
    }

    db_->Refresh();
    db_->CompletePending(true);
    db_->StopSession();

    return update_counter;
}

template <typename KeyT, typename ValueT>
std::string DiskFasterFixture<KeyT, ValueT>::get_base_dir() {
    return DB_FILE_DIR;
}

template <typename KeyT, typename ValueT>
std::string PmemFasterFixture<KeyT, ValueT>::get_base_dir() {
    return DB_NVM_DIR;
}

}  // namespace kv_bm
}  // namespace viper