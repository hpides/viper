#pragma once

#include "common_fixture.hpp"
#include "core/faster.h"
#include "environment/file.h"
#include "device/file_system_disk.h"

namespace viper {
namespace kv_bm {

class FasterFixture : public BaseFixture {
    static constexpr size_t LOG_FILE_SIZE = (1024l * 1024 * 1024) * 1;  // 1 GiB;
    static constexpr size_t LOG_MEMORY_SIZE = (1024l * 1024 * 1024) * 4;  // 1 GiB;
    static constexpr size_t INITIAL_MAP_SIZE = 1L << 23;

  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true);
    void DeInitMap();

    void insert_empty(uint64_t start_idx, uint64_t end_idx) final;
    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) final;
    uint64_t setup_and_delete(uint64_t start_idx, uint64_t end_idx) final;

  protected:
    class FasterKey {
      public:
        FasterKey(uint64_t key)
            : key_{ key } {
        }
        FasterKey(BMKeyFixed key)
            : key_{ key } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterKey));
        }
        inline KeyHash GetHash() const {
            return KeyHash{ Utility::GetHashCode(key_.uuid[0]) };
        }

        inline bool operator==(const FasterKey& other) const {
            return key_ == other.key_;
        }
        inline bool operator!=(const FasterKey& other) const {
            return !(key_ == other.key_);
        }

      private:
        BMKeyFixed key_;
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

        BMValueFixed value_;
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
            , input_{ other.input_ } {
        }

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
    };

/// Context to read the store (after recovery).
    class ReadContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        ReadContext(const FasterKey& key, value_t* result)
            : key_{ key }
            , result_{ &result->value_ } {
        }

        /// Copy (and deep-copy) constructor.
        ReadContext(const ReadContext& other)
            : key_{ other.key_ }
            , result_{ other.result_ } {
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

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        BMValueFixed* result_;
    };

    class DeleteContext : public IAsyncContext {
      public:
        typedef FasterKey key_t;
        typedef FasterValue value_t;

        DeleteContext(const FasterKey& key) : key_{key} {}

        DeleteContext(const DeleteContext& other) : key_{other.key_} {}

        inline const FasterKey& key() const {
            return key_;
        }

        inline uint32_t value_size() const {
            return sizeof(value_t);
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        BMValueFixed* result_;
    };

    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemDisk<handler_t, LOG_FILE_SIZE> disk_t;
    typedef FASTER::core::FasterKv<FasterKey, FasterValue, disk_t> faster_t;

    std::unique_ptr<faster_t> db_;
    std::filesystem::path base_dir_;
    std::filesystem::path db_dir_;
    bool faster_initialized_;
    const uint64_t kRefreshInterval = 64;
    const uint64_t kCompletePendingInterval = 1600;
};

}  // namespace kv_bm
}  // namespace viper