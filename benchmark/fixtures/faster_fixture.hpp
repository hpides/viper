#pragma once

#include "common_fixture.hpp"
#include "core/faster.h"
#include "environment/file.h"
#include "device/file_system_disk.h"

namespace viper {
namespace kv_bm {

class FasterFixture : public FileBasedFixture {
  public:
    void InitMap(const uint64_t num_prefill_inserts = 0, const bool re_init = true);

    void DeInitMap();

    void insert_empty(uint64_t start_idx, uint64_t end_idx) override final;

    void setup_and_insert(uint64_t start_idx, uint64_t end_idx) override final;

    uint64_t setup_and_find(uint64_t start_idx, uint64_t end_idx) override final;

  protected:
    class FasterKey {
      public:
        FasterKey(uint64_t key)
            : key_{ key } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterKey));
        }
        inline KeyHash GetHash() const {
            return KeyHash{ Utility::GetHashCode(key_) };
        }

        inline bool operator==(const FasterKey& other) const {
            return key_ == other.key_;
        }
        inline bool operator!=(const FasterKey& other) const {
            return key_ != other.key_;
        }

      private:
        uint64_t key_;
    };

    class FasterValue {
      public:
        FasterValue()
            : value_{0 } {
        }
        FasterValue(const FasterValue& other)
            : value_{other.value_ } {
        }

        inline static constexpr uint32_t size() {
            return static_cast<uint32_t>(sizeof(FasterValue));
        }

        union {
            uint64_t value_;
            std::atomic<uint64_t> atomic_value_;
        };
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
            value.atomic_value_.store(input_);
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

        ReadContext(const FasterKey& key, uint64_t* result)
            : key_{ key }
            , result_{ result } {
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
            *result_ = value.atomic_value_;
        }

      protected:
        /// The explicit interface requires a DeepCopy_Internal() implementation.
        Status DeepCopy_Internal(IAsyncContext*& context_copy) {
            return IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

      private:
        FasterKey key_;
        uint64_t* result_;
    };

    typedef FASTER::environment::QueueIoHandler handler_t;
    typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
    typedef FASTER::core::FasterKv<FasterKey, FasterValue, disk_t> faster_t;

    std::unique_ptr<faster_t> db_;
    bool faster_initialized_;
    const uint64_t kRefreshInterval = 64;
    const uint64_t kCompletePendingInterval = 1600;
};

}  // namespace kv_bm
}  // namespace viper