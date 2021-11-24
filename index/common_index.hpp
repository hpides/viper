//
// Created by bysoulwarden on 2021/10/25.
//

#ifndef VIPER_COMMON_INDEX_H
#define VIPER_COMMON_INDEX_H

#include <hdr_histogram.h>
#include <set>

namespace viper::index {
    using offset_size_t = uint64_t;
    using block_size_t = uint64_t;
    using page_size_t = uint8_t;
    using data_offset_size_t = uint16_t;

    struct KeyValueOffset {
        static constexpr offset_size_t INVALID = 0xFFFFFFFFFFFFFFFF;

        union {
            offset_size_t offset;
            struct {
                block_size_t block_number: 45;
                page_size_t page_number: 3;
                data_offset_size_t data_offset: 16;
            };
        };

        KeyValueOffset() : offset{INVALID} {}

        static KeyValueOffset NONE() { return KeyValueOffset{INVALID}; }

        explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

        KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const data_offset_size_t slot)
                : block_number{block_number}, page_number{page_number}, data_offset{slot} {}

        static KeyValueOffset Tombstone() {
            return KeyValueOffset{};
        }

        inline std::tuple<block_size_t, page_size_t, data_offset_size_t> get_offsets() const {
            return {block_number, page_number, data_offset};
        }

        inline offset_size_t get_offset() const {
            return offset;
        }

        inline bool is_tombstone() const {
            return offset == INVALID;
        }

        inline bool operator==(const KeyValueOffset &rhs) const { return offset == rhs.offset; }

        inline bool operator!=(const KeyValueOffset &rhs) const { return offset != rhs.offset; }
    };

    template<typename T1, typename T2>
    struct BulkComparator
    {
        bool operator() (const std::pair<T1, T2>& lhs,
                         const std::pair<T1, T2>& rhs) const
        {
            return lhs.first < rhs.first;
        }
    };

    template<typename KeyType>
    class BaseIndex {
    public:
        hdr_histogram *op_hdr;
        hdr_histogram *retrain_hdr;
        std::chrono::high_resolution_clock::time_point start;

        virtual BaseIndex* bulk_load(std::vector<std::pair<uint64_t, KeyValueOffset>> * vector){
            return nullptr;
        }


        virtual hdr_histogram *GetOpHdr() {
            if (op_hdr == nullptr) {
                hdr_init(1, 1000000000, 4, &op_hdr);
            }
            return op_hdr;
        }

        hdr_histogram *GetRetrainHdr() {
            if (retrain_hdr == nullptr) {
                hdr_init(1, 1000000000, 4, &retrain_hdr);
            }
            return retrain_hdr;
        }

        void LogRetrainStart() {
            if (retrain_hdr == nullptr) {
                return;
            }
            start = std::chrono::high_resolution_clock::now();
        }
        void LogRetrainEnd() {
            if (retrain_hdr == nullptr) {
                return;
            }
            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value(retrain_hdr, duration.count());
        }

        BaseIndex() {
            op_hdr = nullptr;
            retrain_hdr = nullptr;
        }

        virtual ~BaseIndex() {};

        virtual bool SupportBulk(){
            return false;
        }

        virtual uint64_t GetIndexSize() { throw std::runtime_error("GetIndexSize not implemented"); }

        virtual KeyValueOffset Insert(const KeyType &k, KeyValueOffset o) {
            std::chrono::high_resolution_clock::time_point start;
            if (op_hdr != nullptr) {
                start = std::chrono::high_resolution_clock::now();
            }
            KeyValueOffset ret = CoreInsert(k, o);
            if (op_hdr == nullptr) {
                return ret;
            }
            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value(op_hdr, duration.count());
            return ret;
        }

        virtual KeyValueOffset
        Insert(const KeyType &k, KeyValueOffset o, std::function<bool(KeyType, KeyValueOffset)> f) {
            std::chrono::high_resolution_clock::time_point start;
            if (op_hdr != nullptr) {
                start = std::chrono::high_resolution_clock::now();
            }
            KeyValueOffset ret = CoreInsert(k, o, f);
            if (op_hdr == nullptr) {
                return ret;
            }
            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value(op_hdr, duration.count());
            return ret;
        }

        virtual KeyValueOffset CoreInsert(const KeyType &, KeyValueOffset) {
            throw std::runtime_error("Insert not implemented");
        }

        virtual KeyValueOffset
        CoreInsert(const KeyType & k, KeyValueOffset o, std::function<bool(KeyType, KeyValueOffset)> f) {
            return CoreInsert(k,o);
        }

        virtual KeyValueOffset Get(const KeyType &k) {
            std::chrono::high_resolution_clock::time_point start;
            if (op_hdr != nullptr) {
                start = std::chrono::high_resolution_clock::now();
            }
            KeyValueOffset ret = CoreGet(k);
            if (op_hdr == nullptr) {
                return ret;
            }
            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value(op_hdr, duration.count());
            return ret;
        }

        virtual KeyValueOffset Get(const KeyType &k, std::function<bool(KeyType, KeyValueOffset)> f) {
            std::chrono::high_resolution_clock::time_point start;
            if (op_hdr != nullptr) {
                start = std::chrono::high_resolution_clock::now();
            }
            KeyValueOffset ret = CoreGet(k, f);
            if (op_hdr == nullptr) {
                return ret;
            }
            const auto end = std::chrono::high_resolution_clock::now();
            const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
            hdr_record_value(op_hdr, duration.count());
            return ret;
        }

        virtual KeyValueOffset CoreGet(const KeyType &) {
            throw std::runtime_error("Get not implemented");
        }

        virtual KeyValueOffset CoreGet(const KeyType & k, std::function<bool(KeyType, KeyValueOffset)> f) {
            return CoreGet(k);
        }
    };
}
#endif //VIPER_COMMON_INDEX_H