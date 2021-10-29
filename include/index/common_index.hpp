//
// Created by bysoulwarden on 2021/10/25.
//

#ifndef VIPER_COMMON_INDEX_H
#define VIPER_COMMON_INDEX_H



namespace viper::index{
    using offset_size_t = uint64_t;
    using block_size_t = uint64_t;
    using page_size_t = uint8_t;
    using data_offset_size_t = uint16_t;

    struct KeyValueOffset {
        static constexpr offset_size_t INVALID = 0xFFFFFFFFFFFFFFFF;

        union {
            offset_size_t offset;
            struct {
                block_size_t block_number : 45;
                page_size_t page_number : 3;
                data_offset_size_t data_offset : 16;
            };
        };

        KeyValueOffset() : offset{INVALID} {}

        //KeyValueOffset(KeyValueOffset &k) : offset{k.get_offset()} {}

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

        inline  offset_size_t get_offset() const {
            return offset;
        }

        inline bool is_tombstone() const {
            return offset == INVALID;
        }

        inline bool operator==(const KeyValueOffset& rhs) const { return offset == rhs.offset; }
        inline bool operator!=(const KeyValueOffset& rhs) const { return offset != rhs.offset; }
    };


    template <typename KeyType>
    class BaseIndex{
    public:
        virtual KeyValueOffset Insert(const KeyType&, KeyValueOffset){
            throw std::runtime_error("Insert not implemented");
        }
        virtual KeyValueOffset Insert(const KeyType&, KeyValueOffset, std::function<bool(KeyType,KeyValueOffset)>){
            throw std::runtime_error("Insert not implemented");
        }
        virtual KeyValueOffset Get(const KeyType&){
            throw std::runtime_error("Get not implemented");
        }
        virtual KeyValueOffset Get(const KeyType&,std::function<bool(KeyType,KeyValueOffset)>){
            throw std::runtime_error("Get not implemented");
        }
    };
}
#endif //VIPER_COMMON_INDEX_H