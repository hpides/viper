#pragma once

#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <libpmemobj++/mutex.hpp>
#include <bitset>
#include <libpmem.h>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <filesystem>

namespace viper {

namespace pobj = pmem::obj;

static constexpr uint16_t NUM_SLOTS_PER_PAGE = 254;
static constexpr uint32_t PAGE_SIZE = 4 * 1024; // 4kb
static constexpr uint32_t NUM_DIMMS = 6;

using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using slot_size_t = uint8_t;

namespace internal {

template <typename K, typename V>
struct ViperPage {
    using VEntry = std::pair<K, V>;
    std::bitset<NUM_SLOTS_PER_PAGE> free_slots;
    std::array<VEntry, NUM_SLOTS_PER_PAGE> data;

    ViperPage() {
        free_slots.flip();
    }
};

template <typename VPage, size_t num_pages>
struct ViperPageBlock {
    static constexpr uint64_t num_slots_per_block = NUM_SLOTS_PER_PAGE * num_pages;
    /**
     * Array to store all persistent ViperPages.
     * Don't use a vector here because a ViperPage uses arrays and the whole struct would be moved on a vector resize,
     * making all pointers invalid.
     */
    std::array<VPage, num_pages> v_pages;

    ViperPageBlock() {
        for (int i = 0; i < num_pages; ++i) {
            v_pages[i] = VPage();
        }
    }
};

class KeyValueOffset {
  public:
    KeyValueOffset() : offset{0xFFFFFFFFFFFFFFFF} {}

    KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const slot_size_t slot)
        : offset{shift_numbers(block_number, page_number, slot)} {}

    inline std::tuple<block_size_t, page_size_t, slot_size_t> get_offsets() const {
        return {get_block_number(), get_page_number(), get_slot_number()};
    }

    inline block_size_t get_block_number() const {
        return offset & 0xFFFFFFFFFFFF0000u; // Bit 1 to 48
    }

    inline page_size_t get_page_number() const {
        return (offset & 0xFF00u) >> 8u; // Bits 49 to 56
    }

    inline slot_size_t get_slot_number() const {
        return offset & 0xFFu; // Bits 57 to 64
    }

  protected:
    static offset_size_t shift_numbers(const block_size_t block_number, const page_size_t page_number, const slot_size_t slot) {
        offset_size_t kv_offset = block_number << 16u;
        kv_offset |= static_cast<offset_size_t>(page_number) << 8u;
        kv_offset |= static_cast<offset_size_t>(slot);
        return kv_offset;
    }

    offset_size_t offset;
};

} // namespace internal

template <typename K, typename V>
struct ViperRoot {
    using VPage = internal::ViperPage<K, V>;
    static constexpr uint64_t v_page_size = sizeof(VPage);
    static constexpr uint64_t num_pages_per_block = NUM_DIMMS * (PAGE_SIZE / v_page_size);
    using VPageBlock = internal::ViperPageBlock<VPage, num_pages_per_block>;
    using VPageBlocks = pobj::vector<pobj::persistent_ptr<VPageBlock>>;

    VPageBlocks v_page_blocks;

    void create_new_block() {
        pobj::persistent_ptr<VPageBlock> new_block = pobj::make_persistent<VPageBlock>();
        v_page_blocks.push_back(new_block);
    }
};

template <typename K, typename V>
class Viper {
    using KVOffset = internal::KeyValueOffset;
    using MapType = tbb::concurrent_hash_map<K, KVOffset>;
    using VPage = internal::ViperPage<K, V>;
    using VRoot = ViperRoot<K, V>;
    using VPageBlock = typename VRoot::VPageBlock;
    using VPageBlocks = typename VRoot::VPageBlocks;

  public:
    Viper(const std::string& pool_file, uint64_t pool_size);
    Viper(pobj::pool<ViperRoot<K, V>>&& v_pool);
    Viper(const pobj::pool<ViperRoot<K, V>>& v_pool);
    ~Viper();

    bool put(K key, V value);
    V get(K key);
    bool remove(K key);

  protected:
    Viper(const pobj::pool<ViperRoot<K, V>>& v_pool, bool own_pool);
    pobj::pool<VRoot> init_pool(const std::string& pool_file, uint64_t pool_size);

    inline block_size_t get_next_block();
    inline VPage* get_v_page(block_size_t block_number, page_size_t page_number);


    MapType map_;
    pobj::pool<VRoot> v_pool_;
    pobj::persistent_ptr<VRoot> v_root_;
    std::vector<VPageBlock*> blocks_;
    const bool owns_pool_ = false;

    page_size_t num_pages_per_block_;
    std::atomic<uint32_t> current_block_;
    uint64_t current_page_;

    std::atomic<uint64_t> current_size_;

};

template <typename K, typename V>
Viper<K, V>::Viper(const std::string& pool_file, const uint64_t pool_size) : Viper{init_pool(pool_file, pool_size)} {}

template <typename K, typename V>
Viper<K, V>::Viper(pobj::pool<ViperRoot<K, V>>&& v_pool) : Viper{v_pool, true} {}

template <typename K, typename V>
Viper<K, V>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool) : Viper{v_pool, false} {}

template <typename K, typename V>
Viper<K, V>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool, bool owns_pool) :
    v_pool_{v_pool}, v_root_{v_pool_.root()}, map_{VRoot::VPageBlock::num_slots_per_block}, current_block_{0},
    current_page_{0}, current_size_{0}, num_pages_per_block_{VRoot::num_pages_per_block}, owns_pool_{owns_pool} {
    // TODO: build map here and stuff
    blocks_.push_back(v_root_->v_page_blocks[0].get());
}

template <typename K, typename V>
Viper<K, V>::~Viper() {
    if (owns_pool_) {
        std::cout << "Closing pool file." << std::endl;
        v_pool_.close();
    }

}

template <typename K, typename V>
bool Viper<K, V>::put(K key, V value) {
    block_size_t block_number;
    page_size_t v_page_number;
    VPage* v_page;
    std::bitset<NUM_SLOTS_PER_PAGE> free_slots;
    slot_size_t free_slot_idx;

    // Find free slot
    do {
        block_number = get_next_block();
        v_page_number = static_cast<page_size_t>(++current_page_ % num_pages_per_block_);
        v_page = get_v_page(block_number, v_page_number);
        free_slots = v_page->free_slots;
        free_slot_idx = free_slots._Find_first();
    } while (free_slot_idx == free_slots.size());

    v_page->data[free_slot_idx] = {key, value};
    const typename VPage::VEntry* entry_ptr = v_page->data.data() + free_slot_idx;
    pmemobj_persist(v_pool_.handle(), entry_ptr, sizeof(typename VPage::VEntry));
    free_slots.flip(free_slot_idx);
    // TODO: maybe work directly on v_page->free_slots pointer
    // pmemobj_persist(v_pool_.handle(), &(v_page->free_slots), sizeof(free_slots));
    pmem_memcpy_persist(&(v_page->free_slots), &free_slots, sizeof(free_slots));

    const KVOffset kv_offset{block_number, v_page_number, free_slot_idx};
    typename MapType::accessor accessor;
    const bool is_new_item = map_.insert(accessor, {key, kv_offset});
    if (!is_new_item) {
        accessor->second = kv_offset;
    }

    return true;
}

template <typename K, typename V>
V Viper<K, V>::get(K key) {
    typename MapType::const_accessor result;
    const bool found = map_.find(result, key);
    if (!found) {
        throw std::runtime_error("Key '" + std::to_string(key) + "' not found.");
    }

    const KVOffset kv_offset = result->second;
    const auto [block_number, page_number, slot_number] = kv_offset.get_offsets();
    // TODO: check how this is optimized by compiler
    return blocks_[block_number]->v_pages[page_number].data[slot_number].second;
}

template <typename K, typename V>
block_size_t Viper<K, V>::get_next_block() {
    return current_block_;
}

template <typename K, typename V>
internal::ViperPage<K, V>* Viper<K, V>::get_v_page(const block_size_t block_number, const page_size_t page_number) {
    // TODO: check how this is optimized by compiler
    VPageBlock* next_v_block = blocks_[block_number];
    return &(next_v_block->v_pages[page_number]);
}

template <typename K, typename V>
pobj::pool<ViperRoot<K, V>> Viper<K, V>::init_pool(const std::string& pool_file, const uint64_t pool_size) {
    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    if (std::filesystem::exists(pool_file)) {
        std::cout << "Opening pool file " << pool_file << std::endl;
        return pmem::obj::pool<VRoot>::open(pool_file, "");
    } else {
        std::cout << "Creating pool file " << pool_file << std::endl;
        pobj::pool<VRoot> v_pool = pmem::obj::pool<VRoot>::create(pool_file, "", pool_size, S_IRWXU);
        pobj::transaction::run(v_pool, [&] {
            v_pool.root()->create_new_block();
        });
        return v_pool;
    }
}

}  // namespace viper

