#pragma once

#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <libpmemobj++/mutex.hpp>
#include <bitset>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <filesystem>

namespace viper {

namespace pobj = pmem::obj;

namespace internal {

static constexpr uint16_t NUM_SLOTS_PER_PAGE = 256;
static constexpr uint16_t NUM_SLOT_BITSETS_PER_PAGE = NUM_SLOTS_PER_PAGE / 8;
static constexpr uint32_t PAGE_SIZE = 4 * 1024; // 4kb
static constexpr uint32_t NUM_DIMMS = 6;

using SlotBitsetType = uint8_t;

template <typename K, typename V>
struct ViperPage {
//    std::array<SlotBitsetType, NUM_SLOT_BITSETS_PER_PAGE> free_slots;
    std::bitset<NUM_SLOTS_PER_PAGE> free_slots;
    std::array<std::pair<K, V>, NUM_SLOTS_PER_PAGE> data;
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
        // TODO: persist v_pages here
    }
};

template <typename K, typename V>
struct ViperRoot {
    using VPage = ViperPage<K, V>;
    static constexpr uint64_t v_page_size = sizeof(VPage);
    static constexpr uint64_t num_pages_per_block = NUM_DIMMS * (PAGE_SIZE / v_page_size);
    using VPageBlock = ViperPageBlock<VPage, num_pages_per_block>;
    using VPageBlocks = pobj::vector<pobj::persistent_ptr<VPageBlock>>;

    VPageBlocks v_page_blocks;

    void create_new_block() {
        v_page_blocks.emplace_back();
    }
};

} // namespace internal

template <typename K, typename V>
class Viper {
    using DataOffsetType = uint64_t;
    using MapType = tbb::concurrent_hash_map<K, DataOffsetType>;
    using VPage = internal::ViperPage<K, V>;
    using VRoot = internal::ViperRoot<K, V>;

  public:
    Viper(std::string_view pool_file, uint64_t pool_size);
    ~Viper();

    bool put(K key, V value);
    V get(K key);
    bool remove(K key);

  protected:
    MapType map_;
    pobj::pool<VRoot> v_pool_;


};

template <typename K, typename V>
Viper<K, V>::Viper(const std::string_view pool_file, const uint64_t pool_size)
    : map_(VRoot::VPageBlock::num_slots_per_block) {

    if (std::filesystem::exists(pool_file)) {
        std::cout << "Opening pool file " << pool_file << std::endl;
        v_pool_ = pobj::pool<VRoot>::open({pool_file}, "");
        // TODO: build map here and stuff
    } else {
        std::cout << "Creating pool file " << pool_file << std::endl;
        v_pool_ = pobj::pool<VRoot>::create({pool_file}, "", pool_size, S_IRWXU);
    }
}

template <typename K, typename V>
Viper<K, V>::~Viper() {
    std::cout << "Closing pool file." << std::endl;
    v_pool_.close();
}

template <typename K, typename V>
bool Viper<K, V>::put(K key, V value) {

}

}  // namespace viper

