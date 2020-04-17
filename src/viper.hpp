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

using SlotBitsetType = uint8_t;

namespace internal {

template <typename K, typename V>
struct ViperPage {
    std::bitset<NUM_SLOTS_PER_PAGE> free_slots;
    std::array<std::pair<K, V>, NUM_SLOTS_PER_PAGE> data;

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

template <typename K, typename V>
struct ViperRoot {
    using VPage = ViperPage<K, V>;
    static constexpr uint64_t v_page_size = sizeof(VPage);
    static constexpr uint64_t num_pages_per_block = NUM_DIMMS * (PAGE_SIZE / v_page_size);
    using VPageBlock = ViperPageBlock<VPage, num_pages_per_block>;
    using VPageBlocks = pobj::vector<pobj::persistent_ptr<VPageBlock>>;

    VPageBlocks v_page_blocks;

    void create_new_block() {
        pobj::persistent_ptr<VPageBlock> new_block = pobj::make_persistent<VPageBlock>();
        v_page_blocks.push_back(new_block);
    }
};

} // namespace internal

template <typename K, typename V>
class Viper {
    using DataOffsetType = const std::pair<K, V>*;
    using MapType = tbb::concurrent_hash_map<K, DataOffsetType>;
    using VPage = internal::ViperPage<K, V>;
    using VRoot = internal::ViperRoot<K, V>;
    using VPageBlock = typename VRoot::VPageBlock;
    using VPageBlocks = typename VRoot::VPageBlocks;

  public:
    Viper(const std::string& pool_file, uint64_t pool_size);
    ~Viper();

    bool put(K key, V value);
    V get(K key);
    bool remove(K key);

  protected:
    VPage* get_free_v_page();


    MapType map_;
    pobj::pool<VRoot> v_pool_;
    pobj::persistent_ptr<VRoot> v_root_;
};

template <typename K, typename V>
Viper<K, V>::Viper(const std::string& pool_file, const uint64_t pool_size)
    : map_(VRoot::VPageBlock::num_slots_per_block) {

    int sds_write_value = 0;
    pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);

    if (std::filesystem::exists(pool_file)) {
        std::cout << "Opening pool file " << pool_file << std::endl;
        v_pool_ = pobj::pool<VRoot>::open(pool_file, "");
        // TODO: build map here and stuff
    } else {
        std::cout << "Creating pool file " << pool_file << std::endl;
        v_pool_ = pobj::pool<VRoot>::create(pool_file, "", pool_size, S_IRWXU);
        pobj::transaction::run(v_pool_, [&] {
           v_pool_.root()->create_new_block();
        });
    }

    v_root_ = v_pool_.root();
}

template <typename K, typename V>
Viper<K, V>::~Viper() {
    std::cout << "Closing pool file." << std::endl;
    v_pool_.close();
}

template <typename K, typename V>
bool Viper<K, V>::put(K key, V value) {
    VPage* v_page = get_free_v_page();
    std::bitset<NUM_SLOTS_PER_PAGE>& free_slots = v_page->free_slots;
    const size_t free_slot_idx = free_slots._Find_first();

    if (free_slot_idx < free_slots.size()) {
        v_page->data[free_slot_idx] = {key, value};
        const std::pair<K, V>* entry_ptr = v_page->data.data() + free_slot_idx;
        pmemobj_persist(v_pool_.handle(), entry_ptr, sizeof(K) + sizeof(V));
        free_slots.flip(free_slot_idx);
        pmemobj_persist(v_pool_.handle(), &(v_page->free_slots), sizeof(free_slots));

        typename MapType::accessor accessor;
        map_.insert(accessor, key);
        accessor->second = entry_ptr;
        return true;
    } else {
        // TODO: No free slot;
    }

    return false;
}

template <typename K, typename V>
internal::ViperPage<K, V>* Viper<K, V>::get_free_v_page() {
    VPageBlocks& v_blocks = v_root_->v_page_blocks;
    VPageBlock& next_v_block = *(v_blocks[0]);
    VPage* v_page = &(next_v_block.v_pages[0]);
    return v_page;
}

}  // namespace viper

