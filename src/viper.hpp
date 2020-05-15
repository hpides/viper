#pragma once

#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <bitset>
#include <libpmem.h>
#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/container/vector.hpp>
#include <filesystem>
#include <thread>
#include <cmath>
#include <tbb/concurrent_queue.h>

namespace viper {

//#define RANDOM_PAGE
//#define RANDOM_BLOCK_PAGE
#define ROUND_ROBIN_PAGE

//#define PRINT_LOOP

namespace pobj = pmem::obj;

using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using slot_size_t = uint8_t;
using version_lock_size_t = uint64_t;

static constexpr uint16_t PAGE_SIZE = 4 * 1024; // 4kb
static constexpr uint16_t MIN_PAGE_SIZE = PAGE_SIZE / 4; // 1kb
static constexpr uint16_t MAX_PAGE_SIZE = PAGE_SIZE * 6; // 1kb
static constexpr uint8_t NUM_DIMMS = 6;
static constexpr uint64_t VBLOCK_SIZE = PAGE_SIZE * NUM_DIMMS;
static constexpr double RESIZE_THRESHOLD = 0.75;
static constexpr block_size_t NUM_BLOCKS_PER_CREATE = 10000;
static constexpr slot_size_t BASE_NUM_SLOTS_PER_PAGE = 64;

// Most significant bit of version_lock_size_t sized counter
static constexpr version_lock_size_t LOCK_BIT = 1ul << (sizeof(version_lock_size_t) * 8 - 1);
static constexpr version_lock_size_t COUNTER_MASK = ~LOCK_BIT;

static constexpr block_size_t NULL_BLOCK = std::numeric_limits<block_size_t>::max();

namespace internal {

static constexpr uint8_t VBLOCK_ALLOC_CLASS_ID = 200;
static constexpr char VBLOCK_ALLOC_SETTING[] = "heap.alloc_class.200.desc";
static constexpr pobj_alloc_class_desc VBLOCK_BASE_ALLOC_DESC {
    // TODO: experiment with units_per_block
    .unit_size = VBLOCK_SIZE, .alignment = VBLOCK_SIZE, .units_per_block = 1,
    .header_type = pobj_header_type::POBJ_HEADER_NONE, .class_id = VBLOCK_ALLOC_CLASS_ID
};


template <typename K, typename V>
constexpr slot_size_t get_num_slots_per_page() {
//    const uint32_t entry_size = sizeof(K) + sizeof(V);
//    uint16_t current_page_size = MIN_PAGE_SIZE;
//    slot_size_t num_slots_per_page = 64;
//    const uint16_t page_overhead = sizeof(version_lock_size_t) + sizeof(std::bitset<BASE_NUM_SLOTS_PER_PAGE>);
//
//    while ((entry_size * num_slots_per_page) - 16 > current_page_size) {
//        current_page_size *= 2;
//    }
//    assert(current_page_size <= MAX_PAGE_SIZE && "Cannot fit 64 KV pairs into single page!");
//
//    while ((num_slots_per_page * entry_size) + page_overhead + std::ceil(num_slots_per_page / 8) > PAGE_SIZE) {
//        num_slots_per_page--;
//    }
//    assert(num_slots_per_page > 0 && "Cannot fit KV pair into single page!");

    // Hard code for now based on 8 byte key + 8 byte value
//    return 31;
    return 63;
}

template <typename K, typename V>
struct alignas(MIN_PAGE_SIZE) ViperPage {
    using VEntry = std::pair<K, V>;
    static constexpr slot_size_t num_slots_per_page = get_num_slots_per_page<K, V>();


    std::atomic<version_lock_size_t> version_lock;
    std::bitset<num_slots_per_page> free_slots;
    std::array<VEntry, num_slots_per_page> data;

    ViperPage() {
//        std::cout << "PAGE ADDR: " << this << std::endl;
        const size_t page_size = sizeof(*this);
//        static_assert(sizeof(*this) >= 1024, "VPage needs to be at least 1024 byte!");
        static_assert(((page_size & (page_size - 1)) == 0), "VPage needs to be at least 1024 byte!");
        static_assert(PAGE_SIZE % sizeof(*this) == 0, "VPage not page size conform!");
        version_lock = 0;
        free_slots.flip();
        assert(free_slots.all());
    }
};

template <typename VPage, page_size_t num_pages>
struct ViperPageBlock {
    static constexpr uint64_t num_slots_per_block = VPage::num_slots_per_page * num_pages;
    /**
     * Array to store all persistent ViperPages.
     * Don't use a vector here because a ViperPage uses arrays and the whole struct would be moved on a vector resize,
     * making all pointers invalid.
     */
    std::array<VPage, num_pages> v_pages;
} __attribute__((aligned(PAGE_SIZE)));

class KeyValueOffset {
  public:
    KeyValueOffset() : offset{0xFFFFFFFFFFFFFFFF} {}

    KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const slot_size_t slot)
        : offset{shift_numbers(block_number, page_number, slot)} {}

    inline std::tuple<block_size_t, page_size_t, slot_size_t> get_offsets() const {
        return {get_block_number(), get_page_number(), get_slot_number()};
    }

    inline block_size_t get_block_number() const {
        return (offset & 0xFFFFFFFFFFFF0000u) >> 16u; // Bit 1 to 48
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
    static constexpr page_size_t num_pages_per_block = NUM_DIMMS * (PAGE_SIZE / v_page_size);
    using VPageBlock = internal::ViperPageBlock<VPage, num_pages_per_block>;
    using VPageBlocks = pobj::vector<pobj::persistent_ptr<VPageBlock>>;

    VPageBlocks v_page_blocks;

    VPageBlock* create_new_block() {
        static_assert(sizeof(VPageBlock) % PAGE_SIZE == 0, "VBlock must be a multiple of the page size");

        // Use the custom allocation descriptor to ensure page-aligned allocation.
        const pobj::allocation_flag vblock_alloc_flag =
            pobj::allocation_flag::class_id(internal::VBLOCK_ALLOC_CLASS_ID);
        pobj::persistent_ptr<VPageBlock> new_block = pobj::make_persistent<VPageBlock>(vblock_alloc_flag);
//        std::cout << "BLOCK ADDR: " << new_block << " - virtual: " << new_block.get() << std::endl;
        v_page_blocks.push_back(new_block);
        return new_block.get();
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
    explicit Viper(pobj::pool<ViperRoot<K, V>>&& v_pool);
    explicit Viper(const pobj::pool<ViperRoot<K, V>>& v_pool);
    ~Viper();

    class ConstAccessor {
        friend class Viper<K, V>;
        using MapType = tbb::concurrent_hash_map<K, internal::KeyValueOffset>;
        using MapConstAccessor = typename MapType::const_accessor;

      public:
        ConstAccessor() = default;

        const V& operator*() const { return *value_; }
        const V* operator->() const { return value_; }

        ConstAccessor(const ConstAccessor& other) = delete;
        ConstAccessor& operator=(const ConstAccessor& other) = delete;
        ConstAccessor(ConstAccessor&& other) noexcept = default;
        ConstAccessor& operator=(ConstAccessor&& other) noexcept = default;
        ~ConstAccessor() = default;

      protected:
        MapConstAccessor map_accessor_;
        V* value_;
    };

    class Accessor : public ConstAccessor {
        using MapAccessor = typename ConstAccessor::MapType::accessor;
      public:
        V& operator*() { return *value_; }
        V* operator->() { return value_; }

      protected:
        MapAccessor map_accessor_;
        V* value_;
    };

    bool put(const K& key, const V& value);
    bool get(const K& key, Accessor& accessor);
    bool get(const K& key, ConstAccessor& accessor) const;

    template <typename UpdateFn>
    bool update(const K& key, UpdateFn update_fn);

    bool remove(const K& key);

    size_t count() const;

  protected:
    Viper(const pobj::pool<ViperRoot<K, V>>& v_pool, bool owns_pool);
    pobj::pool<VRoot> init_pool(const std::string& pool_file, uint64_t pool_size);

    inline block_size_t get_next_block();
    inline VPage* get_v_page(block_size_t block_number, page_size_t page_number);
    void add_v_page_blocks(block_size_t num_blocks = 1);

    pobj::pool<VRoot> v_pool_;
    pobj::persistent_ptr<VRoot> v_root_;
    MapType map_;

    const bool owns_pool_;
    std::vector<VPageBlock*> v_blocks_;
    // Store counter for each page. This does not need to be atomic because only one thread can access a page at a time.
    std::vector<std::array<slot_size_t, VRoot::num_pages_per_block>> block_remaining_slots_;

    const uint16_t num_slots_per_block_;
    const page_size_t num_pages_per_block_;
    const uint16_t capacity_per_block_;
    std::atomic<uint64_t> current_block_;
    std::atomic<uint64_t> current_page_;

    size_t current_size_;
    size_t current_capacity_;
    std::atomic<uint16_t> current_block_capacity_;
    size_t resize_at_;
    const double resize_threshold_;
    std::atomic<bool> is_resizing_;
    std::unique_ptr<std::thread> resize_thread_;

    pobj_alloc_class_desc vblock_alloc_description_;

    std::atomic<uint64_t> loop_sum_ = 0;
    std::atomic<uint64_t> loop_counter_ = 0;
    std::atomic<uint64_t> loop5_counter_ = 0;
    std::atomic<uint64_t> loop10_counter_ = 0;
    std::atomic<uint64_t> loop100_counter_ = 0;
    std::atomic<uint64_t> loop100_sum_ = 0;
    uint64_t loop_max_ = 0;
#if defined(PAGE_QUEUE)
    tbb::concurrent_bounded_queue<std::tuple<block_size_t, page_size_t, VPage*>> page_queue_;
#elif defined(RANDOM_BLOCK_PAGE) || defined(RANDOM_PAGE)
    std::random_device rnd_dev_;
    std::default_random_engine rnd_engine_{rnd_dev_()};
#endif
};

template <typename K, typename V>
Viper<K, V>::Viper(const std::string& pool_file, const uint64_t pool_size) : Viper{init_pool(pool_file, pool_size)} {}

template <typename K, typename V>
Viper<K, V>::Viper(pobj::pool<ViperRoot<K, V>>&& v_pool) : Viper{v_pool, true} {}

template <typename K, typename V>
Viper<K, V>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool) : Viper{v_pool, false} {}

template <typename K, typename V>
Viper<K, V>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool, bool owns_pool) :
    v_pool_{v_pool}, v_root_{v_pool_.root()}, map_{VRoot::VPageBlock::num_slots_per_block}, owns_pool_{owns_pool},
    num_slots_per_block_{VRoot::VPageBlock::num_slots_per_block}, num_pages_per_block_{VRoot::num_pages_per_block},
    capacity_per_block_{static_cast<uint16_t>(num_slots_per_block_ - num_pages_per_block_)}, current_block_{0},
    current_page_{0}, current_size_{0}, current_capacity_{0}, current_block_capacity_{capacity_per_block_},
    resize_threshold_{RESIZE_THRESHOLD}, is_resizing_{false},
    vblock_alloc_description_{internal::VBLOCK_BASE_ALLOC_DESC} {
    // TODO: build map here and stuff

    loop_counter_ = 0;

    pmemobj_ctl_set(v_pool_.handle(), internal::VBLOCK_ALLOC_SETTING, &vblock_alloc_description_);

    add_v_page_blocks(NUM_BLOCKS_PER_CREATE);

#ifdef PAGE_QUEUE
    page_queue_.set_capacity(num_pages_per_block_);
    for (int page_num = 0; page_num < v_blocks_[0]->v_pages.size(); ++page_num) {
        page_queue_.push(std::make_tuple(0, page_num, &(v_blocks_[0]->v_pages[page_num])));
    }
#endif
}

template <typename K, typename V>
Viper<K, V>::~Viper() {
    std::cout << "LOOP TIMES: " << loop_sum_.load() << std::endl;
    std::cout << "LOOP COUNTER: " << loop_counter_.load() << std::endl;
    std::cout << "LOOP COUNTER 5: " << loop5_counter_.load() << std::endl;
    std::cout << "LOOP COUNTER 10: " << loop10_counter_.load() << std::endl;
    std::cout << "LOOP COUNTER 100: " << loop100_counter_.load() << std::endl;
    std::cout << "LOOP COUNTER 100 SUM: " << loop100_sum_.load() << std::endl;
    std::cout << "LOOP COUNTER MAX: " << loop_max_ << std::endl;
    if (owns_pool_) {
        std::cout << "Closing pool file." << std::endl;
        v_pool_.close();
    }
}

template <typename K, typename V>
bool Viper<K, V>::put(const K& key, const V& value) {
    block_size_t block_number;
    page_size_t v_page_number;
    VPage* v_page;
    std::tuple<block_size_t, page_size_t, VPage*> page_info;
    version_lock_size_t lock_value;
    std::bitset<VPage::num_slots_per_page>* free_slots;
    std::bitset<VPage::num_slots_per_page> free_slot_checker;
    slot_size_t free_slot_idx;

    // Find free slot
    do {

#if defined(ROUND_ROBIN_PAGE)
        block_number = get_next_block();
        v_page_number = current_page_++ % num_pages_per_block_;
        v_page = get_v_page(block_number, v_page_number);
#elif defined(PAGE_QUEUE)
        page_queue_.pop(page_info);
        block_number = std::get<0>(page_info);
        v_page_number = std::get<1>(page_info);
        v_page = std::get<2>(page_info);
#elif defined(RANDOM_BLOCK_PAGE)
        std::uniform_int_distribution<block_size_t> rnd_block(0, v_blocks_.size() - 1);
        std::uniform_int_distribution<block_size_t> rnd_page(0, num_pages_per_block_ - 1);
        block_number = rnd_block(rnd_engine_);
        v_page_number = rnd_page(rnd_engine_);
        v_page = &(v_blocks_[block_number]->v_pages[v_page_number]);
#elif defined(RANDOM_PAGE)
        block_number = get_next_block();
        std::uniform_int_distribution<block_size_t> rnd_page(0, num_pages_per_block_ - 1);
        v_page_number = rnd_page(rnd_engine_);
        v_page = &(v_blocks_[block_number]->v_pages[v_page_number]);
#else
        static_assert(false, "No page selection mode selected!");
#endif

// TODO
        uint64_t one_counter = 0;
        bool in_loop = false;

        // Lock v_page
        std::atomic<version_lock_size_t>& v_lock = v_page->version_lock;

        // We expect the lock bit to be unset
        lock_value = v_lock.load() & ~LOCK_BIT;

#if defined(PRINT_LOOP)
        std::stringbuf msg_buf;
        std::ostream msg{&msg_buf};
        msg << "THREAD: " << std::this_thread::get_id();
        msg << " | block id: " << block_number;
        msg << " | page id: " << static_cast<uint64_t>(v_page_number);
        msg << " | lock value: " << lock_value;
        msg << std::endl;
        std::cout << msg_buf.str();
#endif
        // Compare and swap until we are the thread to set the lock bit
        bool skip_page = false;
        while (!v_lock.compare_exchange_weak(lock_value, lock_value | LOCK_BIT)) {
            lock_value &= ~LOCK_BIT;
//            ++loop_counter_;
//            ++one_counter;

            if (++one_counter == 3) {
                skip_page = true;
                break;
            }

#if defined(PRINT_LOOP)
            std::stringbuf msg_buf1;
            std::ostream msg1{&msg_buf1};
            msg1 << "LOOP COUNTER: " << one_counter;
            msg1 << " | THREAD: " << std::this_thread::get_id();
            msg1 << " | block id: " << block_number;
            msg1 << " | page id: " << static_cast<uint64_t>(v_page_number);
            msg1 << " | lock value: " << lock_value;
            msg1 << std::endl;
            std::cout << msg_buf1.str();

            in_loop = true;
            if (one_counter > 95) {
                std::stringbuf msg_buf2;
                std::ostream msg2{&msg_buf2};
                msg2 << ">95 COUNTER: " << one_counter;
                msg2 << " | THREAD: " << std::this_thread::get_id();
                msg2 << " | block id: " << block_number;
                msg2 << " | page id: " << static_cast<uint64_t>(v_page_number);
                msg2 << " | lock value: " << lock_value;
                msg2 << std::endl;
                std::cout << msg_buf2.str();
            }
            if (one_counter == 99) {
                in_loop = true;
            }
//            std::cout << thread_msg.str() + " blocked\n";
#endif
        }

        if (skip_page) {
            continue;
        }

#if defined(PRINT_LOOP)
        if (in_loop) {
            ++loop_sum_;
        }

        if (one_counter > 5) {
            ++loop5_counter_;
        }
        if (one_counter > 10) {
            ++loop10_counter_;
        }
        if (one_counter > 100) {
            ++loop100_counter_;
            loop100_sum_.fetch_add(one_counter);
            loop_max_ = std::max(loop_max_, one_counter);
        }
#endif

        // We now have the lock on this page
        free_slots = &(v_page->free_slots);
        free_slot_idx = free_slots->_Find_first();
        free_slot_checker = *free_slots;

        // Always keep one slot free for updates
        if (free_slot_checker.reset(free_slot_idx).none()) {
            // Free lock on page and restart
// TODO
            v_lock.store(lock_value & ~LOCK_BIT, std::memory_order_release);

#if defined(PAGE_QUEUE)
            page_queue_.push(std::make_tuple(block_number + 1, v_page_number, &v_blocks_[block_number + 1]->v_pages[v_page_number]));
#endif
            continue;
        }

        // We have found a free slot on this page
        break;
    } while (true);

//    --(block_remaining_slots_[block_number][v_page_number]);
#if defined(RANDOM_PAGE) || defined(ROUND_ROBIN_PAGE)
    --current_block_capacity_;
#endif

//    bool is_new_item = true;
    v_page->data[free_slot_idx] = {key, value};
    typename VPage::VEntry* entry_ptr = v_page->data.data() + free_slot_idx;
    pmemobj_persist(v_pool_.handle(), entry_ptr, sizeof(typename VPage::VEntry));

    free_slots->flip(free_slot_idx);
    pmemobj_persist(v_pool_.handle(), free_slots, sizeof(free_slots));

    const KVOffset kv_offset{block_number, v_page_number, free_slot_idx};
    bool is_new_item;
    {
        // Scope this so the accessor is free'd as soon as possible.
        typename MapType::accessor accessor;
        is_new_item = map_.insert(accessor, {key, kv_offset});
        if (!is_new_item) {
            accessor->second = kv_offset;
        }
    }

    // Unlock the v_page and increment the version counter
    // Bump version number and unset lock bit
// TODO
    std::atomic<version_lock_size_t>& v_lock = v_page->version_lock;
    version_lock_size_t old_version_number = lock_value & COUNTER_MASK;
    version_lock_size_t new_version_lock = (old_version_number + 1) & ~LOCK_BIT;
    v_lock.store(new_version_lock, std::memory_order_release);

#ifdef PAGE_QUEUE
    page_queue_.push(page_info);
#endif

//    this is apparently really expensive
    if (++current_size_ < resize_at_) {
        // Enough capacity, no need to resize.
        return is_new_item;
    }

    bool expected_resizing = false;
    const bool should_resize = is_resizing_.compare_exchange_strong(expected_resizing, true);
    if (should_resize) {
        // Only one thread can ever get here because for all others the atomic exchange above fails.
        resize_thread_ = std::make_unique<std::thread>([this] {
            this->add_v_page_blocks(NUM_BLOCKS_PER_CREATE);
        });
        resize_thread_->detach();
    }

    return is_new_item;
}

template <typename K, typename V>
bool Viper<K, V>::get(const K& key, Accessor& accessor) {
    auto& result = accessor.map_accessor_;
    const bool found = map_.find(result, key);
    if (!found) {
        return false;
    }

    const KVOffset kv_offset = result->second;
    const auto [block_number, page_number, slot_number] = kv_offset.get_offsets();
    // TODO: check how this is optimized by compiler
    accessor.value_ = &(v_blocks_[block_number]->v_pages[page_number].data[slot_number].second);
    return true;
}

template <typename K, typename V>
bool Viper<K, V>::get(const K& key, ConstAccessor& accessor) const {
    auto& result = accessor.map_accessor_;
    const bool found = map_.find(result, key);
    if (!found) {
        return false;
    }

    const KVOffset kv_offset = result->second;
    const auto [block_number, page_number, slot_number] = kv_offset.get_offsets();
    // TODO: check how this is optimized by compiler
    accessor.value_ = &(v_blocks_[block_number]->v_pages[page_number].data[slot_number].second);
    return true;
}

template <typename K, typename V>
template <typename UpdateFn>
bool Viper<K, V>::update(const K& key, UpdateFn update_fn) {
    typename MapType::accessor result;
    const bool found = map_.find(result, key);
    if (!found) {
        return false;
    }

    update_fn(result->second);
    return true;
}

template <typename K, typename V>
size_t Viper<K, V>::count() const {
    return current_size_; //.load();
}

template <typename K, typename V>
block_size_t Viper<K, V>::get_next_block() {
    if (current_block_capacity_.load(std::memory_order_acquire) == 0) {
        // No more capacity in current block
        uint16_t expected_capacity = 0;
        const bool swap_successful = current_block_capacity_.compare_exchange_strong(expected_capacity, capacity_per_block_);
        if (swap_successful) {
            return ++current_block_;
        }
    }

    return current_block_.load(std::memory_order_acquire);
}

template <typename K, typename V>
internal::ViperPage<K, V>* Viper<K, V>::get_v_page(const block_size_t block_number, const page_size_t page_number) {
    // TODO: check how this is optimized by compiler
    VPageBlock* next_v_block = v_blocks_[block_number];
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
template <typename K, typename V>
void Viper<K, V>::add_v_page_blocks(const block_size_t num_blocks) {
    const block_size_t num_blocks_before = v_blocks_.size();
    current_capacity_ += capacity_per_block_ * num_blocks;
    resize_at_ = current_capacity_ * resize_threshold_;

//    std::cout << "Adding block (size: " << v_blocks_.size() << ")" << std::endl;
    // Keep 1 slot per page free to allow for updates in same page;
    const slot_size_t page_capacity = VPage::num_slots_per_page - 1;

    const block_size_t num_total_blocks = num_blocks_before + num_blocks;
    for (block_size_t block_id = num_blocks_before; block_id < num_total_blocks; ++block_id) {
        pobj::transaction::run(v_pool_, [&] {
            VPageBlock* new_block = v_pool_.root()->create_new_block();
            v_blocks_.push_back(new_block);
        });
        block_remaining_slots_.emplace_back();
        block_remaining_slots_.back().fill(page_capacity);
    }

    is_resizing_ = false;
//    std::cout << "Block added (size: " << v_blocks_.size() << ")" << std::endl;
}

}  // namespace viper
