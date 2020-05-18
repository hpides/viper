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
using version_lock_t = uint64_t;

static constexpr uint16_t PAGE_SIZE = 4 * 1024; // 4kb
static constexpr uint16_t MIN_PAGE_SIZE = PAGE_SIZE / 4; // 1kb
static constexpr uint8_t NUM_DIMMS = 6;
static constexpr uint64_t VBLOCK_SIZE = PAGE_SIZE * NUM_DIMMS;
static constexpr double RESIZE_THRESHOLD = 0.75;
static constexpr block_size_t NUM_BLOCKS_PER_CREATE = 140000; // 6 dimms
//static constexpr block_size_t NUM_BLOCKS_PER_CREATE = 840000; // 1 dimm
//static constexpr block_size_t NUM_BLOCKS_PER_CREATE = 35000; // 24 dimms

// Most significant bit of version_lock_t sized counter
static constexpr version_lock_t LOCK_BIT = 1ul << (sizeof(version_lock_t) * 8 - 1);
static constexpr version_lock_t COUNTER_MASK = ~LOCK_BIT;

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
//    const uint16_t page_overhead = sizeof(version_lock_t) + sizeof(std::bitset<BASE_NUM_SLOTS_PER_PAGE>);
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
    //    return 63;

    // Hard code for now based on 16 byte key + 200 byte value
    return 18;
}

template <typename K, typename V>
struct alignas(MIN_PAGE_SIZE) ViperPage {
    using VEntry = std::pair<K, V>;
    static constexpr slot_size_t num_slots_per_page = get_num_slots_per_page<K, V>();

    std::atomic<version_lock_t> version_lock;
    std::bitset<num_slots_per_page> free_slots;
    std::array<VEntry, num_slots_per_page> data;

    ViperPage() {
        static constexpr size_t v_page_size = sizeof(*this);
        static_assert(((v_page_size & (v_page_size - 1)) == 0), "VPage needs to be a power of 2!");
        static_assert(PAGE_SIZE % v_page_size == 0, "VPage not page size conform!");
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

    explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

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

    inline offset_size_t get_raw_offset() const {
        return offset;
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

template <typename K, typename V, typename HashCompare>
class Viper {
    using ViperT = Viper<K, V, HashCompare>;
    using KVOffset = internal::KeyValueOffset;
    using MapType = tbb::concurrent_hash_map<K, KVOffset, HashCompare>;
    using VPage = internal::ViperPage<K, V>;
    using VRoot = ViperRoot<K, V>;
    using VPageBlock = typename VRoot::VPageBlock;
    using VPageBlocks = typename VRoot::VPageBlocks;

  public:
    Viper(const std::string& pool_file, uint64_t pool_size);
    explicit Viper(pobj::pool<VRoot>&& v_pool);
    explicit Viper(const pobj::pool<VRoot>& v_pool);
    ~Viper();

    class ConstAccessor {
        friend class Viper<K, V, HashCompare>;
        using MapType = tbb::concurrent_hash_map<K, internal::KeyValueOffset, HashCompare>;
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

    class ConstClient {
        friend class Viper<K, V, HashCompare>;
      public:
        bool get(const K& key, ConstAccessor& accessor) const;
      protected:
        explicit ConstClient(const ViperT& viper);
        const ViperT& const_viper_;
    };

    class Client : public ConstClient {
        friend class Viper<K, V, HashCompare>;
      public:
        bool put(const K& key, const V& value);

        bool get(const K& key, Accessor& accessor);
        bool get(const K& key, ConstAccessor& accessor) const;

        template <typename UpdateFn>
        bool update(const K& key, UpdateFn update_fn);

        bool remove(const K& key);

        ~Client();

      protected:
        Client(uint64_t id, ViperT& viper);
        inline void update_access_information();
        inline void info_sync(bool force = false);

        enum PageStrategy : uint8_t { BlockBased, DimmBased };

        const uint64_t id_;
        ViperT& viper_;

        PageStrategy strategy_;
        block_size_t v_block_number_;
        page_size_t v_page_number_;
        VPageBlock* v_block_;
        VPage* v_page_;

        // Dimm-based
        block_size_t end_v_block_number_;

        // Block-based
        page_size_t num_v_pages_processed_;

        uint64_t op_count_;
        int size_delta_;
    };

    Client get_client();
    ConstClient get_const_client() const;
    size_t get_size_estimate() const;

  protected:
    bool get(const K& key, Accessor& accessor);
    bool get(const K& key, ConstAccessor& accessor) const;

    template <typename UpdateFn>
    bool update(const K& key, UpdateFn update_fn);
    bool remove(const K& key);

    // Actual private methods
    Viper(const pobj::pool<VRoot>& v_pool, bool owns_pool);
    pobj::pool<VRoot> init_pool(const std::string& pool_file, uint64_t pool_size);

    void get_new_access_information(Client* client);
    void get_dimm_based_access(Client* client);
    void get_block_based_access(Client* client);
    void remove_client(Client* client);

    inline block_size_t get_next_block();
    inline VPage* get_v_page(block_size_t block_number, page_size_t page_number);
    void add_v_page_blocks(block_size_t num_blocks = 1);

    pobj::pool<VRoot> v_pool_;
    VRoot* v_root_;
    MapType map_;

    const bool owns_pool_;
    std::vector<VPageBlock*> v_blocks_;

    const uint16_t num_slots_per_block_;
    const page_size_t num_pages_per_block_;
    const uint16_t capacity_per_block_;
    std::atomic<uint64_t> current_block_;
    std::atomic<size_t> current_size_;
    std::atomic<uint64_t> current_page_;
    std::atomic<uint64_t> current_block_page_;

    std::atomic<uint64_t> current_client_id_;
    std::atomic<uint64_t> num_active_clients_;

    size_t current_capacity_;
    std::atomic<uint16_t> current_block_capacity_;
    size_t resize_at_;
    const double resize_threshold_;
    std::atomic<bool> is_resizing_;
    std::unique_ptr<std::thread> resize_thread_;

    pobj_alloc_class_desc vblock_alloc_description_;

#if defined(PRINT_LOOP)
    std::atomic<size_t> page_find_counter_ = 0;
    std::atomic<uint64_t> loop_sum_ = 0;
    std::atomic<uint64_t> loop_counter_ = 0;
#endif

#if defined(PAGE_QUEUE)
    tbb::concurrent_bounded_queue<std::tuple<block_size_t, page_size_t, VPage*>> page_queue_;
#elif defined(RANDOM_BLOCK_PAGE) || defined(RANDOM_PAGE)
    std::random_device rnd_dev_;
    std::default_random_engine rnd_engine_{rnd_dev_()};
#endif
    void trigger_resize();
};

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Viper(const std::string& pool_file, const uint64_t pool_size) : Viper{init_pool(pool_file, pool_size)} {}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Viper(pobj::pool<ViperRoot<K, V>>&& v_pool) : Viper{v_pool, true} {}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool) : Viper{v_pool, false} {}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Viper(const pobj::pool<ViperRoot<K, V>>& v_pool, bool owns_pool) :
    v_pool_{v_pool}, v_root_{v_pool_.root().get()}, map_{VRoot::VPageBlock::num_slots_per_block}, owns_pool_{owns_pool},
    num_slots_per_block_{VRoot::VPageBlock::num_slots_per_block}, num_pages_per_block_{VRoot::num_pages_per_block},
    capacity_per_block_{static_cast<uint16_t>(num_slots_per_block_ - num_pages_per_block_)},
    resize_threshold_{RESIZE_THRESHOLD}, vblock_alloc_description_{internal::VBLOCK_BASE_ALLOC_DESC} {

    // TODO: build map here and stuff
    current_block_ = 0;
    current_page_ = 0;
    current_block_page_ = 0;
    current_size_ = 0;
    current_capacity_ = 0;
    current_block_capacity_ = capacity_per_block_;
    is_resizing_ = false;
    current_client_id_ = 0;
    num_active_clients_ = 0;

    srand(time(NULL));

    pmemobj_ctl_set(v_pool_.handle(), internal::VBLOCK_ALLOC_SETTING, &vblock_alloc_description_);

    add_v_page_blocks(NUM_BLOCKS_PER_CREATE);
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::~Viper() {
#if defined(PRINT_LOOP)
    std::cout << "LOOP TIMES: " << loop_sum_.load() << std::endl;
    std::cout << "LOOP COUNTER: " << loop_counter_.load() << std::endl;
    std::cout << "PAGE FIND COUNTER: " << page_find_counter_.load() << std::endl;
#endif
    if (owns_pool_) {
        std::cout << "Closing pool file." << std::endl;
        v_pool_.close();
    }
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::get(const K& key, Accessor& accessor) {
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

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::get(const K& key, ConstAccessor& accessor) const {
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

template <typename K, typename V, typename HC>
template <typename UpdateFn>
bool Viper<K, V, HC>::update(const K& key, UpdateFn update_fn) {
    typename MapType::accessor result;
    const bool found = map_.find(result, key);
    if (!found) {
        return false;
    }

    update_fn(result->second);
    return true;
}

template <typename K, typename V, typename HC>
size_t Viper<K, V, HC>::get_size_estimate() const {
    return current_size_.load(std::memory_order_acquire);
}

template <typename K, typename V, typename HC>
pobj::pool<ViperRoot<K, V>> Viper<K, V, HC>::init_pool(const std::string& pool_file, const uint64_t pool_size) {
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
template <typename K, typename V, typename HC>
void Viper<K, V, HC>::add_v_page_blocks(const block_size_t num_blocks) {
    const block_size_t num_blocks_before = v_blocks_.size();
    current_capacity_ += capacity_per_block_ * num_blocks;
    resize_at_ = current_capacity_ * resize_threshold_;

//    std::cout << "Adding block (size: " << v_blocks_.size() << ")" << std::endl;

    const block_size_t num_total_blocks = num_blocks_before + num_blocks;
    for (block_size_t block_id = num_blocks_before; block_id < num_total_blocks; ++block_id) {
        pobj::transaction::run(v_pool_, [&] {
            VPageBlock* new_block = v_root_->create_new_block();
            v_blocks_.push_back(new_block);
        });
    }

    is_resizing_.store(false, std::memory_order_release);
//    std::cout << "Block added (size: " << v_blocks_.size() << ")" << std::endl;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_new_access_information(Client* client) {
    // Get insert/delete count info
    client->info_sync(true);

    // Check if resize necessary
    if (current_size_ >= resize_at_) {
        trigger_resize();
    }

// TODO
//    if (num_active_clients_.load(std::memory_order_acquire) <= NUM_DIMMS) {
//        return get_block_based_access(client);
//    } else {
//        return get_dimm_based_access(client);
//    }

    return get_block_based_access(client);
//    return get_dimm_based_access(client);
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_block_based_access(Client* client) {
    offset_size_t raw_block_page = current_block_page_.load(std::memory_order_acquire);
    KVOffset new_offset{};
    block_size_t client_block;
    page_size_t client_page;
    do {
        const KVOffset v_block_page = KVOffset{raw_block_page};
        client_block = v_block_page.get_block_number();
        client_page = v_block_page.get_page_number();

        const block_size_t new_block = client_block + 1;
        // Chose random offset to evenly distribute load on all DIMMs
        const page_size_t new_page = rand() % num_pages_per_block_;
        new_offset = KVOffset{new_block, new_page, 0};
    } while (!current_block_page_.compare_exchange_weak(raw_block_page, new_offset.get_raw_offset()));

    client->strategy_ = Client::PageStrategy::BlockBased;
    client->v_block_number_ = client_block;
    client->v_page_number_ = client_page;
    client->num_v_pages_processed_ = 0;
    client->v_block_ = v_blocks_[client_block];
    client->v_page_ = &(client->v_block_->v_pages[client_page]);
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_dimm_based_access(Client* client) {
    const block_size_t block_stride = 600;

    offset_size_t raw_block_page = current_block_page_.load(std::memory_order_acquire);
    KVOffset new_offset{};
    block_size_t client_block;
    page_size_t client_page;
    do {
        const KVOffset v_block_page = KVOffset{raw_block_page};
        client_block = v_block_page.get_block_number();
        client_page = v_block_page.get_page_number();

        block_size_t new_block = client_block;
        page_size_t new_page = client_page + 1;
        if (new_page == num_pages_per_block_) {
            new_block++;
            new_page = 0;
        }
        new_offset = KVOffset{new_block, new_page, 0};
    } while (!current_block_page_.compare_exchange_weak(raw_block_page, new_offset.get_raw_offset()));

    client->strategy_ = Client::PageStrategy::DimmBased;
    client->v_block_number_ = client_block;
    client->end_v_block_number_ = client_block + block_stride - 1;
    client->v_page_number_ = client_page;
    client->v_block_ = v_blocks_[client_block];
    client->v_page_ = &(client->v_block_->v_pages[client_page]);
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::trigger_resize() {
    bool expected_resizing = false;
    const bool should_resize = is_resizing_.compare_exchange_strong(expected_resizing, true);
    if (should_resize) {
        // Only one thread can ever get here because for all others the atomic exchange above fails.
        resize_thread_ = std::make_unique<std::thread>([this] {
            add_v_page_blocks(NUM_BLOCKS_PER_CREATE);
        });
        resize_thread_->detach();
    }
}

template <typename K, typename V, typename HC>
typename Viper<K, V, HC>::Client Viper<K, V, HC>::get_client() {
    const uint64_t client_id = current_client_id_++;
    ++num_active_clients_;

    Client client{client_id, *this};
    get_new_access_information(&client);
    return client;
}

template <typename K, typename V, typename HC>
typename Viper<K, V, HC>::ConstClient Viper<K, V, HC>::get_const_client() const {
    return ConstClient{*this};
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::remove_client(Viper::Client* client) {
    client->info_sync(true);
    --num_active_clients_;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::put(const K& key, const V& value) {
    // Lock v_page. We expect the lock bit to be unset.
// TODO
    std::atomic<version_lock_t>& v_lock = v_page_->version_lock;
    version_lock_t lock_value = v_lock.load() & ~LOCK_BIT;
    // Compare and swap until we are the thread to set the lock bit
    while (!v_lock.compare_exchange_weak(lock_value, lock_value | LOCK_BIT)) {
        lock_value &= ~LOCK_BIT;
    }

    // We now have the lock on this page
    std::bitset<VPage::num_slots_per_page>* free_slots = &v_page_->free_slots;
    const slot_size_t free_slot_idx = free_slots->_Find_first();
    std::bitset<VPage::num_slots_per_page> free_slot_checker = *free_slots;

    // Always keep one slot free for updates
    if (free_slot_checker.reset(free_slot_idx).none()) {
        // Free lock on page and restart
//        v_lock.store(lock_value & ~LOCK_BIT, std::memory_order_release);
        update_access_information();
        return put(key, value);
    }

    // We have found a free slot on this page
    v_page_->data[free_slot_idx] = {key, value};
    typename VPage::VEntry* entry_ptr = v_page_->data.data() + free_slot_idx;
    pmemobj_persist(viper_.v_pool_.handle(), entry_ptr, sizeof(typename VPage::VEntry));

    free_slots->flip(free_slot_idx);
    pmemobj_persist(viper_.v_pool_.handle(), free_slots, sizeof(free_slots));

    const KVOffset kv_offset{v_block_number_, v_page_number_, free_slot_idx};
    bool is_new_item;
    {
        // Scope this so the accessor is free'd as soon as possible.
        typename MapType::accessor accessor;
        is_new_item = viper_.map_.insert(accessor, {key, kv_offset});
        if (!is_new_item) {
            accessor->second = kv_offset;
        }
    }

    // Unlock the v_page and increment the version counter
    // Bump version number and unset lock bit
    const version_lock_t old_version_number = lock_value & COUNTER_MASK;
    v_lock.store(old_version_number + 1, std::memory_order_release);

    // We have added one value, so +1
    ++size_delta_;
    info_sync();

    return is_new_item;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::get(const K& key, Viper::Accessor& accessor) {
    return false;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::get(const K& key, Viper::ConstAccessor& accessor) const {
    return static_cast<const Viper<K, V, HC>::ConstClient*>(this)->get(key, accessor);
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::ConstClient::get(const K& key, Viper::ConstAccessor& accessor) const {
    return false;
}

template <typename K, typename V, typename HC>
template <typename UpdateFn>
bool Viper<K, V, HC>::Client::update(const K& key, UpdateFn update_fn) {
    return false;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::remove(const K& key) {
    return false;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::Client::update_access_information() {
    if (strategy_ == PageStrategy::DimmBased) {
        if (v_block_number_ == end_v_block_number_) {
            // No more allocated pages, need new range
            return viper_.get_new_access_information(this);
        }
        ++v_block_number_;
        v_block_ = viper_.v_blocks_[v_block_number_];
        v_page_ = &(v_block_->v_pages[v_page_number_]);
    } else if (strategy_ == PageStrategy::BlockBased) {
        if (++num_v_pages_processed_ == viper_.num_pages_per_block_) {
            // No more pages, need new block
            return viper_.get_new_access_information(this);
        }
        v_page_number_ = (v_page_number_ + 1) % viper_.num_pages_per_block_;
        v_page_ = &(v_block_->v_pages[v_page_number_]);
    } else {
        throw std::runtime_error("Unknown page strategy.");
    }
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::Client::info_sync(const bool force) {
    if (force || ++op_count_ == 1000) {
        viper_.current_size_.fetch_add(size_delta_);
        op_count_ = 0;
        size_delta_ = 0;
    }
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::ConstClient::ConstClient(const ViperT& viper) : const_viper_{viper} {}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Client::Client(const uint64_t id, ViperT& viper) : ConstClient{viper}, id_{id}, viper_{viper} {
    op_count_ = 0;
    size_delta_ = 0;
    num_v_pages_processed_ = 0;
    v_page_number_ = 0;
    v_block_number_ = 0;
    end_v_block_number_ = 0;
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Client::~Client() {
    viper_.remove_client(this);
}

}  // namespace viper
