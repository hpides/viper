#pragma once

#include <iostream>
#include <tbb/concurrent_hash_map.h>
#include <bitset>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <libpmem.h>
#include <thread>
#include <cmath>
#include <linux/mman.h>
#include <sys/mman.h>
#include <atomic>
#include <assert.h>

#ifndef NDEBUG
#define DEBUG_LOG(msg) (std::cout << msg << std::endl)
#else
#define DEBUG_LOG(msg) do {} while(0)
#endif

namespace viper {

using offset_size_t = uint64_t;
using block_size_t = uint64_t;
using page_size_t = uint8_t;
using slot_size_t = uint16_t;
using version_lock_t = uint8_t;

static constexpr uint16_t PAGE_SIZE = 4 * 1024; // 4kb
static constexpr uint8_t NUM_DIMMS = 6;
static constexpr size_t BLOCK_SIZE = NUM_DIMMS * PAGE_SIZE;
static constexpr size_t ONE_GB = 1024l * 1024 * 1024;

// Most significant bit of version_lock_t sized counter
static constexpr version_lock_t LOCK_BYTE = 255;
static constexpr version_lock_t FREE_BYTE = 0;

static constexpr auto VIPER_MAP_PROT = PROT_WRITE | PROT_READ | PROT_EXEC;
static constexpr auto VIPER_MAP_FLAGS = MAP_SHARED | MAP_SYNC;

struct ViperConfig {
    double resize_threshold = 0.85;
    uint8_t num_recovery_threads = 64;
    size_t dax_alignment = ONE_GB;
    bool force_dimm_based = false;
    bool force_block_based = false;
};

namespace internal {

template <typename K, typename V>
constexpr slot_size_t get_num_slots_per_page() {
    if constexpr (std::is_same_v<K, std::string>) {
        return 502;
    }

    const uint32_t entry_size = sizeof(K) + sizeof(V);
    uint16_t current_page_size = PAGE_SIZE;

    const uint16_t page_overhead = sizeof(version_lock_t) + 1;
    while (entry_size + page_overhead > current_page_size && current_page_size <= BLOCK_SIZE) {
        current_page_size += PAGE_SIZE;
    }

    page_size_t num_pages_per_block = current_page_size / PAGE_SIZE;
    if (num_pages_per_block == 4 || num_pages_per_block == 5) {
        num_pages_per_block = 6;
        current_page_size = BLOCK_SIZE;
    }

    uint16_t num_slots_per_page_large = current_page_size / entry_size;
    if (num_slots_per_page_large > 255) {
        num_slots_per_page_large--;
    }
    slot_size_t num_slots_per_page = num_slots_per_page_large;
    while ((num_slots_per_page * entry_size) + page_overhead +
                std::ceil((double) num_slots_per_page / 8) > current_page_size) {
        num_slots_per_page--;
    }
    assert(num_slots_per_page > 0 && "Cannot fit KV pair into single page!");
    return num_slots_per_page;
}

struct KeyValueOffset {
    static constexpr offset_size_t TOMBSTONE = 0xFFFFFFFFFFFFFFFF;

    union {
        offset_size_t offset;
        struct {
            block_size_t block_number : 52;
            page_size_t page_number : 3;
            slot_size_t slot_number : 9;
        };
    };

    KeyValueOffset() : offset{TOMBSTONE} {}

    explicit KeyValueOffset(const offset_size_t offset) : offset(offset) {}

    KeyValueOffset(const block_size_t block_number, const page_size_t page_number, const slot_size_t slot)
        : block_number{block_number}, page_number{page_number}, slot_number{slot} {}

    static KeyValueOffset Tombstone() {
        return KeyValueOffset{};
    }

    inline std::tuple<block_size_t, page_size_t, slot_size_t> get_offsets() const {
        return {block_number, page_number, slot_number};
    }

    inline bool is_tombstone() const {
        return offset == TOMBSTONE;
    }
};

struct VarSizeOffset {
    union {
        uint64_t offset_info;
        struct {
            uint64_t block_number : 48;
            uint8_t page_number : 4;
            uint16_t data_offset : 12;
        };
    };

    VarSizeOffset(uint64_t block_number, uint8_t page_number, uint16_t data_offset)
        : block_number(block_number), page_number(page_number), data_offset(data_offset) {}
};

struct VarSizeEntry {
    union {
        uint64_t size_info;
        struct {
            uint32_t key_size;
            uint32_t value_size;
        };
    };
    char* data;

    VarSizeEntry(const uint64_t keySize, const uint64_t valueSize)
        : key_size{static_cast<uint32_t>(keySize)}, value_size{static_cast<uint32_t>(valueSize)} {}
};

struct VarEntryAccessor {
    uint32_t key_size;
    uint32_t value_size;
    char* key_data = nullptr;
    char* value_data = nullptr;

    VarEntryAccessor(const char* raw_entry) {
        const VarSizeEntry* entry = reinterpret_cast<const VarSizeEntry*>(raw_entry);
        key_size = entry->key_size;
        value_size = entry->value_size;
        key_data = const_cast<char*>(raw_entry) + sizeof(entry->size_info);
        value_data = key_data + key_size;
    }

    VarEntryAccessor(const char* raw_key_entry, const char* raw_value_entry) : VarEntryAccessor{raw_key_entry} {
        assert(value_size == 0);
        assert(key_data != nullptr);
        const VarSizeEntry* value_entry = reinterpret_cast<const VarSizeEntry*>(raw_value_entry);
        value_size = value_entry->value_size;
        value_data = const_cast<char*>(raw_value_entry) + sizeof(value_entry->size_info);
    }

    const std::string_view key() const {
        return std::string_view{key_data, key_size};
    }

    const std::string_view value() const {
        assert(value_data != nullptr);
        return std::string_view{value_data, value_size};
    }
};

template <typename K, typename V>
struct alignas(PAGE_SIZE) ViperPage {
    using VEntry = std::pair<K, V>;
    static constexpr slot_size_t num_slots_per_page = get_num_slots_per_page<K, V>();

    std::atomic<version_lock_t> version_lock;
    std::bitset<num_slots_per_page> free_slots;

    std::array<VEntry, num_slots_per_page> data;

    void init() {
        static constexpr size_t v_page_size = sizeof(*this);
        static_assert(((v_page_size & (v_page_size - 1)) == 0), "VPage needs to be a power of 2!");
        static_assert(PAGE_SIZE % alignof(*this) == 0, "VPage not page size conform!");
        version_lock = 0;
        free_slots.set();
    }
};

struct alignas(PAGE_SIZE) ViperDataPage {
    static constexpr size_t METADATA_SIZE = sizeof(owner) + sizeof(next);
    static constexpr uint16_t DATA_SIZE = PAGE_SIZE - METADATA_SIZE;

    ViperPage<std::string, std::string>* owner;
    ViperDataPage* next;

    std::array<char, DATA_SIZE> data;

    ViperDataPage() {
        static_assert(sizeof(*this) == PAGE_SIZE, "String VPage too big...");
    }
};

template <>
struct alignas(PAGE_SIZE) ViperPage<std::string, std::string> {
    using VarEntryOffset = internal::VarSizeOffset;
    static constexpr slot_size_t num_slots_per_page = 501;

    std::atomic<version_lock_t> version_lock;
    std::bitset<num_slots_per_page> free_slots;
    ViperDataPage* current_data_page;
    char* next_insert_pos;
    std::array<VarEntryOffset, num_slots_per_page> data;

    void init() {
        static constexpr size_t v_page_size = sizeof(*this);
        static_assert(((v_page_size & (v_page_size - 1)) == 0), "VPage needs to be a power of 2!");
        static_assert(PAGE_SIZE % alignof(*this) == 0, "VPage not page size conform!");
        static_assert(PAGE_SIZE == v_page_size, "VPage not 4096 Byte!");
        version_lock = 0;
        free_slots.set();
        current_data_page = nullptr;
        next_insert_pos = nullptr;
    }
};

template <typename VPage, page_size_t num_pages>
struct alignas(PAGE_SIZE) ViperPageBlock {
    static constexpr uint64_t num_slots_per_block = VPage::num_slots_per_page * num_pages;
    /**
     * Array to store all persistent ViperPages.
     * Don't use a vector here because a ViperPage uses arrays and the whole struct would be moved on a vector resize,
     * making all pointers invalid.
     */
    std::array<VPage, num_pages> v_pages;
};

template <typename KeyT>
struct KeyCompare {
    static size_t hash(const KeyT& a) {
        return std::hash<KeyT>{}(a);
    }
    static bool equal(const KeyT& a, const KeyT& b) { return a == b; }
};

} // namespace internal

struct ViperFileMetadata {
    const size_t block_offset;
    const size_t block_size;
    const size_t alloc_size;
    block_size_t num_used_blocks;
    block_size_t num_allocated_blocks;
    size_t total_mapped_size;
};

struct ViperFileMapping {
    const size_t mapped_size;
    void* const start_addr;
};

struct ViperBase {
    const int file_descriptor;
    const bool is_new_db;
    ViperFileMetadata* const v_metadata;
    std::vector<ViperFileMapping> v_mappings;
};

template <typename ValueT>
struct ValueAccessor {
    typedef ValueT* type;
    typedef ValueT* ptr_type;
    typedef const ValueT* const_ptr_type;
    typedef const ValueT& ref_type;

    static ptr_type to_ptr_type(type& x) { return x; }
    static const_ptr_type to_ptr_type(const type& x) { return x; }
};

template <>
struct ValueAccessor<std::string> {
    typedef std::string_view type;
    typedef std::string_view* ptr_type;
    typedef const std::string_view* const_ptr_type;
    typedef const std::string_view& ref_type;

    static ptr_type to_ptr_type(type& x) { return &x; }
    static const_ptr_type to_ptr_type(const type& x) { return &x; }
};

template <typename K, typename V, typename HashCompare = internal::KeyCompare<K>>
class Viper {
    using ViperT = Viper<K, V, HashCompare>;
    using VPage = internal::ViperPage<K, V>;
    using VDataPage = internal::ViperDataPage;
    using KVOffset = internal::KeyValueOffset;
    using VarOffset = internal::VarSizeOffset;
    using MapType = tbb::concurrent_hash_map<K, KVOffset, HashCompare>;
    static constexpr uint64_t v_page_size = sizeof(VPage);
    static_assert(BLOCK_SIZE % v_page_size == 0, "Page needs to fit into block.");
    static constexpr page_size_t num_pages_per_block = BLOCK_SIZE / v_page_size;
    using VPageBlock = internal::ViperPageBlock<VPage, num_pages_per_block>;
    using VDataPageBlock = internal::ViperPageBlock<internal::ViperDataPage, num_pages_per_block>;

  public:
    static std::unique_ptr<Viper<K, V, HashCompare>> create(const std::string& pool_file, uint64_t initial_pool_size,
                                                            ViperConfig v_config = ViperConfig{});
    static std::unique_ptr<Viper<K, V, HashCompare>> open(const std::string& pool_file, ViperConfig v_config = ViperConfig{});
    static std::unique_ptr<Viper<K, V, HashCompare>> open(ViperBase v_base, ViperConfig v_config = ViperConfig{});
    Viper(ViperBase v_base, bool owns_pool, ViperConfig v_config);
    ~Viper();

    class ConstAccessor {
        friend class Viper<K, V, HashCompare>;
        using MapConstAccessor = typename MapType::const_accessor;

      public:
        ConstAccessor() = default;

        const typename ValueAccessor<V>::ref_type operator*() const { return *operator->(); }
        const typename ValueAccessor<V>::const_ptr_type operator->() const { return ValueAccessor<V>::to_ptr_type(value_); }

        ConstAccessor(const ConstAccessor& other) = delete;
        ConstAccessor& operator=(const ConstAccessor& other) = delete;
        ConstAccessor(ConstAccessor&& other) noexcept = default;
        ConstAccessor& operator=(ConstAccessor&& other) noexcept = default;
        ~ConstAccessor() = default;

      protected:
        MapConstAccessor map_accessor_;
        typename ValueAccessor<V>::type value_{};
    };

    class Accessor : public ConstAccessor {
        using MapAccessor = typename ConstAccessor::MapType::accessor;
      public:
        typename ValueAccessor<V>::ref_type operator*() { return *value_; }
        typename ValueAccessor<V>::ptr_type operator->() { return value_; }

      protected:
        MapAccessor map_accessor_;
        typename ValueAccessor<V>::type value_{};
    };

    class ConstClient {
        friend class Viper<K, V, HashCompare>;
      public:
        bool get(const K& key, ConstAccessor& accessor) const;
      protected:
        explicit ConstClient(const ViperT& viper);
        inline const typename ValueAccessor<V>::type get_const_value_from_offset(KVOffset offset) const;
        const ViperT& const_viper_;
    };

    class Client : public ConstClient {
        friend class Viper<K, V, HashCompare>;
      public:
        bool put(const K& key, const V& value);

        bool get(const K& key, Accessor& accessor);
        inline bool get(const K& key, ConstAccessor& accessor) const;

        template <typename UpdateFn>
        bool update(const K& key, UpdateFn update_fn);

        bool remove(const K& key);

        ~Client();

      protected:
        Client(ViperT& viper);
        inline void update_access_information();
        inline void update_data_page_information(internal::ViperPage<K, V>* v_page);
        inline void info_sync(bool force = false);
        inline typename ValueAccessor<V>::type get_value_from_offset(KVOffset offset);
        void free_occupied_slot(block_size_t block_number, page_size_t page_number, slot_size_t slot_number);

        enum PageStrategy : uint8_t { BlockBased, DimmBased };

        ViperT& viper_;
        PageStrategy strategy_;

        // Fixed size entries
        block_size_t v_block_number_;
        page_size_t v_page_number_;
        VPageBlock* v_block_;
        VPage* v_page_;

        // Variable size entries
        block_size_t v_data_block_number_;
        page_size_t v_data_page_number_;
        VDataPageBlock* v_data_block_;
        VDataPage* v_data_page_;

        // Dimm-based
        block_size_t end_v_block_number_;

        // Block-based
        page_size_t num_v_pages_processed_;
        page_size_t num_v_data_pages_processed_;

        uint16_t op_count_;
        int size_delta_;
    };

    Client get_client();
    ConstClient get_const_client() const;
    size_t get_size_estimate() const;

  protected:
    static ViperBase init_pool(const std::string& pool_file, uint64_t pool_size,
                               bool is_new_pool, ViperConfig v_config);

    void get_new_access_information(Client* client);
    void get_dimm_based_access(Client* client);
    void get_block_based_access(Client* client);
    void get_new_data_access_information(Client* client);
    KVOffset get_new_block();
    void remove_client(Client* client);

    ViperFileMapping allocate_v_page_blocks();
    void add_v_page_blocks(ViperFileMapping mapping);
    void recover_database();
    void trigger_resize();

    ViperBase v_base_;
    const bool owns_pool_;
    ViperConfig v_config_;

    MapType map_;

    std::vector<VPageBlock*> v_blocks_;
    const uint16_t num_slots_per_block_;
    std::atomic<size_t> current_size_;
    std::atomic<offset_size_t> current_block_page_;

    size_t current_capacity_;
    size_t resize_at_;
    const double resize_threshold_;
    std::atomic<bool> is_resizing_;
    std::atomic<bool> is_v_blocks_resizing_;
    std::unique_ptr<std::thread> resize_thread_;
    std::atomic<uint8_t> num_active_clients_;
    const uint8_t num_recovery_threads_;
};

template <typename K, typename V, typename HC>
std::unique_ptr<Viper<K, V, HC>> Viper<K, V, HC>::create(const std::string& pool_file, uint64_t initial_pool_size,
                                                         ViperConfig v_config) {
    return std::make_unique<Viper<K, V, HC>>(init_pool(pool_file, initial_pool_size, true, v_config), true, v_config);
}

template <typename K, typename V, typename HC>
std::unique_ptr<Viper<K, V, HC>> Viper<K, V, HC>::open(const std::string& pool_file, ViperConfig v_config) {
    return std::make_unique<Viper<K, V, HC>>(init_pool(pool_file, 0, false, v_config), true, v_config);
}

template <typename K, typename V, typename HC>
std::unique_ptr<Viper<K, V, HC>> Viper<K, V, HC>::open(ViperBase v_base, ViperConfig v_config) {
    return std::make_unique<Viper<K, V, HC>>(v_base, false, v_config);
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Viper(ViperBase v_base, const bool owns_pool, const ViperConfig v_config) :
    v_base_{v_base}, map_{}, owns_pool_{owns_pool}, v_config_{v_config},
    num_slots_per_block_{VPageBlock::num_slots_per_block}, resize_threshold_{v_config.resize_threshold},
    num_recovery_threads_{v_config.num_recovery_threads} {

    current_block_page_ = 0;
    current_size_ = 0;
    current_capacity_ = 0;
    is_resizing_ = false;
    num_active_clients_ = 0;

    if (v_config_.force_block_based && v_config_.force_dimm_based) {
        throw std::runtime_error("Cannot force both block- and dimm-based.");
    }

    std::srand(std::time(nullptr));

    if (v_base_.v_mappings.empty()) {
        throw new std::runtime_error("Need to have at least one memory section mapped.");
    }

    for (ViperFileMapping mapping : v_base_.v_mappings) {
        add_v_page_blocks(mapping);
    }

    if (!v_base_.is_new_db) {
        DEBUG_LOG("Recovering existing database.");
        recover_database();
    }

    current_block_page_ = KVOffset{v_base.v_metadata->num_used_blocks, 0, 0}.offset;
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::~Viper() {
    if (owns_pool_) {
        DEBUG_LOG("Closing pool file.");
        munmap(v_base_.v_metadata, v_base_.v_metadata->block_offset);
        for (const ViperFileMapping mapping : v_base_.v_mappings) {
            munmap(mapping.start_addr, mapping.mapped_size);
        }
        close(v_base_.file_descriptor);
    }
}

template <typename K, typename V, typename HC>
size_t Viper<K, V, HC>::get_size_estimate() const {
    return current_size_.load(std::memory_order_acquire);
}

template <typename K, typename V, typename HC>
ViperBase Viper<K, V, HC>::init_pool(const std::string& pool_file, uint64_t pool_size,
                                     bool is_new_pool, ViperConfig v_config) {
    DEBUG_LOG((is_new_pool ? "Creating" : "Opening") << " pool file " << pool_file);

    const int fd = ::open(pool_file.c_str(), O_RDWR);
    if (fd < 0) {
        throw std::runtime_error("Cannot open dax device: " + pool_file + " | " + std::strerror(errno));
    }

    size_t alloc_size = v_config.dax_alignment;
    if (!is_new_pool) {
        void* metadata_addr = mmap(nullptr, alloc_size, PROT_READ, MAP_SHARED, fd, 0);
        if (metadata_addr == nullptr) {
            throw std::runtime_error("Cannot mmap pool file: " + pool_file + " | " + std::strerror(errno));
        }
        const ViperFileMetadata* metadata = static_cast<ViperFileMetadata*>(metadata_addr);
        if (metadata->num_allocated_blocks > 0 && metadata->block_size > 0) {
            pool_size = metadata->total_mapped_size;
            alloc_size = metadata->alloc_size;
        } else {
            DEBUG_LOG("Opening empty database. Creating new one instead.");
            is_new_pool = true;
        }
        munmap(metadata_addr, PAGE_SIZE);
    }

    if (pool_size % alloc_size != 0) {
        throw std::runtime_error("Pool needs to be allocated in 1 GB chunks.");
    }

    const size_t num_alloc_chunks = pool_size / alloc_size;
    if (num_alloc_chunks == 0) {
        throw std::runtime_error("Pool too small: " + std::to_string(pool_size));
    }

    void* pmem_addr = mmap(nullptr, pool_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, fd, 0);
    if (pmem_addr == nullptr || pmem_addr == reinterpret_cast<void*>(0xffffffffffffffff)) {
        throw std::runtime_error("Cannot mmap pool file: " + pool_file + " | " + std::strerror(errno));
    }

    if (is_new_pool) {
        ViperFileMetadata v_metadata{ .block_offset = PAGE_SIZE, .block_size = sizeof(VPageBlock),
                                      .alloc_size = alloc_size, .num_used_blocks = 0,
                                      .num_allocated_blocks = 0, .total_mapped_size = pool_size};
        pmem_memcpy_persist(pmem_addr, &v_metadata, sizeof(v_metadata));
    }

    ViperFileMetadata* metadata = static_cast<ViperFileMetadata*>(pmem_addr);
    char* map_start_addr = static_cast<char*>(pmem_addr);

    // Each alloc chunk gets its own mapping.
    std::vector<ViperFileMapping> mappings;
    mappings.reserve(num_alloc_chunks);

    block_size_t num_allocated_blocks = 0;

    // First chunk is special because of the initial metadata chunk
    ViperFileMapping initial_mapping{ .mapped_size = alloc_size - metadata->block_offset,
                                      .start_addr = map_start_addr + metadata->block_offset };
    mappings.push_back(initial_mapping);
    num_allocated_blocks += initial_mapping.mapped_size / sizeof(VPageBlock);

    // Remaining chunks are all the same
    for (size_t alloc_chunk = 1; alloc_chunk < num_alloc_chunks; ++alloc_chunk) {
        char* alloc_chunk_start = map_start_addr + (alloc_chunk * alloc_size);
        ViperFileMapping mapping{ .mapped_size = alloc_size, .start_addr = alloc_chunk_start};
        mappings.push_back(mapping);
        num_allocated_blocks += alloc_size / sizeof(VPageBlock);
    }

    // Update num allocated blocks with correct counting of allocation chunks.
    metadata->num_allocated_blocks = num_allocated_blocks;
    DEBUG_LOG((is_new_pool ? "Allocated " : "Recovered ") << num_allocated_blocks << " blocks in "
                << num_alloc_chunks << " GiB.");

    return ViperBase{ .file_descriptor = fd, .is_new_db = is_new_pool,
                      .v_metadata = metadata, .v_mappings = std::move(mappings) };
}

template <typename K, typename V, typename HC>
ViperFileMapping Viper<K, V, HC>::allocate_v_page_blocks() {
    const size_t offset = v_base_.v_metadata->total_mapped_size;
    const int fd = v_base_.file_descriptor;
    const size_t alloc_size = v_base_.v_metadata->alloc_size;
    void* pmem_addr = mmap(nullptr, alloc_size, VIPER_MAP_PROT, VIPER_MAP_FLAGS, fd, offset);
    if (pmem_addr == nullptr) {
        throw std::runtime_error(std::string("Cannot mmap extra memory: ") + std::strerror(errno));
    }
    const block_size_t num_blocks_to_map = alloc_size / sizeof(VPageBlock);
    v_base_.v_metadata->num_allocated_blocks += num_blocks_to_map;
    v_base_.v_metadata->total_mapped_size += alloc_size;
    pmem_persist(v_base_.v_metadata, sizeof(ViperFileMetadata));
    DEBUG_LOG("Allocated " << num_blocks_to_map << " blocks in " << (alloc_size / ONE_GB) << " GiB.");

    ViperFileMapping mapping{alloc_size, pmem_addr};
    v_base_.v_mappings.push_back(mapping);
    return mapping;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::add_v_page_blocks(ViperFileMapping mapping) {
    VPageBlock* start_block = reinterpret_cast<VPageBlock*>(mapping.start_addr);
    const block_size_t num_blocks_to_map = mapping.mapped_size / sizeof(VPageBlock);

    is_v_blocks_resizing_.store(true, std::memory_order_release);
    v_blocks_.reserve(v_blocks_.size() + num_blocks_to_map);
    is_v_blocks_resizing_.store(false, std::memory_order_release);
    for (block_size_t block_offset = 0; block_offset < num_blocks_to_map; ++block_offset) {
        v_blocks_.push_back(start_block + block_offset);
    }

    current_capacity_ += num_slots_per_block_ * num_blocks_to_map;
    resize_at_ = current_capacity_ * resize_threshold_;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::recover_database() {
    auto start = std::chrono::high_resolution_clock::now();

    // Pre-size map to avoid resizing during recovery.
    const block_size_t num_used_blocks = v_base_.v_metadata->num_used_blocks;
    map_.rehash(num_used_blocks * num_slots_per_block_);

    DEBUG_LOG("Re-inserting values from " << num_used_blocks << " blocks.");

    std::vector<std::thread> recovery_threads;
    recovery_threads.reserve(num_recovery_threads_);

    std::vector<std::vector<KVOffset>> duplicate_entries;
    duplicate_entries.resize(num_recovery_threads_);

    auto recover = [&](const size_t thread_num, const block_size_t start_block, const block_size_t end_block) {
        std::vector<KVOffset>& duplicates = duplicate_entries[thread_num];

        size_t num_entries = 0;
        for (block_size_t block_num = start_block; block_num < end_block; ++block_num) {
            VPageBlock* block = v_blocks_[block_num];
            for (page_size_t page_num = 0; page_num < num_pages_per_block; ++page_num) {
                const VPage& page = block->v_pages[page_num];
                if (page.version_lock == 0) {
                    // Page is empty
                    continue;
                }
                for (slot_size_t slot_num = 0; slot_num < VPage::num_slots_per_page; ++slot_num) {
                    if (page.free_slots[slot_num] != 0) {
                        // No data, continue
                        continue;
                    }
                    // Data is present

                    if constexpr (std::is_same_v<K, std::string>) {
                        throw std::runtime_error{"not implemented yet"};
                    } else {
                        ++num_entries;
                        typename MapType::accessor accessor;
                        const K& key = page.data[slot_num].first;
                        const KVOffset offset{block_num, page_num, slot_num};
                        const bool is_new = map_.insert(accessor, {key, offset});
                        if (!is_new) {
                            duplicates.push_back(accessor->second);
                            accessor->second = offset;
                        }
                    }
                }
            }
        }
        current_size_.fetch_add(num_entries);

        if (duplicates.size() > 0) {
            for (const KVOffset offset : duplicates) {
                const auto[block, page, slot] = offset.get_offsets();
                v_blocks_[block]->v_pages[page].free_slots.set(slot);
            }
        }
    };

    // We give each thread + 1 blocks to avoid leaving out blocks at the end.
    const block_size_t num_blocks_per_thread = (num_used_blocks / num_recovery_threads_) + 1;

    for (size_t thread_num = 0; thread_num < num_recovery_threads_; ++thread_num) {
        const block_size_t start_block = thread_num * num_blocks_per_thread;
        const block_size_t end_block = std::min(start_block + num_blocks_per_thread, num_used_blocks);
        recovery_threads.emplace_back(recover, thread_num, start_block, end_block);
    }

    for (std::thread& thread : recovery_threads) {
        thread.join();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    DEBUG_LOG("RECOVERY DURATION: " << duration << " ms.");
    DEBUG_LOG("Re-inserted " << current_size_ << " keys.");
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_new_access_information(Client* client) {
    // Get insert/delete count info
    client->info_sync(true);

    // Check if resize necessary
    // TODO: change to number of free blocks for resizing
//    if (current_size_ >= resize_at_) {
//        trigger_resize();
//    }

    const KVOffset block_page{current_block_page_.load()};
    if (block_page.block_number > resize_threshold_ * v_blocks_.size()) {
        trigger_resize();
    }

    if (v_config_.force_dimm_based) {
        get_dimm_based_access(client);
    } else {
        get_block_based_access(client);
    }

    if constexpr (std::is_same_v<K, std::string>) {
        client->update_data_page_information(client->v_page_);
    }
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_new_data_access_information(Client* client) {
    // Get insert/delete count info
    client->info_sync(true);

    // Check if resize necessary
    // TODO: change to number of free blocks for resizing
//    if (current_size_ >= resize_at_) {
//        trigger_resize();
//    }

    const KVOffset block_page{current_block_page_.load()};
    if (block_page.block_number > resize_threshold_ * v_blocks_.size()) {
        trigger_resize();
    }

    const auto [client_block, client_page, _] = get_new_block().get_offsets();

    client->v_data_block_number_ = client_block;
    client->v_data_page_number_ = client_page;
    client->num_v_data_pages_processed_ = 0;

    while (is_v_blocks_resizing_.load(std::memory_order_acquire)) {
        // Wait for vector's memmove to complete. Otherwise we might encounter a segfault.
    }

    client->v_data_block_ = reinterpret_cast<VDataPageBlock*>(v_blocks_[client_block]);
    client->v_data_page_ = &(client->v_data_block_->v_pages[client_page]);
    client->v_data_page_->next = nullptr;
    client->v_page_->next_insert_pos = client->v_data_page_->data.data();

    v_base_.v_metadata->num_used_blocks++;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_block_based_access(Client* client) {
    const auto [client_block, client_page, _] = get_new_block().get_offsets();

    client->strategy_ = Client::PageStrategy::BlockBased;
    client->v_block_number_ = client_block;
    client->v_page_number_ = client_page;
    client->num_v_pages_processed_ = 0;

    while (is_v_blocks_resizing_.load(std::memory_order_acquire)) {
        // Wait for vector's memmove to complete. Otherwise we might encounter a segfault.
    }

    client->v_block_ = v_blocks_[client_block];
    client->v_page_ = &(client->v_block_->v_pages[client_page]);
    client->v_page_->init();

    v_base_.v_metadata->num_used_blocks++;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::get_dimm_based_access(Client* client) {
    const block_size_t block_stride = 6;

    offset_size_t raw_block_page = current_block_page_.load(std::memory_order_acquire);
    KVOffset new_offset{};
    block_size_t client_block;
    page_size_t client_page;
    do {
        const KVOffset v_block_page = KVOffset{raw_block_page};
        client_block = v_block_page.block_number;
        client_page = v_block_page.page_number;

        block_size_t new_block = client_block;
        page_size_t new_page = client_page + 1;
        if (new_page == num_pages_per_block) {
            new_block++;
            new_page = 0;
        }
        new_offset = KVOffset{new_block, new_page, 0};
    } while (!current_block_page_.compare_exchange_weak(raw_block_page, new_offset.offset));

    client->strategy_ = Client::PageStrategy::DimmBased;
    client->v_block_number_ = client_block;
    client->end_v_block_number_ = client_block + block_stride - 1;
    client->v_page_number_ = client_page;

    while (is_v_blocks_resizing_.load(std::memory_order_acquire)) {
        // Wait for vector's memmove to complete. Otherwise we might encounter a segfault.
    }
    client->v_block_ = v_blocks_[client_block];
    client->v_page_ = &(client->v_block_->v_pages[client_page]);
}

template <typename K, typename V, typename HC>
internal::KeyValueOffset Viper<K, V, HC>::get_new_block() {
    offset_size_t raw_block_page = current_block_page_.load(std::memory_order_acquire);
    KVOffset new_offset{};
    block_size_t client_block;
    page_size_t client_page;
    do {
        const KVOffset v_block_page{raw_block_page};
        client_block = v_block_page.block_number;
        client_page = v_block_page.page_number;

        const block_size_t new_block = client_block + 1;
        assert(new_block < v_blocks_.size());

        // Choose random offset to evenly distribute load on all DIMMs
        const page_size_t new_page = rand() % num_pages_per_block;
        new_offset = KVOffset{new_block, new_page, 0};
    } while (!current_block_page_.compare_exchange_weak(raw_block_page, new_offset.offset));

    return KVOffset{raw_block_page};
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::trigger_resize() {
    bool expected_resizing = false;
    const bool should_resize = is_resizing_.compare_exchange_strong(expected_resizing, true);
    if (!should_resize) {
        return;
    }

    // Only one thread can ever get here because for all others the atomic exchange above fails.
    resize_thread_ = std::make_unique<std::thread>([this] {
        DEBUG_LOG("Start resizing.");
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(71, &cpuset);
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        ViperFileMapping mapping = allocate_v_page_blocks();
        add_v_page_blocks(mapping);
        is_resizing_.store(false, std::memory_order_release);
        DEBUG_LOG("End resizing.");
    });
    resize_thread_->detach();
}

template <typename K, typename V, typename HC>
typename Viper<K, V, HC>::Client Viper<K, V, HC>::get_client() {
    Client client{*this};
    ++num_active_clients_;
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
    std::atomic<version_lock_t>& v_lock = v_page_->version_lock;
    version_lock_t lock_value = v_lock.load() & FREE_BYTE;
    // Compare and swap until we are the thread to set the lock bit
    while (!v_lock.compare_exchange_weak(lock_value, lock_value | LOCK_BYTE)) {
        lock_value &= FREE_BYTE;
    }

    // We now have the lock on this page
    std::bitset<VPage::num_slots_per_page>* free_slots = &v_page_->free_slots;
    const slot_size_t free_slot_idx = free_slots->_Find_first();

    if (free_slot_idx >= free_slots->size()) {
        // Page is full. Free lock on page and restart.
        v_lock.store(FREE_BYTE, std::memory_order_release);
        update_access_information();
        return put(key, value);
    }

    if constexpr (std::is_same_v<K, std::string>) {
        // Variable length record
        internal::VarSizeEntry entry{key.size(), value.size()};
        bool is_inserted = false;
        char* insert_pos = v_page_->next_insert_pos;
        const size_t entry_length = entry.key_size + entry.value_size;
        const size_t meta_size = sizeof(entry.size_info);

        ptrdiff_t offset_in_page = insert_pos - reinterpret_cast<char*>(v_page_->current_data_page);
        assert(offset_in_page <= v_page_size);
        block_size_t current_block_number = v_data_block_number_;
        page_size_t current_page_number = v_data_page_number_;

        if (offset_in_page + meta_size + entry_length > v_page_size) {
            // This page is not big enough to fit the record. Allocate new one.
            update_data_page_information(v_page_);

            if (offset_in_page + meta_size + entry.key_size < v_page_size) {
                // Key fits, value does not. Add key to old page and value to new one.
                // 0 size indicates value is on next page.
                entry.value_size = 0;
                entry.data = insert_pos + meta_size;
                pmem_memcpy_persist(entry.data, key.data(), entry.key_size);
                pmem_memcpy_persist(insert_pos, &entry.size_info, meta_size);

                // Add value to new page.
                // 0 size indicates key is on previous page.
                insert_pos = v_data_page_->data.data();
                internal::VarSizeEntry value_entry{0, value.size()};
                value_entry.data = insert_pos + meta_size;
                pmem_memcpy(value_entry.data, value.data(), value_entry.value_size, 0);
                pmem_memcpy(insert_pos, &value_entry.size_info, meta_size, 0);
                pmem_persist(insert_pos, meta_size + value_entry.value_size);

                v_page_->next_insert_pos = insert_pos + meta_size + value_entry.value_size;
                is_inserted = true;
            } else {
                // Both key and value don't fit. Add both to new page. 0 sizes indicate that both are on next page.
                if (offset_in_page + meta_size <= v_page_size) {
                    // 0-entry metadata fits into current page.
                    internal::VarSizeEntry next_page_entry{0, 0};
                    pmem_memcpy_persist(insert_pos, &next_page_entry.size_info, meta_size);
                }

                insert_pos = v_data_page_->data.data();
                offset_in_page = v_data_page_->METADATA_SIZE;
                current_block_number = v_data_block_number_;
                current_page_number = v_data_page_number_;
            }
        }

        if (!is_inserted) {
            // Entire record fits into current page.
            entry.data = insert_pos + meta_size;
            pmem_memcpy(entry.data, key.data(), entry.key_size, 0);
            pmem_memcpy(entry.data + entry.key_size, value.data(), entry.value_size, 0);
            pmem_memcpy(insert_pos, &entry.size_info, meta_size, 0);
            pmem_persist(insert_pos, meta_size + entry_length);
            v_page_->next_insert_pos = insert_pos + meta_size + entry_length;
        }

        ptrdiff_t next_pos = v_page_->next_insert_pos - reinterpret_cast<char*>(v_page_->current_data_page);
        assert(next_pos <= v_page_size);

        const uint16_t data_offset = offset_in_page - v_data_page_->METADATA_SIZE;
        const VarOffset var_offset{current_block_number, current_page_number, data_offset};
        v_page_->data[free_slot_idx] = var_offset;
        VarOffset* entry_ptr = v_page_->data.data() + free_slot_idx;
        pmem_persist(entry_ptr, sizeof(VarOffset));
    } else {
        // Fixed size entry.
        // We have found a free slot on this page. Persist data.
        v_page_->data[free_slot_idx] = {key, value};
        typename VPage::VEntry* entry_ptr = v_page_->data.data() + free_slot_idx;
        pmem_persist(entry_ptr, sizeof(typename VPage::VEntry));
    }

    free_slots->reset(free_slot_idx);
    pmem_persist(free_slots, sizeof(*free_slots));

    // Store data in DRAM map.
    const KVOffset kv_offset{v_block_number_, v_page_number_, free_slot_idx};
    bool is_new_item;
    KVOffset old_offset;
    {
        // Scope this so the accessor is freed as soon as possible.
        typename MapType::accessor accessor;
        is_new_item = viper_.map_.insert(accessor, {key, kv_offset});
        if (!is_new_item) {
            old_offset = accessor->second;
            accessor->second = kv_offset;
        }
    }

    // Unlock the v_page
    v_lock.store(FREE_BYTE, std::memory_order_release);

    // Need to free slot at old location for this key
    if (!is_new_item && !old_offset.is_tombstone()) {
        const auto [block_number, page_number, slot_number] = old_offset.get_offsets();
        free_occupied_slot(block_number, page_number, slot_number);
    }

    // We have added one value, so +1
    size_delta_++;
    info_sync();

    return is_new_item;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::get(const K& key, Viper::Accessor& accessor) {
    auto& result = accessor.map_accessor_;
    const bool found = viper_.map_.find(result, key);
    if (!found) {
        return false;
    }
    const KVOffset kv_offset = result->second;
    if (kv_offset.is_tombstone()) {
        return false;
    }

    accessor.value_ = get_value_from_offset(kv_offset);
    return true;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::ConstClient::get(const K& key, Viper::ConstAccessor& accessor) const {
    auto& result = accessor.map_accessor_;
    const bool found = const_viper_.map_.find(result, key);
    if (!found) {
        return false;
    }
    const KVOffset kv_offset = result->second;
    if (kv_offset.is_tombstone()) {
        return false;
    }

    accessor.value_ = get_const_value_from_offset(kv_offset);
    return true;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::get(const K& key, Viper::ConstAccessor& accessor) const {
    return static_cast<const Viper<K, V, HC>::ConstClient*>(this)->get(key, accessor);
}

template <typename K, typename V, typename HC>
template <typename UpdateFn>
bool Viper<K, V, HC>::Client::update(const K& key, UpdateFn update_fn) {
    if constexpr (std::is_same_v<K, std::string>) {
        throw std::runtime_error("In-place update not supported for variable length records!");
    }

    typename MapType::accessor result;
    const bool found = viper_.map_.find(result, key);
    if (!found) {
        return false;
    }
    const KVOffset kv_offset = result->second;
    if (kv_offset.is_tombstone()) {
        return false;
    }

    V* value = get_value_from_offset(kv_offset);
    update_fn(value);
    return true;
}

template <typename K, typename V, typename HC>
bool Viper<K, V, HC>::Client::remove(const K& key) {
    typename MapType::const_accessor result;
    const bool found = viper_.map_.find(result, key);
    if (!found) {
        return false;
    }
    const KVOffset offset = result->second;
    if (offset.is_tombstone()) {
        return false;
    }

    const auto [block_number, page_number, slot_number] = offset.get_offsets();
    free_occupied_slot(block_number, page_number, slot_number);
    viper_.map_.erase(result);
    return true;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::Client::free_occupied_slot(const block_size_t block_number,
                                                 const page_size_t page_number,
                                                 const slot_size_t slot_number) {
    while (viper_.is_v_blocks_resizing_.load(std::memory_order_acquire)) {
        // Wait for vector's memmove to complete. Otherwise we might encounter a segfault.
    }
    VPage& v_page = viper_.v_blocks_[block_number]->v_pages[page_number];
    std::atomic<version_lock_t>& v_lock = v_page.version_lock;
    version_lock_t lock_value = v_lock.load(std::memory_order_acquire) & FREE_BYTE;
    while (!v_lock.compare_exchange_weak(lock_value, lock_value | LOCK_BYTE)) {
        lock_value &= FREE_BYTE;
    }

    // We have the lock now. Free slot.
    std::bitset<VPage::num_slots_per_page>* free_slots = &v_page_->free_slots;
    free_slots->set(slot_number);
    pmem_persist(free_slots, sizeof(*free_slots));

    v_lock.store(FREE_BYTE, std::memory_order_release);

    --size_delta_;
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::Client::update_access_information() {
    if (strategy_ == PageStrategy::DimmBased) {
        if (v_block_number_ == end_v_block_number_) {
            // No more allocated pages, need new range
            viper_.get_new_access_information(this);
        } else {
            ++v_block_number_;
            v_block_ = viper_.v_blocks_[v_block_number_];
            v_page_ = &(v_block_->v_pages[v_page_number_]);
        }
    } else if (strategy_ == PageStrategy::BlockBased) {
        if (++num_v_pages_processed_ == viper_.num_pages_per_block) {
            // No more pages, need new block
            viper_.get_new_access_information(this);
        } else {
            v_page_number_ = (v_page_number_ + 1) % viper_.num_pages_per_block;
            v_page_ = &(v_block_->v_pages[v_page_number_]);
        }
    } else {
        throw std::runtime_error("Unknown page strategy.");
    }

    // Make sure new page is initialized correctly
    v_page_->init();

    if constexpr (std::is_same_v<K, std::string>) {
        update_data_page_information(v_page_);
    }
}

template <typename K, typename V, typename HC>
void Viper<K, V, HC>::Client::update_data_page_information(internal::ViperPage<K, V>* v_page) {
    if (v_data_page_ == nullptr || num_v_data_pages_processed_ == viper_.num_pages_per_block) {
        // No more pages, need new block
        viper_.get_new_data_access_information(this);
    } else {
        v_data_page_number_ = (v_data_page_number_ + 1) % viper_.num_pages_per_block;
        v_data_page_ = &(v_data_block_->v_pages[v_data_page_number_]);
    }

    v_data_page_->owner = v_page;
    v_data_page_->next = nullptr;

    if (v_page->current_data_page != nullptr) {
        // New VDataPage for existing page.
        assert(v_page->current_data_page->next == nullptr);
        v_page->current_data_page->next = v_data_page_;
        pmem_persist(&(v_page->current_data_page->next), sizeof(VDataPage*));
    }

    v_page->current_data_page = v_data_page_;
    v_page->next_insert_pos = v_data_page_->data.data();
    pmem_persist(&(v_page->current_data_page), sizeof(VDataPage*));
    num_v_data_pages_processed_++;
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
Viper<K, V, HC>::Client::Client(ViperT& viper) : ConstClient{viper}, viper_{viper} {
    op_count_ = 0;
    size_delta_ = 0;
    num_v_pages_processed_ = 0;
    v_block_number_ = 0;
    v_page_number_ = 0;
    v_data_block_number_ = 0;
    v_data_page_number_ = 0;
    end_v_block_number_ = 0;
    v_page_ = nullptr;
    v_data_page_ = nullptr;
    v_block_ = nullptr;
    v_data_block_ = nullptr;
}

template <typename K, typename V, typename HC>
Viper<K, V, HC>::Client::~Client() {
    viper_.remove_client(this);
}

template <typename K, typename V, typename HC>
const typename ValueAccessor<V>::type Viper<K, V, HC>::ConstClient::get_const_value_from_offset(Viper::KVOffset offset) const {
    const auto [block, page, slot] = offset.get_offsets();
    return &(const_viper_.v_blocks_[block]->v_pages[page].data[slot].second);
}

template <>
const typename ValueAccessor<std::string>::type Viper<std::string, std::string, internal::KeyCompare<std::string>>::ConstClient::get_const_value_from_offset(Viper::KVOffset offset) const {
    const auto [block, page, slot] = offset.get_offsets();

    const internal::VarSizeOffset var_offset = const_viper_.v_blocks_[block]->v_pages[page].data[slot];
    const block_size_t var_block = var_offset.block_number;
    const page_size_t var_page = var_offset.page_number;
    const uint32_t var_data_offset = var_offset.data_offset;

    const VDataPageBlock* v_data_block = reinterpret_cast<VDataPageBlock*>(const_viper_.v_blocks_[var_block]);
    const VDataPage& v_data_page = v_data_block->v_pages[var_page];
    const char* raw_data = &v_data_page.data[var_data_offset];
    internal::VarEntryAccessor var_entry{raw_data};
    if (var_entry.value_size == 0) {
        // Value is on next page
        const char* raw_value_data = &v_data_page.next->data[0];
        var_entry = internal::VarEntryAccessor{raw_data, raw_value_data};
    }
    return var_entry.value();
}

template <typename K, typename V, typename HC>
typename ValueAccessor<V>::type Viper<K, V, HC>::Client::get_value_from_offset(Viper::KVOffset offset) {
    const auto [block, page, slot] = offset.get_offsets();
    return &(viper_.v_blocks_[block]->v_pages[page].data[slot].second);
}

template <>
std::string_view Viper<std::string, std::string, internal::KeyCompare<std::string>>::Client::get_value_from_offset(Viper::KVOffset offset) {
    const auto [block, page, slot] = offset.get_offsets();

    const internal::VarSizeOffset var_offset = viper_.v_blocks_[block]->v_pages[page].data[slot];
    const block_size_t var_block = var_offset.block_number;
    const page_size_t var_page = var_offset.page_number;
    const uint32_t var_page_offset = var_offset.data_offset;

    const VDataPageBlock* v_data_block = reinterpret_cast<VDataPageBlock*>(const_viper_.v_blocks_[var_block]);
    const VDataPage& v_data_page = v_data_block->v_pages[var_page];
    const char* raw_data = &v_data_page.data[var_page_offset];
    internal::VarEntryAccessor var_entry{raw_data};
    if (var_entry.value_size == 0) {
        // Value is on next page
        const char* raw_value_data = &v_data_page.next->data[0];
        var_entry = internal::VarEntryAccessor{raw_data, raw_value_data};
    }
    return var_entry.value();
}

}  // namespace viper
