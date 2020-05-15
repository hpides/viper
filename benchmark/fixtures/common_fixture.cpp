#include "common_fixture.hpp"


std::string viper::kv_bm::random_file(const std::filesystem::path& base_dir) {
    if (!std::filesystem::exists(base_dir)) {
        if (!std::filesystem::create_directories(base_dir)) {
            throw std::runtime_error{"Could not create dir: " + base_dir.string() + "\n"};
        }
    }
    std::string str("abcdefghijklmnopqrstuvwxyz");
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(str.begin(), str.end(), generator);
    std::string file_name = str.substr(0, 15) + ".file";
    std::filesystem::path file{file_name};
    return base_dir / file;
}

void viper::kv_bm::BaseFixture::log_find_count(benchmark::State& state, uint64_t num_found, uint64_t num_expected) {
    state.counters["found"] = num_found;
    if (num_found != num_expected) {
        std::cout << "DID NOT FIND ALL ENTRIES (" + std::to_string(num_found)
            + "/" + std::to_string(num_expected) + ")\n";
    }
}

bool viper::kv_bm::is_init_thread(const benchmark::State& state) {
    // Use idx = 1 because 0 starts all threads first before continuing.
    return state.threads == 1 || state.thread_index == 1;
}

void viper::kv_bm::set_cpu_affinity() {
    const auto native_thread_handle = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int cpu : viper::kv_bm::CPUS) {
        CPU_SET(cpu, &cpuset);
    }
    int rc = pthread_setaffinity_np(native_thread_handle, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}

void viper::kv_bm::set_cpu_affinity(uint16_t thread_idx) {
    if (thread_idx >= viper::kv_bm::CPUS.size()) {
        throw std::runtime_error("Thread index too high!");
    }
    const auto native_thread_handle = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(viper::kv_bm::CPUS[thread_idx], &cpuset);
    int rc = pthread_setaffinity_np(native_thread_handle, sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
        std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
}
