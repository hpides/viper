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
