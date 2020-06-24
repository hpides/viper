#include <iostream>
#include "benchmark.hpp"

namespace viper::kv_bm {

std::string get_time_string() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d-%H-%M");
    return ss.str();
}

std::string get_output_file(const std::string& bm_name) {
    return std::string("--benchmark_out=") + RESULT_FILE_DIR + bm_name + "_" + get_time_string() + ".json";
}


int bm_main(std::vector<std::string> args) {
    std::vector<char*> cstrings;
    cstrings.reserve(args.size());

    for (size_t i = 0; i < args.size(); ++i) {
        cstrings.push_back(const_cast<char*>(args[i].c_str()));
    }

    if (args.size() == 1) {
        std::cout << "NOT STORING RESULTS!" << std::endl;
    }

    int argc = args.size();
    char** argv = &cstrings[0];
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    return 0;
}

}  // namespace viper::kv_bm
