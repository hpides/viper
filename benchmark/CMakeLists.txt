SET(
        BASE_BENCHMARK_FILES

        benchmark.cpp
        benchmark.hpp
        fixtures/common_fixture.cpp
        fixtures/common_fixture.hpp
        fixtures/viper_fixture.hpp
)

SET(
        ALL_SYSTEMS_BENCHMARK_FILES

        ${BASE_BENCHMARK_FILES}
        fixtures/rocksdb_fixture.hpp
        fixtures/faster_fixture.hpp
        fixtures/pmem_kv_fixture.hpp
        fixtures/dram_map_fixture.hpp
)

# GOOGLE BENCHMARK
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
add_subdirectory(third_party/googlebench)

# ROCKSDB
add_subdirectory(third_party/rocksdb)
include_directories(third_party/rocksdb/include)

# FASTER
include_directories(third_party/faster/cc/src)
add_subdirectory(third_party/faster/cc)

# HdrHistogram
add_subdirectory(third_party/hdr_histogram)
include_directories(third_party/hdr_histogram/src)

# DASH
#add_definitions(-DPMEM -DUSE_PMEM=ON)
#add_subdirectory(third_party/dash)
#include_directories(third_party/dash/src)
#include_directories("${CMAKE_CURRENT_BINARY_DIR}/../_deps/epoch_reclaimer-src")

add_executable(all_ops_bm all_ops_benchmark.cpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(all_ops_bm ${LINK_LIBS})
target_link_libraries(all_ops_bm benchmark rocksdb faster uuid aio) # dash)
set_target_properties(all_ops_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(access_pattern_bm access_pattern_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(access_pattern_bm ${LINK_LIBS})
target_link_libraries(access_pattern_bm benchmark)
set_target_properties(access_pattern_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(nvm_util_benchmark util_benchmark.cpp fixtures/common_fixture.cpp fixtures/common_fixture.hpp)
target_link_libraries(nvm_util_benchmark ${LINK_LIBS})
target_link_libraries(nvm_util_benchmark benchmark)
set_target_properties(nvm_util_benchmark PROPERTIES LINKER_LANGUAGE CXX)
