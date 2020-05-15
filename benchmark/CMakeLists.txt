SET(
    BENCHMARK_FILES

    benchmark.cpp
    benchmark.hpp
    fixtures/common_fixture.cpp
    fixtures/common_fixture.hpp
    fixtures/dram_map_fixture.cpp
    fixtures/dram_map_fixture.hpp
    fixtures/pmem_map_fixture.cpp
    fixtures/pmem_map_fixture.hpp
    fixtures/hybrid_map_fixture.cpp
    fixtures/hybrid_map_fixture.hpp
    fixtures/viper_fixture.cpp
    fixtures/viper_fixture.hpp
    fixtures/rocksdb_fixture.cpp
    fixtures/rocksdb_fixture.hpp
    fixtures/faster_fixture.cpp
    fixtures/faster_fixture.hpp
    fixtures/pmem_log_fixture.cpp
    fixtures/pmem_log_fixture.hpp
#    fixtures/dash_fixture.cpp
#    fixtures/dash_fixture.hpp
    benchmark_data_types.hpp
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

#add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
add_executable(nvm_kv_bm benchmark.cpp ${BENCHMARK_FILES})
target_link_libraries(nvm_kv_bm ${LINK_LIBS})
target_link_libraries(nvm_kv_bm benchmark rocksdb faster uuid aio) # dash)
#target_link_libraries(nvm_kv_bm -fsanitize=thread)
set_target_properties(nvm_kv_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(nvm_util_benchmark util_benchmark.cpp fixtures/common_fixture.cpp fixtures/common_fixture.hpp)
target_link_libraries(nvm_util_benchmark ${LINK_LIBS})
target_link_libraries(nvm_util_benchmark benchmark)
set_target_properties(nvm_util_benchmark PROPERTIES LINKER_LANGUAGE CXX)
