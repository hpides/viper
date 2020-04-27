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

add_executable(nvm_kv_bm benchmark.cpp ${BENCHMARK_FILES})
target_link_libraries(nvm_kv_bm ${LINK_LIBS})
target_link_libraries(nvm_kv_bm benchmark rocksdb faster uuid aio)
set_target_properties(nvm_kv_bm PROPERTIES LINKER_LANGUAGE CXX)
