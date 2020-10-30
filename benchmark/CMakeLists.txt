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
        fixtures/cceh_fixture.hpp
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

add_executable(all_ops_bm all_ops_benchmark.cpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(all_ops_bm ${LINK_LIBS})
target_link_libraries(all_ops_bm benchmark rocksdb faster uuid aio hdr_histogram_static)
set_target_properties(all_ops_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(access_pattern_bm access_pattern_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(access_pattern_bm ${LINK_LIBS})
target_link_libraries(access_pattern_bm benchmark hdr_histogram_static)
set_target_properties(access_pattern_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(update_bm update_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(update_bm ${LINK_LIBS})
target_link_libraries(update_bm benchmark hdr_histogram_static)
set_target_properties(update_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(recovery_bm recovery_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(recovery_bm ${LINK_LIBS})
target_link_libraries(recovery_bm benchmark hdr_histogram_static)
set_target_properties(recovery_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(reclaim_bm reclaim_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(reclaim_bm ${LINK_LIBS})
target_link_libraries(reclaim_bm benchmark hdr_histogram_static)
set_target_properties(reclaim_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(kv_size_bm key_value_size_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(kv_size_bm ${LINK_LIBS})
target_link_libraries(kv_size_bm benchmark faster rocksdb uuid aio hdr_histogram_static)
set_target_properties(kv_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(variable_size_bm variable_size_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(variable_size_bm ${LINK_LIBS})
target_link_libraries(variable_size_bm benchmark faster rocksdb uuid aio hdr_histogram_static)
set_target_properties(variable_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(ycsb_bm ycsb_bm.cpp fixtures/ycsb_common.cpp fixtures/ycsb_common.hpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(ycsb_bm ${LINK_LIBS})
target_link_libraries(ycsb_bm benchmark rocksdb faster uuid aio hdr_histogram_static)
set_target_properties(ycsb_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(breakdown_bm breakdown_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(breakdown_bm ${LINK_LIBS})
target_link_libraries(breakdown_bm benchmark hdr_histogram_static)
set_target_properties(breakdown_bm PROPERTIES LINKER_LANGUAGE CXX)
