include(ExternalProject)

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
#        fixtures/rocksdb_fixture.hpp
        fixtures/faster_fixture.hpp
        fixtures/pmem_kv_fixture.hpp
        fixtures/cceh_fixture.hpp
        fixtures/dash_fixture.hpp
        fixtures/utree.hpp
        fixtures/utree_fixture.hpp
        fixtures/crl.hpp
        fixtures/crl_fixture.hpp
        fixtures/tbb_fixture.hpp
)

cmake_host_system_information(RESULT host_name QUERY HOSTNAME)
message(STATUS "Running on host: ${host_name}")
if ("${host_name}" STREQUAL "nvram-01")
    message(STATUS "Known host: nvram01")
    add_definitions(-DNVRAM01)
elseif("${host_name}" STREQUAL "nvram-02")
    message(STATUS "Known host: nvram02")
    add_definitions(-DNVRAM02)
else()
    message(STATUS "Unknown host: ${host_name}")
endif()

# GOOGLE BENCHMARK
FetchContent_Declare(
        google_benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.5.2
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
FetchContent_MakeAvailable(google_benchmark)

# libpmemobj++
include_directories("/scratch/pmem/libpmemobj-cpp/include")

# TBB
set(TBB_TEST OFF CACHE INTERNAL "")
set(TBB_EXAMPLES OFF CACHE INTERNAL "")
FetchContent_Declare(
        oneTBB
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG        v2021.1.1
)
FetchContent_MakeAvailable(oneTBB)
include_directories(${oneTBB_SOURCE_DIR}/include)
#include_directories("/opt/intel/oneapi/tbb/latest/include/")


# PmemKV
cmake_policy(SET CMP0077 NEW)
set(OLD_CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH})
set(CMAKE_PREFIX_PATH "/scratch/pmem/libpmemobj-cpp/;/scratch/pmem/pmdk;"
                      "/scratch/pmem/memkind;${oneTBB_BUILD_DIR}")
FetchContent_Declare(
        pmemkv
        GIT_REPOSITORY https://github.com/pmem/pmemkv.git
        GIT_TAG 1.4
)
set(BUILD_DOC OFF)
set(BUILD_EXAMPLES OFF)
set(BUILD_TESTS OFF)
set(BUILD_JSON_CONFIG OFF)
set(ENGINE_VCMAP OFF)
set(ENGINE_VSMAP OFF)
set(ENGINE_CACHING OFF)
set(ENGINE_STREE OFF)
set(ENGINE_TREE3 OFF)
FetchContent_MakeAvailable(pmemkv)
include_directories(${pmemkv_SOURCE_DIR}/src)
set(CMAKE_PREFIX_PATH ${OLD_CMAKE_PREFIX_PATH})


# PMEM-ROCKSDB
set(PMEM_ROCKSDB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/pmem-rocksdb")
ExternalProject_Add(
        pmem_rocksdb
        GIT_REPOSITORY https://github.com/lawben/pmem-rocksdb.git
        GIT_TAG 8352e95
        PREFIX ${PMEM_ROCKSDB_PREFIX}
        CONFIGURE_COMMAND ""
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        UPDATE_DISCONNECTED 1
        LOG_BUILD ON
        BUILD_IN_SOURCE 1

        # Use DRAM cache for now
        # BUILD_COMMAND $(MAKE) shared_lib -j ROCKSDB_BC_ON_DCPMM=1 ROCKSDB_KVS_ON_DCPMM=1 ROCKSDB_WAL_ON_DCPMM=1 DISABLE_WARNING_AS_ERROR=1
        BUILD_COMMAND $(MAKE) shared_lib -j ROCKSDB_KVS_ON_DCPMM=1 ROCKSDB_WAL_ON_DCPMM=1 DISABLE_WARNING_AS_ERROR=1
)
# Use DRAM cache for now
#add_definitions(-DBC_ON_DCPMM=1 -DKVS_ON_DCPMM=1 -DWAL_ON_DCPMM=1)
add_definitions(-DKVS_ON_DCPMM=1 -DWAL_ON_DCPMM=1)

link_directories(${PMEM_ROCKSDB_PREFIX}/src/pmem_rocksdb)
include_directories(${PMEM_ROCKSDB_PREFIX}/src/pmem_rocksdb/include)
include_directories(${PMEM_ROCKSDB_PREFIX}/src/pmem_rocksdb/dcpmm)


# Dash
FetchContent_Declare(
        epoch_reclaimer
        GIT_REPOSITORY https://github.com/XiangpengHao/epoch-reclaimer.git
        GIT_TAG        master
)
if (NOT epoch_reclaimer_POPULATED)
    FetchContent_Populate(epoch_reclaimer)
endif ()
include_directories(${epoch_reclaimer_SOURCE_DIR})
add_definitions(-DPMEM)
FetchContent_Declare(
        dash
        GIT_REPOSITORY https://github.com/baotonglu/dash.git
        GIT_TAG        7e1551f
)
if (NOT dash_POPULATED)
    FetchContent_Populate(dash)
endif ()
include_directories(${dash_SOURCE_DIR}/src)
execute_process(COMMAND sed -i -e "s/, (void\\*)pool_addr//g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s/pmemobj_create_addr/pmemobj_create/g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s/pmemobj_open_addr/pmemobj_open/g" ${dash_SOURCE_DIR}/src/allocator.h)
execute_process(COMMAND sed -i -e "s#std::cout << \"Directory_Doubling#//#g" ${dash_SOURCE_DIR}/src/ex_finger.h)

# FASTER
FetchContent_Declare(
        faster
        GIT_REPOSITORY https://github.com/lawben/faster.git
        GIT_TAG        08bd5c4
        SOURCE_SUBDIR  cc
)
FetchContent_MakeAvailable(faster)
include_directories(${faster_SOURCE_DIR}/cc/src)

# HdrHistogram
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE INTERNAL "")
FetchContent_Declare(
        hdr_histogram
        GIT_REPOSITORY https://github.com/HdrHistogram/HdrHistogram_c.git
        GIT_TAG 0.11.2
)
FetchContent_MakeAvailable(hdr_histogram)

add_executable(all_ops_bm all_ops_benchmark.cpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(all_ops_bm ${LINK_LIBS})
target_link_libraries(all_ops_bm benchmark snappy pmemkv faster tbb uuid aio hdr_histogram_static)
# TODO: add rocksdb when needed
add_dependencies(all_ops_bm pmem_rocksdb)
target_link_libraries(all_ops_bm rocksdb)
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
target_link_libraries(kv_size_bm benchmark faster tbb uuid pmemkv aio hdr_histogram_static)
set_target_properties(kv_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(variable_size_bm variable_size_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(variable_size_bm ${LINK_LIBS})
target_link_libraries(variable_size_bm benchmark faster uuid aio tbb pmemkv hdr_histogram_static)
set_target_properties(variable_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(ycsb_bm ycsb_bm.cpp fixtures/ycsb_common.cpp fixtures/ycsb_common.hpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(ycsb_bm ${LINK_LIBS})
target_link_libraries(ycsb_bm benchmark faster pmemkv tbb uuid aio hdr_histogram_static)
set_target_properties(ycsb_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(latency_bw_bm latency_bw_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(latency_bw_bm ${LINK_LIBS} benchmark hdr_histogram_static)
set_target_properties(latency_bw_bm PROPERTIES LINKER_LANGUAGE CXX)
