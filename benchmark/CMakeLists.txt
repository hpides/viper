cmake_minimum_required(VERSION 3.18)

include(ExternalProject)
include(FetchContent)

cmake_policy(SET CMP0077 NEW)

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

# PMDK
set(PMDK_INCLUDE_HINTS ${VIPER_PMDK_PATH}/include)
set(PMDK_LIB_HINTS ${VIPER_PMDK_PATH}/lib)

find_path(PMDK_INCLUDE_DIRS libpmem.h HINTS ${PMDK_INCLUDE_HINTS})
find_library(PMDK_LIBRARIES NAMES pmem libpmem HINTS ${PMDK_LIB_HINTS})

set(PMDK_FOUND true)
if (NOT PMDK_INCLUDE_DIRS OR "${PMDK_INCLUDE_DIRS}" STREQUAL "")
    message(FATAL_ERROR "libpmem include directory not found in default locations or custom path (${PMDK_INCLUDE_HINTS}).")
endif ()
if (NOT PMDK_LIBRARIES OR "${PMDK_LIBRARIES}" STREQUAL "")
    message(FATAL_ERROR "libmem not found in default locations or custom path (${PMDK_LIB_HINTS}).")
endif ()

message(STATUS "Found PMDK. Including ${PMDK_INCLUDE_DIRS} and linking ${PMDK_LIBRARIES}")
include_directories(${PMDK_INCLUDE_DIRS})

# libpmemobj + libpmempool
find_library(PMEMOBJ_LIBRARIES NAMES pmemobj libpmemobj HINTS ${PMDK_LIB_HINTS})
find_library(PMEMPOOL_LIBRARIES NAMES pmempool libpmempool HINTS ${PMDK_LIB_HINTS})

# libpmemobj++
# Use -DLIBPMEMOBJ++_PATH=/path/to/libpmemobj++ for custom path
set(LIBPMEMOBJ++_PATH "/scratch/pmem/libpmemobj-cpp" CACHE PATH
        "Path to custom libpmemobj++ install directory")
include_directories(${LIBPMEMOBJ++_PATH}/include)

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


# PmemKV
set(OLD_CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH})
set(CMAKE_PREFIX_PATH "${LIBPMEMOBJ++_PATH};${VIPER_PMDK_PATH}")

cmake_policy(SET CMP0077 NEW)
set(BUILD_DOC OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(BUILD_JSON_CONFIG OFF CACHE BOOL "" FORCE)
set(ENGINE_VCMAP OFF CACHE BOOL "" FORCE)
set(ENGINE_VSMAP OFF CACHE BOOL "" FORCE)
set(ENGINE_CACHING OFF CACHE BOOL "" FORCE)
set(ENGINE_STREE OFF CACHE BOOL "" FORCE)
set(ENGINE_TREE3 OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        pmemkv
        GIT_REPOSITORY https://github.com/pmem/pmemkv.git
        GIT_TAG 1.4
)
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

set(PMEM_LIBS ${PMDK_LIBRARIES} ${PMEMOBJ_LIBRARIES} ${PMEMPOOL_LIBRARIES})

if (CMAKE_BUILD_TYPE MATCHES Release)
    message(STATUS "BUILDING BENCHMARKS IN RELEASE")
    add_compile_options(-O3 -Ofast -mtune=native)
else()
    message(WARNING "Building benchmarks in ${CMAKE_BUILD_TYPE}. This may impact performance!")
endif()

add_executable(all_ops_bm all_ops_benchmark.cpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(all_ops_bm viper ${PMEM_LIBS})
target_link_libraries(all_ops_bm benchmark pmemkv faster tbb uuid aio hdr_histogram_static)
# TODO: fix rocksdb build
#add_dependencies(all_ops_bm pmem_rocksdb)
#target_link_libraries(all_ops_bm rocksdb snappy)
set_target_properties(all_ops_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(update_bm update_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(update_bm viper ${PMEM_LIBS})
target_link_libraries(update_bm benchmark hdr_histogram_static)
set_target_properties(update_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(recovery_bm recovery_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(recovery_bm viper ${PMEM_LIBS})
target_link_libraries(recovery_bm benchmark hdr_histogram_static)
set_target_properties(recovery_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(reclaim_bm reclaim_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(reclaim_bm viper ${PMEM_LIBS})
target_link_libraries(reclaim_bm benchmark hdr_histogram_static)
set_target_properties(reclaim_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(kv_size_bm key_value_size_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(kv_size_bm viper ${PMEM_LIBS})
target_link_libraries(kv_size_bm benchmark faster tbb uuid pmemkv aio hdr_histogram_static)
set_target_properties(kv_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(variable_size_bm variable_size_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(variable_size_bm viper ${PMEM_LIBS})
target_link_libraries(variable_size_bm benchmark faster uuid aio tbb pmemkv hdr_histogram_static)
set_target_properties(variable_size_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(ycsb_bm ycsb_bm.cpp fixtures/ycsb_common.cpp fixtures/ycsb_common.hpp ${ALL_SYSTEMS_BENCHMARK_FILES})
target_link_libraries(ycsb_bm viper ${PMEM_LIBS})
target_link_libraries(ycsb_bm benchmark faster pmemkv tbb uuid aio hdr_histogram_static)
set_target_properties(ycsb_bm PROPERTIES LINKER_LANGUAGE CXX)

add_executable(latency_bw_bm latency_bw_bm.cpp ${BASE_BENCHMARK_FILES})
target_link_libraries(latency_bw_bm viper ${PMEM_LIBS})
target_link_libraries(latency_bw_bm benchmark hdr_histogram_static)
target_compile_options(latency_bw_bm PRIVATE -march=native)
set_target_properties(latency_bw_bm PROPERTIES LINKER_LANGUAGE CXX)
