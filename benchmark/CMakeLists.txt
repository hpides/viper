project(ycsb)
include(FetchContent)
set(JEMALLOC_DIR "/usr/lib/x86_64-linux-gnu")
set(MKL_LINK_DIRECTORY "/opt/intel/mkl/lib/intel64")
set(MKL_INCLUDE_DIRECTORY "/opt/intel/mkl/include")
link_directories(${JEMALLOC_DIR})
link_directories(${MKL_LINK_DIRECTORY})
include_directories(${MKL_INCLUDE_DIRECTORY})
set(mass
        ${CMAKE_SOURCE_DIR}/index/mass/masstree.c
        ${CMAKE_SOURCE_DIR}/index/skiplist/arena.cc
        ${CMAKE_SOURCE_DIR}/index/bwtree/bwtree.cpp
        ${CMAKE_SOURCE_DIR}/index/wormhole/kv.c
        ${CMAKE_SOURCE_DIR}/index/wormhole/lib.c
        ${CMAKE_SOURCE_DIR}/index/wormhole/wh.c
        )
SET(
        src


        ${CMAKE_CURRENT_SOURCE_DIR}/benchmark.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/benchmark.hpp
        ${CMAKE_CURRENT_SOURCE_DIR}/fixtures/common_fixture.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/fixtures/common_fixture.hpp
        ${CMAKE_CURRENT_SOURCE_DIR}/fixtures/viper_fixture.hpp
        ${CMAKE_CURRENT_SOURCE_DIR}/ycsb_bm.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/fixtures/ycsb_common.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/fixtures/ycsb_common.hpp
        ${mass}
)
FetchContent_Declare(
        google_benchmark
        GIT_REPOSITORY https://github.com/google/benchmark.git
        GIT_TAG v1.5.2
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
FetchContent_MakeAvailable(google_benchmark)

set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE INTERNAL "")
FetchContent_Declare(
        hdr_histogram
        GIT_REPOSITORY https://github.com/HdrHistogram/HdrHistogram_c.git
        GIT_TAG 0.11.2
)
FetchContent_MakeAvailable(hdr_histogram)

add_executable(ycsb ${src})
target_compile_options(ycsb PRIVATE -march=native -mtune=native -mclwb -pthread -g)
target_link_libraries(ycsb pthread stdc++fs atomic stdc++ viper benchmark hdr_histogram_static  mkl_rt jemalloc)
