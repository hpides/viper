project(ycsb)
include(FetchContent)
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

add_executable(t t.cpp)
target_compile_options(t PRIVATE -march=native -mclwb -pthread)
target_link_libraries(t viper pthread stdc++fs)

add_executable(ycsb ${src})
target_compile_options(ycsb PRIVATE -march=native -mclwb -pthread)
target_link_libraries(ycsb pthread stdc++fs stdc++ viper benchmark hdr_histogram_static)
