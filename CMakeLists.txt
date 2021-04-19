cmake_minimum_required(VERSION 3.14)
project(viper)

set(CMAKE_CXX_STANDARD 17)
include(FetchContent)

add_compile_options(-march=native)
message("Build type: ${CMAKE_BUILD_TYPE}")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("BUILDING DEBUG")
else()
    message("BUILDING RELEASE")
    add_compile_options(-O3 -Ofast -mtune=native)
endif()

# PMDK
set(PMDK_PATH "/scratch/pmem/pmdk")
set(PMDK_LIB "${PMDK_PATH}/lib/libpmem.so")
include_directories("${PMDK_PATH}/include")
link_directories("${PMDK_PATH}/lib")

# memkind
link_directories("/scratch/pmem/memkind/lib")

SET(LINK_LIBS pthread pmem pmemobj pmempool memkind)

# Concurrent Queue
FetchContent_Declare(
        concurrent_queue

        GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
        GIT_TAG 1.0.2
)
FetchContent_MakeAvailable(concurrent_queue)
set(CONCURRENT_QUEUE_DIR "${CMAKE_CURRENT_BINARY_DIR}/_deps/concurrent_queue-src")
include_directories(${CONCURRENT_QUEUE_DIR})

# Include viper sources
include_directories(include)

add_subdirectory(benchmark)

# VIPER
add_compile_options(-pthread)
add_executable(playground playground.cpp)
target_link_libraries(playground ${LINK_LIBS})
