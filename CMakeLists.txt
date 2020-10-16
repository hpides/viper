cmake_minimum_required(VERSION 3.14)
project(nvm_kv_bm)

set(CMAKE_CXX_STANDARD 17)
include(FetchContent)

message("Build type: ${CMAKE_BUILD_TYPE}")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("BUILDING DEBUG")
    find_library(libpmemobj REQUIRED PATHS /usr/local/lib64/pmdk_debug)
    set(PMEM_LIB_PATH /usr/local/lib64/pmdk_debug)
else()
    message("BUILDING RELEASE")
    find_library(libpmemobj REQUIRED)
    set(PMEM_LIB_PATH /usr/local/lib64/)
    add_compile_options(-march=native -mtune=native)
endif()

# Intel TBB
set(TBB_DIR ${TBB_ROOT}/cmake)
find_package(TBB REQUIRED tbb)
include_directories(BEFORE ${TBB_ROOT}/include)

find_library(libpmemobj++ REQUIRED PATHS /usr/local/include)
find_library(libpmemkv REQUIRED)
find_library(memkind REQUIRED PATHS /usr/local/lib/)

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${PMEM_LIB_PATH};/usr/local/lib")
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PATH}:${PMEM_LIB_PATH}:/usr/local/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(LINK_LIBS pthread pmem pmemobj pmemkv pmempool pmemlog /usr/local/lib/libmemkind.so.0 ${TBB_IMPORTED_TARGETS})

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
include_directories(src)

add_subdirectory(benchmark)

# VIPER
add_compile_options(-pthread)
#add_subdirectory(src)

add_executable(playground playground.cpp)
target_link_libraries(playground ${LINK_LIBS})
target_link_libraries(playground viper)

