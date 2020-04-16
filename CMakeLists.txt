cmake_minimum_required(VERSION 3.14)
project(nvm_kv_bm)

set(CMAKE_CXX_STANDARD 17)

message("Build type: ${CMAKE_BUILD_TYPE}")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("BUILDING DEBUG")
    find_library(libpmemobj REQUIRED PATHS /usr/local/lib64/pmdk_debug)
    set(PMEM_LIB_PATH /usr/local/lib64/pmdk_debug)
else()
    message("BUILDING RELEASE")
    find_library(libpmemobj REQUIRED)
    set(PMEM_LIB_PATH /usr/local/lib64/)
    add_compile_options(-march=native)
endif()

find_library(libpmemobj++ REQUIRED PATHS /usr/local/include)
find_library(libpmemkv REQUIRED)
find_library(tbb REQUIRED)
find_library(memkind REQUIRED PATHS /usr/local/lib/)

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${PMEM_LIB_PATH};/usr/local/lib")
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PATH}:${PMEM_LIB_PATH}:/usr/local/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

#add_compile_options(-pthread -Wall -Wextra -pedantic -Werror -Wno-unused-parameter -Wno-dollar-in-identifier-extension -Wno-unknown-pragmas -Wno-subobject-linkage)

set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)
add_subdirectory(third_party/googlebench)
add_subdirectory(third_party/googletest)

add_subdirectory(src/)

add_executable(nvm_kv_bm benchmark.cpp)
target_link_libraries(nvm_kv_bm pmemobj tbb pmemkv pmempool /usr/local/lib/libmemkind.so.0)
target_link_libraries(nvm_kv_bm benchmark gtest)

add_executable(playground playground.cpp)
target_link_libraries(playground pmemobj tbb pmemkv pmempool /usr/local/lib/libmemkind.so.0)
target_link_libraries(playground viper)