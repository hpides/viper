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
    add_compile_options(-march=native -mtune=native)
endif()

# Intel TBB
set(TBB_DIR ${TBB_ROOT}/cmake)
find_package(TBB REQUIRED tbb)
include_directories(BEFORE ${TBB_ROOT}/include)

#include(${TBB_ROOT}/cmake/TBBBuild.cmake)
#tbb_build(TBB_ROOT ${TBB_ROOT} CONFIG_DIR TBB_DIR)
#find_package(TBB REQUIRED tbb)
#include_directories(${TBB_ROOT}/include)

#SET(TBB_INSTALL_DIR /home/lawrence.benson/oneTBB)
#SET(TBB_INCLUDE ${TBB_INSTALL_DIR}/include)
#SET(TBB_LIBRARY_RELEASE ${TBB_INSTALL_DIR}/build/linux_intel64_gcc_cc9.1.1_libc2.17_kernel3.10.0_release)
#SET(TBB_LIBRARY_DEBUG ${TBB_INSTALL_DIR}/build/linux_intel64_gcc_cc9.1.1_libc2.17_kernel3.10.0_debug)
#include_directories(${TBB_INCLUDE} BEFORE)

find_library(libpmemobj++ REQUIRED PATHS /usr/local/include)
find_library(libpmemkv REQUIRED)
#find_library(tbb REQUIRED PATHS ${TBB_LIBRARY_RELEASE} NO_DEFAULT_PATH)
find_library(memkind REQUIRED PATHS /usr/local/lib/)

SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${PMEM_LIB_PATH};/usr/local/lib")
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PATH}:${PMEM_LIB_PATH}:/usr/local/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(LINK_LIBS pthread pmem pmemobj ${TBB_IMPORTED_TARGETS} pmemkv pmempool pmemlog /usr/local/lib/libmemkind.so.0)

# Include viper sources
include_directories(src)

add_subdirectory(benchmark)

# VIPER
add_compile_options(-pthread)
add_subdirectory(src)

add_executable(playground playground.cpp)
target_link_libraries(playground ${LINK_LIBS})
target_link_libraries(playground viper)

