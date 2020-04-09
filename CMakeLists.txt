cmake_minimum_required(VERSION 3.14)
project(nvm_kv_bm)

set(CMAKE_CXX_STANDARD 17)

find_library(libpmemobj REQUIRED)
find_library(libpmemobj++ REQUIRED PATHS /usr/local/include)

SET(CMAKE_EXE_LINKER_FLAGS
        "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,/usr/local/lib64")

set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PATH}:/usr/local/lib64/")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

add_library(nvm_kv_bm_lib benchmark.cpp benchmark.hpp)
target_link_libraries(nvm_kv_bm_lib pmemobj)

add_executable(nvm_kv_bm main.cpp)
target_link_libraries(nvm_kv_bm nvm_kv_bm_lib)
