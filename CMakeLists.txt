cmake_minimum_required(VERSION 3.14)
project(viper)

set(CMAKE_CXX_STANDARD 17)
include(FetchContent)

message("Build type: ${CMAKE_BUILD_TYPE}")
if (CMAKE_BUILD_TYPE MATCHES Debug)
    message("BUILDING DEBUG")
else()
    message("BUILDING RELEASE")
    add_compile_options(-march=native -mtune=native)
endif()

# PMDK
set(PMDK_INCLUDE_HINTS ${CMAKE_INCLUDE_PATH} "/usr/include" "/usr/local/include")
set(PMDK_HINTS "/usr/local/lib64" "/usr/local/lib" "/usr/lib" "/usr/lib64")
find_path(PMEM_INCLUDE_DIRS libpmem.h REQUIRED HINTS ${PMDK_INCLUDE_HINTS})
find_library(PMEM_LIBRARIES NAMES pmem libpmem REQUIRED HINTS ${PMDK_HINTS})

if (NOT PMEM_INCLUDE_DIRS OR "${PMEM_INCLUDE_DIRS}" STREQUAL "")
    message(FATAL_ERROR "ERROR: libpmem include directory not found.")
endif ()
if (NOT PMEM_LIBRARIES OR "${PMEM_LIBRARIES}" STREQUAL "")
    message(FATAL_ERROR "ERROR: libpmem library not found.")
endif ()

include_directories(${LIBPMEM_INCLUDE_DIRS})
message(STATUS "PMDK provided. Including ${PMEM_INCLUDE_DIRS} and linking ${PMEM_LIBRARIES}.")

# memkind
find_library(MEMKIND_LIBS NAMES memkind REQUIRED HINTS ${PMDK_HINTS})
if (NOT MEMKIND_LIBS OR "${MEMKIND_LIBS}" STREQUAL "")
    message(FATAL_ERROR "ERROR: libmemkind library not found.")
endif ()
message(STATUS "Found memkind at ${MEMKIND_LIBS}.")

#SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath -Wl,${PMEM_LIB_PATH};/usr/local/lib")
#set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PATH}:${PMEM_LIB_PATH}:/usr/local/lib")
#set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(LINK_LIBS pthread pmem pmemobj pmempool ${MEMKIND_LIBS})
#pmemkv pmemlog /usr/local/lib/libmemkind.so.0

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
add_executable(playground playground.cpp)
target_link_libraries(playground ${LINK_LIBS})
