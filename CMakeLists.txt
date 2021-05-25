cmake_minimum_required(VERSION 3.14)
project(viper)

set(CMAKE_CXX_STANDARD 17)

### OPTIONS ###
option(VIPER_BUILD_PLAYGROUND "Set ON if playground should be built." OFF)

option(VIPER_BUILD_BENCHMARKS "Set OM if benchmarks should be built." OFF)

option(VIPER_CONCURRENT_QUEUE_PROVIDED "Set ON if the concurrentqueue dependency is provided and should not be
                                        downloaded by Viper." OFF)

option(VIPER_PMDK_PROVIDED "Set OFF if libpmem is not handled by your project and should be included by Viper.
                            This should be ON if you are already including libpmem want to avoid conflicting paths for
                            libpmem, as Viper will also look at default install locations." ON)
set(VIPER_PMDK_PATH "/usr" CACHE STRING "Path to custom PMDK install directory")

###############

# VIPER
add_library(viper INTERFACE)
target_include_directories(viper INTERFACE include/)
target_compile_options(viper INTERFACE -march=native)

# PMDK
if (NOT ${VIPER_PMDK_PROVIDED})
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

    target_include_directories(viper INTERFACE ${PMDK_INCLUDE_DIRS})
    target_link_libraries(viper INTERFACE ${PMDK_LIBRARIES})
    message(STATUS "Found PMDK. Including ${PMDK_INCLUDE_DIRS} and linking ${PMDK_LIBRARIES}")
endif()

# CONCURRENTQUEUE
if (NOT ${VIPER_CONCURRENT_QUEUE_PROVIDED})
    include(FetchContent)
    FetchContent_Declare(
            concurrentqueue

            GIT_REPOSITORY https://github.com/cameron314/concurrentqueue.git
            GIT_TAG v1.0.3
    )
    FetchContent_MakeAvailable(concurrentqueue)
    target_link_libraries(viper INTERFACE concurrentqueue)
endif()

# VIPER PLAYGROUND
if (${VIPER_BUILD_PLAYGROUND})
    add_executable(playground playground.cpp)

    target_compile_options(playground PRIVATE -march=native -pthread)
    message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
    if (CMAKE_BUILD_TYPE MATCHES Release)
        message(STATUS "BUILDING RELEASE")
        target_compile_options(playground PRIVATE -O3 -Ofast -mtune=native)
    endif()

    target_link_libraries(playground viper pthread)
endif()

# VIPER BENCHMARKS
if (${VIPER_BUILD_BENCHMARKS})
    if (${VIPER_PMDK_PROVIDED})
        message(FATAL_ERROR "PMDK cannot be provided for a benchmark build!
                             Use -DVIPER_PMDK_PROVIDED=OFF and specify -DVIPER_PMDK_PATH=/path/to/pmdk")
    endif()
    add_subdirectory(benchmark)
endif()
