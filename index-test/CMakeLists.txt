project(test)
include_directories(
        ${CMAKE_SOURCE_DIR}/include
)
add_executable(test test.cpp)
target_compile_options(test PRIVATE -march=native -mclwb -pthread)
target_link_libraries(test viper pthread stdc++fs)