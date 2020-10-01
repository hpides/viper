add_library(
        viper

        viper.hpp
)

target_link_libraries(viper PRIVATE ${TBB_IMPORTED_TARGETS} CCEH)
set_target_properties(viper PROPERTIES LINKER_LANGUAGE CXX)
