add_executable(TestSkipList test_skipList.cpp)

#添加依赖的库
target_link_libraries(TestSkipList
  PRIVATE
    lsmEngine
)

set_target_properties(TestSkipList PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)
