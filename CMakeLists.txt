cmake_minimum_required(VERSION 3.14)
project(dehua_lsm)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 设置编译类型为 Debug 以包含调试信息
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# 调试标志
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# 添加子目录
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(dh-server)