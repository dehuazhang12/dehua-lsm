# 收集 src 目录下所有 .cpp 文件
file(GLOB_RECURSE LSMENGINE_SRC 
    ${CMAKE_CURRENT_SOURCE_DIR}/skipList/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/iterator/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/memTable/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/config/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/block/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/sst/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/wal/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/redisWrapper/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/logger/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/lsmEngine/*.cpp
)

# 创建统一的 lsmEngine 库
add_library(lsmEngine ${LSMENGINE_SRC})

# 包含所有头文件目录
target_include_directories(lsmEngine 
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/skipList/include
        ${CMAKE_CURRENT_SOURCE_DIR}/iterator/include
        ${CMAKE_CURRENT_SOURCE_DIR}/memTable/include
        ${CMAKE_CURRENT_SOURCE_DIR}/config/include
        ${CMAKE_CURRENT_SOURCE_DIR}/block/include
        ${CMAKE_CURRENT_SOURCE_DIR}/sst/include
        ${CMAKE_CURRENT_SOURCE_DIR}/wal/include
        ${CMAKE_CURRENT_SOURCE_DIR}/redisWrapper/include
        ${CMAKE_CURRENT_SOURCE_DIR}/logger/include
        ${CMAKE_CURRENT_SOURCE_DIR}/lsmEngine/include
)

# 查找依赖包
find_package(spdlog REQUIRED)

# 链接外部依赖
target_link_libraries(lsmEngine 
    PUBLIC 
        spdlog::spdlog
)

# 设置输出目录
set_target_properties(lsmEngine PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib
)