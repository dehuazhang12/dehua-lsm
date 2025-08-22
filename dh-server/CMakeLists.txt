file(GLOB_RECURSE SERVER_SRC
    "*.cpp"
)
add_executable(server ${SERVER_SRC})
target_link_libraries(server
    lsmEngine   
)
target_include_directories(server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)
set_target_properties(server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin
)