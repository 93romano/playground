file(GLOB_RECURSE TEST_SOURCES "*.cpp")
add_executable(tests
    ${TEST_SOURCES}
    ../src/page.cpp
    ../src/storage_manager.cpp
    ../src/buffer_pool_manager.cpp
    ../src/btree.cpp
    ../src/record.cpp
    ../src/sql_parser.cpp
    ../src/database.cpp
)