file(GLOB_RECURSE TEST_SOURCES "*.cpp")
add_executable(tests ${TEST_SOURCES} ../src/page.cpp ../src/storage_manager.cpp)