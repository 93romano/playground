cmake_minimum_required(VERSION 3.10)
project(SimpleDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src)

file(GLOB_RECURSE SOURCES "src/*.cpp" "src/*.h")

add_executable(simpledb ${SOURCES})

add_subdirectory(tests)