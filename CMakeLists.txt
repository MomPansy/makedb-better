cmake_minimum_required(VERSION 3.10)
project(makedb LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(WIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
endif()

add_library(page_lib
src/page_directory.cpp
src/slotted_page.cpp
src/file_storage.cpp
src/row.cpp
src/table.cpp
src/schema.cpp
src/page_manager.cpp
src/parser.cpp
)

target_compile_definitions(page_lib PUBLIC DISABLE_BTREE)
# Ensure the library knows where to find its headers.
target_include_directories(page_lib
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
)
target_include_directories(page_lib
    PUBLIC
        ${PROJECT_SOURCE_DIR}/src
)

enable_testing()

