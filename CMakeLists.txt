cmake_minimum_required(VERSION 3.5)
project(destor C)

set(CMAKE_C_STANDARD 11)

file(GLOB_RECURSE INCLUDES LIST_DIRECTORIES true "include/")
file(GLOB_RECURSE SOURCES "src/*.c")

add_executable(destor ${SOURCES})

target_compile_options(destor PRIVATE -O3)

target_include_directories(destor PRIVATE "include/")

target_link_libraries(destor pthread glib-2.0 isal_crypto)