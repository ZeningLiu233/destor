cmake_minimum_required(VERSION 3.5)
project(destor C)

set(CMAKE_C_STANDARD 11)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB REQUIRED glib-2.0)

file(GLOB_RECURSE SOURCES "src/*.c")

add_executable(destor ${SOURCES})

target_compile_options(destor PRIVATE -O3)

target_include_directories(destor PRIVATE ${GLIB_INCLUDE_DIRS} "include/")

target_link_libraries(destor pthread ${GLIB_LIBRARIES} isal_crypto m)