cmake_minimum_required(VERSION 3.5)
project(destor C)

set(CMAKE_C_STANDARD 11)

file(GLOB_RECURSE INCLUDE_DIRS LIST_DIRECTORIES true "include/*")
file(GLOB_RECURSE SOURCES "src/*.c")

add_executable(destor ${SOURCES})

target_include_directories(destor PRIVATE "include/")
foreach(dir ${INCLUDE_DIRS})
    if(IS_DIRECTORY ${dir})
        target_include_directories(destor PRIVATE ${dir})
    endif()
endforeach()

target_compile_options(destor PRIVATE -O3)

target_link_libraries(destor glib-2.0 pthread ssl crypto)