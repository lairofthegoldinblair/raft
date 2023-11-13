cmake_minimum_required (VERSION 3.15)

project (raft)

include(FetchContent)
FetchContent_Declare(
  flatbuffers
  GIT_REPOSITORY https://github.com/google/flatbuffers.git
  GIT_TAG        v23.3.3
)
set(FETCHCONTENT_QUIET OFF)
FetchContent_MakeAvailable(flatbuffers)

include_directories ( ${flatbuffers_SOURCE_DIR}/include )

# We have some of our own CMake Find Modules
set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")

######################################################
######################################################
# Packages and libraries 
######################################################
######################################################

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)

#
# Boost
#
find_package( Boost 1.55 COMPONENTS log system thread unit_test_framework REQUIRED )
include_directories ( ${Boost_INCLUDE_DIR} )

#
# Miscellaneous libraries builtin to Linux
#
find_library(LIB_DL dl)
find_library(LIB_PTHREAD pthread)
find_library(LIB_RT rt)

#
# Setup CXX17 flag
#
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++17" COMPILER_SUPPORTS_CXX17)
if(COMPILER_SUPPORTS_CXX17)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++17 support. Please use a different C++ compiler.")
endif()

######################################################
######################################################
# Subdirectories of the project
######################################################
######################################################
include_directories ( "${PROJECT_SOURCE_DIR}/src" )

add_subdirectory ( src )
add_subdirectory ( test )

######################################################
######################################################
# Unit testing support
######################################################
######################################################
enable_testing()
add_test(raft-unit-test ${CMAKE_CURRENT_BINARY_DIR}/test/raft-unit-test)
add_test(raft-protocol-operation-test ${CMAKE_CURRENT_BINARY_DIR}/test/raft-protocol-operation-test)
add_test(raft-asio-unit-test ${CMAKE_CURRENT_BINARY_DIR}/test/raft-asio-unit-test)
add_test(raft-linearizability-unit-test ${CMAKE_CURRENT_BINARY_DIR}/test/raft-linearizability-unit-test)

