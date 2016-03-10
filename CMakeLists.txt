cmake_minimum_required (VERSION 2.8)

project (raft)

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
# Setup CXX11 flag
#
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
if(COMPILER_SUPPORTS_CXX11)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++0x")
else()
        message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
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

