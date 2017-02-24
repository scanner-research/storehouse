# Copyright 2016 Carnegie Mellon University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.2.0 FATAL_ERROR)

project(Storehouse)

option(BUILD_STATIC "" OFF)

enable_testing()

include(ExternalProject)

######################
###     Setup     ####
######################
# Verify C++11 support
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support.")
endif()

### Helper macros ####
MACRO(INSTALL_HEADERS_WITH_DIRECTORY HEADER_LIST)

FOREACH(HEADER ${${HEADER_LIST}})
STRING(REGEX MATCH "(.*)[/\\]" DIR ${HEADER})
INSTALL(FILES ${HEADER} DESTINATION include/${DIR})
ENDFOREACH(HEADER)

ENDMACRO(INSTALL_HEADERS_WITH_DIRECTORY)


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

set(GLOBAL_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(THIRDPARTY_SOURCE_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()


###### DEPENDENCIES #######

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
if(NOT APPLE AND UNIX)
  set(CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} -pthread -ldl -lrt")
endif()

###### PROJECT LIBRARIES #######
find_package(Boost COMPONENTS thread program_options python REQUIRED)
find_package(GFlags REQUIRED)
find_package(Glog REQUIRED)
find_package(CURL REQUIRED)
find_package(PythonLibs 2.7 EXACT REQUIRED)

set(GTEST_INCLUDE_DIRS
  "${GLOBAL_OUTPUT_PATH}/googletest/include")
set(GTEST_LIBRARIES
  "${GLOBAL_OUTPUT_PATH}/googletest/lib/libgtest.a")
set(GTEST_LIB_MAIN
  "${GLOBAL_OUTPUT_PATH}/googletest/lib/libgtest_main.a")

set(STOREHOUSE_LIBRARIES
  "${CUSTOM_LIBRARIES}"
  "${GLOG_LIBRARIES}"
  "${CURL_LIBRARIES}"
  "${OPENSSL_LIBRARIES}"
  "${STOREHOUSE_LIBRARIES}"
  "${Boost_LIBRARIES}"
  "${PythonLibs_LIBRARIES}")

include_directories(
  "."
  "${GLOG_INCLUDE_DIRS}"
  "${GTEST_INCLUDE_DIRS}"
  "${PYTHON_INCLUDE_DIRS}")

set(AWS_MODULES core s3)
foreach(MODULE ${AWS_MODULES})
  find_package(aws-cpp-sdk-${MODULE} REQUIRED CONFIG
    PATHS "thirdparty/build/bin/aws-sdk-cpp/lib/cmake/aws-cpp-sdk-${MODULE}")
  list(APPEND STOREHOUSE_LIBRARIES aws-cpp-sdk-${MODULE})
endforeach()

add_subdirectory(storehouse)

if(BUILD_STATIC)
  set(BUILD_MODE STATIC)
else()
  set(BUILD_MODE SHARED)
endif()
add_library(storehouse ${BUILD_MODE}
  storehouse/storage_backend.cpp
  storehouse/storage_config.cpp
  storehouse/storehouse_python.cpp
  storehouse/util.cpp
  $<TARGET_OBJECTS:posix_storage_lib>
  $<TARGET_OBJECTS:s3_storage_lib>)

target_link_libraries(storehouse PUBLIC
  "${STOREHOUSE_LIBRARIES}")

if(UNIX)
  set(PLATFORM_LINK_FLAGS "-pthread -ldl -lrt")
endif()

set(PUBLIC_HEADER_FILES
  storehouse/storage_backend.h
  storehouse/storage_config.h)

install(TARGETS storehouse
  EXPORT StorehouseTarget
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)

install(FILES ${CUSTOM_LIBRARIES}
  DESTINATION lib)

if(BUILD_STATIC)
  set(STOREHOUSE_LIBRARY "${CMAKE_INSTALL_PREFIX}/lib/libstorehouse.a")
else()
  set(STOREHOUSE_LIBRARY "${CMAKE_INSTALL_PREFIX}/lib/libstorehouse.so")
endif()
set(STOREHOUSE_LIBRARIES ${CUSTOM_LIBRARIES})

configure_file(cmake/StorehouseConfig.cmake.in
  "${PROJECT_BINARY_DIR}/cmake/StorehouseConfig.cmake" @ONLY)

install(FILES "${PROJECT_BINARY_DIR}/cmake/StorehouseConfig.cmake"
  DESTINATION cmake)

install_headers_with_directory(PUBLIC_HEADER_FILES)

install(EXPORT StorehouseTarget
  DESTINATION cmake)
