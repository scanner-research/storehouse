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

cmake_minimum_required(VERSION 3.4.0 FATAL_ERROR)

project(Storage)

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
# find_package(LibLZMA REQUIRED)
find_package(OpenSSL REQUIRED) # for GoogleAPIClient
# find_package(BZip2 REQUIRED)
find_package(Boost COMPONENTS thread program_options REQUIRED)
find_package(GFlags REQUIRED)
find_package(Glog REQUIRED)
find_package(CURL REQUIRED)
# find_package(Iconv REQUIRED)

ExternalProject_Add(JsonCpp
  GIT_REPOSITORY "https://github.com/open-source-parsers/jsoncpp.git"
  GIT_TAG "1.7.2"

  UPDATE_COMMAND ""

  SOURCE_DIR "${THIRDPARTY_SOURCE_DIR}/jsoncpp"
  CMAKE_ARGS
      -DCMAKE_INSTALL_PREFIX=${GLOBAL_OUTPUT_PATH}/jsoncpp
      -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      -DCMAKE_SHARED_LINKER_FLAGS=${CMAKE_SHARED_LINKER_FLAGS}
)

# Google API cpp client
ExternalProject_Add(GoogleAPICppClient
  DEPENDS JsonCpp
  GIT_REPOSITORY "https://github.com/abpoms/google-api-cpp-client.git"
  GIT_TAG "0f6cd03b33ed6a863a6d8d1a30eade4b7766835a"

  UPDATE_COMMAND ""

  SOURCE_DIR "${THIRDPARTY_SOURCE_DIR}/google-api-cpp-client"
  INSTALL_DIR ${GLOBAL_OUTPUT_PATH}/google-api-cpp-client
  CMAKE_ARGS
      -DCMAKE_INSTALL_PREFIX=${GLOBAL_OUTPUT_PATH}/google-api-cpp-client
      -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
      -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
      -DCMAKE_SHARED_LINKER_FLAGS=${CMAKE_SHARED_LINKER_FLAGS}
      -DBZIP2_INCLUDE_DIR=${BZIP2_INCLUDE_DIR}
      -DCMAKE_LIBRARY_PATH=${CMAKE_LIBRARY_PATH}
      -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
      -DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}
      -DCURL_INCLUDE_DIR=${CURL_INCLUDE_DIR}
      -DCURL_LIBRARIES=${CURL_LIBRARIES}

  CMAKE_CACHE_ARGS
      -DJSONCPP_INCLUDE_DIRS:FILEPATH=${THIRDPARTY_SOURCE_DIR}/jsoncpp/include
      -DJSONCPP_LIBRARY:FILEPATH=${GLOBAL_OUTPUT_PATH}/jsoncpp/lib/libjsoncpp.a
      -DGLOG_ROOT_DIR:FILEPATH=${GLOG_ROOT_DIR}
      -DGFLAGS_ROOT_DIR:FILEPATH=${GFLAGS_ROOT_DIR}
      -DOPENSSL_ROOT_DIR:FILEPATH=${OPENSSL_ROOT_DIR}
      -DBOOST_ROOT:FILEPATH=${BOOST_ROOT}
)

# googletest
ExternalProject_Add(GoogleTest
  GIT_REPOSITORY "https://github.com/google/googletest"
  GIT_TAG 0a439623f75c029912728d80cb7f1b8b48739ca4

  UPDATE_COMMAND ""

  SOURCE_DIR "${THIRDPARTY_SOURCE_DIR}/googletest"

  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${GLOBAL_OUTPUT_PATH}/googletest
      -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
      -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}

  INSTALL_DIR "${GLOBAL_OUTPUT_PATH}/googletest"
)

set(GTEST_INCLUDE_DIRS
  "${GLOBAL_OUTPUT_PATH}/googletest/include")
set(GTEST_LIBRARIES
  "${GLOBAL_OUTPUT_PATH}/googletest/lib/libgtest.a")
set(GTEST_LIB_MAIN
  "${GLOBAL_OUTPUT_PATH}/googletest/lib/libgtest_main.a")

set(LIGHTSCAN_LIBRARIES
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogle_storage_api.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_oauth2.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_jsoncpp.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_json.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_http.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_curl_http.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_utils.a"
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/lib/libgoogleapis_internal.a"
  "${GLOBAL_OUTPUT_PATH}/jsoncpp/lib/libjsoncpp.a"
  "${GLOG_LIBRARIES}"
  "${CURL_LIBRARIES}"
  "${OPENSSL_LIBRARIES}"
  "${LIGHTSCAN_LIBRARIES}")

include_directories(
  "."
  "${GLOBAL_OUTPUT_PATH}/google-api-cpp-client/include"
  "${GLOBAL_OUTPUT_PATH}/jsoncpp/include/"
  "${GLOG_INCLUDE_DIRS}"
  "${GTEST_INCLUDE_DIRS}")

add_subdirectory(storage)

add_library(storage
  storage/storage_backend.cpp
  storage/storage_config.cpp
  storage/util.cpp
  $<TARGET_OBJECTS:gcs_storage_lib>
  $<TARGET_OBJECTS:posix_storage_lib>)

if(UNIX)
  set(PLATFORM_LINK_FLAGS "-pthread -ldl -lrt")
endif()

set(PUBLIC_HEADER_FILES
  storage/storage_backend.h
  storage/storage_config.h)


install(TARGETS storage
  EXPORT StorageTarget
  ARCHIVE DESTINATION lib)

set(STORAGE_LIBRARY "${CMAKE_INSTALL_PREFIX}/lib/libstorage.a")

configure_file(cmake/StorageConfig.cmake.in
  "${PROJECT_BINARY_DIR}/cmake/StorageConfig.cmake" @ONLY)

install(FILES "${PROJECT_BINARY_DIR}/cmake/StorageConfig.cmake"
  DESTINATION cmake)

install_headers_with_directory(PUBLIC_HEADER_FILES)

install(EXPORT StorageTarget
  DESTINATION cmake)
