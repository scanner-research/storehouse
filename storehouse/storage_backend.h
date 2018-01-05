/* Copyright 2016 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "storehouse/storage_config.h"

#include <glog/logging.h>

#include <unistd.h>
#include <memory>
#include <string>
#include <vector>

namespace storehouse {

////////////////////////////////////////////////////////////////////////////////
/// StoreResult
enum class StoreResult {
  Success,
  EndOfFile,
  FileExists,
  FileDoesNotExist,
  TransientFailure,
  ReadFailure,
  RemoveFailure,
  SaveFailure,
  MkDirFailure,
};

std::string store_result_to_string(StoreResult result);

////////////////////////////////////////////////////////////////////////////////
/// FileInfo
struct FileInfo {
  uint64_t size;
  bool file_exists;
  bool file_is_folder;
};

////////////////////////////////////////////////////////////////////////////////
/// RandomReadFile
class RandomReadFile {
 public:
  virtual ~RandomReadFile(){};

  StoreResult read(uint64_t offset, size_t size, std::vector<uint8_t>& data);

  virtual StoreResult read(uint64_t offset, size_t size, uint8_t* data,
                           size_t& size_read) = 0;

  virtual StoreResult get_size(uint64_t& size) = 0;

  virtual const std::string path() = 0;
};

////////////////////////////////////////////////////////////////////////////////
/// WriteFile
class WriteFile {
 public:
  virtual ~WriteFile(){};

  StoreResult append(const std::vector<uint8_t>& data);

  virtual StoreResult append(size_t size, const uint8_t* data) = 0;

  virtual StoreResult save() = 0;

  virtual const std::string path() = 0;
};

////////////////////////////////////////////////////////////////////////////////
/// StorageBackend
class StorageBackend {
 public:
  virtual ~StorageBackend() {}

  static StorageBackend* make_from_config(const StorageConfig* config);

  /* get_file_info
   *
   */
  virtual StoreResult get_file_info(const std::string& name,
                                    FileInfo& file_info) = 0;

  /* make_random_read_file
   *
   */
  virtual StoreResult make_random_read_file(const std::string& name,
                                            RandomReadFile*& file) = 0;

  /* make_write_file
   *
   */
  virtual StoreResult make_write_file(const std::string& name,
                                      WriteFile*& file) = 0;

  /* make_dir
   *
   */
  virtual StoreResult make_dir(const std::string& name) = 0;

  /* delete_file
   *
   */
  virtual StoreResult delete_file(const std::string& name) = 0;

  /* delete_dir
   *
   */
  virtual StoreResult delete_dir(const std::string& name,
                                 bool recursive = false) = 0;
};

////////////////////////////////////////////////////////////////////////////////
/// Utilities
StoreResult make_unique_random_read_file(StorageBackend* storage,
                                         const std::string& name,
                                         std::unique_ptr<RandomReadFile>& file);

StoreResult make_unique_write_file(StorageBackend* storage,
                                   const std::string& name,
                                   std::unique_ptr<WriteFile>& file);

std::vector<uint8_t> read_entire_file(RandomReadFile* file, uint64_t& pos,
                                      size_t read_size = 1048576);

void exit_on_error(StoreResult result);

#define EXP_BACKOFF(expression__, status__)                             \
  do {                                                                  \
    int sleep_debt__ = 1;                                               \
    while (true) {                                                      \
      const storehouse::StoreResult result__ = (expression__);          \
      if (result__ == storehouse::StoreResult::TransientFailure) {      \
        double sleep_time__ =                                           \
          (sleep_debt__ + (static_cast<double>(rand()) / RAND_MAX));    \
        if (sleep_debt__ < 64) {                                        \
          sleep_debt__ *= 2;                                            \
        } else {                                                        \
          LOG(WARNING) << "EXP_BACKOFF: reached max backoff.";          \
        }                                                               \
        LOG(WARNING) << "EXP_BACKOFF: transient failure, sleeping for " \
                     << sleep_time__ << ".";                            \
        usleep(sleep_time__ * 1000000);                                 \
        continue;                                                       \
      }                                                                 \
      status__ = result__;                                              \
      break;                                                            \
    }                                                                   \
  } while (0);

#define BACKOFF_FAIL(expression__)        \
  do {                                    \
    storehouse::StoreResult result___;    \
    EXP_BACKOFF(expression__, result___); \
    storehouse::exit_on_error(result___); \
  } while (0);

#define RETURN_ON_ERROR(expression)                      \
  do {                                                   \
    const storehouse::StoreResult result = (expression); \
    if (result != storehouse::StoresResult::Success) {   \
      return result;                                     \
    }                                                    \
  } while (0);
}
