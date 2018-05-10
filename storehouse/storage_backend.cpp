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

#include "storehouse/storage_backend.h"
// #include "storehouse/gcs/gcs_storage.h"
#include "storehouse/posix/posix_storage.h"
#include "storehouse/s3/s3_storage.h"
#include "storehouse/util.h"

#include <cstdlib>

namespace storehouse {

StoreResult RandomReadFile::read(uint64_t offset, size_t size,
                                 std::vector<uint8_t>& data) {
  size_t orig_data_size = data.size();
  data.resize(orig_data_size + size);
  size_t size_read;
  StoreResult result =
    this->read(offset, size, data.data() + orig_data_size, size_read);
  if (result != StoreResult::Success) {
    return result;
  }
  if (size_read != size) {
    LOG(ERROR) << "Expected read of size " << size << " but only read "
               << size_read;
    return StoreResult::ReadFailure;
  }
  return result;
}

StoreResult WriteFile::append(const std::vector<uint8_t>& data) {
  return this->append(data.size(), data.data());
}

StorageBackend* StorageBackend::make_from_config(const StorageConfig* config) {
  // if (const GCSConfig *gcs_config = dynamic_cast<const GCSConfig *>(config))
  // {
  //   return new GCSStorage(*gcs_config);
  // } else
  if (const PosixConfig* d_config = dynamic_cast<const PosixConfig*>(config)) {
    return new PosixStorage(*d_config);
  } else if (const S3Config* s3_config =
               dynamic_cast<const S3Config*>(config)) {
    return new S3Storage(*s3_config);
  }
  return nullptr;
}

std::string store_result_to_string(StoreResult result) {
  switch (result) {
    case StoreResult::Success:
      return "Success";
    case StoreResult::FileExists:
      return "FileExists";
    case StoreResult::FileDoesNotExist:
      return "FileDoesNotExist";
    case StoreResult::EndOfFile:
      return "EndOfFile";
    case StoreResult::TransientFailure:
      return "TransientFailure";
    case StoreResult::ReadFailure:
      return "ReadFailure";
    case StoreResult::RemoveFailure:
      return "RemoveFailure";
    case StoreResult::SaveFailure:
      return "SaveFailure";
    case StoreResult::MkDirFailure:
      return "MkDirFailure";
  }
  return "<Undefined>";
}

StoreResult make_unique_random_read_file(
  StorageBackend* storage, const std::string& name,
  std::unique_ptr<RandomReadFile>& file) {
  RandomReadFile* ptr = nullptr;

  int sleep_debt = 1;
  StoreResult result;
  // Exponential backoff for transient failures
  while (true) {
    result = storage->make_random_read_file(name, ptr);
    if (result == StoreResult::TransientFailure) {
      double sleep_time =
        (sleep_debt + (static_cast<double>(rand()) / RAND_MAX));
      if (sleep_debt < 64) {
        sleep_debt *= 2;
      } else {
        LOG(FATAL) << "Reached max backoff for " << name << ".";
        exit(1);
      }
      LOG(WARNING) << "Transient failure for " << name << ", sleeping for "
                   << sleep_time << ".";
      usleep(sleep_time * 1000000);
      continue;
    }
    break;
  }
  file.reset(ptr);
  return result;
}

StoreResult make_unique_write_file(StorageBackend* storage,
                                   const std::string& name,
                                   std::unique_ptr<WriteFile>& file) {
  WriteFile* ptr = nullptr;

  int sleep_debt = 1;
  StoreResult result;
  // Exponential backoff for transient failures
  while (true) {
    result = storage->make_write_file(name, ptr);
    if (result == StoreResult::TransientFailure) {
      double sleep_time =
        (sleep_debt + (static_cast<double>(rand()) / RAND_MAX));
      if (sleep_debt < 64) {
        sleep_debt *= 2;
      } else {
        LOG(FATAL) << "Reached max backoff for " << name << ".";
        exit(1);
      }
      LOG(WARNING) << "Transient failure for " << name << ", sleeping for "
                   << sleep_time << ".";
      usleep(sleep_time * 1000000);
      continue;
    }
    break;
  }
  file.reset(ptr);
  return result;
}

std::vector<uint8_t> read_entire_file(RandomReadFile* file, uint64_t& pos, size_t read_size) {
  // Load the entire input
  std::vector<uint8_t> bytes;
  {
    while (true) {
      size_t prev_size = bytes.size();
      bytes.resize(bytes.size() + read_size);
      size_t size_read;
      StoreResult result;
      EXP_BACKOFF(
        file->read(pos, read_size, bytes.data() + prev_size, size_read),
        result);
      assert(result == StoreResult::Success ||
             result == StoreResult::EndOfFile);
      pos += size_read;
      if (result == StoreResult::EndOfFile) {
        bytes.resize(prev_size + size_read);
        break;
      }
    }
  }
  return bytes;
}

void exit_on_error(StoreResult result, const std::string& exit_msg) {
  if (result == StoreResult::Success) return;

  LOG(FATAL) << "Exiting due to failed operation result: "
             << store_result_to_string(result) << ", " << exit_msg;
  std::exit(EXIT_FAILURE);
}
}
