/* Copyright 2016 Carnegie Mellon University, NVIDIA Corporation
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

#include "storehouse/posix/posix_storage.h"
#include "storehouse/util.h"

#include <glog/logging.h>

#include <libgen.h>
#include <string.h>
#include <sys/stat.h>
#include <cstdlib>

namespace storehouse {

////////////////////////////////////////////////////////////////////////////////
/// PosixRandomReadFile
class PosixRandomReadFile : public RandomReadFile {
 public:
  PosixRandomReadFile(const std::string& file_path) : file_path_(file_path) {
    fp_ = fopen(file_path.c_str(), "r");
    position_ = 0;
  }

  ~PosixRandomReadFile() {
    if (fp_ != NULL) {
      fclose(fp_);
    }
  }

  StoreResult read(uint64_t offset, size_t size, uint8_t* data,
                   size_t& size_read) override {
    if (fp_ == NULL) {
      return StoreResult::ReadFailure;
    }

    if (position_ != offset) {
      if (fseek(fp_, offset, SEEK_SET) != 0) {
        LOG_IF(FATAL, ferror(fp_))
          << "PosixRandomReadFile: Error in seeking file " << file_path_.c_str()
          << " to position " << offset;
      }
      position_ = offset;
    }

    size_read = fread(data, sizeof(uint8_t), size, fp_);
    position_ += size_read;

    LOG_IF(FATAL, ferror(fp_))
      << "PosixRandomReadFile: Error in reading file " << file_path_.c_str()
      << " at position " << offset << ", "
      << "size " << size << ".";

    if (feof(fp_)) {
      return StoreResult::EndOfFile;
    } else {
      return StoreResult::Success;
    }
  }

  StoreResult get_size(uint64_t& size) override {
    if (fp_ == NULL) {
      return StoreResult::ReadFailure;
    }

    int fd = fileno(fp_);
    struct stat stat_buf;
    int rc = fstat(fd, &stat_buf);
    if (rc == 0) {
      size = stat_buf.st_size;
      return StoreResult::Success;
    } else {
      return StoreResult::FileDoesNotExist;
    }
  }

  const std::string path() override { return file_path_; }

 private:
  const std::string file_path_;
  FILE* fp_;
  int position_;
};

////////////////////////////////////////////////////////////////////////////////
/// PosixWriteFile
class PosixWriteFile : public WriteFile {
 public:
  PosixWriteFile(const std::string& file_path) : file_path_(file_path) {
    VLOG(1) << "PosixWriteFile: opening " << file_path.c_str()
            << " for writing.";
    char* path;
    path = strdup(file_path.c_str());
    LOG_IF(FATAL, path == NULL)
      << "PosixWriteFile: could not strdup " << file_path.c_str();
    LOG_IF(FATAL, mkdir_p(dirname(path), S_IRWXU) != 0)
      << "PosixWriteFile: could not mkdir " << path;
    free(path);
    fp_ = fopen(file_path.c_str(), "w");
    LOG_IF(FATAL, fp_ == NULL) << "PosixWriteFile: could not open "
                               << file_path.c_str() << " for writing.";
  }

  ~PosixWriteFile() {
    save();
    if (fp_ != NULL) {
      fclose(fp_);
    }
  }

  StoreResult append(size_t size, const uint8_t* data) override {
    size_t size_written = fwrite(data, sizeof(uint8_t), size, fp_);
    LOG_IF(FATAL, size_written != size)
      << "PosixWriteFile: did not write all " << size << " "
      << "bytes for file " << file_path_.c_str() << ".";
    return StoreResult::Success;
  }

  StoreResult save() override {
    fflush(fp_);
    return StoreResult::Success;
  }

  const std::string path() override { return file_path_; }

 private:
  const std::string file_path_;
  FILE* fp_;
};

////////////////////////////////////////////////////////////////////////////////
/// PosixStorage
PosixStorage::PosixStorage(PosixConfig config) {}

PosixStorage::~PosixStorage() {}

StoreResult PosixStorage::get_file_info(const std::string& name,
                                        FileInfo& file_info) {
  struct stat stat_buf;
  int rc = stat(name.c_str(), &stat_buf);
  if (rc == 0 && !S_ISDIR(stat_buf.st_mode)) {
    file_info.size = stat_buf.st_size;
    return StoreResult::Success;
  } else {
    return StoreResult::FileDoesNotExist;
  }
}

StoreResult PosixStorage::make_random_read_file(const std::string& name,
                                                RandomReadFile*& file) {
  FileInfo file_info;
  StoreResult result;
  if ((result = get_file_info(name, file_info)) != StoreResult::Success) {
    return result;
  }
  file = new PosixRandomReadFile(name);
  return StoreResult::Success;
}

StoreResult PosixStorage::make_write_file(const std::string& name,
                                          WriteFile*& file) {
  file = new PosixWriteFile(name);
  return StoreResult::Success;
}

StoreResult PosixStorage::delete_file(const std::string& name) {
  FileInfo file_info;
  StoreResult result = get_file_info(name, file_info);
  if (result != StoreResult::Success) {
    return result;
  }

  if (remove(name.c_str()) < 0) {
    return StoreResult::RemoveFailure;
  }
  return StoreResult::Success;
}
}
