#include "storehouse/s3/s3_storage.h"

#include <aws/s3/model/Bucket.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/Aws.h>
#include <fstream>
#include <sstream>

namespace storehouse {

using Aws::S3::S3Client;

class S3RandomReadFile : public RandomReadFile {
 public:
  S3RandomReadFile(const std::string& name, const std::string& bucket,
                   S3Client* client)
      : name_(name), bucket_(bucket), client_(client) {}

  StoreResult read(uint64_t offset, size_t requested_size, uint8_t* data,
                   size_t& size_read) override {
    uint64_t file_size;
    auto result = get_size(file_size);
    int64_t size_to_read = std::min((int64_t)(file_size - offset), (int64_t)requested_size);
    size_to_read = std::max(size_to_read, (int64_t)0);

    size_read = 0;
    if (result != StoreResult::Success
        || (size_to_read == 0 && requested_size == 0)) {
      return result;
    }

    if (size_to_read == 0 && requested_size > 0) {
      return StoreResult::EndOfFile;
    }

    Aws::S3::Model::GetObjectRequest object_request;

    std::stringstream range_request;
    range_request << "bytes=" << offset << "-" << (offset + size_to_read - 1);

    object_request.WithBucket(bucket_).WithKey(name_).WithRange(range_request.str());

    auto get_object_outcome = client_->GetObject(object_request);

    if (get_object_outcome.IsSuccess()) {
      size_read = get_object_outcome.GetResult().GetContentLength();
      get_object_outcome.GetResult().GetBody().
          rdbuf()->sgetn((char*)data, size_read);

      if (size_read != requested_size) {
        return StoreResult::EndOfFile;
      }
      return StoreResult::Success;
    } else {
      LOG(WARNING) << "Error opening file: " <<
          get_full_path() << " - " <<
          get_object_outcome.GetError().GetMessage();

      return StoreResult::ReadFailure;
    }
  }

  StoreResult get_size(uint64_t& size) override {
    Aws::S3::Model::HeadObjectRequest object_request;
    object_request.WithBucket(bucket_).WithKey(name_);

    auto head_object_outcome = client_->HeadObject(object_request);

    if (head_object_outcome.IsSuccess()) {
      size = (uint64_t)head_object_outcome.GetResult().GetContentLength();
    } else {
      LOG(WARNING) << "Error getting size - HeadObject error: " <<
          head_object_outcome.GetError().GetExceptionName() << " " <<
          head_object_outcome.GetError().GetMessage() <<
          " for object: " << get_full_path();
      return StoreResult::ReadFailure;
    }

    return StoreResult::Success;
  }

  const std::string path() override { return name_; }

 private:
  std::string bucket_;
  std::string name_;
  S3Client* client_;

  std::string get_full_path() {
    return bucket_ + "/" + name_;
  }
};

class S3WriteFile : public WriteFile {
 public:
  S3WriteFile(const std::string& name, const std::string& bucket,
                   S3Client* client)
      : name_(name), bucket_(bucket), client_(client) {
    tmpfilename_ = strdup("/tmp/scannerXXXXXX");
    int temp_fd;

    temp_fd = mkstemp(tmpfilename_);
    LOG_IF(FATAL, temp_fd == -1) << "Failed to create temp file for writing";
    tfp_ = fdopen(temp_fd, "wb+");

    LOG_IF(FATAL, tfp_ == NULL) << "Failed to open temp file for writing";

    has_changed_ = true;

    LOG(WARNING) << "Make s3 file with " << name;
  }

  ~S3WriteFile() {
    save();
    free(tmpfilename_);
    int err = unlink(tmpfilename_);
    LOG_IF(FATAL, err < 0) << "Unlink temp file failed with error: " << strerror(errno);
    if (tfp_ != NULL) {
      std::fclose(tfp_);
    }
  }

  StoreResult append(size_t size, const uint8_t* data) override {
    size_t size_written = fwrite(data, sizeof(uint8_t), size, tfp_);
    LOG_IF(FATAL, size_written != size)
      << "S3WriteFile: did not write all " << size << " "
      << "bytes for to tmp file for file " << get_full_path() << ".";
    has_changed_ = true;
    return StoreResult::Success;
  }

  StoreResult save() override {
    if (!has_changed_) { return StoreResult::Success; }

    std::fflush(tfp_);

    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
            tmpfilename_, std::ios_base::in | std::ios_base::binary);

    Aws::S3::Model::PutObjectRequest put_object_request;
    put_object_request.WithKey(name_).WithBucket(bucket_);

    put_object_request.SetBody(input_data);
    auto put_object_outcome = client_->PutObject(put_object_request);

    if(!put_object_outcome.IsSuccess()) {
      LOG(WARNING) << "Save Error: error while putting object: " <<
          get_full_path() << " - " <<
          put_object_outcome.GetError().GetExceptionName() << " " <<
          put_object_outcome.GetError().GetMessage();
      return StoreResult::SaveFailure;
    }

    has_changed_ = false;

    return StoreResult::Success;
  }

  const std::string path() override { return name_; }

 private:
  std::string bucket_;
  std::string name_;
  S3Client* client_;
  FILE* tfp_;
  char* tmpfilename_;
  bool has_changed_;

  std::string get_full_path() {
    return bucket_ + "/" + name_;
  }
};

uint64_t S3Storage::num_clients = 0;
std::mutex S3Storage::num_clients_mutex;

S3Storage::S3Storage(S3Config config) : bucket_(config.bucket) {
  std::lock_guard<std::mutex> guard(num_clients_mutex);
  if (num_clients == 0) {
    Aws::InitAPI(sdk_options_);
  }
  num_clients++;

  Aws::Client::ClientConfiguration cc;
  cc.scheme = Aws::Http::Scheme::HTTP;
  cc.region = config.endpointRegion;
  cc.endpointOverride = config.endpointOverride;

  client_ = new S3Client(cc);
}

S3Storage::~S3Storage() {
  std::lock_guard<std::mutex> guard(num_clients_mutex);
  delete client_;

  num_clients--;
  if (num_clients == 0) {
    Aws::ShutdownAPI(sdk_options_);
  }
}

StoreResult S3Storage::get_file_info(const std::string& name,
                                     FileInfo& file_info) {
  S3RandomReadFile s3read_file(name, bucket_, client_);
  file_info.file_exists = false;
  file_info.file_is_folder = (name[name.length()-1] == '/');
  auto result = s3read_file.get_size(file_info.size);
  if (result == StoreResult::Success) {
  	file_info.file_exists = true;
  }
  return result;
}

StoreResult S3Storage::make_random_read_file(const std::string& name,
                                             RandomReadFile*& file) {
  file = new S3RandomReadFile(name, bucket_, client_);
  return StoreResult::Success;
}

StoreResult S3Storage::make_write_file(const std::string& name,
                                       WriteFile*& file) {
  file = new S3WriteFile(name, bucket_, client_);
  return StoreResult::Success;
}

StoreResult S3Storage::make_dir(const std::string& name) {
  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.WithKey(name + "/").WithBucket(bucket_);
  auto put_object_outcome = client_->PutObject(put_object_request);

  if(!put_object_outcome.IsSuccess()) {
    LOG(WARNING) << "Save Error: error while making dir: " <<
        bucket_ << "/" << name << " - " <<
        put_object_outcome.GetError().GetExceptionName() << " " <<
        put_object_outcome.GetError().GetMessage();
    return StoreResult::MkDirFailure;
  }
  return StoreResult::Success;
}

StoreResult S3Storage::delete_file(const std::string& name) {
  return StoreResult::Success;
}

StoreResult S3Storage::delete_dir(const std::string& name, bool recursive) {
  return StoreResult::Success;
}
}
