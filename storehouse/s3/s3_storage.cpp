#include "storehouse/s3/s3_storage.h"

#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/Bucket.h>

namespace storehouse {

using Aws::S3::S3Client;

class S3RandomReadFile : public RandomReadFile {
public:
  S3RandomReadFile(const std::string& name,
                   const std::string& bucket,
                   S3Client* client)
    : name_(name), bucket_(bucket), client_(client) {
  }

  StoreResult read(
    uint64_t offset,
    size_t size,
    uint8_t* data,
    size_t& size_read) override
  {
    Aws::S3::Model::GetObjectRequest object_request;
    const Aws::String bucket = bucket_.c_str();
    const Aws::String name = name_.c_str();
    object_request.WithBucket(bucket).WithKey(name);
    auto outcome = client_->GetObject(object_request);
    if (!outcome.IsSuccess()) {
      LOG(FATAL) << outcome.GetError().GetMessage();
    }

    size_read = outcome.GetResult().GetBody().rdbuf()->sgetn(
      (char*) data, size);
  }

  StoreResult get_size(uint64_t& size) override {
    return StoreResult::Success;
  }

  const std::string path() override {
    return name_;
  }

private:
  std::string bucket_;
  std::string name_;
  S3Client* client_;
};

S3Storage::S3Storage(S3Config config)
  : bucket_(config.bucket)
{
  Aws::InitAPI(sdk_options_);

  client_ = new S3Client;
}

S3Storage::~S3Storage() {
  delete client_;
  Aws::ShutdownAPI(sdk_options_);
}

StoreResult S3Storage::get_file_info(
  const std::string& name, FileInfo& file_info)
{
  return StoreResult::Success;
}

StoreResult S3Storage::make_random_read_file(
  const std::string& name,
  RandomReadFile*& file)
{
  file = new S3RandomReadFile(name, bucket_, client_);
  return StoreResult::Success;
}

StoreResult S3Storage::make_write_file(
  const std::string& name,
  WriteFile*& file)
{
  return StoreResult::Success;
}

StoreResult S3Storage::delete_file(
  const std::string& name)
{
  return StoreResult::Success;
}

}
