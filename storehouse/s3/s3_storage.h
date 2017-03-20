#pragma once

#include "storehouse/storage_backend.h"
#include "storehouse/storage_config.h"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>

namespace storehouse {

struct S3Config : public StorageConfig {
  std::string bucket;
};

class S3Storage : public StorageBackend {
 public:
  S3Storage(S3Config config);
  ~S3Storage();

  StoreResult get_file_info(const std::string& name,
                            FileInfo& file_info) override;

  StoreResult make_random_read_file(const std::string& name,
                                    RandomReadFile*& file) override;

  StoreResult make_write_file(const std::string& name,
                              WriteFile*& file) override;

  StoreResult delete_file(const std::string& name) override;

 private:
  Aws::SDKOptions sdk_options_;
  Aws::S3::S3Client* client_;
  std::string bucket_;
};
}
