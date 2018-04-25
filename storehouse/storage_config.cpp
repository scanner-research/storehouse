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

#include "storehouse/storage_config.h"
// #include "storehouse/gcs/gcs_storage.h"
#include "storehouse/posix/posix_storage.h"
#include "storehouse/s3/s3_storage.h"

namespace storehouse {

// StorageConfig *StorageConfig::make_gcs_config(
//   const std::string &certificates_path,
//   const std::string &key,
//   const std::string &bucket)
// {
//   GCSConfig *config = new GCSConfig;
//   config->certificates_path = certificates_path;
//   config->key = key;
//   config->bucket = bucket;
//   return config;
// }

StorageConfig* StorageConfig::make_posix_config() { return new PosixConfig; }

StorageConfig* StorageConfig::make_s3_config(const std::string& bucket,
    const std::string& region, const std::string& endpoint) {
  S3Config* config = new S3Config;
  config->bucket = bucket;
  config->endpointOverride = endpoint;
  config->endpointRegion = region;
  return config;
}

StorageConfig* StorageConfig::make_gcs_config(const std::string& bucket) {
		S3Config* config = new S3Config;
  	config->bucket = bucket;
  	config->endpointOverride = "storage.googleapis.com";
  	config->endpointRegion = "US";

  	return config;
}

StorageConfig* StorageConfig::make_config(const std::string& type, const std::map<std::string, std::string>& args) {
  auto check_key = [&](std::string key) {
    if (args.count(key) == 0) {
      LOG(WARNING) << "StorageConfig " << type << " is missing required argument " << key;
      return false;
    }
    return true;
  };

  StorageConfig* sc_config = nullptr;
  if (type == "posix") {
    sc_config = StorageConfig::make_posix_config();
  } else if (type == "gcs") {
    if (!check_key("bucket")) {
      return sc_config;
    }
    sc_config = StorageConfig::make_gcs_config(args.at("bucket"));
  } else if (type == "s3") {
    if (!check_key("bucket") || !check_key("region") || !check_key("endpoint")) {
      return sc_config;
    }
    sc_config = StorageConfig::make_s3_config(args.at("bucket"), args.at("region"),
                                              args.at("endpoint"));
  } else {
    LOG(WARNING) << "Not a valid storage config type";
  }
  return sc_config;
}

}
