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

#pragma once

#include <memory>
#include <string>

namespace storehouse {

class StorageConfig {
 public:
  virtual ~StorageConfig(){};

  // static StorageConfig* make_gcs_config(
  //   const std::string& certificates_path,
  //   const std::string& key,
  //   const std::string& bucket);

  static StorageConfig* make_posix_config();

  static StorageConfig* make_s3_config(
    const std::string& bucket, 
    const std::string& region, 
    const std::string& endpoint);

  static StorageConfig* make_gcs_config(const std::string& bucket);
};
}
