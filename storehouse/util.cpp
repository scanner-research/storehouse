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

#include "storehouse/util.h"

#include <errno.h>
#include <limits.h> /* PATH_MAX */
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h> /* mkdir(2) */
#include <unistd.h>   /* access(2) */

namespace storehouse {

// Stolen from
// https://gist.github.com/JonathonReinhart/8c0d90191c38af2dcadb102c4e202950
int mkdir_p(const char* path, mode_t mode) {
  /* Adapted from http://stackoverflow.com/a/2336245/119527 */
  const size_t len = strlen(path);
  char _path[PATH_MAX];
  char* p;

  errno = 0;

  /* Copy string so its mutable */
  if (len > sizeof(_path) - 1) {
    errno = ENAMETOOLONG;
    return -1;
  }
  strcpy(_path, path);

  /* Iterate the string */
  for (p = _path + 1; *p; p++) {
    if (*p == '/') {
      /* Temporarily truncate */
      *p = '\0';
      /* check if file exists before mkdir to avoid EACCES */
      if (access(_path, F_OK) != 0) {
        /* fail if error is anything but file does not exist */
        if (errno != ENOENT) {
          return -1;
        }
        if (mkdir(_path, mode) != 0) {
          if (errno != EEXIST) return -1;
        }
      }

      *p = '/';
    }
  }

  if (access(_path, F_OK) != 0) {
    /* fail if error is anything but file does not exist */
    if (errno != ENOENT) {
      return -1;
    }
    if (mkdir(_path, mode) != 0) {
      if (errno != EEXIST) return -1;
    }
  }

  return 0;
}

void temp_file(FILE** fp, std::string& name) {
  char n[] = "/tmp/lightscanXXXXXX";
  int fd = mkstemp(n);
  *fp = fdopen(fd, "wb+");
  name = std::string(n);
}
}
