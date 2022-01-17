/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_ELECTION_TEST_ENV_H
#define _OB_ELECTION_TEST_ENV_H

#include <dirent.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "election/ob_election_async_log.h"
#include "election/ob_election_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::election;

namespace oceanbase {
namespace tests {
namespace election {
static const char* tmp_dir = "tmpdir";

class ElectionTestEnv {
public:
  ElectionTestEnv()
  {}

  ~ElectionTestEnv()
  {}

  int checkdir(const char* dir)
  {
    int ret = 0;
    struct stat statbuf;

    ret = stat(dir, &statbuf);
    if (0 == ret) {
      if (!S_ISDIR(statbuf.st_mode)) {
        ret = -1;
      }
    } else if (ENOENT == errno) {
      ret = mkdir(dir, S_IRWXU | S_IRGRP | S_IXGRP);
    }

    return ret;
  }

  int init(const char* envname)
  {
    int ret = 0;

    if (NULL == envname) {
      ret = -1;
      ASYNC_LOG(WARN, "argument error. ret=%d, envname=%p", ret, envname);
    } else {
      if (0 != (ret = checkdir(tmp_dir))) {
        ASYNC_LOG(WARN, "checkdir error. ret=%d, dir=%s", ret, tmp_dir);
      }
    }

    if (0 == ret) {
      if (0 != (ret = chdir(tmp_dir))) {
        ASYNC_LOG(WARN, "chdir error. ret=%d, dir=%s", ret, tmp_dir);
      }
    }

    if (0 == ret) {
      if (0 != (ret = checkdir(envname))) {
        ASYNC_LOG(WARN, "checkdir error. ret=%d, envname=%s", ret, envname);
      }
    }

    if (0 == ret) {
      if (0 != (ret = chdir(envname))) {
        ASYNC_LOG(WARN, "chdir error. ret=%d, envname=%s", ret, envname);
      }
    }

    if (0 == ret) {
      DIR* dirp = NULL;
      struct dirent* dp = NULL;

      dirp = opendir(".");
      while (NULL != (dp = readdir(dirp))) {
        if (0 == strcmp(dp->d_name, ".") || 0 == strcmp(dp->d_name, "..")) {
          continue;
        } else {
          unlink(dp->d_name);
        }
      }
      closedir(dirp);
    }

    return ret;
  }
};

}  // namespace election
}  // namespace tests
}  // namespace oceanbase

#endif
