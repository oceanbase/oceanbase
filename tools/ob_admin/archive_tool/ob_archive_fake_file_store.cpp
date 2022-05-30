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

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <utime.h>
#include <dirent.h>

#include "ob_archive_fake_file_store.h"

namespace oceanbase
{
using namespace clog;
namespace archive
{
int ObArchiveFakeFileStore::init_for_dump(const char *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), K(path));
  } else {
    MEMCPY(path_, path, MAX_PATH_LENGTH);
    inited_ = true;
  }
  return ret;
}

int ObArchiveFakeFileStore::read_data_direct(const ObArchiveReadParam &param,
    ObReadBuf &rbuf,
    ObReadRes &res)
{
  int ret = OB_SUCCESS;
  int fd = 0;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveLogFileStore not init", K(ret), K(param));
  } else if (-1 == (fd = ::open(path_, O_RDONLY))) {
    ret = OB_IO_ERROR;
    ARCHIVE_LOG(WARN, "open fail", K(ret), K(fd));
  } else {
    const int64_t buf_size = param.read_len_;
    const int64_t offset = param.offset_;
    int64_t read_size = 0;
    int64_t one_read_size = 0;
    while (OB_SUCC(ret) && read_size < buf_size) {
      one_read_size = ::pread(fd, rbuf.buf_ + read_size, buf_size - read_size, offset + read_size);
      if (0 == one_read_size) {
        break;
      } else if (one_read_size < 0) {
        ret = OB_IO_ERROR;
        ARCHIVE_LOG(ERROR, "one read size small than zero", K(ret));
      } else {
        read_size += one_read_size;
      }
    }
    if (OB_SUCC(ret)) {
      ::close(fd);
      res.buf_ = rbuf.buf_;
      res.data_len_ = read_size;
    }
  }

  return ret;
}

}
}
