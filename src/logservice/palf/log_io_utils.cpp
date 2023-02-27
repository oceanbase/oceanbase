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
#include "log_io_uitls.h"
#include "log_define.h"
namespace oceanbase
{
namespace palf
{

const int64_t RETRY_INTERVAL = 10*1000;
int openat_with_retry(const int dir_fd,
                      const char *block_path,
                      const int flag,
                      const int mode,
                      int &fd)
{
  int ret = OB_SUCCESS;
  if (-1 == dir_fd || NULL == block_path || -1 == flag || -1 == mode) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(dir_fd), KP(block_path), K(flag), K(mode));
  } else {
    do {
      if (-1 == (fd = ::openat(dir_fd, block_path, flag, mode))) {
        ret = convert_sys_errno();
        PALF_LOG(ERROR, "open block failed", K(ret), K(errno), K(block_path), K(dir_fd));
        ob_usleep(RETRY_INTERVAL);
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } while (OB_FAIL(ret));
  }
  return ret;
}
int close_with_retry(const int fd)
{
  int ret = OB_SUCCESS;
  if (-1 == fd) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(fd));
  } else {
    do {
      if (-1 == (::close(fd))) {
        ret = convert_sys_errno();
        PALF_LOG(ERROR, "open block failed", K(ret), K(errno), K(fd));
        ob_usleep(RETRY_INTERVAL);
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } while (OB_FAIL(ret));
  }
  return ret;
}
} // end namespace palf
} // end namespace oceanbase