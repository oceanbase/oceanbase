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
#include "log_io_utils.h"
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
int close_with_ret(const int fd)
{
  int ret = OB_SUCCESS;
  if (-1 == fd) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(fd));
  } else if (-1 == (::close(fd))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "close block failed", K(ret), K(errno), K(fd));
  } else {
  }
  return ret;
}

int rename_with_retry(const char *src_name,
                      const char *dest_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_name) || OB_ISNULL(dest_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", KP(src_name), KP(dest_name));
  } else {
    do {
      if (-1 == ::rename(src_name, dest_name)) {
        ret  = convert_sys_errno();
        PALF_LOG(WARN, "rename file failed", KR(ret), K(src_name), K(dest_name));
        // for xfs, source file not exist and dest file exist after rename return ENOSPC, therefore, next rename will return
        // OB_NO_SUCH_FILE_OR_DIRECTORY.
        if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
          ret = OB_SUCCESS;
          PALF_LOG(WARN, "rename file failed, source file not exist, return OB_SUCCESS.", K(src_name), K(dest_name));
        } else {
          ob_usleep(RETRY_INTERVAL);
        }
      }
    } while(OB_ALLOCATE_DISK_SPACE_FAILED == ret);
  }
  return ret;
}
} // end namespace palf
} // end namespace oceanbase
