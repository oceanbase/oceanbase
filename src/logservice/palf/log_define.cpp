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

#include "log_define.h"
#include "lib/list/ob_dlist.h"
#include "share/ob_errno.h"
#include "linux/falloc.h" // FALLOC_FL_ZERO_RANGE for linux kernel 4.9
#include "log_block_pool_interface.h"

namespace oceanbase
{
namespace palf
{

int scan_dir(const char *dir_name, ObBaseDirFunctor &functor)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = NULL;
  struct dirent *result = NULL;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      PALF_LOG(WARN, "Fail to open dir, ", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      PALF_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else {
    while ((NULL != (result = ::readdir(open_dir))) && OB_SUCC(ret)) {
      if (0 != STRCMP(result->d_name, ".") && 0 != STRCMP(result->d_name, "..")
          && OB_FAIL((functor.func)(result))) {
        PALF_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
      }
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  return ret;
}

int convert_sys_errno()
{
  int ret = OB_IO_ERROR;
  switch (errno) {
  case EACCES:
    ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
    break;
  case ENOENT:
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    break;
  case EEXIST:
  case ENOTEMPTY:
    ret = OB_FILE_OR_DIRECTORY_EXIST;
    break;
  case EAGAIN:
    ret = OB_EAGAIN;
    break;
  case EDQUOT:
  case ENOSPC:
    ret = OB_ALLOCATE_DISK_SPACE_FAILED;
    break;
  default:
    ret = OB_IO_ERROR;
  }
  return ret;
}

int block_id_to_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu", block_id);
  }
  return ret;
}

int block_id_to_tmp_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.tmp", block_id);
  }
  return ret;
}

int block_id_to_flashback_string(const block_id_t block_id, char *str, const int64_t str_len)
{
  int ret = OB_SUCCESS;
  if (false == is_valid_block_id(block_id) || OB_ISNULL(str) || 0 >= str_len) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    ret = databuff_printf(str, str_len, pos, "%lu.flashback", block_id);
  }
  return ret;
}

bool is_number(const char* str)
{
	bool bool_ret = true;
  for (int64_t i = 0; true == bool_ret; ++i) {
    if ('\0' == str[i]) {
      break;
    } else if (!isdigit(str[i])) {
      bool_ret = false;
		}
  }
	return bool_ret;
}

int GetBlockCountFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else {
    const char *entry_name = entry->d_name;
		// NB: if there is '0123' or 'xxx.flashback' in log directory,
		// restart will be failed, the solution is that read block.
    if (false == is_number(entry_name) && false == is_flashback_block(entry_name)) {
			ret = OB_ERR_UNEXPECTED;
			PALF_LOG(WARN, "this is block is not used for palf!!!", K(ret), K(entry_name));
      // do nothing, skip invalid block like tmp
    } else {
			count_ ++;
    }
  }
  return ret;
}

int TrimLogDirectoryFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid args", K(ret), KP(entry));
  } else {
    const char *entry_name = entry->d_name;
		bool str_is_number = is_number(entry_name);
		bool str_is_flashback_block = is_flashback_block(entry_name);
    if (false == str_is_number && false == str_is_flashback_block) {
			ret = OB_ERR_UNEXPECTED;
			PALF_LOG(WARN, "this is block is not used for palf!!!", K(ret), K(entry_name));
      // do nothing, skip invalid block like tmp
    } else {
			if (true == str_is_flashback_block
					&& OB_FAIL(rename_flashback_to_normal_(entry_name))) {
				PALF_LOG(ERROR, "rename_flashback_to_normal failed", K(ret), K(dir_), K(entry_name));
			}
			if (OB_SUCC(ret)) {
        uint32_t block_id = static_cast<uint32_t>(strtol(entry->d_name, nullptr, 10));
        if (LOG_INVALID_BLOCK_ID == min_block_id_ || block_id < min_block_id_) {
          min_block_id_ = block_id;
        }
        if (LOG_INVALID_BLOCK_ID == max_block_id_ || block_id > max_block_id_) {
          max_block_id_ = block_id;
        }
			}
		}
  }
  return ret;
}

int TrimLogDirectoryFunctor::rename_flashback_to_normal_(const char *file_name)
{
  int ret = OB_SUCCESS;
	int dir_fd = -1;
	char normal_file_name[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
	MEMCPY(normal_file_name, file_name, strlen(file_name) - strlen(FLASHBACK_SUFFIX));
	const int64_t SLEEP_TS_US = 10 * 1000;
	if (-1 == (dir_fd = ::open(dir_, O_DIRECTORY | O_RDONLY))) {
		ret = convert_sys_errno();
	} else if (OB_FAIL(try_to_remove_block_(dir_fd, normal_file_name))) {
    PALF_LOG(ERROR, "try_to_remove_block_ failed", K(file_name), K(normal_file_name));
  } else {
    do {
      if (-1 == ::renameat(dir_fd, file_name, dir_fd, normal_file_name)) {
        ret = convert_sys_errno();
      } else if (-1 == ::fsync(dir_fd)) {
        ret = convert_sys_errno();
      } else {
        ret = OB_SUCCESS;
        break;
      }
      usleep(SLEEP_TS_US);
    } while (OB_FAIL(ret));
	}
	if (-1 != dir_fd) {
		::close(dir_fd);
	}

  return ret;
}

int TrimLogDirectoryFunctor::try_to_remove_block_(const int dir_fd, const char *file_name)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  if (-1 == (fd = ::openat(dir_fd, file_name, LOG_READ_FLAG))) {
    ret = convert_sys_errno();
  }
  // if file not exist, return OB_SUCCESS;
  if (OB_FAIL(ret)) {
    if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      PALF_LOG(INFO, "before rename flashback to normal and after delete normal file, restart!!!", K(file_name));
    } else {
      PALF_LOG(ERROR, "open file failed", K(file_name));
    }
  } else if (OB_FAIL(log_block_pool_->remove_block_at(dir_fd, file_name))) {
    PALF_LOG(ERROR, "remove_block_at failed", K(dir_fd), K(file_name));
  }
  if (-1 != fd && -1 == ::close(fd)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "close fd failed", K(file_name));
  }
  return ret;
}

int reuse_block_at(const int dir_fd, const char *block_path)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  if (-1 == (fd = ::openat(dir_fd, block_path, LOG_WRITE_FLAG))) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::openat failed", K(ret), K(block_path));
  } else if (-1 == ::fallocate(fd, FALLOC_FL_ZERO_RANGE, 0, PALF_PHY_BLOCK_SIZE)) {
    ret = convert_sys_errno();
    PALF_LOG(ERROR, "::fallocate failed", K(ret), K(block_path));
  } else {
    PALF_LOG(INFO, "reuse_block_at success", K(ret), K(block_path));
  }

  if (-1 != fd) {
    ::close(fd);
  }
  return ret;
}
} // namespace palf
} // namespace oceanbase
