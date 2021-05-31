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

#include "ob_storage_file.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"

namespace oceanbase {
namespace common {

int get_file_path(const common::ObString& uri, char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t offset = strlen(OB_FILE_PREFIX);

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (!uri.prefix_match(OB_FILE_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid uri", K(ret), K(uri));
  } else if (OB_FAIL(
                 databuff_printf(buf, buf_size, "%.*s", static_cast<int>(uri.length() - offset), uri.ptr() + offset))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (strlen(buf) <= 0 && buf[0] != '/') {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid file path", K(ret), K(uri), K(buf));
  }
  return ret;
}

int lock_file(int fd)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  struct flock lock;

  lock.l_type = F_WRLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid fd", K(ret), K(fd));
  } else if (0 != fcntl(fd, F_SETLK, &lock)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(
        WARN, "failed to lock file", K(ret), K(fd), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int unlock_file(int fd)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  struct flock lock;

  lock.l_type = F_UNLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid fd", K(ret), K(fd));
  } else if (0 != fcntl(fd, F_SETLK, &lock)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(
        WARN, "failed to lock file", K(ret), K(fd), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

ObStorageFileUtil::ObStorageFileUtil()
{}

ObStorageFileUtil::~ObStorageFileUtil()
{}

int ObStorageFileUtil::is_exist(const common::ObString& uri, const common::ObString& storage_info, bool& exist)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);

  exist = false;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 == ::access(path, F_OK)) {
    exist = true;
  } else if (ENOENT != errno) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "failed to stat file",
        K(ret),
        K(path),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int ObStorageFileUtil::get_file_length(
    const common::ObString& uri, const common::ObString& storage_info, int64_t& file_length)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  struct stat64 file_info;
  UNUSED(storage_info);

  file_length = -1;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 != ::stat64(path, &file_info)) {
    if (ENOENT == errno) {
      ret = OB_BACKUP_FILE_NOT_EXIST;
      STORAGE_LOG(INFO, "file not exist", K(ret), K(path));
    } else {
      ret = OB_IO_ERROR;
      STORAGE_LOG(
          WARN, "file not exist", K(ret), K(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else if (S_ISDIR(file_info.st_mode)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "uri is a dir", K(ret), K(path));
  } else {
    file_length = file_info.st_size;
  }

  return ret;
}

int ObStorageFileUtil::del_file(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 != ::unlink(path)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "failed to del file",
        K(ret),
        K(path),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int ObStorageFileUtil::write_single_file(
    const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileWriter writer;

  if (OB_FAIL(writer.open(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
  } else {
    if (OB_FAIL(writer.write(buf, size))) {
      STORAGE_LOG(WARN, "failed to writer", K(ret), K(size), K(uri));
    }

    if (OB_SUCCESS != (tmp_ret = writer.close())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      STORAGE_LOG(WARN, "failed to close", K(ret), K(tmp_ret), K(uri));
    }
  }
  return ret;
}

int ObStorageFileUtil::mkdir(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else {
    // find the first not exist dir
    int64_t pos = 0;
    bool found_exist_dir = false;
    for (pos = strlen(path) - 1; OB_SUCC(ret) && pos >= 0 && !found_exist_dir; --pos) {
      if (path[pos] == '/') {
        path[pos] = '\0';
        if (0 == ::access(path, F_OK)) {
          STORAGE_LOG(INFO, "path exist", K(pos), K(path));
          found_exist_dir = true;
        } else if (ENOENT != errno) {
          ret = OB_IO_ERROR;
          STORAGE_LOG(WARN,
              "check is parent dir exist",
              K(ret),
              K(path),
              K(errno),
              "errno",
              strerror_r(errno, errno_buf, sizeof(errno_buf)));
        }
        path[pos] = '/';
      }
    }

    // create dirs
    int64_t end_pos = strlen(path) - 1;
    for (int64_t i = pos + 1; OB_SUCC(ret) && i <= end_pos; ++i) {
      if (path[i] == '/' || end_pos == i) {
        if (end_pos != i) {
          path[i] = '\0';
        }
        if (0 == ::access(path, F_OK)) {
        } else if (ENOENT == errno) {
          if (0 != ::mkdir(path, S_IRWXU)) {
            if (EEXIST == errno) {
              if (0 != ::access(path, F_OK)) {
                ret = OB_IO_ERROR;
                STORAGE_LOG(WARN,
                    "parent dir is not exist",
                    K(ret),
                    K(path),
                    K(errno),
                    "errno",
                    strerror_r(errno, errno_buf, sizeof(errno_buf)));
              }
            } else {
              ret = OB_IO_ERROR;
              STORAGE_LOG(WARN,
                  "failed to create parent dir",
                  K(ret),
                  K(path),
                  K(errno),
                  "errno",
                  strerror_r(errno, errno_buf, sizeof(errno_buf)));
            }
          } else {
            STORAGE_LOG(INFO, "succeed to create parent dir", K(path), K(uri));
          }
        } else {
          ret = OB_IO_ERROR;
          STORAGE_LOG(WARN,
              "check is parent dir exist",
              K(ret),
              K(path),
              K(errno),
              "errno",
              strerror_r(errno, errno_buf, sizeof(errno_buf)));
        }
        if (end_pos != i) {
          path[i] = '/';
        }
      }
    }
  }

  return ret;
}

int ObStorageFileUtil::update_file_modify_time(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else {
    // touch file timestamp to current
    if (0 != ::utime(path, NULL)) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN,
          "fail to change file time",
          K(ret),
          K(path),
          " return error: ",
          strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  }

  return ret;
}

int ObStorageFileUtil::list_files(const common::ObString& uri, const common::ObString& storage_info,
    common::ObIAllocator& allocator, common::ObIArray<common::ObString>& file_names)
{
  UNUSED(storage_info);
  int ret = OB_SUCCESS;
  file_names.reset();
  struct dirent entry;
  struct dirent* result;
  DIR* open_dir = NULL;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  ObString tmp_string;
  ObString file_name;
  char dir_path[OB_MAX_URI_LENGTH];

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "dir path is invalid", K(ret), K(dir_path));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      OB_LOG(WARN, "fail to open dir", K(ret), K(dir_path), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  }

  while (OB_SUCC(ret) && NULL != open_dir) {
    tmp_string.reset();
    file_name.reset();
    if (0 != ::readdir_r(open_dir, &entry, &result)) {
      ret = OB_IO_ERROR;
      OB_LOG(WARN, "read dir error", K(ret), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    } else if (NULL != result && (DT_REG == entry.d_type || DT_UNKNOWN == entry.d_type) &&
               0 != strcmp(entry.d_name, ".") && 0 != strcmp(entry.d_name, "..")) {
      // save regular file and unknown file
      tmp_string.assign(entry.d_name, static_cast<int32_t>(strlen(entry.d_name)));
      if (OB_FAIL(ob_write_string(allocator, tmp_string, file_name))) {
        OB_LOG(WARN, "fail to save file name", K(ret), K(tmp_string));
      } else if (OB_FAIL(file_names.push_back(file_name))) {
        OB_LOG(WARN, "fail to push backup file name", K(ret), K(file_name));
      }
    } else if (NULL == result) {
      break;  // end file
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }

  OB_LOG(INFO, "list files count", K(dir_path), K(file_names.count()), K(ret));
  return ret;
}

int ObStorageFileUtil::del_dir(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char dir_path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);
  struct stat st;
  DIR* open_dir = NULL;
  struct dirent entry;
  struct dirent* result;
  int64_t dir_file_count = 0;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(::lstat(dir_path, &st))) {
    if (ENOENT == errno) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "dir do not exist", K(dir_path), "errono_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN,
          "failed to ls stat dir",
          K(ret),
          K(dir_path),
          "errono_str",
          strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else if (!S_ISDIR(st.st_mode)) {
    ret = OB_DIR_NOT_EXIST;
    STORAGE_LOG(WARN, "path is not to dir", K(ret), K(dir_path));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      OB_LOG(WARN, "fail to open dir", K(ret), K(dir_path), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir && 0 == dir_file_count) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = OB_IO_ERROR;
        OB_LOG(WARN, "read dir error", K(ret), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
      } else if (NULL != result && (DT_REG == entry.d_type || DT_UNKNOWN == entry.d_type || (DT_DIR == entry.d_type)) &&
                 0 != strcmp(entry.d_name, ".") && 0 != strcmp(entry.d_name, "..")) {
        ++dir_file_count;
      } else if (NULL == result) {
        break;  // end file
      }
    }
    if (OB_SUCC(ret) && 0 == dir_file_count) {
      if (0 != ::rmdir(dir_path)) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(
            WARN, "failed to del file", K(ret), K(dir_path), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  return ret;
}

int ObStorageFileUtil::get_pkeys_from_dir(
    const common::ObString& uri, const common::ObString& storage_info, common::ObIArray<ObPartitionKey>& pkeys)
{
  UNUSED(storage_info);
  int ret = OB_SUCCESS;
  pkeys.reset();
  struct dirent entry;
  struct dirent* result;
  DIR* open_dir = NULL;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  char dir_path[OB_MAX_URI_LENGTH];
  const int64_t start_ts = ObTimeUtil::current_time();

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "get_pkeys_from_dir invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      OB_LOG(WARN, "fail to open dir", K(ret), K(dir_path), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  }

  ObPartitionKey pkey;
  // tmp file name format tableid_partitionid.tmp.timestamp
  const char* tmp_file_mark = ".tmp.";
  while (OB_SUCC(ret)) {
    pkey.reset();
    if (0 != ::readdir_r(open_dir, &entry, &result)) {
      ret = OB_IO_ERROR;
      OB_LOG(WARN, "read dir error", K(ret), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    } else if (NULL == result) {
      break;
    } else if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
      continue;
    } else if (NULL != strstr(entry.d_name, tmp_file_mark)) {
      // skip tmp file
      continue;
    } else if ((DT_REG == entry.d_type || DT_UNKNOWN == entry.d_type)) {
      // the format of each pkey_name is tableid_partitionid.
      // For example, 1102810162659331_0, 1102810162659331 is table id, 0 is partition id.
      const char* pkey_name = entry.d_name;
      ObString::obstr_size_t object_end = 0;
      char* endptr = NULL;
      uint64_t table_id;
      uint64_t partition_id;

      if (OB_FAIL(ob_strtoull(pkey_name, endptr, table_id))) {
        OB_LOG(WARN, "failed to stroull", K(ret), K(pkey_name));
      } else if (OB_ISNULL(endptr)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull table id enptr unexpected", K(ret), K(pkey_name), KP(endptr));
      } else if (*endptr != '_') {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull table id endptr unexpected", K(ret), K(pkey_name), K(*endptr));
      } else if (OB_INVALID_ID == table_id) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull table id over flow", K(ret), K(pkey_name));
      } else if (FALSE_IT(object_end = endptr - pkey_name + 1)) {
      } else if (object_end >= strlen(pkey_name)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "object end length unexpected", K(ret), K(object_end), K(strlen(pkey_name)));
      } else if (OB_FAIL(ob_strtoull(pkey_name + object_end, endptr, partition_id))) {
        OB_LOG(WARN, "failed to stroull", K(ret), K(pkey_name));
      } else if (OB_ISNULL(endptr)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull partition id endptr unexpected", K(ret), K(pkey_name), KP(endptr));
      } else if (*endptr != 0) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull partition id endptr unexpected", K(ret), K(pkey_name), K(*endptr));
      } else if (OB_INVALID_ID == partition_id) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "stroull partition id over flow", K(ret), K(pkey_name));
      } else if (OB_FAIL(pkey.init(table_id, partition_id, 0))) {
        OB_LOG(WARN, "failed to init pkey", K(ret), K(table_id), K(partition_id));
      } else if (OB_FAIL(pkeys.push_back(pkey))) {
        OB_LOG(WARN, "failed to push back pkey", K(ret), K(pkey));
      }
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }

  const int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
  OB_LOG(INFO, "list files", K(ret), K(dir_path), "count", pkeys.count(), "cost_ts", cost_ts);
  return ret;
}

int ObStorageFileUtil::get_partition_ids_from_dir(const char* dir_path, common::ObIArray<int64_t>& partition_ids)
{
  int ret = OB_SUCCESS;
  partition_ids.reset();
  struct dirent entry;
  struct dirent* result;
  DIR* open_dir = NULL;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  const int64_t start_ts = ObTimeUtil::current_time();

  if (OB_ISNULL(dir_path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "dir path is invalid", K(ret), KP(dir_path));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      OB_LOG(WARN, "fail to open dir", K(ret), K(dir_path), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  }

  while (OB_SUCC(ret) && NULL != open_dir) {
    if (0 != ::readdir_r(open_dir, &entry, &result)) {
      ret = OB_IO_ERROR;
      OB_LOG(WARN, "read dir error", K(ret), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    } else if (NULL != result && (DT_DIR == entry.d_type || DT_UNKNOWN == entry.d_type) &&
               0 != strcmp(entry.d_name, ".") && 0 != strcmp(entry.d_name, "..")) {
      // find directory and unknown file
      const int64_t partition_id = atoll(entry.d_name);
      struct stat64 file_info;
      char child_dir_path[OB_MAX_URI_LENGTH];
      if (OB_FAIL(databuff_printf(child_dir_path, sizeof(child_dir_path), "%s/%ld", dir_path, partition_id))) {
        STORAGE_LOG(WARN, "failed to fill path", K(ret), K(dir_path));
      } else if (0 != ::stat64(child_dir_path, &file_info)) {
        if (ENOENT == errno) {
          ret = OB_BACKUP_FILE_NOT_EXIST;
          STORAGE_LOG(INFO, "file not exist", K(ret), K(child_dir_path));
        } else {
          ret = OB_IO_ERROR;
          STORAGE_LOG(WARN,
              "can not get file state",
              K(ret),
              K(child_dir_path),
              K(errno),
              "errno",
              strerror_r(errno, errno_buf, sizeof(errno_buf)));
        }
      } else if (!S_ISDIR(file_info.st_mode)) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "uri is a not dir", K(ret), K(child_dir_path));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(partition_ids.push_back(partition_id))) {
        STORAGE_LOG(WARN, "failed to push partition id into array", K(ret), K(partition_id));
      }

      if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {  // 1s
        OB_LOG(INFO, "list files", K(ret), K(dir_path), "count", partition_ids.count());
      }

    } else if (NULL == result) {
      break;  // end file
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  const int64_t cost_ts = ObTimeUtil::current_time() - start_ts;
  OB_LOG(INFO, "list files", K(ret), K(dir_path), "count", partition_ids.count(), "cost_ts", cost_ts);
  return ret;
}

int ObStorageFileUtil::delete_tmp_files(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char dir_path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  UNUSED(storage_info);
  struct stat st;
  DIR* open_dir = NULL;
  struct dirent entry;
  struct dirent* result;
  const int64_t now_ts = ObTimeUtil::current_time();
  int64_t MAX_OBSOLETE_INTERVAL = 60 * 60L * 1000 * 1000;  // 1h
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_OBSOLETE_INTERVAL) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      MAX_OBSOLETE_INTERVAL = 1L * 1000 * 1000;  // 1s
      ret = OB_SUCCESS;
    }
  }
#endif

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(::lstat(dir_path, &st))) {
    if (ENOENT == errno) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "dir do not exist", K(dir_path), "errono_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN,
          "failed to ls stat dir",
          K(ret),
          K(dir_path),
          "errono_str",
          strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else if (!S_ISDIR(st.st_mode)) {
    ret = OB_DIR_NOT_EXIST;
    STORAGE_LOG(WARN, "path is not to dir", K(ret), K(dir_path));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      OB_LOG(WARN, "fail to open dir", K(ret), K(dir_path), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = OB_IO_ERROR;
        OB_LOG(WARN, "read dir error", K(ret), K(strerror_r(errno, errno_buf, sizeof(errno_buf))));
      } else if (NULL != result && (DT_REG == entry.d_type || DT_UNKNOWN == entry.d_type) &&
                 0 != strcmp(entry.d_name, ".") && 0 != strcmp(entry.d_name, "..")) {
        int64_t tmp_file_timestamp = 0;
        bool is_tmp_file = false;

        if (OB_FAIL(get_tmp_file_format_timestamp(entry.d_name, is_tmp_file, tmp_file_timestamp))) {
          STORAGE_LOG(WARN, "failed to get tmp file format timestamp", K(ret), K(entry.d_name));
        } else if (!is_tmp_file) {
          // do nothing
        } else if (now_ts - tmp_file_timestamp < MAX_OBSOLETE_INTERVAL) {
          if (REACH_TIME_INTERVAL(100 * 1000) /*100ms*/) {
            STORAGE_LOG(INFO, "tmp file can not delete", K(now_ts), K(tmp_file_timestamp), K(MAX_OBSOLETE_INTERVAL));
          }
        } else {
          char tmp_file_path[OB_MAX_URI_LENGTH] = "";
          if (OB_FAIL(databuff_printf(tmp_file_path, OB_MAX_URI_LENGTH, "%s/%s", dir_path, entry.d_name))) {
            STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
          } else if (0 != ::unlink(tmp_file_path)) {
            ret = OB_IO_ERROR;
            STORAGE_LOG(WARN,
                "failed to del file",
                K(ret),
                K(tmp_file_path),
                K(errno),
                "errno",
                strerror_r(errno, errno_buf, sizeof(errno_buf)));
          }
        }
      } else if (NULL == result) {
        break;  // end file
      }
    }
  }
  // close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  return ret;
}

int ObStorageFileUtil::get_tmp_file_format_timestamp(const char* file_name, bool& is_tmp_file, int64_t& timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;
  const char* tmp_file_format = ".tmp.";
  is_tmp_file = true;

  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "check need delete tmp file get invalid argument", K(ret), KP(file_name));
  } else {
    const int64_t file_name_length = strlen(file_name);
    const int64_t tmp_file_format_length = strlen(tmp_file_format);
    int64_t timestamp_position = -1;
    bool found = true;

    for (int64_t i = file_name_length - 1; i >= 0; --i) {
      found = true;
      for (int64_t j = 0; j < tmp_file_format_length && found; ++j) {
        if (i - j < 0) {
          found = false;
        } else if (file_name[i - j] != tmp_file_format[tmp_file_format_length - j - 1]) {
          found = false;
        }
      }

      if (found) {
        timestamp_position = i + 1;
        break;
      }
    }

    if (!found) {
      is_tmp_file = false;
    } else if (timestamp_position >= file_name_length) {
      // found
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tmp file formate is unexpected", K(ret), K(file_name));
    } else if (OB_FAIL(ob_atoll(file_name + timestamp_position, timestamp))) {
      STORAGE_LOG(WARN, "failed to get tmp file timestamp", K(ret), K(file_name));
    }
  }
  return ret;
}

ObStorageFileReader::ObStorageFileReader() : fd_(-1), is_opened_(false), file_length_(-1)
{
  path_[0] = '\0';
}

ObStorageFileReader::~ObStorageFileReader()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG(ERROR, "reader is not closed", K(path_), K(fd_));
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close reader", K(tmp_ret), K(path_), K(fd_));
    }
  }
}

int ObStorageFileReader::open(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  struct stat64 file_info;
  UNUSED(storage_info);

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path_, sizeof(path_)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (-1 == (fd_ = ::open(path_, O_RDONLY))) {
    if (ENOENT == errno) {
      ret = OB_BACKUP_FILE_NOT_EXIST;
    } else {
      ret = OB_IO_ERROR;
    }
    STORAGE_LOG(WARN,
        "failed to open read file",
        K(ret),
        K(path_),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    is_opened_ = true;
    if (0 != ::fstat64(fd_, &file_info)) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN,
          "file not exist",
          K(ret),
          K(fd_),
          K(path_),
          K(errno),
          "errno",
          strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else if (S_ISDIR(file_info.st_mode)) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN, "uri is a dir", K(ret), K(path_));
    } else {
      file_length_ = file_info.st_size;
    }

    if (OB_FAIL(ret)) {
      if (OB_SUCCESS != (tmp_ret = close())) {
        STORAGE_LOG(WARN, "failed to close", K(ret), K(tmp_ret), K(path_));
      }
    }
  }
  return ret;
}

int ObStorageFileReader::pread(char* buf, const int64_t buf_size, int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  read_size = 0;

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_size));
  } else {
    int64_t one_read_size = 0;
    while (OB_SUCC(ret) && read_size < buf_size) {
      one_read_size = ::pread(fd_, buf + read_size, buf_size - read_size, offset + read_size);
      if (0 == one_read_size) {
        break;
      } else if (one_read_size < 0) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN,
            "failed to read file",
            K(ret),
            K(read_size),
            K(buf_size),
            K(path_),
            K(errno),
            "errno",
            strerror_r(errno, errno_buf, sizeof(errno_buf)));
      } else {
        read_size += one_read_size;
      }
    }
  }

  return ret;
}

int ObStorageFileReader::close()
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (0 != ::close(fd_)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "failed to close read file",
        K(ret),
        K(path_),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }
  is_opened_ = false;
  fd_ = -1;
  return ret;
}

ObStorageFileBaseWriter::ObStorageFileBaseWriter() : fd_(-1), is_opened_(false), file_length_(-1), has_error_(false)
{
  path_[0] = '\0';
}
ObStorageFileBaseWriter::~ObStorageFileBaseWriter()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG(ERROR, "writer is not closed", K(path_), K(fd_));
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close writer", K(tmp_ret), K(path_), K(fd_));
    }
  }
}

int ObStorageFileBaseWriter::open(const int flags)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  ObStorageFileUtil util;

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret));
  } else if (strlen(path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid path", K(ret), K(path_));
  } else if (-1 == (fd_ = ::open(path_, flags, S_IRUSR | S_IWUSR))) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "failed to open write file",
        K(ret),
        K(path_),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    file_length_ = 0;
    is_opened_ = true;
    STORAGE_LOG(DEBUG, "succceed to open file for write", K(path_), K(fd_), K(flags));
  }

  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObStorageFileBaseWriter::write(const char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];
  int64_t write_size = 0;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || buf_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_size));
  } else {
    int64_t one_write_size = 0;
    while (OB_SUCC(ret) && write_size < buf_size) {
      one_write_size = ::write(fd_, buf + write_size, buf_size - write_size);
      if (one_write_size < 0) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN,
            "failed to write file",
            K(ret),
            K(one_write_size),
            K(errno),
            K(path_),
            "errno_str",
            strerror_r(errno, errno_buf, sizeof(errno_buf)));
      } else {
        write_size += one_write_size;
      }
    }

    if (OB_SUCC(ret)) {
      if (write_size != buf_size) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(ERROR, "write size not match buf size", K(ret), K(path_));
      } else {
        file_length_ += write_size;
      }
    }
  }

  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObStorageFileBaseWriter::close()
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (0 != ::close(fd_)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN,
        "failed to close write file",
        K(ret),
        K(fd_),
        K(path_),
        K(errno),
        "errno",
        strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    STORAGE_LOG(DEBUG, "succeed to close write file", K(path_), K(fd_));
  }
  is_opened_ = false;
  fd_ = -1;
  return ret;
}

ObStorageFileWriter::ObStorageFileWriter()
{
  real_path_[0] = '\0';
}

ObStorageFileWriter::~ObStorageFileWriter()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG(ERROR, "wrtier is not closed", K(path_), K(fd_));
    has_error_ = true;
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close wrtier", K(tmp_ret), K(path_), K(fd_));
    }
  }
}

int ObStorageFileWriter::open(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const char* TMP_NAME_FORMAT = "%s.tmp.%ld";
  ObStorageFileUtil util;
  const int64_t cur_ts = ObTimeUtil::current_time();
  int flags = O_CREAT | O_WRONLY | O_TRUNC | O_EXCL;
  UNUSED(storage_info);

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, real_path_, sizeof(real_path_)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(path_, sizeof(path_), TMP_NAME_FORMAT, real_path_, cur_ts))) {
    STORAGE_LOG(WARN, "failed to fill tmp_path", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageFileBaseWriter::open(flags))) {
    STORAGE_LOG(WARN, "failed to do_open", K(ret), K(uri), K(flags));
  }

  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObStorageFileWriter::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN];

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (OB_FAIL(ObStorageFileBaseWriter::close())) {
    STORAGE_LOG(WARN, "failed to do close", K(ret), K(path_), K(real_path_));
  }

  if (OB_SUCC(ret)) {
#ifdef ERRSIM
    ret = E(EventTable::EN_FILE_SYSTEM_RENAME_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      has_error_ = true;
    }
#endif
    if (has_error_) {
      STORAGE_LOG(WARN, "writer has error, skip rename file", K(path_), K(real_path_));
    } else if (0 != ::rename(path_, real_path_)) {
      ret = OB_IO_ERROR;
      STORAGE_LOG(WARN,
          "failed to rename meta file",
          K(ret),
          K(real_path_),
          K(path_),
          K(errno),
          "errno",
          strerror_r(errno, errno_buf, sizeof(errno_buf)));
      // rename failed ,try delete file
      if (0 != ::remove(path_)) {
        tmp_ret = OB_IO_ERROR;
        STORAGE_LOG(WARN,
            "failed to remove file",
            K(tmp_ret),
            K(path_),
            K(errno),
            "errno",
            strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    } else {
      STORAGE_LOG(INFO, "succeed to rename file after close", K(path_), K(real_path_));
    }
  }
  return ret;
}

ObStorageFileAppender::ObStorageFileAppender() : need_unlock_(false), open_mode_(StorageOpenMode::CREATE_OPEN_LOCK)
{}

ObStorageFileAppender::ObStorageFileAppender(StorageOpenMode mode) : need_unlock_(false), open_mode_(mode)
{}

ObStorageFileAppender::~ObStorageFileAppender()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG(ERROR, "appender is not closed");
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close appender", K(tmp_ret), K(fd_));
    }
  }
}

int ObStorageFileAppender::open(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileUtil util;
  int flags = 0;
  bool need_lock = false;
  UNUSED(storage_info);

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(get_open_flag_and_mode_(flags, need_lock))) {
    STORAGE_LOG(WARN, "failed to get open flag and mode", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path_, sizeof(path_)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageFileBaseWriter::open(flags))) {
    STORAGE_LOG(WARN, "failed to do_open", K(ret), K(uri), K(flags));
  } else if (!need_lock) {
    // do nothing
  } else if (OB_FAIL(lock_file(fd_))) {
    STORAGE_LOG(WARN, "failed to lock file", K(ret), K(path_), K(flags));
  } else {
    need_unlock_ = true;
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close appender", K(tmp_ret), K(uri));
    }
  }
  return ret;
}

int ObStorageFileAppender::close()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (need_unlock_ && OB_FAIL(unlock_file(fd_))) {
    STORAGE_LOG(WARN, "failed to unlock file", K(ret), K(path_));
  }
  need_unlock_ = false;

  if (OB_SUCCESS != (tmp_ret = ObStorageFileBaseWriter::close())) {
    STORAGE_LOG(WARN, "failed to close appender", K(ret), K(tmp_ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  return ret;
}

// At present, ObStorageFileAppender uses two working modes to achieve mutual exclusion of dual writes:
// 1. Add file lock, used in NFS V4 and other protocols that support file lock, open mode is opened for creation
// 2. Only the creator is allowed to open, and the mutually exclusive mode is guaranteed. When the file needs to be
// created, use the mutually exclusive creation method to open
//    When the creator closes the file and wants to append data, use the non-creation method to open it. therefore,
//    The second way of mutual exclusion requires the caller to guarantee
int ObStorageFileAppender::get_open_flag_and_mode_(int& flag, bool& need_lock)
{
  int ret = OB_SUCCESS;
  need_lock = false;
  switch (open_mode_) {
    case StorageOpenMode::CREATE_OPEN_LOCK:
      flag = O_CREAT | O_RDWR | O_APPEND;
      need_lock = true;
      break;
    case StorageOpenMode::EXCLUSIVE_CREATE:
      flag = O_CREAT | O_RDWR | O_APPEND | O_EXCL;
      break;
    case StorageOpenMode::ONLY_OPEN_UNLOCK:
      flag = O_RDWR | O_APPEND;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected open mode", K(open_mode_));
  }
  return ret;
}

ObStorageFileMetaWrapper::ObStorageFileMetaWrapper()
{}
ObStorageFileMetaWrapper::~ObStorageFileMetaWrapper()
{}

int ObStorageFileMetaWrapper::get(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileReader reader;
  UNUSED(storage_info);
  read_size = 0;

  if (uri.empty() || OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri), KP(buf), K(buf_size));
  } else if (OB_FAIL(reader.open(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to open reader", K(ret));
  } else {
    if (reader.get_length() > buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(WARN, "buf not enough", K(ret), K(buf_size), "meta size", reader.get_length());
    } else if (OB_FAIL(reader.pread(buf, buf_size, 0 /*offset*/, read_size))) {
      STORAGE_LOG(WARN, "failed to read meta", K(ret), K(uri));
    }
    if (OB_SUCCESS != (tmp_ret = reader.close())) {
      STORAGE_LOG(WARN, "failed to close meta reader", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}
int ObStorageFileMetaWrapper::set(
    const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileWriter writer;

  if (uri.empty() || OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri), KP(buf), K(buf_size));
  } else if (OB_FAIL(writer.open(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
  } else {
    if (OB_FAIL(writer.write(buf, buf_size))) {
      STORAGE_LOG(WARN, "failed to write meta", K(ret));
    }

    if (OB_SUCCESS != (tmp_ret = writer.close())) {
      ret = OB_SUCC(ret) ? tmp_ret : ret;
      STORAGE_LOG(WARN, "failed to close meta writer", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase
