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

namespace oceanbase
{
namespace common
{

static void convert_io_error(const int sys_err, int &ob_error_code)
{
  if (ENOENT == sys_err) {
    ob_error_code = OB_BACKUP_FILE_NOT_EXIST;
  } else if (EPERM == sys_err || EACCES == sys_err) {
    ob_error_code = OB_BACKUP_PERMISSION_DENIED;
  } else if (ENOSPC == sys_err || EDQUOT == sys_err) {
    ob_error_code = OB_BACKUP_DEVICE_OUT_OF_SPACE;
  } else {
    ob_error_code = OB_IO_ERROR;
  }
}

int get_file_path(const common::ObString &uri, char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t offset = strlen(OB_FILE_PREFIX);

  if (uri.empty() || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri), KP(buf));
  } else if (!uri.prefix_match(OB_FILE_PREFIX)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid uri", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(buf, buf_size, "%.*s",
             static_cast<int>(uri.length() - offset), uri.ptr() + offset))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (strlen(buf) <= 0 && buf[0] != '/') {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid file path", K(ret), K(uri), KCSTRING(buf));
  }
  return ret;
}

int lock_file(int fd)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct flock lock;

  lock.l_type = F_WRLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid fd", K(ret), K(fd));
  } else if (0 != fcntl(fd, F_SETLK, &lock)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to lock file",
        K(ret), K(fd), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int unlock_file(int fd)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct flock lock;

  lock.l_type = F_UNLCK;
  lock.l_start = 0;
  lock.l_whence = SEEK_SET;
  lock.l_len = 0;

  if (fd < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid fd", K(ret), K(fd));
  } else if (0 != fcntl(fd, F_SETLK, &lock)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to lock file",
        K(ret), K(fd), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;

}

ObStorageFileUtil::ObStorageFileUtil()
{
}

ObStorageFileUtil::~ObStorageFileUtil()
{
}

int ObStorageFileUtil::is_exist(const common::ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  exist = false;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 == ::access(path, F_OK)) {
    exist = true;
  } else if (ENOENT != errno && ENOTDIR != errno) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to stat file",
        K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int ObStorageFileUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct stat64 file_info;

  file_length = -1;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 != ::stat64(path, &file_info)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "file not exist",
        K(ret), K(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else if (S_ISDIR(file_info.st_mode)) {
    ret = OB_IO_ERROR;
    STORAGE_LOG(WARN, "uri is a dir", K(ret), KCSTRING(path));
  } else {
    file_length = file_info.st_size;
  }

  return ret;
}

int ObStorageFileUtil::head_object_meta(const common::ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct stat64 file_info;
  obj_meta.reset();

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 != ::stat64(path, &file_info)) {
    if (ENOENT == errno || ENOTDIR == errno) {
    } else {
      convert_io_error(errno, ret);
      STORAGE_LOG(WARN, "file not exist",
          K(ret), K(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else if (S_ISDIR(file_info.st_mode)) {
    obj_meta.is_exist_ = true;
    obj_meta.length_ = -1;
    obj_meta.type_ = ObStorageObjectMetaType::OB_FS_DIR;
  } else {
    obj_meta.is_exist_ = true;
    obj_meta.length_ = file_info.st_size;
    obj_meta.type_ = ObStorageObjectMetaType::OB_FS_FILE;
  }

  return ret;
}

int ObStorageFileUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (0 != ::unlink(path)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to del file",
        K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }

  return ret;
}

int ObStorageFileUtil::write_single_file(const common::ObString &uri, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileWriter writer;

  if (OB_FAIL(writer.open(uri))) {
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

int ObStorageFileUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path, sizeof(path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else {
    // find the first not exist dir
    int64_t pos = 0;
    bool found_exist_dir = false;
    const int64_t end_pos = strlen(path) - 1;
    for (pos = end_pos; OB_SUCC(ret) && pos >= 0 && !found_exist_dir; --pos) {
      if (path[pos] == '/' || pos == end_pos) {
        if (pos != end_pos) {
          path[pos] = '\0';
        }

        if (0 == ::access(path, F_OK)) {
          STORAGE_LOG(INFO, "path exist", K(pos), KCSTRING(path));
          found_exist_dir = true;
        } else if (ENOTDIR == errno) {
          ret = OB_FILE_ALREADY_EXIST;
          STORAGE_LOG(WARN, "already exist the same name file", K(ret), KCSTRING(path),
            K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
        } else if (ENOENT != errno) {
          convert_io_error(errno, ret);
          STORAGE_LOG(WARN, "check is parent dir exist",
              K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
        }

        if (pos != end_pos) {
          path[pos] = '/';
        }
      }
    }

    // create dirs
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
                convert_io_error(errno, ret);
                STORAGE_LOG(WARN, "parent dir is not exist",
                    K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
              }
            } else {
              convert_io_error(errno, ret);
              STORAGE_LOG(WARN, "failed to create parent dir",
                  K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
            }
          } else {
            STORAGE_LOG(INFO, "succeed to create parent dir", KCSTRING(path), K(uri));
          }
        } else {
          convert_io_error(errno, ret);
          STORAGE_LOG(WARN, "check is parent dir exist",
              K(ret), KCSTRING(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
        }
        if (end_pos != i) {
          path[i] = '/';
        }
      }
    }
  }

  return ret;
}

int ObStorageFileUtil::list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  struct dirent entry;
  struct dirent *result;
  DIR *open_dir = NULL;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  ObString file_name;
  char dir_path[OB_MAX_URI_LENGTH] = "";
  char sub_dir_path[OB_MAX_URI_LENGTH] = "";
  bool is_file = false;
  int64_t size = 0;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "dir path is invalid", K(ret), KCSTRING(dir_path));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      convert_io_error(errno, ret);
      OB_LOG(WARN, "fail to open dir", K(ret), KCSTRING(dir_path),
          KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  }

  while (OB_SUCC(ret) && NULL != open_dir) {
    file_name.reset();
    is_file = false;
    size  = 0;
    if (0 != ::readdir_r(open_dir, &entry, &result)) {
      convert_io_error(errno, ret);
      OB_LOG(WARN, "read dir error", K(ret),
          KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    } else if (NULL != result) {
      if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
        is_file = false;
      } else if (DT_REG == entry.d_type && !op.need_get_file_size()) {
        is_file = true;
      } else if ((DT_REG == entry.d_type && op.need_get_file_size()) || DT_UNKNOWN == entry.d_type) {
        int pret = snprintf(sub_dir_path, OB_MAX_URI_LENGTH, "%s/%s", dir_path, entry.d_name);
        if (pret < 0 || pret >= OB_MAX_URI_LENGTH) {
          ret = OB_BUF_NOT_ENOUGH;
          OB_LOG(WARN, "format dir path fail", K(ret), KCSTRING(dir_path));
        } else {
          struct stat sb;
          if (-1 == ::stat(sub_dir_path, &sb)) {
            convert_io_error(errno, ret);
            OB_LOG(WARN, "stat fail", K(ret),
                KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
          } else if (!S_ISREG(sb.st_mode)) {
            is_file = false;
          } else {
            is_file = true;
            size = static_cast<int64_t>(sb.st_size);
          }
        }
      }
      if (OB_SUCC(ret) && is_file) {
        if (op.need_get_file_size()) {
          op.set_size(size);
        }
        if (OB_FAIL(op.func(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), KCSTRING(entry.d_name));
        }
      }
    } else {
      break;//end file
    }
  }
  //close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }

  return ret;
}

int ObStorageFileUtil::list_files(const common::ObString &uri, ObStorageListCtxBase &ctx_base)
{
  int ret = OB_SUCCESS;
  char dir_path[OB_MAX_URI_LENGTH] = "";
  ObStorageListFilesCtx &list_ctx = static_cast<ObStorageListFilesCtx &>(ctx_base);

  if (OB_UNLIKELY(uri.empty() || !list_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(list_ctx), KCSTRING(dir_path));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else {
    list_ctx.rsp_num_ = 0;
    list_ctx.has_next_ = false;

    struct dirent *result;
    char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
    ObString file_name;
    char sub_dir_path[OB_MAX_URI_LENGTH] = "";
    bool is_file = false;
    bool is_appendable_file = false;
    int64_t size = 0;

    if (!list_ctx.already_open_dir_) {
      if (OB_ISNULL(list_ctx.open_dir_ = ::opendir(dir_path))) {
        if (ENOENT != errno) {
          convert_io_error(errno, ret);
          OB_LOG(WARN, "fail to open dir", K(ret), KCSTRING(dir_path),
              KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
        }
      } else {
        list_ctx.already_open_dir_ = true;
      }
    }

    while (OB_SUCC(ret) && NULL != list_ctx.open_dir_) {
      file_name.reset();
      is_file = false;
      is_appendable_file = false;
      size = 0;
      if (list_ctx.rsp_num_ >= list_ctx.max_list_num_) {
        list_ctx.has_next_ = true;
        break;
      }

      if (0 != ::readdir_r(list_ctx.open_dir_, &(list_ctx.next_entry_), &result)) {
        convert_io_error(errno, ret);
        OB_LOG(WARN, "read dir error", K(ret),
            KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
      } else if (NULL != result) {
        if (0 == strcmp(list_ctx.next_entry_.d_name, ".") || 0 == strcmp(list_ctx.next_entry_.d_name, "..")) {
          // not a file
        } else if (DT_DIR == list_ctx.next_entry_.d_type) {
          if (OB_FAIL(check_is_appendable(uri, list_ctx.next_entry_, is_appendable_file))) {
            OB_LOG(WARN, "fail to check is_appendable", K(ret), K(uri));
          } else if (is_appendable_file) {
            is_file = true;
          }
        } else if (DT_REG == list_ctx.next_entry_.d_type && !list_ctx.need_size_) {
          is_file = true;
        } else if ((DT_REG == list_ctx.next_entry_.d_type && list_ctx.need_size_) || DT_UNKNOWN == list_ctx.next_entry_.d_type) {
          int pret = snprintf(sub_dir_path, OB_MAX_URI_LENGTH, "%s/%s", dir_path, list_ctx.next_entry_.d_name);
          if (pret < 0 || pret >= OB_MAX_URI_LENGTH) {
            ret = OB_BUF_NOT_ENOUGH;
            OB_LOG(WARN, "format dir path fail", K(ret), KCSTRING(dir_path));
          } else {
            struct stat sb;
            if (-1 == ::stat(sub_dir_path, &sb)) {
              convert_io_error(errno, ret);
              OB_LOG(WARN, "stat fail", K(ret), KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
            } else if (!S_ISREG(sb.st_mode)) {
              // not a file
            } else {
              is_file = true;
              size = static_cast<int64_t>(sb.st_size);
            }
          }
        }

        if (OB_SUCC(ret) && is_file) {
          const int64_t name_len = strlen(list_ctx.next_entry_.d_name);
          if (name_len >= list_ctx.max_name_len_) {
            ret = OB_SIZE_OVERFLOW;
            OB_LOG(WARN, "can't hold the dentry name", K(ret), K(list_ctx.max_name_len_), K(name_len));
          } else {
            MEMCPY(list_ctx.name_arr_[list_ctx.rsp_num_], list_ctx.next_entry_.d_name, name_len);
            if (is_appendable_file) {
              list_ctx.name_arr_[list_ctx.rsp_num_][name_len] = '/';
              list_ctx.name_arr_[list_ctx.rsp_num_][name_len + 1] = '\0';
            } else {
              list_ctx.name_arr_[list_ctx.rsp_num_][name_len] = '\0';
            }
            if (list_ctx.need_size_) {
              list_ctx.size_arr_[list_ctx.rsp_num_] = size;
            }
            ++list_ctx.rsp_num_;
          }
        }
      } else {
        list_ctx.has_next_ = false;
        break; // end file
      }
    }
    // close dir
    if (!list_ctx.has_next_ && list_ctx.already_open_dir_ && NULL != list_ctx.open_dir_) {
      ::closedir(list_ctx.open_dir_);
    }
  }

  return ret;
}

int ObStorageFileUtil::check_is_appendable(
    const common::ObString &uri,
    struct dirent &cur_entry,
    bool &is_appendable_file)
{
  int ret = OB_SUCCESS;
  ObStorageObjectMetaBase obj_meta;
  char logic_apendable_obj_name[OB_MAX_URI_LENGTH] = { 0 };
  char tmp_uri_buf[OB_MAX_URI_LENGTH] = "";
  if (OB_FAIL(databuff_printf(logic_apendable_obj_name, sizeof(logic_apendable_obj_name), "%s/%s",
                              uri.ptr(), cur_entry.d_name))) {
    OB_LOG(WARN, "fail to construct logic_apendable_obj_name", K(ret), K(uri), K(cur_entry.d_name));
  } else if (OB_FAIL(construct_fragment_full_name(logic_apendable_obj_name,
                                                  OB_S3_APPENDABLE_FORMAT_META,
                                                  tmp_uri_buf, sizeof(tmp_uri_buf)))) {
    OB_LOG(WARN, "fail to construct fragment full name", K(ret), K(uri), K(cur_entry.d_name));
  } else {
    if (OB_FAIL(head_object_meta(tmp_uri_buf, obj_meta))) {
      OB_LOG(WARN, "fail to head object meta", K(ret), K(tmp_uri_buf));
    } else {
      is_appendable_file = obj_meta.is_exist_;
    }
  }
  return ret;
}

int ObStorageFileUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  char dir_path[OB_MAX_URI_LENGTH];
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  struct stat st;
  DIR *open_dir = NULL;
  struct dirent entry;
  struct dirent *result;
  int64_t dir_file_count = 0;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(::lstat(dir_path, &st))) {
    if (ENOENT == errno) {
      ret = OB_SUCCESS;
      STORAGE_LOG(INFO, "dir do not exist", KCSTRING(dir_path),
          "errono_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else {
      convert_io_error(errno, ret);
      STORAGE_LOG(WARN, "failed to ls stat dir", K(ret),
          KCSTRING(dir_path), "errono_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    }
  } else if (!S_ISDIR(st.st_mode)) {
    ret = OB_DIR_NOT_EXIST;
    STORAGE_LOG(WARN, "path is not to dir", K(ret), KCSTRING(dir_path));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      convert_io_error(errno, ret);
      OB_LOG(WARN, "fail to open dir", K(ret), KCSTRING(dir_path),
          KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir && 0 == dir_file_count) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        convert_io_error(errno, ret);
        OB_LOG(WARN, "read dir error", K(ret),
            KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
      } else if (NULL != result
          && (DT_REG == entry.d_type || DT_UNKNOWN  == entry.d_type || (DT_DIR == entry.d_type))
          && 0 != strcmp(entry.d_name, ".")
          && 0 != strcmp(entry.d_name, "..")) {
        ++dir_file_count;
      } else if (NULL == result) {
        break;//end file
      }
    }
    if (OB_SUCC(ret) && 0 == dir_file_count) {
      if (0 != ::rmdir(dir_path)) {
        convert_io_error(errno, ret);
        STORAGE_LOG(WARN, "failed to del file",
            K(ret), KCSTRING(dir_path), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    }
  }
  //close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }
  return ret;
}

int ObStorageFileUtil::list_directories(
    const ObString &uri,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  struct dirent entry;
  struct dirent *result;
  DIR *open_dir = NULL;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  ObString directory_name;
  char dir_path[OB_MAX_URI_LENGTH]= "";
  char sub_dir_path[OB_MAX_URI_LENGTH] = "";
  int32_t dir_path_len = 0;
  bool is_directory = false;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "dir path is invalid", K(ret), KCSTRING(dir_path));
  } else if (OB_FAIL(get_file_path(uri, dir_path, sizeof(dir_path)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_path))) {
    if (ENOENT != errno) {
      convert_io_error(errno, ret);
      OB_LOG(WARN, "fail to open dir", K(ret), KCSTRING(dir_path),
          KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    }
  } else {
    dir_path_len = strlen(dir_path);
  }

  while (OB_SUCC(ret) && NULL != open_dir) {
    directory_name.reset();
    is_directory = false;
    if (0 != ::readdir_r(open_dir, &entry, &result)) {
      convert_io_error(errno, ret);
      OB_LOG(WARN, "read dir error", K(ret),
          KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
    } else if (NULL != result) {
      if (0 == strcmp(entry.d_name, ".") || 0 == strcmp(entry.d_name, "..")) {
        //do nothing
        is_directory = false;
      } else if (DT_DIR  == entry.d_type) {
        is_directory = true;
      } else if (DT_UNKNOWN == entry.d_type) {
        int pret = snprintf(sub_dir_path, OB_MAX_URI_LENGTH, "%s/%s", dir_path, entry.d_name);
        if (pret < 0 || pret >= OB_MAX_URI_LENGTH) {
          ret = OB_BUF_NOT_ENOUGH;
          OB_LOG(WARN, "format tenant_id fail", K(ret), KCSTRING(dir_path));
        } else {
          struct stat sb;
          if (-1 == stat(sub_dir_path, &sb)) {
            convert_io_error(errno, ret);
            OB_LOG(WARN, "read dir path error", K(ret),
                KCSTRING(strerror_r(errno, errno_buf, sizeof(errno_buf))));
          } else if (!S_ISDIR(sb.st_mode)) {
            is_directory = false;
          } else {
            is_directory = true;
          }
        }
      }
      if (OB_SUCC(ret) && is_directory) {
        if (OB_FAIL(op.func(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), KCSTRING(entry.d_name));
        }
      }
    } else {
      break;//end file
    }
  }
  //close dir
  if (NULL != open_dir) {
    ::closedir(open_dir);
  }

  OB_LOG(INFO, "list directories count", KCSTRING(dir_path), K(ret));
  return ret;
}

int ObStorageFileUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  is_tagging = false;
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(uri));
  } else {
    is_tagging = false;
  }
  return ret;
}

int ObStorageFileUtil::del_unmerged_parts(const ObString &uri)
{
  UNUSED(uri);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

ObStorageFileReader::ObStorageFileReader()
  : fd_(-1),
    is_opened_(false),
    file_length_(-1)
{
  path_[0] = '\0';
}

ObStorageFileReader::~ObStorageFileReader()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "reader is not closed", KCSTRING(path_), K(fd_));
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to close reader", K(tmp_ret), KCSTRING(path_), K(fd_));
    }
  }
}

int ObStorageFileReader::open(const common::ObString &uri,
    common::ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct stat64 file_info;
  UNUSED(storage_info);

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path_, sizeof(path_)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (-1 == (fd_ = ::open(path_, O_RDONLY))) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to open read file",
        K(ret), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    if (head_meta) {
      if (0 != ::fstat64(fd_, &file_info)) {
        convert_io_error(errno, ret);
        STORAGE_LOG(WARN, "file not exist",
            K(ret), K(fd_), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      } else if (S_ISDIR(file_info.st_mode)) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "uri is a dir", K(ret), KCSTRING(path_));
      } else {
        file_length_ = file_info.st_size;
      }
    }

    if (OB_SUCC(ret)) {
      is_opened_ = true;
    } else {
      if (OB_SUCCESS != (tmp_ret = close())) {
        STORAGE_LOG(WARN, "failed to close", K(ret), K(tmp_ret), KCSTRING(path_));
      }
    }
  }
  return ret;
}

int ObStorageFileReader::pread(
    char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
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
        convert_io_error(errno, ret);
        STORAGE_LOG(WARN, "failed to read file",
            K(ret), K(read_size), K(buf_size),
            KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
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
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (is_opened_ && 0 != ::close(fd_)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to close read file",
        K(ret), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }
  is_opened_ = false;
  fd_ = -1;
  return ret;
}

ObStorageFileBaseWriter::ObStorageFileBaseWriter()
  : fd_(-1),
    is_opened_(false),
    file_length_(-1),
    has_error_(false)
{
  path_[0] = '\0';
}
ObStorageFileBaseWriter::~ObStorageFileBaseWriter()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "writer is not closed", KCSTRING(path_), K(fd_));
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to close writer", K(tmp_ret), KCSTRING(path_), K(fd_));
    }
  }
}

int ObStorageFileBaseWriter::open(const int flags)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  ObStorageFileUtil util;

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret));
  } else if (strlen(path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid path", K(ret), KCSTRING(path_));
  } else if (-1 == (fd_ = ::open(path_, flags, S_IRUSR | S_IWUSR))) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to open write file",
        K(ret), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    file_length_ = 0;
    is_opened_ = true;
    STORAGE_LOG(DEBUG, "succceed to open file for write", KCSTRING(path_), K(fd_), K(flags));
  }

  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
  return ret;
}

int ObStorageFileBaseWriter::write(const char *buf,const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  int64_t write_size = 0;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
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
        convert_io_error(errno, ret);
        STORAGE_LOG(WARN, "failed to write file",
            K(ret), K(one_write_size), K(errno), KCSTRING(path_),
            "errno_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      } else {
        write_size += one_write_size;
      }
    }

    if (OB_SUCC(ret)) {
      if (write_size != buf_size) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(ERROR, "write size not match buf size", K(ret), KCSTRING(path_));
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

int ObStorageFileBaseWriter::pwrite(const char *buf, const int64_t buf_size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  int64_t write_size = 0;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || buf_size < 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_size), K(offset));
  } else {
    int64_t one_write_size = 0;
    while (OB_SUCC(ret) && write_size < buf_size) {
      one_write_size = ::pwrite(fd_, buf + write_size, buf_size - write_size, offset + write_size);
      if (one_write_size < 0) {
        convert_io_error(errno, ret);
        STORAGE_LOG(WARN, "failed to write file",
            K(ret), K(one_write_size), K(errno), KCSTRING(path_),
            "errno_str", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      } else {
        write_size += one_write_size;
      }
    }

    if (OB_SUCC(ret)) {
      if (write_size != buf_size) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(ERROR, "write size not match buf size", K(ret), KCSTRING(path_));
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
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (0 != ::close(fd_)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to close write file",
        K(ret), K(fd_), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  } else {
    STORAGE_LOG(DEBUG, "succeed to close write file", KCSTRING(path_), K(fd_));
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
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "wrtier is not closed", KCSTRING(path_), K(fd_));
    has_error_ = true;
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to close wrtier", K(tmp_ret), KCSTRING(path_), K(fd_));
    }
  }
}

int ObStorageFileWriter::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  UNUSED(storage_info);
  int ret = OB_SUCCESS;
  const char *TMP_NAME_FORMAT = "%s.tmp.%ld";
  ObStorageFileUtil util;
  const int64_t cur_ts = ObTimeUtil::current_time();
  int flags = O_CREAT | O_WRONLY | O_TRUNC | O_EXCL | O_SYNC;

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
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (OB_FAIL(ObStorageFileBaseWriter::close())) {
    STORAGE_LOG(WARN, "failed to do close", K(ret), KCSTRING(path_), KCSTRING(real_path_));
  }

  if (OB_SUCC(ret)) {
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_FILE_SYSTEM_RENAME_ERROR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
#endif
    if (has_error_) {
      STORAGE_LOG(WARN, "writer has error, skip rename file", KCSTRING(path_), KCSTRING(real_path_));

      // has error, try delete file regardless of whether the temporary file exists
      if (0 != ::remove(path_)) {
        tmp_ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "failed to remove file", K(tmp_ret), KCSTRING(path_),
            K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    } else if (0 != ::rename(path_, real_path_)) {
      convert_io_error(errno, ret);
      STORAGE_LOG(WARN, "failed to rename meta file",
          K(ret), KCSTRING(real_path_), KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      //rename failed ,try delete file
      if (0 != ::remove(path_)) {
        tmp_ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "failed to remove file", K(tmp_ret), KCSTRING(path_),
            K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    } else {
      STORAGE_LOG(INFO, "succeed to rename file after close", KCSTRING(path_), KCSTRING(real_path_));
    }
  }
  has_error_ = false;
  return ret;
}

ObStorageFileAppender::ObStorageFileAppender()
  : need_unlock_(false),
    open_mode_(StorageOpenMode::CREATE_OPEN_LOCK)
{
}

ObStorageFileAppender::ObStorageFileAppender(StorageOpenMode mode)
  : need_unlock_(false),
    open_mode_(mode)
{
}

ObStorageFileAppender::~ObStorageFileAppender()
{
  int tmp_ret = OB_SUCCESS;
  if (is_opened_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "appender is not closed");
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG_RET(WARN, tmp_ret, "failed to close appender", K(tmp_ret), K(fd_));
    }
  }
}

int ObStorageFileAppender::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  UNUSED(storage_info);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageFileUtil util;
  int flags = 0;
  bool need_lock = false;

  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(get_open_flag_and_mode_(flags, need_lock))) {
    STORAGE_LOG(WARN, "failed to get open flag and mode", K(ret), K(uri));
  } else if (OB_FAIL(get_file_path(uri, path_, sizeof(path_)))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageFileBaseWriter::open(flags))) {
    STORAGE_LOG(WARN, "failed to do_open", K(ret), K(uri), K(flags));
  } else if (! need_lock) {
    // do nothing
  } else if (OB_FAIL(lock_file(fd_))) {
    STORAGE_LOG(WARN, "failed to lock file", K(ret), KCSTRING(path_), K(flags));
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
    STORAGE_LOG(WARN, "failed to unlock file", K(ret), KCSTRING(path_));
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
// 2. Only the creator is allowed to open, and the mutually exclusive mode is guaranteed. When the file needs to be created, use the mutually exclusive creation method to open
//    When the creator closes the file and wants to append data, use the non-creation method to open it. therefore,
//    The second way of mutual exclusion requires the caller to guarantee
int ObStorageFileAppender::get_open_flag_and_mode_(int &flag, bool &need_lock)
{
  int ret = OB_SUCCESS;
  need_lock = false;
  switch (open_mode_) {
  case StorageOpenMode::CREATE_OPEN_LOCK:
    flag = O_CREAT | O_RDWR | O_APPEND | O_SYNC;
    need_lock = true;
    break;
  case StorageOpenMode::EXCLUSIVE_CREATE:
    flag = O_CREAT | O_RDWR | O_APPEND | O_EXCL | O_SYNC;
    break;
  case StorageOpenMode::ONLY_OPEN_UNLOCK:
    flag = O_RDWR | O_APPEND | O_SYNC;
    break;
  case StorageOpenMode::CREATE_OPEN_NOLOCK:
    flag = O_CREAT | O_RDWR | O_SYNC;
    need_lock = false;
    break;
  default:
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected open mode", K(open_mode_));
  }
  return ret;
}

int ObStorageFileMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  UNUSED(offset);
  return write(buf, size);
}

int ObStorageFileMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";
  struct stat64 file_info;

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  }

  if (OB_SUCC(ret)) {
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_FILE_SYSTEM_RENAME_ERROR) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    has_error_ = true;
  }
#endif
    if (has_error_) {
      STORAGE_LOG(WARN, "multipart writer has error, skip complete",
          KCSTRING(path_), KCSTRING(real_path_));
    } else if (0 != ::stat64(path_, &file_info)) {
      convert_io_error(errno, ret);
      STORAGE_LOG(WARN, "fail to get file length",
          K(ret), K_(path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else if (OB_UNLIKELY(file_info.st_size == 0)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "no parts have been uploaded!", K(ret), K(file_info.st_size), K_(path));
    } else if (0 != ::rename(path_, real_path_)) {
      convert_io_error(errno, ret);
      STORAGE_LOG(WARN, "failed to complete", K(ret), KCSTRING(real_path_),
          KCSTRING(path_), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
    } else {
      STORAGE_LOG(INFO, "succeed to rename file after complete",
          KCSTRING(path_), KCSTRING(real_path_));
    }
  }
  return ret;
}

int ObStorageFileMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  char errno_buf[OB_MAX_ERROR_MSG_LEN] = "";

  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K(fd_));
  } else if (0 != ::remove(path_)) {
    convert_io_error(errno, ret);
    STORAGE_LOG(WARN, "failed to abort(remove tmp file)", K(ret), KCSTRING(path_),
        K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
  }
  return ret;
}

int ObStorageFileMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret), K_(fd));
  } else if (OB_FAIL(ObStorageFileBaseWriter::close())) {
    STORAGE_LOG(WARN, "failed to do close", K(ret), KCSTRING(path_), KCSTRING(real_path_));
  }
  return ret;
}

}//common
}//oceanbase
