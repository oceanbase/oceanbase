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

#include "lib/restore/ob_storage.h"
#include "ob_storage_hdfs_jni_base.h"
#include "ob_storage_hdfs_cache.h"
#include "sql/engine/connector/ob_java_helper.h"

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/utility.h"
#include "lib/allocator/ob_malloc.h"

namespace oceanbase
{
namespace common
{

/*--------------------------------ObStorageHdfsBase--------------------------------*/
ObStorageHdfsBase::ObStorageHdfsBase()
    : is_inited_(false),
      is_opened_readable_file_(false), is_opened_writable_file_(false),
      hdfs_read_file_(nullptr), hdfs_client_(nullptr), namenode_buf_(nullptr),
      path_buf_(nullptr)
{
}

void ObStorageHdfsBase::reset()
{
  if (OB_NOT_NULL(namenode_buf_)) {
    ob_free(namenode_buf_);
    namenode_buf_ = nullptr;
  }
  if (OB_NOT_NULL(path_buf_)) {
    ob_free(path_buf_);
    path_buf_ = nullptr;
  }
  is_inited_ = false;
  if (is_opened_readable_file_ && OB_NOT_NULL(hdfs_read_file_) &&
      OB_NOT_NULL(hdfs_client_) && OB_LIKELY(hdfs_client_->is_valid_client()) &&
      1 == obHdfsFileIsOpenForRead(hdfs_read_file_)) {
    int ret = OB_SUCCESS;
    OB_LOG(TRACE, "reset to close readable file", K(ret));
    hdfsFS hdfs_fs = hdfs_client_->get_hdfs_fs();
    obHdfsCloseFile(hdfs_fs, hdfs_read_file_);
  }
  if (OB_NOT_NULL(hdfs_client_)) {
    hdfs_client_->release();
  }
  // TODO(bitao): add writable file to deconstruct
  if (is_opened_writable_file_) {}
  hdfs_read_file_ = nullptr;
  is_opened_readable_file_ = false;
  is_opened_writable_file_ = false;
}

ObStorageHdfsBase::~ObStorageHdfsBase()
{
  reset();
}

int ObStorageHdfsBase::parse_namenode_and_path(const ObString &uri_str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri_str)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "fail to uri str", K(ret), K(uri_str));
  } else {
    // Length of namenode and path must be less than total uri.
    const int64_t uri_len = uri_str.length();
    if (OB_ISNULL(namenode_buf_ =
                      static_cast<char *>(ob_malloc(uri_len, "HdfsNamenode")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory for namenode", K(ret), K(uri_len), K(uri_str));
    } else if (OB_ISNULL(path_buf_ = static_cast<char *>(
                            ob_malloc(uri_len, "HdfsFilePath")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to allocate memory for file path", K(ret), K(uri_len), K(uri_str));
    } else if (OB_FAIL(ObHdfsCacheUtils::get_namenode_and_path_from_uri(
                   namenode_buf_, uri_len, path_buf_, uri_len, uri_str))) {
      OB_LOG(WARN, "failed to get namenode and path", K(ret), K(uri_str),
             K_(namenode_buf), K_(path_buf));
    }
  }
  // Free if failed and it is not null
  if (OB_FAIL(ret) && OB_NOT_NULL(namenode_buf_)) {
    ob_free(namenode_buf_);
    namenode_buf_ = nullptr;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(path_buf_)) {
    ob_free(path_buf_);
    path_buf_ = nullptr;
  }
  return ret;
}

int ObStorageHdfsBase::get_or_create_fs(const ObString &uri,
                                        ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(uri) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri or storage info is null", K(ret), K(uri), K(storage_info));
  } else if (OB_FAIL(parse_namenode_and_path(uri))) {
    OB_LOG(WARN, "failed to parse namenode and path", K(ret), K(uri));
  } else if (OB_ISNULL(namenode_buf_)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to get hdfs username", K(ret),K(uri));
  } else {
    if (OB_FAIL(ObHdfsFsCache::instance()->get_connection(
            namenode_buf_, hdfs_client_, storage_info))) {
      OB_LOG(WARN, "failed to get connection", K(ret));
    } else if (OB_ISNULL(hdfs_client_) || OB_LIKELY(!hdfs_client_->is_valid_client())) {
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to get hdfs file system", K(ret));
    }
  }

  return ret;
}

int ObStorageHdfsBase::get_or_create_read_file(const ObString &uri)
{
  int ret = OB_SUCCESS;
  if (!is_opened_readable_file_) {
    OB_LOG(TRACE, "start get or create read file", K(ret), K(uri), K(path_buf_),
           KP(hdfs_read_file_));
    if (OB_ISNULL(hdfs_client_) || OB_LIKELY(!hdfs_client_->is_valid_client())) {
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "hdfs client is not inited", K(ret));
    } else {
      OB_LOG(TRACE, "start to open read file", K(ret), K(uri), K(path_buf_));
      // buffer size is `0` means using default value `4096`.
      hdfsFS hdfs_fs = hdfs_client_->get_hdfs_fs();
      hdfs_read_file_ = obHdfsOpenFile(hdfs_fs, path_buf_, O_RDONLY, 0, 0, 0);
      if (OB_ISNULL(hdfs_read_file_)) {
        ret = OB_HDFS_OPEN_FILE_ERROR;
        OB_LOG(WARN, "failed to open hdfs readable file", K(ret), K_(path_buf));
      } else if (0 == obHdfsFileIsOpenForRead(hdfs_read_file_)) {
        ret = OB_HDFS_OPEN_FILE_ERROR;
        OB_LOG(WARN, "failed to get the opened hdfs readable file", K(ret), K_(path_buf));
      }
    }
  }

  if (OB_SUCC(ret) && !is_opened_readable_file_) {
    is_opened_readable_file_ = true;
  }
  return ret;
}

int ObStorageHdfsBase::get_hdfs_file_meta(const ObString &uri, ObStorageObjectMetaBase &meta)
{
  int ret = OB_SUCCESS;
  meta.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "hdfs base not inited", K(ret));
  } else if (OB_ISNULL(hdfs_client_) || OB_LIKELY(!hdfs_client_->is_valid_client())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "hdfs base with null file system inited", K(ret));
  } else {
    hdfsFS hdfs_fs = hdfs_client_->get_hdfs_fs();
    hdfsFileInfo* file_info = obHdfsGetPathInfo(hdfs_fs, path_buf_);
    if (OB_ISNULL(file_info)) {
      ret = OB_HDFS_PATH_NOT_FOUND;
      OB_LOG(WARN, "failed to get file info", K(ret), K(uri), K_(path_buf));
    } else {
      meta.length_ = file_info->mSize;
      meta.is_exist_ = true;
      if (file_info->mKind == kObjectKindDirectory) {
        meta.type_ = ObStorageObjectMetaType::OB_FS_DIR;
      } else if (file_info->mKind == kObjectKindFile) {
        meta.type_ = ObStorageObjectMetaType::OB_FS_FILE;
      }
      OB_LOG(TRACE, "get file info", K(ret), K(uri), K_(path_buf),
             K(meta.length_), K(meta.is_exist_), K(meta.type_));
      /* 1 means numEntries The size of the array. */
      obHdfsFreeFileInfo(file_info, 1);
    }
  }
  return ret;
}

int ObStorageHdfsBase::open(const ObString &uri,
                            ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char info_str[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "hdfs base alreagy inited", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty()) || !storage_info->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to init hdfs base, invalid arguments", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(storage_info->get_storage_info_str(info_str, sizeof(info_str)))) {
    OB_LOG(WARN, "failed to get storage info str", K(ret), KPC(storage_info));
  } else if (OB_FAIL(get_or_create_fs(uri, storage_info))) {
    OB_LOG(WARN, "failed to get hdfs file system", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

/*--------------------------------ObStorageHdfsJniUtil--------------------------------*/
ObStorageHdfsJniUtil::ObStorageHdfsJniUtil() : is_opened_(false), storage_info_(NULL)
{
}

ObStorageHdfsJniUtil::~ObStorageHdfsJniUtil()
{
}

int ObStorageHdfsJniUtil::open(ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util already open, cannot open again", K(ret));
  } else {
    is_opened_ = true;
    storage_info_ = storage_info;
  }
  return ret;
}

void ObStorageHdfsJniUtil::close()
{
  is_opened_ = false;
  storage_info_ = nullptr;
}

int ObStorageHdfsJniUtil::is_exist(const common::ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "failed to head object meta", K(ret), K(uri));
  } else {
    exist = obj_meta.is_exist_;
  }
  return ret;
}

int ObStorageHdfsJniUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObStorageObjectMetaBase obj_meta;
  file_length = 0;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "failed to head object meta", K(ret), K(uri));
  } else if (!obj_meta.is_exist_) {
    OB_LOG(WARN, "file is not exist", K(ret), K(uri));
  } else {
    file_length = obj_meta.length_;
  }
  return ret;
}

int ObStorageHdfsJniUtil::head_object_meta(const common::ObString &uri,
                                           ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  ObStorageObjectMetaBase meta;
  ObStorageHdfsBase hdfs_base;
  obj_meta.reset();
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "failed to open hdfs base", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_hdfs_file_meta(uri, meta))) {
    OB_LOG(WARN, "failed to get hdfs file meta", K(ret), K(uri));
  } else {
    obj_meta.is_exist_ = meta.is_exist_;
    obj_meta.type_ = meta.type_;
    if (obj_meta.is_exist_) {
      obj_meta.length_ = meta.length_;
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObStorageHdfsBase hdfs_base;
  hdfsFS hdfs_fs = nullptr;
  char *file_path = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments in hdfs", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_or_create_fs(uri, storage_info_))) {
    OB_LOG(WARN, "failed to get or create file system", K(ret), K(uri));
  } else if (OB_ISNULL(hdfs_fs = hdfs_base.get_fs())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null hdfs file system", K(ret), K(uri));
  } else if (OB_ISNULL(file_path = hdfs_base.get_path())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null file path", K(ret), K(uri));
  } else {
    // recursive if path is a directory and set to non-zero else zero.
    // 0 means that is not a directory.
    // res is 0 means success, else failed.
    const int res = obHdfsDelete(hdfs_fs, file_path, 0);
    if (0 != res) {
      ret = OB_HDFS_ERROR;
      char *ex = obHdfsGetLastExceptionRootCause();
      OB_LOG(WARN, "delete file with exception", K(ret), K(ex), K(res));
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::batch_del_files(const ObString &uri,
                  hash::ObHashMap<ObString, int64_t> &files_to_delete,
                  ObIArray<int64_t> &failed_files_idx)
{
  UNUSED(uri);
  UNUSED(files_to_delete);
  UNUSED(failed_files_idx);
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObStorageHdfsJniUtil::write_single_file(const common::ObString &uri, const char *buf, const int64_t size)
{
  UNUSED(uri);
  UNUSED(buf);
  UNUSED(size);
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObStorageHdfsJniUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObStorageHdfsBase hdfs_base;
  hdfsFS hdfs_fs = nullptr;
  char *dir_path = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments in hdfs", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_or_create_fs(uri, storage_info_))) {
    OB_LOG(WARN, "failed to get or create file system", K(ret), K(uri));
  } else if (OB_ISNULL(hdfs_fs = hdfs_base.get_fs())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null hdfs file system", K(ret), K(uri));
  } else if (OB_ISNULL(dir_path = hdfs_base.get_path())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null dir path", K(ret), K(uri));
  } else {
    const int res = obHdfsCreateDirectory(hdfs_fs, static_cast<const char*>(dir_path));
    if (0 != res) {
      ret = OB_HDFS_ERROR;
      char *ex = obHdfsGetLastExceptionRootCause();
      OB_LOG(WARN, "create directory with exception", K(ret), K(ex), K(res));
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageHdfsBase hdfs_base;
  ObExternalIOCounterGuard io_guard;
  char *full_dir_path = nullptr;
  hdfsFS hdfs_fs = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments in hdfs", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_or_create_fs(uri, storage_info_))) {
    OB_LOG(WARN, "fail to get hdfs file system", K(ret), K(uri));
  } else if (OB_ISNULL(full_dir_path = hdfs_base.get_path())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to get dir path", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else if (OB_ISNULL(hdfs_fs = hdfs_base.get_fs())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to get hdfs file system", K(ret), K(uri));
  } else {
    int num_entries = 0;
    hdfsFileInfo* file_infos = obHdfsListDirectory(hdfs_fs, full_dir_path, &num_entries);
    if (OB_ISNULL(file_infos)) {
      ret = OB_HDFS_ERROR;
      OB_LOG(WARN, "failed to get file info", K(ret), K(uri), K(full_dir_path));
    }

    if (OB_SUCC(ret)) {
      dirent entry;
      entry.d_type = DT_REG;

      for (int idx = 0; OB_SUCC(ret) && idx < num_entries; ++idx) {
        hdfsFileInfo file_info = file_infos[idx];
        if (file_info.mKind == kObjectKindDirectory) {
          // do nothing
        } else if (op.need_get_file_size()) {
          if (OB_UNLIKELY(file_info.mSize < 0)) {
            ret = OB_INVALID_ARGUMENT;
            OB_LOG(WARN, "invalid hdfs file size", K(file_info.mSize), K(file_info.mName));
          } else {
            op.set_size(file_info.mSize);
          }

          if (OB_SUCC(ret)) {
            // Note: mName is full with prefix "hdfs://namenode:port/path",
            // And the prefix "hdfs://namenode:port/path" is same as `uri`.
            char *obj_name = file_info.mName;
            if (OB_ISNULL(obj_name)) {
              ret = OB_HDFS_INVALID_ARGUMENT;
              OB_LOG(WARN, "fail to get obj name", K(ret));
            } else {
              const int64_t obj_name_len = strlen(obj_name);
              ObString file_name;
              char *file_name_start = obj_name + uri.length();
              file_name.assign_ptr(
                  file_name_start,
                  static_cast<int64_t>(obj_name_len - uri.length()));
              if (file_name.length() > sizeof(entry.d_name)) {
                ret = OB_SIZE_OVERFLOW;
                OB_LOG(WARN, "file name length is overflow d_name", K(ret), K(file_name), K(file_name.length()));
              } else {
                MEMCPY(entry.d_name, file_name.ptr(), file_name.length());
                entry.d_name[file_name.length()] = '\0'; // set str end
                if (OB_FAIL(op.func(&entry))) {
                  OB_LOG(WARN, "fail to list hdfs files.", K(ret), K(file_name));
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::list_files(const common::ObString &uri, ObStorageListCtxBase &ctx_base)
{
  UNUSED(uri);
  UNUSED(ctx_base);
  int ret = OB_NOT_IMPLEMENT;
  return ret;
}

int ObStorageHdfsJniUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObStorageHdfsBase hdfs_base;
  hdfsFS hdfs_fs = nullptr;
  char *dir_path = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments in hdfs", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_or_create_fs(uri, storage_info_))) {
    OB_LOG(WARN, "failed to get or create file system", K(ret), K(uri));
  } else if (OB_ISNULL(hdfs_fs = hdfs_base.get_fs())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null hdfs file system", K(ret), K(uri));
  } else if (OB_ISNULL(dir_path = hdfs_base.get_path())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null file path", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_end_with_slash(dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(dir_path));
  } else {
    // recursive if path is a directory and set to non-zero else zero.
    int res = obHdfsDelete(hdfs_fs, dir_path, 1);
    if (0 != res) {
      ret = OB_HDFS_ERROR;
      char *ex = obHdfsGetLastExceptionRootCause();
      OB_LOG(WARN, "delete directory with exception", K(ret), K(ex));
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::list_directories(
    const ObString &uri,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageHdfsBase hdfs_base;
  ObExternalIOCounterGuard io_guard;
  char *full_dir_path = nullptr;
  hdfsFS hdfs_fs = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "hdfs util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments in hdfs", K(ret), K(uri));
  } else if (OB_FAIL(hdfs_base.get_or_create_fs(uri, storage_info_))) {
    OB_LOG(WARN, "fail to get hdfs file system", K(ret), K(uri));
  } else if (OB_ISNULL(full_dir_path = hdfs_base.get_path())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to get dir path", K(ret), K(uri));
  } else if (OB_UNLIKELY(!is_end_with_slash(full_dir_path))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
  } else if (OB_ISNULL(hdfs_fs = hdfs_base.get_fs())) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to get hdfs file system", K(ret), K(uri));
  } else {
    int num_entries = 0;
    hdfsFileInfo* file_infos = obHdfsListDirectory(hdfs_fs, full_dir_path, &num_entries);
    if (OB_ISNULL(file_infos)) {
      ret = OB_HDFS_ERROR;
      OB_LOG(WARN, "failed to get file info", K(ret), K(uri), K(full_dir_path));
    }

    if (OB_SUCC(ret)) {
      dirent entry;
      entry.d_type = DT_DIR;

      for (int idx = 0; OB_SUCC(ret) && idx < num_entries; ++idx) {
        hdfsFileInfo file_info = file_infos[idx];
        if (file_info.mKind != kObjectKindDirectory) {
          continue;
        }

        // Note: mName is full with prefix "hdfs://namenode:port/path",
        // And the prefix "hdfs://namenode:port/path" is same as `uri`.
        char *obj_name = file_info.mName;
        int64_t obj_name_len = strlen(obj_name);
        ObString file_name;
        char *file_name_start = obj_name + uri.length();
        file_name.assign_ptr(
            file_name_start,
            static_cast<int64_t>(obj_name_len - uri.length()));

        MEMCPY(entry.d_name, file_name.ptr(), file_name.length());
        entry.d_name[file_name.length()] = '\0'; // set str end
        if (OB_FAIL(op.func(&entry))) {
          OB_LOG(WARN, "fail to list hdfs files.", K(ret));
        }
      }
    }
    if (OB_NOT_NULL(file_infos)) {
      obHdfsFreeFileInfo(file_infos, num_entries);
    }
  }
  return ret;
}

int ObStorageHdfsJniUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  UNUSED(uri);
  UNUSED(is_tagging);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

int ObStorageHdfsJniUtil::del_unmerged_parts(const ObString &uri)
{
  UNUSED(uri);
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

/*--------------------------------ObStorageHdfsReader--------------------------------*/
ObStorageHdfsReader::ObStorageHdfsReader()
    : ObStorageHdfsBase(),
      is_opened_(false),
      has_meta_(false),
      file_length_(-1)
{
}

ObStorageHdfsReader::~ObStorageHdfsReader()
{
}

void ObStorageHdfsReader::reset()
{
  ObStorageHdfsBase::reset();
  is_opened_ = false;
  has_meta_ = false;
  file_length_ = -1;
}

int ObStorageHdfsReader::open(const ObString &uri,
                             ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  ObStorageObjectMetaBase meta;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_HDFS_ERROR;
    OB_LOG(WARN, "HDFS reader already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageHdfsBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open in hdfs base", K(ret), K(uri));
  } else {
    if (head_meta) {
      if (OB_FAIL(get_hdfs_file_meta(uri, meta))) {
        OB_LOG(WARN, "failed to get hdfs object meta", K(ret), K(uri));
      } else if (!meta.is_exist_) {
        ret = OB_BACKUP_FILE_NOT_EXIST;
        OB_LOG(WARN, "backup file is not exist", K(ret), K(uri));
      } else {
        file_length_ = meta.length_;
        has_meta_ = true;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_or_create_read_file(uri))) {
    OB_LOG(WARN, "failed to get read file", K(ret), K(uri));
  } else if (OB_ISNULL(get_hdfs_read_file())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "get a null read file", K(ret), K(uri));
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObStorageHdfsReader::pread(char *buf,
    const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  int64_t get_data_size;
  read_size = 0;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "HDFS reader not opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(buf), K(buf_size), K(offset));
  } else if (OB_UNLIKELY(offset > file_length_)) {
    ret = OB_HDFS_INVALID_ARGUMENT;
    OB_LOG(WARN, "offset exceeds file size", K(ret), K_(file_length), K(offset));
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else {
    hdfsFile readable_file = get_hdfs_read_file();
    hdfsFS fs = get_fs();
    if (OB_ISNULL(readable_file)) {
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to get readable file", K(ret));
    } else if (0 == obHdfsFileIsOpenForRead(readable_file)) {
      ret = OB_HDFS_OPEN_FILE_ERROR;
      OB_LOG(WARN, "failed to get a readable file", K(ret));
    } else if (OB_ISNULL(fs)) {
      ret = OB_HDFS_INVALID_ARGUMENT;
      OB_LOG(WARN, "failed to get file system", K(ret));
    }

    if (OB_FAIL(ret)) {
      /* do nothing */
    } else {
      int64_t temp_offset = offset;
      int32_t last_bytes_read = 0;

      do {
        last_bytes_read = obHdfsPread(fs, readable_file, temp_offset,
                                    buf + read_size, buf_size - read_size);
        OB_LOG(TRACE, "pread stat info in detail", K(ret), K(last_bytes_read),
               K(read_size), K(temp_offset), K(buf_size));
        if (last_bytes_read == -1) {
          ret = OB_HDFS_READ_FILE_ERROR;
          OB_LOG(WARN, "failed to position read file", K(ret), K(temp_offset),
                 K(buf_size), K(read_size));
        } else {
          temp_offset += last_bytes_read;
          read_size += last_bytes_read;
        }
      } while (last_bytes_read > 0 && OB_SUCC(ret));
    }
  }
  return ret;
}

int ObStorageHdfsReader::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  reset();
  return ret;
}

} // common
} // oceanbase
