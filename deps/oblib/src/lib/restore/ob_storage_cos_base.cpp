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

#include "lib/restore/ob_storage_cos_base.h"
#include <stdlib.h>
#include <libgen.h>
#include "common/ob_string_buf.h"
#include "apr_errno.h"
#include "ob_storage.h"
#include "lib/hash/ob_hashset.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::common;

/*--------------------------------GLOBAL---------------------------*/
int init_cos_env()
{
  return qcloud_cos::ObCosEnv::get_instance().init();
}

void fin_cos_env()
{
  // wait doing io finish before destroy cos env.
  const int64_t start_time = ObTimeUtility::current_time();
  const int64_t timeout = ObExternalIOCounter::FLYING_IO_WAIT_TIMEOUT;
  int64_t flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  while(0 < flying_io_cnt) {
    const int64_t end_time = ObTimeUtility::current_time();
    if (end_time - start_time > timeout) {
      OB_LOG(INFO, "force fin_cos_env", K(flying_io_cnt));
      break;
    }
    usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }

  qcloud_cos::ObCosEnv::get_instance().destroy();
}

struct CosListFilesCbArg
{
  common::ObIAllocator &allocator;
  ObString &dir_path;
  ObBaseDirEntryOperator &list_op;

  CosListFilesCbArg(
    common::ObIAllocator &alloc,
    ObString &dir,
    ObBaseDirEntryOperator &op)
    : allocator(alloc),
      dir_path(dir),
      list_op(op) {}

  ~CosListFilesCbArg() {}
};

static int execute_list_callback(
  ObBaseDirEntryOperator &op,
  qcloud_cos::ObCosWrapper::CosListObjPara &para,
  ObString &file_name)
{
  int ret = OB_SUCCESS;
  int64_t dname_size = sizeof(para.last_container_name_.d_name);
  if (dname_size >= (file_name.length() + 1)) {
    if (OB_FAIL(databuff_printf(para.last_container_name_.d_name, dname_size, "%s", file_name.ptr()))) {
      OB_LOG(WARN, "fail to copy file name to entry buf!", K(ret), K(dname_size));
    } else if (OB_FAIL(op.func(&(para.last_container_name_)))) {
      OB_LOG(WARN, "fail to execute list callback!", K(ret), KCSTRING(para.last_container_name_.d_name),
            K(para.last_container_name_.d_type), K(DT_REG), K(dname_size));
    }
  } else {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "file name len is too long!", K(ret), K(file_name.length()), K(sizeof(dirent)));
  }
  return ret;
}

static int handle_object_name_cb(qcloud_cos::ObCosWrapper::CosListObjPara &para)
{
  int ret = OB_SUCCESS;
  para.next_flag_ = true;
  if (OB_ISNULL(para.arg_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret));
  } else if (OB_ISNULL(para.cur_full_path_slice_name_) || 0 >= para.full_path_size_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "current object_name is empty", K(ret));
  } else if (qcloud_cos::ObCosWrapper::CosListObjPara::CosListType::COS_LIST_CB_ARG == para.type_) {
    CosListFilesCbArg *ctx = static_cast<CosListFilesCbArg *>(para.arg_);

    // Returned object name is the whole object path, but we donot need the prefix dir_path.
    // So, we trim the dir_name from object_name path.
    const int dir_name_str_len = strlen(ctx->dir_path.ptr());
    const int object_name_len = para.full_path_size_ - dir_name_str_len;
    const int object_name_start_pos = dir_name_str_len;
    //tmp_file_name like slice name
    ObString tmp_file_name(object_name_len, object_name_len,
      para.cur_full_path_slice_name_ + object_name_start_pos);
    if (OB_FAIL(execute_list_callback(ctx->list_op, para, tmp_file_name))) {
      OB_LOG(WARN, "fail to execute list callback!", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "not supported type", K(ret), K(para.type_));
  }

  return ret;
}

static int handle_directory_name_cb(
    void *arg,
    const qcloud_cos::ObCosWrapper::CosListObjPara::CosListType type,
    const char *object_name,
    int64_t object_size)
{
  int ret = OB_SUCCESS;
  const char delimiter = '/';
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret), KP(arg));
  } else if (OB_ISNULL(object_name) || 0 >= object_size) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "object_name is empty", K(ret), KP(object_name), K(object_size));
  } else if (delimiter != object_name[object_size - 1]) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object name is unexpected", K(ret), KCSTRING(object_name), K(object_size));
  } else {
    const int64_t name_length = object_size - 1;
    if (name_length <= 0) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object is not exist", K(ret), KCSTRING(object_name), K(object_size));
    } else if (qcloud_cos::ObCosWrapper::CosListObjPara::CosListType::COS_LIST_CB_ARG == type) {
      CosListFilesCbArg *ctx = static_cast<CosListFilesCbArg *>(arg);
      ObString directory_name;
      dirent dir_name_entry;
      dir_name_entry.d_type = DT_DIR;
      ObString tmp_directory_name(name_length, name_length, object_name);
      if (OB_FAIL(databuff_printf(dir_name_entry.d_name, sizeof(dir_name_entry.d_name), "%.*s",
                                  tmp_directory_name.length(), tmp_directory_name.ptr()))) {
        OB_LOG(WARN, "fail to set dir name entry", K(ret), K(tmp_directory_name));
      } else if (OB_FAIL(ctx->list_op.func(&dir_name_entry))) {
        OB_LOG(WARN, "fail to call list directories operation callback", K(ret), K(tmp_directory_name));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "not supported type", K(ret), K(type));
    }
  }
  return ret;
}

/*--------------------------------ObStorageCosUtil---------------------------*/

ObStorageCosUtil::ObStorageCosUtil()
  : is_opened_(false), storage_info_(NULL)
{
}

ObStorageCosUtil::~ObStorageCosUtil()
{
}

int ObStorageCosUtil::open(ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util already open, cannot open again", K(ret));
  } else if (OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is null", K(ret));
  } else {
    storage_info_ = storage_info;
    is_opened_ = true;
  }
  return ret;
}

void ObStorageCosUtil::close()
{
  is_opened_ = false;
  storage_info_ = NULL;
}

int ObStorageCosUtil::is_exist(const ObString &uri, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  is_exist = false;
  int64_t file_length = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(get_object_meta_(uri, is_exist, file_length))) {
    OB_LOG(WARN, "fail to get object meta", K(ret), K(uri));
  }
  return ret;
}

int ObStorageCosUtil::get_file_length(const ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  bool is_file_exist = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(get_object_meta_(uri, is_file_exist, file_length))) {
    OB_LOG(WARN, "fail to get object meta", K(ret), K(uri));
  } else if (!is_file_exist) {
    ret = OB_BACKUP_FILE_NOT_EXIST;
    OB_LOG(WARN, "backup file is not exist", K(ret), K(uri));
  }
  return ret;
}

// inner function, won't check params valid or not.
int ObStorageCosUtil::get_object_meta_(
    const ObString &uri,
    bool &is_file_exist,
    int64_t &file_length)
{
  int ret = OB_SUCCESS;

  ObStorageCosBase cos_base;
  qcloud_cos::CosObjectMeta obj_meta;
  is_file_exist = false;
  if (OB_FAIL(cos_base.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
  } else if (OB_FAIL(cos_base.get_cos_file_meta(is_file_exist, obj_meta))) {
    OB_LOG(WARN, "fail to get object meta", K(ret));
  } else {
    file_length = obj_meta.file_length_;
  }
  cos_base.reset();

  return ret;
}

int ObStorageCosUtil::write_single_file(
    const ObString &uri,
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObStorageCosWriter writer;
  if (OB_FAIL(writer.open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open cos writer", K(ret), K(uri), KP_(storage_info));
  } else if (OB_FAIL(writer.write(buf, size))) {
    OB_LOG(WARN, "fail to write into cos", K(ret), K(size), KP(buf));
  } else if (OB_FAIL(writer.close())) {
    OB_LOG(WARN, "fail to close cos writer", K(ret));
  }
  return ret;
}

int ObStorageCosUtil::mkdir(const ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to create dir in cos", K(uri));
  UNUSED(uri);
  return ret;
}

int ObStorageCosUtil::del_dir(const ObString &uri)
{
  int ret = OB_SUCCESS;
  OB_LOG(DEBUG, "no need to del dir in cos", K(uri));
  UNUSED(uri);
  return ret;
}

int ObStorageCosUtil::is_tagging(
    const ObString &uri,
    bool &is_tagging)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  is_tagging = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else if (OB_FAIL(cos_base.is_object_tagging(uri, is_tagging))) {
      OB_LOG(WARN, "fail to check object tag", K(ret), K(uri));
    }
    cos_base.reset();
  }
  return ret;
}

int ObStorageCosUtil::del_file(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else if (OB_FAIL(cos_base.delete_object(uri))) {
      OB_LOG(WARN, "fail to get object meta", K(ret));
    } else {
      OB_LOG(DEBUG, "succ to delete object", K(uri));
    }
    cos_base.reset();
  }
  return ret;
}

int ObStorageCosUtil::list_files(
    const ObString &uri,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    char dir_path_array[OB_MAX_URI_LENGTH] = {0};
    const char *ptr = NULL; // point to actual dir path
    const int min_dir_path_len = 2; // cuz dir_path end with '\0'
    const char *separator = "/";
    const char end_marker = '\0';

    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else {
      const ObString &dir_path_str = cos_base.get_handle().get_object_name();
      const ObString::obstr_size_t dir_path_len = dir_path_str.length();
      int32_t actual_dir_path_len = dir_path_len;
      if (dir_path_len < min_dir_path_len) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "dir_path_str length is unepxected", K(ret), K(dir_path_str), K(dir_path_len));
      } else {
        // Let dir path end with '/'
        if (dir_path_str.ptr()[dir_path_len - 1] != end_marker) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "dir_path_str not end with '\0'");
        } else if (dir_path_str.ptr()[dir_path_len - 2] == *separator) {
          ptr = dir_path_str.ptr();
        } else if (OB_FAIL(databuff_printf(dir_path_array, OB_MAX_URI_LENGTH, "%s/", dir_path_str.ptr()))) {
          OB_LOG(WARN, "fail to deep copy dir path", K(ret), K(dir_path_len), K(OB_MAX_URI_LENGTH));
        } else {
          ptr = dir_path_array;
          actual_dir_path_len += 1;
        }
      }

      if (OB_SUCC(ret)) {
        ObString tmp_dir;
        tmp_dir.assign(const_cast<char *>(ptr), actual_dir_path_len);

        // Construct list object callback arg
        CosListFilesCbArg arg(allocator, tmp_dir, op);
        if (OB_FAIL(cos_base.list_objects(uri, tmp_dir, separator, arg))) {
          OB_LOG(WARN, "fail to list object in cos_base", K(ret), K(uri), K(tmp_dir));
        }
      }
    }
    cos_base.reset();
  }
  return ret;
}

int ObStorageCosUtil::list_directories(
    const ObString &uri,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    char dir_path_array[OB_MAX_URI_LENGTH] = {0};
    const char *ptr = NULL; // point to actual dir path
    const int min_dir_path_len = 2; // cuz dir_path end with '\0'
    const char *delimiter_string = "/";
    const char *next_marker_string = "";
    const char end_marker = '\0';

    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else {
      const ObString &dir_path_str = cos_base.get_handle().get_object_name();
      const ObString::obstr_size_t dir_path_len = dir_path_str.length();
      int32_t actual_dir_path_len = dir_path_len;
      if (dir_path_len < min_dir_path_len) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "dir_path_str length is unepxected", K(ret), K(dir_path_str), K(dir_path_len));
      } else {
        // Let dir path end with '/'
        if (dir_path_str.ptr()[dir_path_len - 1] != end_marker) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "dir_path_str not end with '\0'");
        } else if (dir_path_str.ptr()[dir_path_len - 2] == *delimiter_string) {
          ptr = dir_path_str.ptr();
        } else if (OB_FAIL(databuff_printf(dir_path_array, OB_MAX_URI_LENGTH, "%s/", dir_path_str.ptr()))) {
          OB_LOG(WARN, "fail to deep copy dir path", K(ret), K(dir_path_len), K(OB_MAX_URI_LENGTH));
        } else {
          ptr = dir_path_array;
          actual_dir_path_len += 1;
        }
      }

      if (OB_SUCC(ret)) {
        ObString tmp_dir;
        tmp_dir.assign(const_cast<char *>(ptr), actual_dir_path_len);

        CosListFilesCbArg arg(allocator, tmp_dir, op);
        if (OB_FAIL(cos_base.list_directories(uri, tmp_dir, next_marker_string, delimiter_string, arg))) {
          OB_LOG(WARN, "fail to list directories in cos_base", K(ret), K(uri), K(tmp_dir));
        }
      }
    }
    cos_base.reset();
  }
  return ret;
}

/*--------------------------------ObStorageCosBase---------------------------*/

ObStorageCosBase::ObStorageCosBase()
  : is_opened_(false), handle_()
{
}

ObStorageCosBase::~ObStorageCosBase()
{
  reset();
}

void ObStorageCosBase::reset()
{
  handle_.reset();
  is_opened_ = false;
}

int ObStorageCosBase::init_handle(const ObObjectStorageInfo &storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(handle_.is_inited())) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "handle in cos base already inited", K(ret));
  } else if (OB_FAIL(handle_.init(&storage_info))) {
    OB_LOG(WARN, "fail to init cos wrapper handle", K(ret));
  }
  return ret;
}

int ObStorageCosBase::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(uri.empty()) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(init_handle(*storage_info))) {
    OB_LOG(WARN, "failed to init cos wrapper handle", K(ret), K(uri));
  } else if (OB_FAIL(handle_.create_cos_handle())) {
    OB_LOG(WARN, "failed to create cos handle", K(ret), K(uri));
  } else if (OB_FAIL(handle_.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build bucket and object name", K(ret), K(uri));
  }
  return ret;
}

int ObStorageCosBase::get_cos_file_meta(
    bool &is_file_exist,
    qcloud_cos::CosObjectMeta &obj_meta)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());

    is_file_exist = false;
    if (OB_FAIL(qcloud_cos::ObCosWrapper::head_object_meta(handle_.get_ptr(), bucket_name,
        object_name, is_file_exist, obj_meta))) {
      OB_LOG(WARN, "fail to get object meta", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::delete_object(const ObString &uri)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());

    if (ObIStorageUtil::DELETE == handle_.get_delete_mode()) {
      if (OB_FAIL(qcloud_cos::ObCosWrapper::del(handle_.get_ptr(), bucket_name, object_name))) {
        OB_LOG(WARN, "fail to delete object meta", K(ret), K(uri));
      }
    } else if (ObIStorageUtil::TAGGING == handle_.get_delete_mode()) {
      if (OB_FAIL(qcloud_cos::ObCosWrapper::tag(handle_.get_ptr(), bucket_name, object_name))) {
        OB_LOG(WARN, "fail to tag object", K(ret), K(uri));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "cos delete mode invalid", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::list_objects(
    const ObString &uri,
    const ObString &dir_name_str,
    const char *separator,
    CosListFilesCbArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(separator)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer dir_name(dir_name_str.ptr(), dir_name_str.length());
    qcloud_cos::CosStringBuffer next_marker(separator, strlen(separator) + 1);
    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_objects(handle_.get_ptr(), bucket_name,
        dir_name, next_marker, handle_object_name_cb, (void *)(&arg)))) {
      OB_LOG(WARN, "fail to list objects", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::list_directories(
    const ObString &uri,
    const ObString &dir_name_str,
    const char *next_marker_str,
    const char *delimiter_str,
    CosListFilesCbArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_marker_str) || OB_ISNULL(delimiter_str)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(next_marker_str), KP(delimiter_str));
  } else if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer dir_name(dir_name_str.ptr(), dir_name_str.length());
    qcloud_cos::CosStringBuffer next_marker(next_marker_str, strlen(next_marker_str) + 1);
    qcloud_cos::CosStringBuffer delimiter(delimiter_str, strlen(delimiter_str) + 1);
    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_directories(handle_.get_ptr(), bucket_name, dir_name,
        next_marker, delimiter, handle_directory_name_cb, (void *)(&arg)))) {
      OB_LOG(WARN, "failed to list directories", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::is_object_tagging(
    const ObString &uri,
    bool &is_tagging)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    if (OB_FAIL(qcloud_cos::ObCosWrapper::is_object_tagging(handle_.get_ptr(),
        bucket_name, object_name, is_tagging))) {
      OB_LOG(WARN, "fail to check object tagging", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

/*--------------------------------ObStorageCosReader---------------------------*/

ObStorageCosReader::ObStorageCosReader()
  : ObStorageCosBase(), file_length_(-1)
{
}

ObStorageCosReader::~ObStorageCosReader()
{
}

int ObStorageCosReader::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos reader already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri));
  } else {
    const ObString &bucket_str = handle_.get_bucket_name();
    const ObString &object_str = handle_.get_object_name();
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        bucket_str.ptr(), bucket_str.length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        object_str.ptr(), object_str.length());
    bool is_file_exist = false;
    qcloud_cos::CosObjectMeta obj_meta;
    if (OB_FAIL(qcloud_cos::ObCosWrapper::head_object_meta(handle_.get_ptr(), bucket_name,
        object_name, is_file_exist, obj_meta))) {
      OB_LOG(WARN, "fail to get object meta", K(ret), K(bucket_str), K(object_str));
    } else if (!is_file_exist) {
      ret = OB_BACKUP_FILE_NOT_EXIST;
      OB_LOG(WARN, "backup file is not exist", K(ret), K(bucket_str), K(object_str));
    } else {
      file_length_ = obj_meta.file_length_;
      is_opened_ = true;
    }
  }
  return ret;
}

int ObStorageCosReader::pread(
    char *buf,
    const int64_t buf_size,
    int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (!is_opened_) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos reader cannot read before it is opened", K(ret));
  } else if (NULL == buf || buf_size <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_size), K(offset));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    if (OB_FAIL(qcloud_cos::ObCosWrapper::pread(handle_.get_ptr(), bucket_name,
        object_name, offset, buf, buf_size, read_size))) {
      OB_LOG(WARN, "fail to read object from cos", K(ret), KP(buf), K(buf_size), K(offset));
    }
  }
  return ret;
}

int ObStorageCosReader::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  file_length_ = -1;
  reset();
  return ret;
}

/*--------------------------------ObStorageCosWriter---------------------------*/

ObStorageCosWriter::ObStorageCosWriter()
  : ObStorageCosBase(), file_length_(-1)
{
}

ObStorageCosWriter::~ObStorageCosWriter()
{
  if (is_opened_) {
    close();
  }
}

int ObStorageCosWriter::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos writer already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri));
  } else {
    file_length_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObStorageCosWriter::close()
{
  int ret = OB_SUCCESS;
  file_length_ = -1;
  reset();
  return ret;
}

int ObStorageCosWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  UNUSED(offset);
  return ret;
}

int ObStorageCosWriter::write(const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "cos writer not opened", K(ret));
  } else if (NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    if (OB_FAIL(qcloud_cos::ObCosWrapper::put(handle_.get_ptr(), bucket_name,
        object_name, buf, size))) {
      OB_LOG(WARN, "fail to write object into cos", K(ret), KP(buf), K(size));
    } else {
      file_length_ += size;
    }
  }
  return ret;
}

/*--------------------------------ObStorageCosAppendWriter---------------------------*/

ObStorageCosAppendWriter::ObStorageCosAppendWriter()
  : ObStorageCosBase(),
    file_length_(-1)
{
}

ObStorageCosAppendWriter::~ObStorageCosAppendWriter()
{
}

int ObStorageCosAppendWriter::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos appender already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri));
  } else {
    file_length_ = 0;
    is_opened_ = true;
  }
  return ret;
}

int ObStorageCosAppendWriter::write(
    const char *buf,
    const int64_t size)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const int64_t fake_offset = 0;
  const bool is_pwrite = false;

  if(OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos append writer cannot write before it is not opened", K(ret));
  } else if(NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if (OB_FAIL(do_write(buf, size, fake_offset, is_pwrite))) {
    OB_LOG(WARN, "failed to do write", K(ret), KP(buf), K(size));
  }
  return ret;
}

int ObStorageCosAppendWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const bool is_pwrite = true;

  if(OB_UNLIKELY(!is_opened_)) {
    ret = OB_COS_ERROR;
    OB_LOG(WARN, "cos append writer cannot write before it is not opened", K(ret));
  } else if(NULL == buf || size < 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", KP(buf), K(size), K(ret));
  } else if (OB_FAIL(do_write(buf, size, offset, is_pwrite))) {
    OB_LOG(WARN, "failed to do write", K(ret), KP(buf), K(size), K(offset));
  }
  return ret;
}

int ObStorageCosAppendWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  file_length_ = -1;
  reset();
  return ret;
}

int ObStorageCosAppendWriter::do_write(
    const char *buf,
    const int64_t size,
    const int64_t offset,
    const bool is_pwrite)
{
  int ret = OB_SUCCESS;

  const int64_t start_time = ObTimeUtility::current_time();
  if(NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());

    int64_t pos = 0;
    bool is_exist = false;
    qcloud_cos::CosObjectMeta obj_meta;
    bool is_appendable = true;
    if (OB_FAIL(qcloud_cos::ObCosWrapper::head_object_meta(handle_.get_ptr(), bucket_name,
        object_name, is_exist, obj_meta))) {
      OB_LOG(WARN, "fail to get object meta", K(ret));
    } else if (is_exist) {
      pos = obj_meta.file_length_;
      is_appendable = (obj_meta.type_ == qcloud_cos::CosObjectMeta::COS_OBJ_APPENDABLE);
    }

    if (OB_FAIL(ret)) {
    } else if (!is_appendable) {
      ret = OB_CLOUD_OBJECT_NOT_APPENDABLE;
      OB_LOG(WARN, "we can only append an appendable obj", K(ret), K(is_appendable));
    } else if (is_pwrite && pos != offset) {
      ret = OB_BACKUP_PWRITE_OFFSET_NOT_MATCH;
      OB_LOG(WARN, "offset is not match with real length", K(ret), K(pos), K(offset), K(obj_meta.type_));
    } else if (OB_FAIL(qcloud_cos::ObCosWrapper::append(handle_.get_ptr(), bucket_name,
               object_name, buf, size, offset))) {
      OB_LOG(WARN, "fail to append object in cos", K(ret), KP(buf), K(size), K(offset), K(is_pwrite));

      // If append failed, print the current object meta, to help debugging.
      int tmp_ret = OB_SUCCESS;
      obj_meta.reset();
      is_exist = false;
      if (OB_TMP_FAIL(qcloud_cos::ObCosWrapper::head_object_meta(handle_.get_ptr(), bucket_name,
          object_name, is_exist, obj_meta))) {
        OB_LOG(WARN, "fail to get object meta", K(tmp_ret));
      } else {
        OB_LOG(INFO, "after append fail, we got the object meta", K(is_exist), K(obj_meta.type_),
          K(obj_meta.file_length_));
      }
    } else {
      file_length_ += size;
    }
  }
  return ret;
}

} //common
} //oceanbase
