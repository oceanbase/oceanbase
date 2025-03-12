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

#include "ob_storage_cos_base.h"
#include "ob_storage.h"

namespace oceanbase
{
namespace common
{
using namespace oceanbase::common;

/*--------------------------------GLOBAL---------------------------*/
int init_cos_env()
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(nullptr/*storage_info*/, "COS_GLOBAL_INIT", IO_HANDLED_SIZE_ZERO);
  return qcloud_cos::ObCosEnv::get_instance().init(ob_apr_abort_fn);
}

void fin_cos_env()
{
  int ret = OB_SUCCESS;
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
    ob_usleep(100 * 1000L); // 100ms
    flying_io_cnt = ObExternalIOCounter::get_flying_io_cnt();
  }

  OBJECT_STORAGE_GUARD(nullptr/*storage_info*/, "COS_GLOBAL_DESTROY", IO_HANDLED_SIZE_ZERO);
  qcloud_cos::ObCosEnv::get_instance().destroy();
}

struct CosListFilesCbArg
{
  common::ObIAllocator &allocator_;
  ObString &dir_path_;
  ObBaseDirEntryOperator &list_op_;

  CosListFilesCbArg(
    common::ObIAllocator &allocator,
    ObString &dir,
    ObBaseDirEntryOperator &op)
    : allocator_(allocator),
      dir_path_(dir),
      list_op_(op) {}

  ~CosListFilesCbArg() {}
  int get_dir_path_len() const
  {
    return dir_path_.empty() ? 0 : strlen(dir_path_.ptr());
  }
};

struct CosListFilesCtx
{
  common::ObIAllocator &allocator_;
  ObString &dir_path_;
  ObStorageListObjectsCtx &list_ctx_;

  CosListFilesCtx(
    common::ObIAllocator &allocator,
    ObString &dir,
    ObStorageListObjectsCtx &ctx)
    : allocator_(allocator),
      dir_path_(dir),
      list_ctx_(ctx) {}

  ~CosListFilesCtx() {}

  TO_STRING_KV(K_(dir_path), K_(list_ctx));
};

static int handle_object_name_cb(qcloud_cos::ObCosWrapper::CosListObjPara &para)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(para.arg_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret));
  } else if (OB_ISNULL(para.cur_obj_full_path_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "current object full path is empty", K(ret), K(para.cur_obj_full_path_));
  } else if (qcloud_cos::ObCosWrapper::CosListObjPara::CosListType::COS_LIST_CB_ARG == para.type_) {
    CosListFilesCbArg *ctx = static_cast<CosListFilesCbArg *>(para.arg_);

    // Returned object name is the whole object path, but we donot need the prefix dir_path.
    // So, we trim the object full path to get object name
    const int dir_name_str_len = ctx->get_dir_path_len();
    int64_t object_size = -1;
    if (OB_FAIL(c_str_to_int(para.cur_object_size_str_, para.cur_object_size_str_len_, object_size))) {
      OB_LOG(WARN, "fail to get listed cos object size", K(ret), K(para.cur_object_size_str_));
    } else if (OB_FAIL(handle_listed_object(ctx->list_op_,
                                            para.cur_obj_full_path_ + dir_name_str_len,
                                            para.full_path_size_ - dir_name_str_len,
                                            object_size))) {
      OB_LOG(WARN, "fail to handle listed cos object", K(ret), K(para.cur_obj_full_path_),
          K(dir_name_str_len), K(para.full_path_size_), K(object_size));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "not supported type", K(ret), K(para.type_));
  }

  return ret;
}

static int handle_list_object_ctx(qcloud_cos::ObCosWrapper::CosListObjPara &para)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(para.arg_)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret));
  } else if (qcloud_cos::ObCosWrapper::CosListObjPara::CosListType::COS_PART_LIST_CTX == para.type_) {
    CosListFilesCtx *ctx = static_cast<CosListFilesCtx *>(para.arg_);
    if (para.finish_part_list_) {
      if (OB_FAIL(ctx->list_ctx_.set_next_token(para.next_flag_,
                                                para.next_token_,
                                                para.next_token_size_))) {
        OB_LOG(WARN, "fail to set list ctx next token",
            K(ret), K(para.next_flag_), K(para.next_token_), K(para.next_token_size_));
      }
    } else {
      int64_t object_size = -1;
      if (OB_FAIL(c_str_to_int(para.cur_object_size_str_, para.cur_object_size_str_len_, object_size))) {
        OB_LOG(WARN, "fail to get listed cos object size", K(ret), K(para.cur_object_size_str_));
      } else if (OB_FAIL(ctx->list_ctx_.handle_object(para.cur_obj_full_path_,
                                                      para.full_path_size_,
                                                      object_size))) {
        OB_LOG(WARN, "fail to add listed cos obejct meta into list ctx",
            K(ret), K(para.cur_obj_full_path_), K(para.full_path_size_), K(object_size));
      }
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
    const char *dir_name,
    int64_t dir_name_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "arg is empty", K(ret), KP(arg));
  } else {
    if (qcloud_cos::ObCosWrapper::CosListObjPara::CosListType::COS_LIST_CB_ARG == type) {
      CosListFilesCbArg *ctx = static_cast<CosListFilesCbArg *>(arg);
      if (OB_FAIL(handle_listed_directory(ctx->list_op_, dir_name, dir_name_len))) {
        OB_LOG(WARN, "fail to handle cos directory name",
            K(ret), K(dir_name), K(dir_name_len));
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
    ret = OB_OBJECT_STORAGE_IO_ERROR;
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
  is_exist = false;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else {
    is_exist = obj_meta.is_exist_;
  }
  return ret;
}

int ObStorageCosUtil::get_file_length(const ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  file_length = 0;
  ObStorageObjectMetaBase obj_meta;
  if (OB_FAIL(head_object_meta(uri, obj_meta))) {
    OB_LOG(WARN, "fail to head object meta", K(ret), K(uri));
  } else if (!obj_meta.is_exist_) {
    ret = OB_OBJECT_NOT_EXIST;
    OB_LOG(WARN, "object is not exist", K(ret), K(uri));
  } else {
    file_length = obj_meta.length_;
  }
  return ret;
}

int ObStorageCosUtil::head_object_meta(const ObString &uri, ObStorageObjectMetaBase &obj_meta)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(get_object_meta_(uri, obj_meta.is_exist_, obj_meta.length_))) {
    OB_LOG(WARN, "fail to get object meta", K(ret), K(uri));
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
    ret = OB_OBJECT_STORAGE_IO_ERROR;
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
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else if (OB_FAIL(cos_base.delete_object(uri))) {
      OB_LOG(WARN, "fail to delete object", K(ret), K(uri));
    } else {
      OB_LOG(DEBUG, "succ to delete object", K(uri));
    }
    cos_base.reset();
  }
  return ret;
}

int ObStorageCosUtil::batch_del_files(
    const ObString &uri,
    hash::ObHashMap<ObString, int64_t> &files_to_delete,
    ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else if (OB_FAIL(cos_base.delete_objects(uri, files_to_delete, failed_files_idx))) {
      OB_LOG(WARN, "fail to batch delete objects", K(ret), K(uri));
    } else {
      OB_LOG(DEBUG, "succ to batch delete objects", K(uri));
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
  ObStorageCosBase cos_base;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator(ObModIds::BACKUP);

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(cos_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open cos base", K(ret), K(uri), KPC_(storage_info));
  } else {
    const char *full_dir_path = cos_base.get_handle().get_object_name().ptr();
    const int64_t full_dir_path_len = cos_base.get_handle().get_object_name().length();
    ObString full_dir_path_str(full_dir_path_len, full_dir_path);

    // Construct list object callback arg
    CosListFilesCbArg arg(allocator, full_dir_path_str, op);
    if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
    } else if (OB_FAIL(cos_base.list_objects(uri, full_dir_path_str, arg))) {
      OB_LOG(WARN, "fail to list object in cos_base", K(ret), K(uri), K(full_dir_path_str));
    }
  }

  cos_base.reset();
  return ret;
}

int ObStorageCosUtil::list_files(
    const ObString &uri,
    ObStorageListCtxBase &ctx_base)
{
  int ret = OB_SUCCESS;
  ObStorageCosBase cos_base;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator(ObModIds::BACKUP);
  ObStorageListObjectsCtx &list_ctx = static_cast<ObStorageListObjectsCtx &>(ctx_base);

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty() || !list_ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri), K(list_ctx));
  } else if (OB_FAIL(cos_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open cos base", K(ret), K(uri), KPC_(storage_info));
  } else {
    const char *full_dir_path = cos_base.get_handle().get_object_name().ptr();
    const int64_t full_dir_path_len = cos_base.get_handle().get_object_name().length();
    ObString full_dir_path_str(full_dir_path_len, full_dir_path);

    // Construct list object context
    CosListFilesCtx arg(allocator, full_dir_path_str, list_ctx);
    if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
    } else if (OB_FAIL(cos_base.list_objects(uri, full_dir_path_str, list_ctx.next_token_, arg))) {
      OB_LOG(WARN, "fail to list object in cos_base",
          K(ret), K(list_ctx), K(uri), K(full_dir_path_str));
    }
  }

  cos_base.reset();
  return ret;
}

int ObStorageCosUtil::list_directories(
    const ObString &uri,
    ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObStorageCosBase cos_base;
  ObExternalIOCounterGuard io_guard;
  ObArenaAllocator allocator(ObModIds::BACKUP);

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(cos_base.inner_open(uri, storage_info_))) {
    OB_LOG(WARN, "fail to open cos base", K(ret), K(uri), KPC_(storage_info));
  } else {
    const char *delimiter_string = "/";
    const char *next_marker_string = "";
    const char *full_dir_path = cos_base.get_handle().get_object_name().ptr();
    const int64_t full_dir_path_len = cos_base.get_handle().get_object_name().length();
    ObString full_dir_path_str(full_dir_path_len, full_dir_path);

    CosListFilesCbArg arg(allocator, full_dir_path_str, op);
    if (OB_UNLIKELY(!is_null_or_end_with_slash(full_dir_path))) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "uri is not terminated with '/'", K(ret), K(uri), K(full_dir_path));
    } else if (OB_FAIL(cos_base.list_directories(uri, full_dir_path_str,
                                                 next_marker_string, delimiter_string, arg))) {
      OB_LOG(WARN, "fail to list directories in cos_base", K(ret), K(uri), K(full_dir_path_str));
    }
  }

  cos_base.reset();
  return ret;
}

int ObStorageCosUtil::del_unmerged_parts(const ObString &uri)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos util not opened", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(uri));
  } else {
    ObStorageCosBase cos_base;
    if (OB_FAIL(cos_base.open(uri, storage_info_))) {
      OB_LOG(WARN, "fail to open cos base", K(ret), K(uri));
    } else if (OB_FAIL(cos_base.del_unmerged_parts(uri))) {
      OB_LOG(WARN, "fail to del unmerged parts", K(ret), K(uri));
    } else {
      OB_LOG(DEBUG, "succ to delete object", K(uri));
    }
    cos_base.reset();
  }
  return ret;
}

/*--------------------------------ObStorageCosBase---------------------------*/

ObStorageCosBase::ObStorageCosBase()
  : is_opened_(false), handle_(), checksum_type_(ObStorageChecksumType::OB_MD5_ALGO)
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
// this function will check object name is empty or not
int ObStorageCosBase::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos base", K(ret), K(uri));
  } else if (OB_UNLIKELY(handle_.get_object_name().empty())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "object name is NULL", K(uri), K(ret));
      handle_.reset();
  }
  return ret;
}
// inner_open allow object name is empty
// this function can be used directly in list cases
int ObStorageCosBase::inner_open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri), KPC(storage_info));
  } else if (FALSE_IT(checksum_type_ = storage_info->get_checksum_type())) {
  } else if (OB_UNLIKELY(!is_cos_supported_checksum(checksum_type_))) {
    ret = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    OB_LOG(WARN, "that checksum algorithm is not supported for cos", K(ret), K_(checksum_type));
  } else if (OB_FAIL(init_handle(*storage_info))) {
    OB_LOG(WARN, "failed to init cos wrapper handle", K(ret), K(uri));
  } else if (OB_FAIL(handle_.create_cos_handle(checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO))) {
    OB_LOG(WARN, "failed to create cos handle", K(ret), K(uri));
  } else if (OB_FAIL(handle_.build_bucket_and_object_name(uri))) {
    OB_LOG(WARN, "failed to build bucket and object name", K(ret), K(uri));
  }
  if (OB_FAIL(ret)) {
    handle_.reset();
  }
#ifdef ERRSIM
    if (OB_NOT_NULL(storage_info) && (OB_SUCCESS != EventTable::EN_ENABLE_LOG_OBJECT_STORAGE_CHECKSUM_TYPE)) {
      OB_LOG(ERROR, "errsim backup io with checksum type", "checksum_type", storage_info->get_checksum_type_str());
    }
#endif
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

    if (ObStorageDeleteMode::STORAGE_DELETE_MODE == handle_.get_delete_mode()) {
      if (OB_FAIL(qcloud_cos::ObCosWrapper::del(handle_.get_ptr(), bucket_name, object_name))) {
        OB_LOG(WARN, "fail to delete object meta", K(ret), K(uri));
      }
    } else if (ObStorageDeleteMode::STORAGE_TAGGING_MODE == handle_.get_delete_mode()) {
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

int ObStorageCosBase::delete_objects(
    const ObString &uri,
    hash::ObHashMap<ObString, int64_t> &files_to_delete,
    ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  qcloud_cos::CosStringBuffer *objects_to_delete_list = nullptr;
  const char **succeed_deleted_objects_list = nullptr;
  int64_t *succeed_deleted_object_len_list = nullptr;
  const int64_t n_files_to_delete = files_to_delete.size();
  const int64_t objects_to_delete_list_size = sizeof(qcloud_cos::CosStringBuffer) * n_files_to_delete;
  const int64_t succeed_deleted_objects_list_size = sizeof(const char *) * n_files_to_delete;
  const int64_t succeed_deleted_object_len_list_size = sizeof(int64_t) * n_files_to_delete;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret), K(uri));
  } else if (OB_FAIL(check_files_map_validity(files_to_delete))) {
    OB_LOG(WARN, "files_to_delete is invalid", K(ret), K(uri));
  } else if (OB_UNLIKELY(ObStorageDeleteMode::STORAGE_TAGGING_MODE == handle_.get_delete_mode())) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "batch tagging is not supported", K(ret), K(uri), K(handle_.get_delete_mode()));
  } else if (OB_ISNULL(objects_to_delete_list =
      static_cast<qcloud_cos::CosStringBuffer *>(allocator.alloc(objects_to_delete_list_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for objects_to_delete_list",
        K(ret), K(uri), K(objects_to_delete_list_size));
  } else if (OB_ISNULL(succeed_deleted_objects_list =
      static_cast<const char **>(allocator.alloc(succeed_deleted_objects_list_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for succeed_deleted_objects_list",
        K(ret), K(uri), K(succeed_deleted_objects_list_size));
  } else if (OB_ISNULL(succeed_deleted_object_len_list =
      static_cast<int64_t *>(allocator.alloc(succeed_deleted_object_len_list_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for succeed_deleted_object_len_list",
        K(ret), K(uri), K(succeed_deleted_object_len_list_size));
  } else {
    int64_t idx = 0;
    hash::ObHashMap<ObString, int64_t>::const_iterator iter = files_to_delete.begin();
    while (OB_SUCC(ret) && iter != files_to_delete.end()) {
      objects_to_delete_list[idx].assign_ptr(iter->first.ptr(), iter->first.length());
      iter++;
      idx++;
    }

    int64_t n_succeed_deleted_objects = 0;
    const qcloud_cos::CosStringBuffer &bucket_name = handle_.get_bucket_name_str_buf();
    if (FAILEDx(qcloud_cos::ObCosWrapper::batch_del(handle_.get_ptr(),
        bucket_name, objects_to_delete_list, succeed_deleted_objects_list, succeed_deleted_object_len_list,
        n_files_to_delete, n_succeed_deleted_objects))) {
      OB_LOG(WARN, "fail to batch del objects", K(ret), K(uri), K(n_files_to_delete));
    } else if (n_succeed_deleted_objects < n_files_to_delete) {
      // The succeed_deleted_objects_list contains all the objects that were successfully deleted.
      // By comparing it to files_to_delete, we can identify the objects that failed to be deleted.
      for (int64_t i = 0; OB_SUCC(ret) && i < n_succeed_deleted_objects; i++) {
        if (OB_ISNULL(succeed_deleted_objects_list[i])) {
          ret = OB_OBJECT_STORAGE_IO_ERROR;
          OB_LOG(WARN, "returned object key is null",
              K(ret), K(i), K(n_succeed_deleted_objects), K(n_files_to_delete));
        }
        // COS returns the successfully deleted object in the structure of cos_string_t.
        // Since the data string in cos_string_t does not necessarily end with '\0',
        // we need to use the len in cos_string_t to construct ObString.
        else if (OB_FAIL(files_to_delete.erase_refactored(ObString(succeed_deleted_object_len_list[i], succeed_deleted_objects_list[i])))) {
          OB_LOG(WARN, "fail to erase succeed deleted object", K(ret),
              K(uri), K(i), K(succeed_deleted_objects_list[i]), K(succeed_deleted_object_len_list[i]), K(n_succeed_deleted_objects));
        }
      }

      if (FAILEDx(record_failed_files_idx(files_to_delete, failed_files_idx))) {
        OB_LOG(WARN, "fail to record failed del",
            K(ret), K(uri), K(n_files_to_delete), K(n_succeed_deleted_objects));
      }
    }
  }
  return ret;
}

int ObStorageCosBase::list_objects(
    const ObString &uri,
    const ObString &full_dir_path_str,
    CosListFilesCbArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer full_dir_path(full_dir_path_str.ptr(), full_dir_path_str.length());
    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_objects(handle_.get_ptr(),
        bucket_name, full_dir_path, handle_object_name_cb, (void *)(&arg)))) {
      OB_LOG(WARN, "fail to list objects", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::list_objects(
    const ObString &uri,
    const ObString &full_dir_path_str,
    const char *next_token,
    CosListFilesCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(next_token)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer full_dir_path(full_dir_path_str.ptr(), full_dir_path_str.length());
    qcloud_cos::CosStringBuffer next_marker(next_token, strlen(next_token));

    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_part_objects(handle_.get_ptr(), bucket_name,
        full_dir_path, next_marker, handle_list_object_ctx, (void *)(&ctx)))) {
      OB_LOG(WARN, "fail to list part objects", K(ret), K(uri), K(next_token), K(ctx));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

int ObStorageCosBase::list_directories(
    const ObString &uri,
    const ObString &full_dir_path_str,
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
    qcloud_cos::CosStringBuffer full_dir_path(full_dir_path_str.ptr(), full_dir_path_str.length());
    qcloud_cos::CosStringBuffer next_marker(next_marker_str, strlen(next_marker_str) + 1);
    qcloud_cos::CosStringBuffer delimiter(delimiter_str, strlen(delimiter_str) + 1);

    if (OB_FAIL(qcloud_cos::ObCosWrapper::list_directories(handle_.get_ptr(), bucket_name,
        full_dir_path, next_marker, delimiter, handle_directory_name_cb, (void *)(&arg)))) {
      OB_LOG(WARN, "failed to list directories",
          K(ret), K(uri), K(next_marker_str), K(delimiter_str));
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

int ObStorageCosBase::del_unmerged_parts(const ObString &uri)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_valid())) {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    if (OB_FAIL(qcloud_cos::ObCosWrapper::del_unmerged_parts(handle_.get_ptr(),
                                                             bucket_name, object_name))) {
      OB_LOG(WARN, "fail to del unmerged parts", K(ret), K(uri));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cos wrapper handle not init or create", K(ret));
  }
  return ret;
}

/*--------------------------------ObStorageCosReader---------------------------*/

ObStorageCosReader::ObStorageCosReader()
  : ObStorageCosBase(), has_meta_(false), file_length_(-1)
{
}

ObStorageCosReader::~ObStorageCosReader()
{
}

int ObStorageCosReader::open(const ObString &uri,
    ObObjectStorageInfo *storage_info, const bool head_meta)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos reader already open, cannot open again", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri));
  } else {
    if (head_meta) {
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
        ret = OB_OBJECT_NOT_EXIST;
        OB_LOG(WARN, "object is not exist", K(ret), K(bucket_str), K(object_str));
      } else {
        file_length_ = obj_meta.file_length_;
        has_meta_ = true;
      }
    }

    if (OB_SUCC(ret)) {
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
  ObCosMemAllocator allocator;
  qcloud_cos::ObCosWrapper::Handle *tmp_cos_handle = nullptr;
  ObExternalIOCounterGuard io_guard;

  if (!is_opened_) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos reader cannot read before it is opened", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_size), K(offset));
    // The created cos_handle contains a memory allocator that is not thread-safe and
    // cannot be used concurrently. As the allocator is not designed to handle concurrent calls,
    // using it in parallel (such as calling reader.pread simultaneously from multiple threads)
    // can lead to race conditions, undefined behavior, and potential crashes (core dumps).
    // To maintain thread safety, a new temporary cos_handle should be created for each individual
    // pread operation rather than reusing the same handle. This approach ensures that memory
    // allocation is safely performed without conflicts across concurrent operations.
  } else if (OB_FAIL(handle_.create_cos_handle(
      allocator, checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO, tmp_cos_handle))) {
    OB_LOG(WARN, "fail to create tmp cos handle", K(ret), K_(checksum_type));
  } else {
    // When is_range_read is true, it indicates that only a part of the data is read.
    // When false, it indicates that the entire object is read
    bool is_range_read = true;
    int64_t get_data_size = buf_size;
    if (has_meta_) {
      if (file_length_ < offset) {
        ret = OB_FILE_LENGTH_INVALID;
        OB_LOG(WARN, "File lenth is invilid", K_(file_length), K(offset),
            K(handle_.get_bucket_name()), K(handle_.get_object_name()), K(ret));
      } else {
        get_data_size = MIN(buf_size, file_length_ - offset);
        if (get_data_size == file_length_) {
          // read entire object
          is_range_read = false;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (get_data_size == 0) {
      read_size = 0;
    } else {
      qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
          handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
      qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
          handle_.get_object_name().ptr(), handle_.get_object_name().length());

      if (OB_FAIL(qcloud_cos::ObCosWrapper::pread(tmp_cos_handle, bucket_name,
          object_name, offset, buf, get_data_size, is_range_read, read_size))) {
        OB_LOG(WARN, "fail to read object from cos", K(ret), K(is_range_read),
            KP(buf), K(buf_size), K(offset), K(get_data_size), K_(has_meta));
      }
    }
    if (OB_NOT_NULL(tmp_cos_handle)) {
      qcloud_cos::ObCosWrapper::destroy_cos_handle(tmp_cos_handle);
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
    ret = OB_OBJECT_STORAGE_IO_ERROR;
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
  ObExternalIOCounterGuard io_guard;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "cos writer not opened", K(ret));
  } else if (NULL == buf || size < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
#ifdef ERRSIM
  } else if (OB_FAIL(EventTable::EN_OBJECT_STORAGE_CHECKSUM_ERROR)) {
    OB_LOG(WARN, "fake checksum error", K(ret));
#endif
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
    ret = OB_OBJECT_STORAGE_IO_ERROR;
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
  int ret = OB_NOT_SUPPORTED;
  UNUSED(buf);
  UNUSED(size);
  return ret;
}

int ObStorageCosAppendWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const bool is_pwrite = true;

  if(OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos append writer cannot write before it is not opened", K(ret));
  } else if(NULL == buf || size <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KP(buf), K(size), K(ret), K(offset));
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
      ret = OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH;
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

/*------------------------------ObStorageCosMultiPartWriter---------------------------*/
ObStorageCosMultiPartWriter::ObStorageCosMultiPartWriter()
  : ObStorageCosBase(),
    mod_(ObModIds::BACKUP),
    allocator_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
    base_buf_(NULL),
    base_buf_pos_(0),
    upload_id_(NULL),
    partnum_(0),
    file_length_(-1),
    complete_part_list_(nullptr)
{}

ObStorageCosMultiPartWriter::~ObStorageCosMultiPartWriter()
{
  destroy();
}

void ObStorageCosMultiPartWriter::reuse()
{
  if (is_opened_) {
    if (nullptr != upload_id_) {
      handle_.free_mem(static_cast<void *>(upload_id_));
    }
    if (nullptr != base_buf_) {
      handle_.free_mem(static_cast<void *>(base_buf_));
    }
  }
  upload_id_ = nullptr;
  base_buf_ = nullptr;
  partnum_ = 0;
  file_length_ = -1;
  complete_part_list_ = nullptr;
  reset_part_info();
  ObStorageCosBase::reset();
}

int ObStorageCosMultiPartWriter::open(const ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "uri is empty", K(ret), K(uri));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri));
  } else if (OB_FAIL(ObStoragePartInfoHandler::init())) {
    OB_LOG(WARN, "fail to init part info handler", K(ret), K(uri));
  } else {
    const ObString &bucket_name_str = handle_.get_bucket_name();
    const ObString &object_name_str = handle_.get_object_name();
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        bucket_name_str.ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        object_name_str.ptr(), handle_.get_object_name().length());

    if (OB_FAIL(qcloud_cos::ObCosWrapper::init_multipart_upload(handle_.get_ptr(),
        bucket_name, object_name, upload_id_, complete_part_list_))) {
      OB_LOG(WARN, "fail to init multipartupload", K(ret), K(bucket_name_str), K(object_name_str));
    } else {
      if (OB_ISNULL(upload_id_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "upload_id should not be null", K(ret));
      } else if (OB_ISNULL(base_buf_ = static_cast<char *>(handle_.alloc_mem(COS_MULTIPART_UPLOAD_BUF_SIZE)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OB_LOG(WARN, "fail to alloc buffer for cos multipartupload", K(ret));
      } else {
        is_opened_ = true;
        base_buf_pos_ = 0;
        file_length_ = 0;
      }
    }
  }
  return ret;
}

int ObStorageCosMultiPartWriter::write(const char * buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  int64_t fill_size = 0;
  int64_t buf_pos = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write cos should open first", K(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "buf is NULL or size is invalid", K(ret), KP(buf), K(size));
  }

  while (OB_SUCC(ret) && buf_pos != size) {
    fill_size = std::min(COS_MULTIPART_UPLOAD_BUF_SIZE - base_buf_pos_, size - buf_pos);
    memcpy(base_buf_ + base_buf_pos_, buf + buf_pos, fill_size);
    base_buf_pos_ += fill_size;
    buf_pos += fill_size;
    if (base_buf_pos_ == COS_MULTIPART_UPLOAD_BUF_SIZE) {
      if (OB_FAIL(write_single_part())) {
        OB_LOG(WARN, "fail to write part into cos", K(ret));
      } else {
        base_buf_pos_ = 0;
      }
    }
  }

  // actually, current file size may be smaller than @size. Cuz we may not finish
  // the complete multipartupload.
  if (OB_SUCC(ret)) {
    file_length_ += size;
  }
  return ret;
}

int ObStorageCosMultiPartWriter::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  UNUSED(offset);
  return write(buf, size);
}

int ObStorageCosMultiPartWriter::write_single_part()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const char *etag_header_str = nullptr;

  ++partnum_;
  if (partnum_ > COS_MAX_PART_NUM) {
    ret = OB_OUT_OF_ELEMENT;
    OB_LOG(WARN, "Out of cos element ", K(ret), K_(partnum));
  } else if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write cos should open first", K(ret));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    qcloud_cos::CosStringBuffer upload_id_str = qcloud_cos::CosStringBuffer(
        upload_id_, strlen(upload_id_));
    if (OB_FAIL(qcloud_cos::ObCosWrapper::upload_part_from_buffer(handle_.get_ptr(), bucket_name,
        object_name, upload_id_str, partnum_, base_buf_, base_buf_pos_, etag_header_str))) {
      OB_LOG(WARN, "fail to upload part to cos", K(ret),
          KP_(upload_id), K(handle_.get_bucket_name()), K(handle_.get_object_name()), K_(partnum));
    } else if (OB_FAIL(add_part_info(partnum_, etag_header_str, nullptr/*checksum*/))) {
      OB_LOG(WARN, "fail to add part info", K(ret), K_(upload_id), K_(partnum), K(etag_header_str),
          K(handle_.get_bucket_name()), K(handle_.get_object_name()));
    }
  }
  return ret;
}

static int construct_complete_part_list(
    qcloud_cos::ObCosWrapper::Handle *h,
    const ObStoragePartInfoHandler::PartInfoMap &part_info_map,
    void *complete_part_list)
{
  int ret = OB_SUCCESS;
  const int64_t max_part_id = part_info_map.size();
  ObStoragePartInfoHandler::PartInfo tmp_part_info;

  // allow to be empty parts
  if (OB_ISNULL(h) || OB_UNLIKELY(max_part_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), KP(h), K(max_part_id));
  }
  for (int64_t i = 1; OB_SUCC(ret) && i <= max_part_id; i++) {
    if (OB_FAIL(part_info_map.get_refactored(i, tmp_part_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        OB_LOG(WARN, "part ids should be 1 ~ max_part_id", K(ret), K(i), K(max_part_id));
      } else {
        OB_LOG(WARN, "fail to get part info", K(ret), K(i), K(max_part_id));
      }
    } else if (OB_ISNULL(tmp_part_info.first)) {  // etag
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "etag is null", K(ret), K(i), K(max_part_id));
    } else if (OB_FAIL(qcloud_cos::ObCosWrapper::add_part_info(
          h, complete_part_list, i, tmp_part_info.first))) {
      OB_LOG(WARN, "fail to add part info to cos",
          K(ret), K(i), K(max_part_id), K(tmp_part_info.first));
    }
  }
  return ret;
}

int ObStorageCosMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  const int64_t start_time = ObTimeUtility::current_time();
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos multipart writer cannot close before it is opened", K(ret));
  } else if (0 != base_buf_pos_) {
    if (OB_FAIL(write_single_part())) {
      OB_LOG(WARN, "fail to write the last size to cos", K(ret), K_(base_buf_pos));
    } else {
      base_buf_pos_ = 0;
    }
  }

  if (FAILEDx(construct_complete_part_list(handle_.get_ptr(), part_info_map_, complete_part_list_))) {
    OB_LOG(WARN, "fail to construct complete part list",
          K(ret), K_(upload_id), KP_(complete_part_list),
          K(handle_.get_bucket_name()), K(handle_.get_object_name()));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    qcloud_cos::CosStringBuffer upload_id_str = qcloud_cos::CosStringBuffer(
        upload_id_, strlen(upload_id_));

    if (OB_FAIL(qcloud_cos::ObCosWrapper::complete_multipart_upload(handle_.get_ptr(), bucket_name,
        object_name, upload_id_str, complete_part_list_))) {
      OB_LOG(WARN, "fail to complete multipart upload",
          K(ret), K_(upload_id), KP_(complete_part_list),
          K(handle_.get_bucket_name()), K(handle_.get_object_name()));
    }
  }

  const int64_t total_cost_time = ObTimeUtility::current_time() - start_time;
  if (total_cost_time > 3 * 1000 * 1000) {
    OB_LOG_RET(WARN, OB_ERR_TOO_MUCH_TIME, "cos multipart writer complete cost too much time",
        K(total_cost_time), K(ret));
  }
  return ret;
}

int ObStorageCosMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  reuse();
  return ret;
}

int ObStorageCosMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos multipart writer cannot abort before it is opened", K(ret));
  } else {
    qcloud_cos::CosStringBuffer bucket_name = qcloud_cos::CosStringBuffer(
        handle_.get_bucket_name().ptr(), handle_.get_bucket_name().length());
    qcloud_cos::CosStringBuffer object_name = qcloud_cos::CosStringBuffer(
        handle_.get_object_name().ptr(), handle_.get_object_name().length());
    qcloud_cos::CosStringBuffer upload_id_str = qcloud_cos::CosStringBuffer(
        upload_id_, strlen(upload_id_));

    if (OB_FAIL(qcloud_cos::ObCosWrapper::abort_multipart_upload(handle_.get_ptr(), bucket_name,
        object_name, upload_id_str))) {
      OB_LOG(WARN, "fail to abort multipart upload", K(ret), KP_(upload_id));
    }
  }
  return ret;
}

ObStorageParallelCosMultiPartWriter::ObStorageParallelCosMultiPartWriter()
  : ObStorageCosBase(),
    upload_id_(nullptr),
    upload_id_str_buf_(),
    complete_part_list_(nullptr)
{}

ObStorageParallelCosMultiPartWriter::~ObStorageParallelCosMultiPartWriter()
{
  destroy_();
}

void ObStorageParallelCosMultiPartWriter::reuse_()
{
  if (is_opened_) {
    if (OB_NOT_NULL(upload_id_)) {
      handle_.free_mem(static_cast<void *>(upload_id_));
    }
  }
  upload_id_ = nullptr;
  upload_id_str_buf_.assign_ptr(nullptr, 0);
  complete_part_list_ = nullptr;
  reset_part_info();
  ObStorageCosBase::reset();
}

int ObStorageParallelCosMultiPartWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "already open, cannot open again", K(ret));
  } else if (OB_FAIL(ObStorageCosBase::open(uri, storage_info))) {
    OB_LOG(WARN, "fail to open in cos_base", K(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStoragePartInfoHandler::init())) {
    OB_LOG(WARN, "fail to init part info handler", K(ret), K(uri));
  } else {
    if (OB_FAIL(qcloud_cos::ObCosWrapper::init_multipart_upload(
        handle_.get_ptr(),
        handle_.get_bucket_name_str_buf(),
        handle_.get_object_name_str_buf(),
        upload_id_, complete_part_list_))) {
      OB_LOG(WARN, "fail to init multipartupload",
          K(ret), K(handle_.get_bucket_name()), K(handle_.get_object_name()));
    } else {
      if (OB_ISNULL(upload_id_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "upload_id should not be null",
            K(ret), K(handle_.get_bucket_name()), K(handle_.get_object_name()));
      } else {
        is_opened_ = true;
        upload_id_str_buf_.assign_ptr(upload_id_, strlen(upload_id_));
      }
    }
  }

  if (OB_FAIL(ret)) {
    reuse_();
  }
  return ret;
}

int ObStorageParallelCosMultiPartWriter::upload_part(
    const char *buf, const int64_t size, const int64_t part_id)
{
  int ret = OB_SUCCESS;
  ObCosMemAllocator allocator;
  qcloud_cos::ObCosWrapper::Handle *tmp_cos_handle = nullptr;
  ObExternalIOCounterGuard io_guard;
  const char *etag_header_str = nullptr;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write cos should open first", K(ret));
  } else if (OB_UNLIKELY(part_id < 1 || part_id > COS_MAX_PART_NUM)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "out of cos part num effective range", K(ret), K(part_id));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(buf), K(size));
    // The cos_handle is not suitable for concurrent scenarios,
    // thus it needs to be recreated for each upload_part operation.
  } else if (OB_FAIL(handle_.create_tmp_cos_handle(
      allocator, checksum_type_ == ObStorageChecksumType::OB_MD5_ALGO, tmp_cos_handle))) {
    OB_LOG(WARN, "fail to create tmp cos handle", K(ret), K_(checksum_type));
  } else if (OB_FAIL(qcloud_cos::ObCosWrapper::upload_part_from_buffer(
      tmp_cos_handle,
      handle_.get_bucket_name_str_buf(),
      handle_.get_object_name_str_buf(),
      upload_id_str_buf_, part_id, buf, size, etag_header_str))) {
    OB_LOG(WARN, "fail to upload part to cos",
        K(ret), K_(upload_id), K(handle_.get_bucket_name()), K(handle_.get_object_name()));
  } else if (OB_FAIL(add_part_info(part_id, etag_header_str, nullptr/*checksum*/))) {
    OB_LOG(WARN, "fail to add part info", K(ret), K_(upload_id), K(part_id), K(etag_header_str),
        K(handle_.get_bucket_name()), K(handle_.get_object_name()));
  }

  if (OB_NOT_NULL(tmp_cos_handle)) {
    qcloud_cos::ObCosWrapper::destroy_cos_handle(tmp_cos_handle);
  }
  allocator.reuse();
  return ret;
}

int ObStorageParallelCosMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos multipart writer cannot close before it is opened", K(ret));
  } else if (FAILEDx(construct_complete_part_list(handle_.get_ptr(),
                                                  part_info_map_,
                                                  complete_part_list_))) {
    OB_LOG(WARN, "fail to construct complete part list",
          K(ret), K_(upload_id), KP_(complete_part_list),
          K(handle_.get_bucket_name()), K(handle_.get_object_name()));
  } else {
    if (OB_FAIL(qcloud_cos::ObCosWrapper::complete_multipart_upload(
        handle_.get_ptr(),
        handle_.get_bucket_name_str_buf(),
        handle_.get_object_name_str_buf(),
        upload_id_str_buf_, complete_part_list_))) {
      OB_LOG(WARN, "fail to complete multipart upload",
          K(ret), K_(upload_id), KP_(complete_part_list),
          K(handle_.get_bucket_name()), K(handle_.get_object_name()));
    }
  }

  return ret;
}

int ObStorageParallelCosMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "cos multipart writer cannot abort before it is opened", K(ret));
  } else {
    if (OB_FAIL(qcloud_cos::ObCosWrapper::abort_multipart_upload(
        handle_.get_ptr(),
        handle_.get_bucket_name_str_buf(),
        handle_.get_object_name_str_buf(),
        upload_id_str_buf_))) {
      OB_LOG(WARN, "fail to abort multipart upload",
          K(ret), K_(upload_id), K(handle_.get_bucket_name()), K(handle_.get_object_name()));
    }
  }
  return ret;
}

int ObStorageParallelCosMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  ObExternalIOCounterGuard io_guard;
  reuse_();
  return ret;
}

} //common
} //oceanbase
