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

#include "ob_object_device.h"

namespace oceanbase
{
namespace common
{

const char *OB_STORAGE_ACCESS_TYPES_STR[] = {
    "reader", "nohead_reader", "adaptive_reader",
    "overwriter", "appender", "multipart_writer",
    "direct_multipart_writer", "buffered_multipart_writer"};

const char *get_storage_access_type_str(const ObStorageAccessType &type)
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(static_cast<int64_t>(OB_STORAGE_ACCESS_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_ACCESS_TYPES_STR), "ObStorageAccessType count mismatch");
  if (type >= OB_STORAGE_ACCESS_READER && type < OB_STORAGE_ACCESS_MAX_TYPE) {
    str = OB_STORAGE_ACCESS_TYPES_STR[type];
  }
  return str;
}

ObObjectDevice::ObObjectDevice()
  : storage_info_(), is_started_(false), lock_(common::ObLatchIds::OBJECT_DEVICE_LOCK),
    storage_id_mod_()
{
  ObMemAttr attr = SET_USE_500("ObjectDevice");
  reader_ctx_pool_.set_attr(attr);
  adaptive_reader_ctx_pool_.set_attr(attr);
  appender_ctx_pool_.set_attr(attr);
  overwriter_ctx_pool_.set_attr(attr);
  multipart_writer_ctx_pool_.set_attr(attr);
  direct_multiwriter_ctx_pool_.set_attr(attr);
  buffered_multiwriter_ctx_pool_.set_attr(attr);
}

int ObObjectDevice::init(const ObIODOpts &opts)
{
  /*dev env has init in device manager, since object device is multiple ins*/
  UNUSED(opts);
  return OB_SUCCESS;
}

void ObObjectDevice::destroy()
{
  /*dev env will destory in device manager*/
  if (is_started_) {
    appender_ctx_pool_.reset();
    reader_ctx_pool_.reset();
    adaptive_reader_ctx_pool_.reset();
    overwriter_ctx_pool_.reset();
    multipart_writer_ctx_pool_.reset();
    direct_multiwriter_ctx_pool_.reset();
    buffered_multiwriter_ctx_pool_.reset();
    //close the util
    util_.close();
    //baseinfo will be free with allocator
    is_started_ = false;
    storage_id_mod_.reset();
  }
}

ObObjectDevice::~ObObjectDevice()
{
  destroy();
  OB_LOG(INFO, "destory the device!", KP(storage_info_str_));
}

/*the app logical use call ObBackupIoAdapter::get_and_init_device*/
/*decription: base_info just related to storage_info,
  base_info is used by the reader/appender*/
int ObObjectDevice::start(const ObIODOpts &opts)
{
  int32_t ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (is_started_) {
    //has inited, no need init again, do nothing
  } else if ((1 != opts.opt_cnt_ && 5 != opts.opt_cnt_ && 6 != opts.opt_cnt_) || NULL == opts.opts_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to start device, args cnt is wrong!", K(opts.opt_cnt_), K(ret));
  } else if (0 != STRCMP(opts.opts_[0].key_, "storage_info")) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to start device, args wrong !", KCSTRING(opts.opts_[0].key_), K(ret));
  } else {
    if (OB_FAIL(storage_info_.set(device_type_, opts.opts_[0].value_.value_str))) {
      OB_LOG(WARN, "failed to build storage_info");
    }

    if (OB_SUCCESS != ret) {
      //mem resource will be free with device destroy
    } else if (OB_FAIL(util_.open(&storage_info_))) {
      OB_LOG(WARN, "fail to open the util!", K(ret), KP(opts.opts_[0].value_.value_str));
    } else if (OB_FAIL(fd_mng_.init())) {
      OB_LOG(WARN, "fail to init fd manager!", K(ret));
    } else {
      is_started_ = true;
      if (OB_FAIL(databuff_printf(storage_info_str_, OB_MAX_URI_LENGTH, "%s", opts.opts_[0].value_.value_str))) {
        OB_LOG(WARN, "fail to copy str to storage info", K(ret));
      }
    }
  }
  return ret;
}

void get_opt_value(ObIODOpts *opts, const char* key, const char*& value)
{
  if (OB_ISNULL(opts) || OB_ISNULL(key)) {
    value = NULL;
  } else {
    value = NULL;
    for (int i = 0; i < opts->opt_cnt_; i++) {
      if (0 == STRCMP(opts->opts_[i].key_, key)) {
        value = opts->opts_[i].value_.value_str;
        break;
      }
    }
  }
}

void get_opt_value(ObIODOpts *opts, const char* key, int64_t& value)
{
  if (OB_ISNULL(opts) || OB_ISNULL(key)) {
    //do nothing
  } else {
    for (int i = 0; i < opts->opt_cnt_; i++) {
      if (0 == STRCMP(opts->opts_[i].key_, key)) {
        value = opts->opts_[i].value_.value_int64;
        break;
      }
    }
  }
}

int ObObjectDevice::get_access_type(ObIODOpts *opts, ObStorageAccessType& access_type_flag)
{
  int ret = OB_SUCCESS;
  ObOptValue opt_value;
  const char* access_type = NULL;
  get_opt_value(opts, "AccessType", access_type);

  if (NULL == access_type) {
    OB_LOG(WARN, "can not find access type!");
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_READER])) {
    access_type_flag = OB_STORAGE_ACCESS_READER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_NOHEAD_READER])) {
    access_type_flag = OB_STORAGE_ACCESS_NOHEAD_READER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_ADAPTIVE_READER])) {
    access_type_flag = OB_STORAGE_ACCESS_ADAPTIVE_READER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_OVERWRITER])) {
    access_type_flag = OB_STORAGE_ACCESS_OVERWRITER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_APPENDER])) {
    access_type_flag = OB_STORAGE_ACCESS_APPENDER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_MULTIPART_WRITER])) {
    access_type_flag = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER])) {
    access_type_flag = OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;
  } else if (0 == STRCMP(access_type , OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER])) {
    access_type_flag = OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invaild access type!", KCSTRING(access_type));
  }
  return ret;
}

int ObObjectDevice::open_for_reader(const char *pathname, void *&ctx, const bool head_meta)
{
  int ret = OB_SUCCESS;
  ObStorageReader *reader = reader_ctx_pool_.alloc();
  if (NULL == reader) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for object device reader! ", K(ret));
  } else {
    if (OB_FAIL(reader->open(pathname, &storage_info_, head_meta))) {
      OB_LOG(WARN, "fail to open for read!", K(ret));
    } else {
      ctx = (void*)reader;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(reader)) {
    reader_ctx_pool_.free(reader);
    reader = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_adaptive_reader_(const char *pathname, void *&ctx)
{
  int ret = OB_SUCCESS;
  ObStorageAdaptiveReader *adaptive_reader = adaptive_reader_ctx_pool_.alloc();
  if (OB_ISNULL(adaptive_reader)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for object device adaptive_reader! ", K(ret), K(pathname));
  } else {
    if (OB_FAIL(adaptive_reader->open(pathname, &storage_info_))) {
      OB_LOG(WARN, "fail to open for read!", K(ret), K(pathname), K_(storage_info));
    } else {
      ctx = (void*)adaptive_reader;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(adaptive_reader)) {
    adaptive_reader_ctx_pool_.free(adaptive_reader);
    adaptive_reader = nullptr;
  }
  return ret;
}

/*ObStorageOssMultiPartWriter is not used int the current version, if we use, later, the open func of
  overwriter maybe need to add para(just like the open func of appender)*/
int ObObjectDevice::open_for_overwriter(const char *pathname, void*& ctx)
{
  int ret = OB_SUCCESS;
  ObStorageWriter *overwriter = overwriter_ctx_pool_.alloc();
  if (NULL == overwriter) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc mem for object device reader! ", K(ret));
  } else {
    if (OB_FAIL(overwriter->open(pathname, &storage_info_))) {
      OB_LOG(WARN, "fail to open for overwrite!", K(ret));
    } else {
      ctx = (void*)overwriter;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(overwriter)) {
    overwriter_ctx_pool_.free(overwriter);
    overwriter = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_appender(const char *pathname, ObIODOpts *opts, void*& ctx)
{
  int ret = OB_SUCCESS;
  ObStorageAppender *appender = NULL;
  ObOptValue opt_value;
  const char* open_mode = NULL;

  get_opt_value(opts, "OpenMode", open_mode);
  StorageOpenMode mode = StorageOpenMode::CREATE_OPEN_LOCK;

  appender = appender_ctx_pool_.alloc();
  if (OB_ISNULL(appender)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to alloc appender!",  K(ret));
  }

  if (NULL == open_mode || 0 == STRCMP(open_mode, "CREATE_OPEN_LOCK")) {
    //just keep the default value
  } else if (0 == STRCMP(open_mode, "EXCLUSIVE_CREATE")) {
    mode = StorageOpenMode::EXCLUSIVE_CREATE;
  } else if (0 == STRCMP(open_mode, "ONLY_OPEN_UNLOCK")) {
    mode = StorageOpenMode::ONLY_OPEN_UNLOCK;
  } else if (0 == STRCMP(open_mode, "CREATE_OPEN_NOLOCK")) {
    mode = StorageOpenMode::CREATE_OPEN_NOLOCK;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "Invalid open mode!", KCSTRING(open_mode), K(ret));
  }

  if (OB_SUCCESS == ret) {
    appender->set_open_mode(mode);
    if (OB_FAIL(appender->open(pathname, &storage_info_))){
      OB_LOG(WARN, "fail to open the appender!", K(ret));
    } else {
      ctx = appender;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(appender)) {
    appender_ctx_pool_.free(appender);
    appender = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_multipart_writer_(const char *pathname, void *&ctx)
{
  int ret = OB_SUCCESS;
  ObStorageMultiPartWriter *multipart_writer = multipart_writer_ctx_pool_.alloc();
  if (OB_ISNULL(multipart_writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc multipart_writer!", K(ret), K(pathname));
  } else {
    if (OB_FAIL(multipart_writer->open(pathname, &storage_info_))) {
      OB_LOG(WARN, "fail to open for multipart_writer!", K(ret), K(pathname), K_(storage_info));
    } else {
      ctx = (void*)multipart_writer;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(multipart_writer)) {
    multipart_writer_ctx_pool_.free(multipart_writer);
    multipart_writer = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_parallel_multipart_writer_(const char *pathname, void *&ctx)
{
  int ret = OB_SUCCESS;
  ObStorageDirectMultiPartWriter *multipart_writer = direct_multiwriter_ctx_pool_.alloc();
  if (OB_ISNULL(multipart_writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc multipart_writer!", K(ret), K(pathname));
  } else {
    if (OB_FAIL(multipart_writer->open(pathname, &storage_info_))) {
      OB_LOG(WARN, "fail to open for multipart_writer!", K(ret), K(pathname), K_(storage_info));
    } else {
      ctx = (void*)multipart_writer;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(multipart_writer)) {
    direct_multiwriter_ctx_pool_.free(multipart_writer);
    multipart_writer = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_buffered_multipart_writer_(const char *pathname, void *&ctx)
{
  int ret = OB_SUCCESS;
  ObStorageBufferedMultiPartWriter *multipart_writer = buffered_multiwriter_ctx_pool_.alloc();
  if (OB_ISNULL(multipart_writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc multipart_writer!", K(ret), K(pathname));
  } else {
    if (OB_FAIL(multipart_writer->open(pathname, &storage_info_))) {
      OB_LOG(WARN, "fail to open for multipart_writer!", K(ret), K(pathname), K_(storage_info));
    } else {
      ctx = (void*)multipart_writer;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(multipart_writer)) {
    buffered_multiwriter_ctx_pool_.free(multipart_writer);
    multipart_writer = nullptr;
  }
  return ret;
}

int ObObjectDevice::release_res(void *ctx, const ObIOFd &fd, ObStorageAccessType access_type)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  /*release the ctx*/
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "ctx is null, invald para!");
  } else {
    if (OB_STORAGE_ACCESS_APPENDER == access_type) {
      ObStorageAppender *appender = static_cast<ObStorageAppender*>(ctx);
      if (OB_FAIL(appender->close())) {
        OB_LOG(WARN, "fail to close the appender!", K(ret), K(access_type));
      }
      appender_ctx_pool_.free(appender);
    } else if (OB_STORAGE_ACCESS_READER == access_type
        || OB_STORAGE_ACCESS_NOHEAD_READER == access_type) {
      ObStorageReader *reader = static_cast<ObStorageReader*>(ctx);
      if (OB_FAIL(reader->close())) {
        OB_LOG(WARN, "fail to close the reader!", K(ret), K(access_type));
      }
      reader_ctx_pool_.free(reader);
    } else if (OB_STORAGE_ACCESS_ADAPTIVE_READER == access_type) {
      ObStorageAdaptiveReader *adaptive_reader = static_cast<ObStorageAdaptiveReader*>(ctx);
      if (OB_FAIL(adaptive_reader->close())) {
        OB_LOG(WARN, "fail to close the adaptive reader!", K(ret), K(access_type));
      }
      adaptive_reader_ctx_pool_.free(adaptive_reader);
    } else if (OB_STORAGE_ACCESS_OVERWRITER == access_type) {
      ObStorageWriter *overwriter = static_cast<ObStorageWriter*>(ctx);
      if (OB_FAIL(overwriter->close())) {
        OB_LOG(WARN, "fail to close the overwriter!", K(ret), K(access_type));
      }
      overwriter_ctx_pool_.free(overwriter);
    } else if (OB_STORAGE_ACCESS_MULTIPART_WRITER == access_type) {
      ObStorageMultiPartWriter *multipart_writer = static_cast<ObStorageMultiPartWriter*>(ctx);
      if (OB_FAIL(multipart_writer->close())) {
        OB_LOG(WARN, "fail to close the multipart writer!", K(ret), K(access_type));
      }
      multipart_writer_ctx_pool_.free(multipart_writer);
    } else if (OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type) {
      ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
      if (OB_FAIL(multipart_writer->close())) {
        OB_LOG(WARN, "fail to close the multipart writer!", K(ret), K(access_type));
      }
      direct_multiwriter_ctx_pool_.free(multipart_writer);
    } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type) {
      ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
      if (OB_FAIL(multipart_writer->close())) {
        OB_LOG(WARN, "fail to close the multipart writer!", K(ret), K(access_type));
      }
      buffered_multiwriter_ctx_pool_.free(multipart_writer);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid access_type!", K(access_type), K(ret));
    }

    if (OB_SUCCESS != (ret_tmp = fd_mng_.release_fd(fd))) {
      ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
      OB_LOG(WARN, "fail to release fd!", K(access_type), K(ret));
    }
  }
  return ret;
}

/*
*  mode is not used in object device
*/
int ObObjectDevice::open(const char *pathname, const int flags, const mode_t mode,
                         ObIOFd &fd, ObIODOpts *opts)
{
  UNUSED(flags);
  UNUSED(mode);
  int ret = OB_SUCCESS;
  void *ctx = NULL;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_MAX_TYPE;
  //validate fd
  if (fd_mng_.validate_fd(fd, false)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "fd should not be a valid one!", K(fd), K(ret));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "pathname is null!", K(ret));
  }

  //handle open logical
  if (OB_SUCC(ret)) {
    if(OB_ISNULL(opts)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "for object device, opts should not be null!", K(ret));
    } else if (OB_FAIL(get_access_type(opts, access_type))) {
      OB_LOG(WARN, "fail to get access type!", KCSTRING(pathname), K(ret));
    } else {
      if (OB_STORAGE_ACCESS_READER == access_type) {
        ret = open_for_reader(pathname, ctx, true/*head_meta*/);
      } else if (OB_STORAGE_ACCESS_NOHEAD_READER == access_type) {
        ret = open_for_reader(pathname, ctx, false/*head_meta*/);
      } else if (OB_STORAGE_ACCESS_ADAPTIVE_READER == access_type) {
        ret = open_for_adaptive_reader_(pathname, ctx);
      } else if (OB_STORAGE_ACCESS_APPENDER == access_type) {
        ret = open_for_appender(pathname, opts, ctx);
      } else if (OB_STORAGE_ACCESS_OVERWRITER == access_type) {
        ret = open_for_overwriter(pathname, ctx);
      } else if (OB_STORAGE_ACCESS_MULTIPART_WRITER == access_type) {
        ret = open_for_multipart_writer_(pathname, ctx);
      } else if (OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type) {
        ret = open_for_parallel_multipart_writer_(pathname, ctx);
      } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type) {
        ret = open_for_buffered_multipart_writer_(pathname, ctx);
      }

      if (OB_FAIL(ret)) {
        OB_LOG(WARN, "fail to open fd!", K(ret), KCSTRING(pathname), K(access_type));
      }
    }
  }


  //if success, alloc a fd, and bind with ctx
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fd_mng_.get_fd(ctx, device_type_, access_type, fd))) {
      OB_LOG(WARN, "fail to alloc fd!", K(ret), K(fd), KCSTRING(pathname), K(access_type));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(ctx)) {
    ObStorageAccesser *storage_accesser = static_cast<ObStorageAccesser *>(ctx);
    if (OB_FAIL(storage_accesser->init(fd, this))) {
      OB_LOG(WARN, "fail to set fd", K(ret), K(fd));
    } else {
      storage_accesser->inc_ref();
    }
  }

  //handle resource free when exception happen
  if (OB_FAIL(ret) && !OB_ISNULL(ctx)) {
    int tmp_ret = OB_SUCCESS;
    OB_LOG(WARN, "fail to open with access type!", K(ret), KCSTRING(pathname), K(access_type), K(ctx));
    if (OB_SUCCESS != (tmp_ret = release_res(ctx, fd, access_type))) {
      OB_LOG(WARN, "fail to release the resource!", K(tmp_ret));
    }
  } else {
    fd.device_handle_ = static_cast<ObIODevice *>(this);
    OB_LOG(DEBUG, "success to open file !", KCSTRING(pathname), K(access_type));
  }

  return ret;
}

int ObObjectDevice::complete(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret), K(fd));
  } else if (flag == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
    ObStorageMultiPartWriter *multipart_writer = static_cast<ObStorageMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->complete())) {
      OB_LOG(WARN, "fail to complete!", K(ret), K(flag));
    }
  } else if (flag == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
    ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->complete())) {
      OB_LOG(WARN, "fail to complete!", K(ret), K(flag));
    }
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->complete())) {
      OB_LOG(WARN, "fail to complete!", K(ret), K(flag));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a multipart writer fd!", K(flag), K(ret));
  }
  return ret;
}

int ObObjectDevice::abort(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret), K(fd));
  } else if (flag == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
    ObStorageMultiPartWriter *multipart_writer = static_cast<ObStorageMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->abort())) {
      OB_LOG(WARN, "fail to abort!", K(ret), K(flag));
    }
  } else if (flag == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
    ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->abort())) {
      OB_LOG(WARN, "fail to abort!", K(ret), K(flag));
    }
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->abort())) {
      OB_LOG(WARN, "fail to abort!", K(ret), K(flag));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a multipart writer fd!", K(flag), K(ret));
  }
  return ret;
}

int ObObjectDevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  void *ctx = nullptr;
  ObStorageAccesser *storage_accesser = nullptr;
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fail to close fd. since fd is invalid!", K(ret), K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "ctx is null", K(ret));
  } else {
    storage_accesser = static_cast<ObStorageAccesser *>(ctx);
    storage_accesser->dec_ref();
  }
  return ret;
}

int ObObjectDevice::release_fd(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  // make sure device's lifecycle is longger than fd and ctx.
  inc_ref();
  int flag = -1;
  void *ctx = NULL;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fail to close fd. since fd is invalid!", K(ret) ,K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_FAIL(release_res(ctx, fd, (ObStorageAccessType)flag))) {
    OB_LOG(WARN, "fail to release the resource!", K(ret), K(flag));
  }
  dec_ref();
  return ret;
}

int ObObjectDevice::seal_for_adaptive(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = NULL;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (OB_STORAGE_ACCESS_APPENDER == flag) {
    ObStorageAppender *appender = static_cast<ObStorageAppender*>(ctx);
    if (OB_FAIL(appender->seal_for_adaptive())) {
      OB_LOG(WARN, "fail to seal!", K(ret), K(flag));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a appender fd!", K(flag), K(ret));
  }
  return ret;
}

int ObObjectDevice::del_unmerged_parts(const char *pathname)
{
  int ret = OB_SUCCESS;
  common::ObString uri(pathname);
  if (OB_FAIL(util_.del_unmerged_parts(uri))) {
    OB_LOG(WARN, "fail to del unmerged parts", K(ret), K(uri));
  }
  return ret;
}

int ObObjectDevice::mkdir(const char *pathname, mode_t mode)
{
  UNUSED(mode);
  common::ObString uri(pathname);
  return util_.mkdir(uri);
}

int ObObjectDevice::rmdir(const char *pathname)
{
  common::ObString uri(pathname);
  return util_.del_dir(uri);
}

int ObObjectDevice::unlink(const char *pathname)
{
  return inner_unlink_(pathname, false/*is_adaptive*/);
}

int ObObjectDevice::adaptive_unlink(const char *pathname)
{
  const bool is_adaptive = true;
  return inner_unlink_(pathname, is_adaptive);
}

int ObObjectDevice::inner_unlink_(const char *pathname, const bool is_adaptive)
{
  int ret = OB_SUCCESS;
  common::ObString uri(pathname);
  if (OB_FAIL(util_.del_file(uri, is_adaptive))) {
    OB_LOG(WARN, "fail to del file", K(ret), K(uri), K(is_adaptive));
  }
  return ret;
}

int ObObjectDevice::batch_del_files(
    const ObIArray<ObString> &files_to_delete, ObIArray<int64_t> &failed_files_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(util_.batch_del_files(files_to_delete, failed_files_idx))) {
    OB_LOG(WARN, "fail to del file", K(ret));
  }
  return ret;
}

int ObObjectDevice::exist(const char *pathname, bool &is_exist)
{
  return inner_exist_(pathname, is_exist, false/*is_adaptive*/);
}

int ObObjectDevice::adaptive_exist(const char *pathname, bool &is_exist)
{
  const bool is_adaptive = true;
  return inner_exist_(pathname, is_exist, is_adaptive);
}

int ObObjectDevice::inner_exist_(const char *pathname, bool &is_exist, const bool is_adaptive)
{
  int ret = OB_SUCCESS;
  common::ObString uri(pathname);
  if (OB_FAIL(util_.is_exist(uri, is_adaptive, is_exist))) {
    OB_LOG(WARN, "fail to check if the file exists", K(ret), K(uri), K(is_adaptive));
  }
  return ret;
}

/*notice: for backup, this interface only return size*/
int ObObjectDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  return inner_stat_(pathname, statbuf, false/*is_adaptive*/);
}

int ObObjectDevice::adaptive_stat(const char *pathname, ObIODFileStat &statbuf)
{
  const bool is_adaptive = true;
  return inner_stat_(pathname, statbuf, is_adaptive);
}

int ObObjectDevice::inner_stat_(const char *pathname,
    ObIODFileStat &statbuf, const bool is_adaptive)
{
  int ret = OB_SUCCESS;
  int64_t length = 0;
  common::ObString uri(pathname);
  if (OB_FAIL(util_.get_file_length(uri, is_adaptive, length))) {
    OB_LOG(WARN, "fail to get file length!", K(ret), K(uri), K(is_adaptive));
  } else {
    statbuf.size_ = length;
  }
  return ret;
}

int ObObjectDevice::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  return inner_scan_dir_(dir_name, op, false/*is_adaptive*/);
}

int ObObjectDevice::adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  const bool is_adaptive = true;
  return inner_scan_dir_(dir_name, op, is_adaptive);
}

int ObObjectDevice::inner_scan_dir_(const char *dir_name,
    ObBaseDirEntryOperator &op, const bool is_adaptive)
{
  common::ObString uri(dir_name);
  int ret = OB_SUCCESS;
  if (op.is_marker_scan() && OB_ISNULL(op.get_marker())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "marker should not be null for marker scan",
        K(ret), K(op.is_marker_scan()), K(dir_name), K(op.get_marker()));
  } else {
    if (op.is_dir_scan()) {
      ret = util_.list_directories(uri, is_adaptive, op);
    } else if (op.is_marker_scan()) {
      ret = util_.list_files_with_marker(uri, op);
    } else {
      ret = util_.list_files(uri, is_adaptive, op);
    }

    if (OB_FAIL(ret)) {
      OB_LOG(WARN, "fail to do list/dir scan!", K(ret), KCSTRING(dir_name),
          "is_dir_scan", op.is_dir_scan(), "is_marker_scan", op.is_marker_scan());
    }
  }
  return ret;
}

int ObObjectDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  common::ObString uri(pathname);
  return util_.is_tagging(uri, is_tagging);
}

int ObObjectDevice::pread(const ObIOFd &fd, const int64_t offset, const int64_t size,
                          void *buf, int64_t &read_size, ObIODPreadChecker *checker)
{
  UNUSED(checker);
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = NULL;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_READER || flag == OB_STORAGE_ACCESS_NOHEAD_READER) {
    ObStorageReader *reader = static_cast<ObStorageReader*>(ctx);
    if (OB_FAIL(reader->pread((char*)buf, size, offset, read_size))) {
      OB_LOG(WARN, "fail to do normal pread!", K(ret), K(flag));
    }
  } else if (flag == OB_STORAGE_ACCESS_ADAPTIVE_READER) {
    ObStorageAdaptiveReader *adaptive_reader = static_cast<ObStorageAdaptiveReader*>(ctx);
    if (OB_FAIL(adaptive_reader->pread((char*)buf, size, offset, read_size))) {
      OB_LOG(WARN, "fail to do adaptive reader pread!", K(ret), K(flag));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd is not a reader fd!", K(flag), K(ret));
  }
  return ret;
}

/*since oss/cos donot support random write, so backup use write interface in most scenario
  only nfs can use pwrite interface
*/
int ObObjectDevice::write(const ObIOFd &fd, const void *buf, const int64_t size, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = NULL;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd), K(ret));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_OVERWRITER) {
    ObStorageWriter *overwriter = static_cast<ObStorageWriter*>(ctx);
    if (OB_FAIL(overwriter->write((char*)buf, size))) {
      OB_LOG(WARN, "fail to do overwrite write!", K(ret));
    }
  } else if (flag == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
    ObStorageMultiPartWriter *multipart_writer = static_cast<ObStorageMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->write((char*)buf, size))) {
      OB_LOG(WARN, "fail to do multipart writer write!", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  if (OB_SUCC(ret)) {
    write_size = size;
  } else {
    write_size = 0;
  }
  return ret;
}

/*object storage does not support random write, so offset is no use
  1.some platform support append write(oss), some donot support(cos)
  2.for object storage, just write fail pr write succ
*/
int ObObjectDevice::pwrite(const ObIOFd &fd, const int64_t offset, const int64_t size,
                           const void *buf, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = NULL;

  UNUSED(offset);

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (OB_STORAGE_ACCESS_APPENDER == flag) {
    ObStorageAppender *appender = static_cast<ObStorageAppender*>(ctx);
    if (OB_FAIL(appender->pwrite((char*)buf, size, offset))) {
      OB_LOG(WARN, "fail to do appender pwrite!", K(ret));
    }
  } else if (OB_STORAGE_ACCESS_MULTIPART_WRITER == flag) {
    ObStorageMultiPartWriter *multipart_writer = static_cast<ObStorageMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->pwrite((char*)buf, size, offset))) {
      OB_LOG(WARN, "fail to do multipart writer pwrite!", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  if (OB_SUCC(ret)) {
    write_size = size;
  } else {
    write_size = 0;
  }
  return ret;
}

int ObObjectDevice::upload_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
    ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->upload_part(buf, size, part_id))) {
      OB_LOG(WARN, "fail to do multipart writer upload!", K(ret), K(part_id));
    }
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->upload_part(buf, size, part_id))) {
      OB_LOG(WARN, "fail to do multipart writer upload!", K(ret), K(part_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  if (OB_SUCC(ret)) {
    write_size = size;
  } else {
    write_size = 0;
  }
  return ret;
}

int ObObjectDevice::buf_append_part(
    const ObIOFd &fd,
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
    ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->buf_append_part(buf, size, tenant_id, is_full))) {
      OB_LOG(WARN, "fail to do multipart writer buf_append_part!", K(ret));
    }
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->buf_append_part(buf, size, tenant_id, is_full))) {
      OB_LOG(WARN, "fail to do multipart writer buf_append_part!", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  return ret;
}

int ObObjectDevice::get_part_id(
    const ObIOFd &fd, bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
    ObStorageDirectMultiPartWriter *multipart_writer = static_cast<ObStorageDirectMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->get_part_id(is_exist, part_id))) {
      OB_LOG(WARN, "fail to do multipart writer get_part_id!", K(ret));
    }
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->get_part_id(is_exist, part_id))) {
      OB_LOG(WARN, "fail to do multipart writer get_part_id!", K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  return ret;
}

int ObObjectDevice::get_part_size(const ObIOFd &fd, const int64_t part_id, int64_t &part_size)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void *ctx = nullptr;

  fd_mng_.get_fd_flag(fd, flag);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", K(flag), K(ret));
  } else if (flag == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
    ObStorageBufferedMultiPartWriter *multipart_writer = static_cast<ObStorageBufferedMultiPartWriter*>(ctx);
    if (OB_FAIL(multipart_writer->get_part_size(part_id, part_size))) {
      OB_LOG(WARN, "fail to do multipart writer get_part_size!", K(ret), K(part_id));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknow access type, not a writable type!", K(flag), K(ret));
  }

  return ret;
}

//the first opt is config name & the return value
int ObObjectDevice::get_config(ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  UNUSED(opts);
  return ret;
}

/*------------------------The following functions are not used in object storage, maybe some will be used in future------------------------*/
/*object device can not used by storage engine if these interfaces do not implement, later maybe has the requirement to support*/
int ObObjectDevice::rename(const char *oldpath, const char *newpath)
{
  UNUSED(oldpath);
  UNUSED(newpath);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "rename is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::reconfig(const ObIODOpts &opts)
{
  UNUSED(opts);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "reconfig is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::seal_file(const ObIOFd &fd)
{
  return seal_for_adaptive(fd);
}

int ObObjectDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  UNUSED(dir_name);
  UNUSED(func);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "scan_dir with callback is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::fsync(const ObIOFd &fd)
{
  UNUSED(fd);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "fsync is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len)
{
  UNUSED(fd);
  UNUSED(mode);
  UNUSED(offset);
  UNUSED(len);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "fallocate is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::truncate(const char *pathname, const int64_t len)
{
  UNUSED(pathname);
  UNUSED(len);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "truncate is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

  //block interfaces
int ObObjectDevice::mark_blocks(ObIBlockIterator &block_iter)
{
  UNUSED(block_iter);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "mark_blocks is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::alloc_block(const ObIODOpts *opts, ObIOFd &block_id)
{
  UNUSED(opts);
  UNUSED(block_id);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "alloc_block is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::alloc_blocks(const ObIODOpts *opts, const int64_t count, ObIArray<ObIOFd> &blocks)
{
  UNUSED(opts);
  UNUSED(count);
  UNUSED(blocks);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "alloc_blocks is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

void ObObjectDevice::free_block(const ObIOFd &block_id)
{
  UNUSED(block_id);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "free_block is not support in object device !", K(device_type_));
}

int ObObjectDevice::fsync_block()
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "fsync_block is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::mark_blocks(const ObIArray<ObIOFd> &blocks)
{
  UNUSED(blocks);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "mark_blocks is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::get_restart_sequence(uint32_t &restart_id) const
{
  UNUSED(restart_id);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_restart_sequence is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

  //async io interfaces
int ObObjectDevice::io_setup(uint32_t max_events, ObIOContext *&io_context)
{
  UNUSED(max_events);
  UNUSED(io_context);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_setup is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::io_destroy(ObIOContext *io_context)
{
  UNUSED(io_context);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_destroy is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::io_prepare_pwrite(const ObIOFd &fd, void *buf, size_t count,
                                      int64_t offset, ObIOCB *iocb, void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_prepare_pwrite is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::io_prepare_pread(const ObIOFd &fd, void *buf, size_t count,
                                     int64_t offset, ObIOCB *iocb, void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_prepare_pread is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::io_submit(ObIOContext *io_context, ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_submit is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::io_cancel(ObIOContext *io_context, ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_cancel is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}
int ObObjectDevice::io_getevents(ObIOContext *io_context, int64_t min_nr,
                                 ObIOEvents *events, struct timespec *timeout)
{
  UNUSED(io_context);
  UNUSED(min_nr);
  UNUSED(events);
  UNUSED(timeout);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "io_getevents is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

ObIOCB *ObObjectDevice::alloc_iocb(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "alloc_iocb is not support in object device !", K(device_type_));
  return NULL;
}

ObIOEvents *ObObjectDevice::alloc_io_events(const uint32_t max_events)
{
  UNUSED(max_events);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "alloc_io_events is not support in object device !", K(device_type_));
  return NULL;
}

void ObObjectDevice::free_iocb(ObIOCB *iocb)
{
  UNUSED(iocb);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "free_iocb is not support in object device !", K(device_type_));
}

void ObObjectDevice::free_io_events(ObIOEvents *io_event)
{
  UNUSED(io_event);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "free_io_events is not support in object device !", K(device_type_));
}

  // space management interface
int64_t ObObjectDevice::get_total_block_size() const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_total_block_size is not support in object device !", K(device_type_));
  return -1;
}

int64_t ObObjectDevice::get_max_block_size(int64_t reserved_size) const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_max_block_size is not support in object device !", K(device_type_));
  return -1;
}

int64_t ObObjectDevice::get_free_block_count() const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_free_block_count is not support in object device !", K(device_type_));
  return -1;
}

int64_t ObObjectDevice::get_reserved_block_count() const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_reserved_block_count is not support in object device !", K(device_type_));
  return -1;
}

int64_t ObObjectDevice::get_max_block_count(int64_t reserved_size) const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "get_max_block_count is not support in object device !", K(device_type_));
  return -1;
}

int ObObjectDevice::check_space_full(
    const int64_t required_size,
    const bool alarm_if_space_full) const
{
  UNUSED(required_size);
  UNUSED(alarm_if_space_full);
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "check_space_full is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::check_write_limited() const
{
  OB_LOG_RET(WARN, OB_NOT_SUPPORTED, "check_write_limited is not support in object device !", K(device_type_));
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::fdatasync(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::lseek(const ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset)
{
  UNUSED(fd);
  UNUSED(offset);
  UNUSED(whence);
  UNUSED(result_offset);
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  UNUSED(fd);
  UNUSED(statbuf);
  return OB_NOT_SUPPORTED;
}

int ObObjectDevice::read(const ObIOFd &fd, void *buf, const int64_t size, int64_t &read_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(read_size);
  return OB_NOT_SUPPORTED;
}

void ObObjectDevice::set_storage_id_mod(const ObStorageIdMod &storage_id_mod)
{
  storage_id_mod_ = storage_id_mod;
}

const ObStorageIdMod &ObObjectDevice::get_storage_id_mod() const
{
  return storage_id_mod_;
}

}
}
