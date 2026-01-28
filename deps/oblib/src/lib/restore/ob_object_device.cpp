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
    "direct_multipart_writer", "buffered_multipart_writer", "util"};

const char *get_storage_access_type_str(const ObStorageAccessType &type)
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(static_cast<int64_t>(OB_STORAGE_ACCESS_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_ACCESS_TYPES_STR), "ObStorageAccessType count mismatch");
  if (type >= OB_STORAGE_ACCESS_READER && type < OB_STORAGE_ACCESS_MAX_TYPE) {
    str = OB_STORAGE_ACCESS_TYPES_STR[type];
  }
  return str;
}

//============= ObObjectDeviceIOEvent =================
int ObObjectDeviceIOEvent::assign(const ObObjectDeviceIOEvent &other)
{
  int ret = OB_SUCCESS;
  ret_code_ = other.ret_code_;
  ret_bytes_ = other.ret_bytes_;
  data_ = other.data_;
  return ret;
}

//============= ObObjectDeviceIOEvents =================
ObObjectDeviceIOEvents::ObObjectDeviceIOEvents()
  : complete_io_cnt_(0)
{
}

ObObjectDeviceIOEvents::~ObObjectDeviceIOEvents()
{
}

int64_t ObObjectDeviceIOEvents::get_complete_cnt() const
{
  return complete_io_cnt_;
}

int ObObjectDeviceIOEvents::get_ith_ret_code(const int64_t i) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(i < 0 || i >= complete_io_cnt_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid argument", KR(ret), K(i), K(complete_io_cnt_), K(io_events_.count()));
  } else {
    ret = io_events_.at(i).ret_code_;
  }
  return ret;
}

int ObObjectDeviceIOEvents::get_ith_ret_bytes(const int64_t i) const
{
  int ret_bytes = 0;
  if (OB_UNLIKELY(i < 0 || i >= complete_io_cnt_)) {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid argument", K(i), K(complete_io_cnt_), K(io_events_.count()));
  } else {
    ret_bytes = io_events_.at(i).ret_bytes_;
  }
  return ret_bytes;
}

void *ObObjectDeviceIOEvents::get_ith_data(const int64_t i) const
{
  void *data = nullptr;
  if (OB_UNLIKELY(i < 0 || i >= complete_io_cnt_)) {
    OB_LOG_RET(WARN, OB_ERR_UNEXPECTED, "invalid argument", K(i), K(complete_io_cnt_), K(io_events_));
  } else {
    data = io_events_.at(i).data_;
  }
  return data;
}

void ObObjectDeviceIOEvents::clear()
{
  complete_io_cnt_ = 0;
  io_events_.reuse();
}

//============= ObObjectDeviceAsyncContext =================
ObObjectDeviceAsyncContextForWait::ObObjectDeviceAsyncContextForWait()
  : is_inited_(false),
    completed_(false),
    ret_code_(OB_ERR_UNEXPECTED),
    bytes_(0),
    cond_()
{
}

ObObjectDeviceAsyncContextForWait::~ObObjectDeviceAsyncContextForWait()
{
  cond_.destroy();
}

int ObObjectDeviceAsyncContextForWait::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "inited twice", KR(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::OBJECT_DEVICE_ASYNC_IO_WAIT))) {
    OB_LOG(WARN, "failed to init cond", KR(ret));
  } else {
    completed_ = false;
    ret_code_ = OB_ERR_UNEXPECTED;
    bytes_ = 0;
    is_inited_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObObjectDeviceAsyncContextForWait::wait()
{
  return cond_.wait_us(0);
}

void ObObjectDeviceAsyncContextForWait::reset()
{
  is_inited_ = false;
  completed_ = false;
  ret_code_ = OB_ERR_UNEXPECTED;
  bytes_ = 0;
  cond_.destroy();
}

int ObObjectDeviceAsyncContextForWait::async_callback(int ret_code, int64_t bytes, void *ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "ctx is null", KR(ret), KP(ctx));
  } else {
    ObObjectDeviceAsyncContextForWait *context = static_cast<ObObjectDeviceAsyncContextForWait *>(ctx);
    ObThreadCondGuard guard(context->cond_);
    context->completed_ = true;
    context->ret_code_ = ret_code;
    context->bytes_ = bytes;
    context->cond_.signal();
  }
  return ret;
}

int ObObjectDeviceAsyncContextForNoWait::async_callback(int ret_code, int64_t bytes, void *ctx)
{
  int ret = OB_SUCCESS;
  ObObjectDeviceIOEvent *io_event = nullptr;
  ObObjectDeviceAsyncContextForNoWait *async_context = nullptr;
  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "object device async io callback ctx is null", KR(ret), KP(ctx));
  } else if (FALSE_IT(async_context = static_cast<ObObjectDeviceAsyncContextForNoWait *>(ctx))) {
  } else if (OB_UNLIKELY(async_context->device_ == nullptr || async_context->ctx_ == nullptr)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "object device async io callback ctx is invalid, device or ctx is null", KR(ret), KP(async_context->device_), KP(async_context->ctx_));
  } else {
    ObObjectDevice *device = async_context->device_;
    ObObjectDeviceIOCB *iocb = static_cast<ObObjectDeviceIOCB *>(async_context->ctx_);

    if (OB_ISNULL(io_event = device->alloc_io_event())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OB_LOG(WARN, "fail to alloc io event", KR(ret));
    } else {
      io_event->ret_code_ = ret_code;
      io_event->ret_bytes_ = bytes;
      io_event->data_ = iocb->ctx_;
    }

    if (OB_FAIL(ret)) {
    } else {
      bool push_success = false;
      do {
        if (OB_FAIL(device->push_io_event(io_event))) {
          if (ret == OB_SIZE_OVERFLOW) {
            ::usleep(1000);
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
              OB_LOG(INFO, "there is no enough space to push io event", KR(ret), KP(io_event));
            }
            ret = OB_SUCCESS;
          } else {
            OB_LOG(WARN, "fail to push io event", KR(ret), KP(io_event));
          }
        } else {
          push_success = true;
        }
      } while (!push_success && OB_SUCC(ret));
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(io_event)) {
        device->free_io_event(io_event);
        io_event = nullptr;
      }
    }
    device->free_async_context(async_context);
  }
  return ret;
}

//============= ObObjectDevice =================
ObObjectDevice::ObObjectDevice(const bool is_local_disk)
  : storage_info_(), is_started_(false), lock_(common::ObLatchIds::OBJECT_DEVICE_LOCK),
    allocator_(), storage_id_mod_(),
    iocb_count_(0), async_context_count_(0), io_event_count_(0),
    is_local_disk_(is_local_disk)
{
  util_ctx_pool_.set_attr(SET_USE_500("OD_Util"));
  reader_ctx_pool_.set_attr(SET_USE_500("OD_Reader"));
  adaptive_reader_ctx_pool_.set_attr(SET_USE_500("OD_AdaptRdr"));
  appender_ctx_pool_.set_attr(SET_USE_500("OD_Appender"));
  overwriter_ctx_pool_.set_attr(SET_USE_500("OD_Overwriter"));
  multipart_writer_ctx_pool_.set_attr(SET_USE_500("OD_MultiPart"));
  direct_multiwriter_ctx_pool_.set_attr(SET_USE_500("OD_DirMulti"));
  buffered_multiwriter_ctx_pool_.set_attr(SET_USE_500("OD_BufMulti"));
  async_reader_ctx_pool_.set_attr(SET_USE_500("OD_AsyncRead"));
  async_writer_ctx_pool_.set_attr(SET_USE_500("OD_AsyncWrite"));
  async_direct_multiwriter_ctx_pool_.set_attr(SET_USE_500("OD_AsyncDirMw"));
  async_buffered_multiwriter_ctx_pool_.set_attr(SET_USE_500("OD_AsyncBufMw"));
  allocator_.set_attr(SET_USE_500("OD_Allocator"));
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
    util_ctx_pool_.reset();
    reader_ctx_pool_.reset();
    adaptive_reader_ctx_pool_.reset();
    appender_ctx_pool_.reset();
    overwriter_ctx_pool_.reset();
    multipart_writer_ctx_pool_.reset();
    direct_multiwriter_ctx_pool_.reset();
    buffered_multiwriter_ctx_pool_.reset();
    async_reader_ctx_pool_.reset();
    async_writer_ctx_pool_.reset();
    async_direct_multiwriter_ctx_pool_.reset();
    async_buffered_multiwriter_ctx_pool_.reset();
    async_io_event_queue_.destroy();
    allocator_.reset();
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

int ObObjectDevice::setup_storage_info(const ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(storage_info_.set(device_type_, opts.opts_[0].value_.value_str))) {
    OB_LOG(WARN, "failed to build storage info", K(ret));
  }
  return ret;
}

/*the app logical use call ObBackupIoAdapter::get_and_init_device*/
/*decription: base_info just related to storage_info,
  base_info is used by the reader/appender*/
int ObObjectDevice::start(const ObIODOpts &opts)
{
  int32_t ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  ObMemAttr attr = SET_USE_500("ObjectDevice");
  if (is_started_) {
    //has inited, no need init again, do nothing
  } else if ((1 != opts.opt_cnt_ && 5 != opts.opt_cnt_ && 6 != opts.opt_cnt_) || NULL == opts.opts_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to start device, args cnt is wrong!", K(opts.opt_cnt_), K(ret));
  } else if (0 != STRCMP(opts.opts_[0].key_, "storage_info")) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to start device, args wrong !", KCSTRING(opts.opts_[0].key_), K(ret));
  } else {
    if (OB_FAIL(setup_storage_info(opts))) {
      OB_LOG(WARN, "failed to setup storage_info", K(ret));
    }

    common::ObObjectStorageInfo &info = get_storage_info();
    if (OB_SUCCESS != ret) {
      //mem resource will be free with device destroy
    } else if (OB_FAIL(util_.open(&info))) {
      OB_LOG(WARN, "fail to open the util!", K(ret), KP(opts.opts_[0].value_.value_str));
    } else if (OB_FAIL(fd_mng_.init())) {
      OB_LOG(WARN, "fail to init fd manager!", K(ret));
    } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
      OB_LOG(WARN, "fail to init allocator!", KR(ret));
    } else if (OB_FAIL(async_io_event_queue_.init(MAX_ASYNC_IO_EVENT_COUNT, &allocator_, attr))) {
      OB_LOG(WARN, "fail to init async io event queue!", KR(ret));
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
  } else if (0 == STRCMP(access_type, OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_UTIL])) {
    access_type_flag = OB_STORAGE_ACCESS_UTIL;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invaild access type!", KCSTRING(access_type));
  }
  return ret;
}

int ObObjectDevice::open_for_util(void *&ctx)
{
  int ret = OB_SUCCESS;
  ObStorageUtil *obj = util_ctx_pool_.alloc();
  if (OB_ISNULL(obj)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to alloc memory for util", KR(ret));
  } else {
    common::ObObjectStorageInfo &info = get_storage_info();
    if (OB_FAIL(obj->open(&info))) {
      OB_LOG(WARN, "failed to open util", KR(ret), K(info));
    } else {
      ctx = (void*)obj;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(obj)) {
    util_ctx_pool_.free(obj);
    obj = nullptr;
  }
  return ret;
}

int ObObjectDevice::open_for_reader(const char *pathname, void *&ctx, const bool head_meta)
{
  return open_ctx_in_pool<ObStorageReader>(pathname, ctx, reader_ctx_pool_, "reader", head_meta);
}

int ObObjectDevice::open_for_adaptive_reader_(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageAdaptiveReader>(pathname, ctx, adaptive_reader_ctx_pool_, "adaptive_reader");
}

/*ObStorageOssMultiPartWriter is not used int the current version, if we use, later, the open func of
  overwriter maybe need to add para(just like the open func of appender)*/
int ObObjectDevice::open_for_overwriter(const char *pathname, void*& ctx)
{
  return open_ctx_in_pool<ObStorageWriter>(pathname, ctx, overwriter_ctx_pool_, "overwriter");
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
  } else if (NULL == open_mode || 0 == STRCMP(open_mode, "CREATE_OPEN_LOCK")) {
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
    common::ObObjectStorageInfo &info = get_storage_info();
    if (OB_FAIL(appender->open(pathname, &info))){
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
  return open_ctx_in_pool<ObStorageMultiPartWriter>(pathname, ctx, multipart_writer_ctx_pool_, "multipart_writer");
}

int ObObjectDevice::open_for_parallel_multipart_writer_(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageDirectMultiPartWriter>(pathname, ctx, direct_multiwriter_ctx_pool_, "direct_multipart_writer");
}

int ObObjectDevice::open_for_buffered_multipart_writer_(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageBufferedMultiPartWriter>(pathname, ctx, buffered_multiwriter_ctx_pool_, "buffered_multipart_writer");
}

int ObObjectDevice::open_for_async_reader(const char *pathname, void *&ctx, const bool head_meta)
{
  return open_ctx_in_pool<ObStorageAsyncReader>(pathname, ctx, async_reader_ctx_pool_, "async_reader", head_meta);
}

int ObObjectDevice::open_for_async_writer(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageAsyncWriter>(pathname, ctx, async_writer_ctx_pool_, "async_writer");
}

int ObObjectDevice::open_for_async_direct_multiwriter(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageAsyncDirectMultiPartWriter>(pathname, ctx, async_direct_multiwriter_ctx_pool_, "async_direct_multiwriter");
}

int ObObjectDevice::open_for_async_buffered_multiwriter(const char *pathname, void *&ctx)
{
  return open_ctx_in_pool<ObStorageAsyncBufferedMultiPartWriter>(pathname, ctx, async_buffered_multiwriter_ctx_pool_, "async_buffered_multiwriter");
}

int ObObjectDevice::release_res(void *ctx, const ObIOFd &fd, ObStorageAccessType access_type)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;

  if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "ctx is null, invalid para!", KR(ret));
  } else {
    // Handle async IO
    if (fd.is_object_storage_async_io_) {
      if (OB_STORAGE_ACCESS_READER == access_type
          || OB_STORAGE_ACCESS_NOHEAD_READER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAsyncReader>(ctx, async_reader_ctx_pool_, access_type, "async_reader"))) {
          OB_LOG(WARN, "fail to close and free async reader!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_OVERWRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAsyncWriter>(ctx, async_writer_ctx_pool_, access_type, "async_writer"))) {
          OB_LOG(WARN, "fail to close and free async writer!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAsyncDirectMultiPartWriter>(ctx, async_direct_multiwriter_ctx_pool_, access_type, "async_direct_multiwriter"))) {
          OB_LOG(WARN, "fail to close and free async direct multiwriter!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAsyncBufferedMultiPartWriter>(ctx, async_buffered_multiwriter_ctx_pool_, access_type, "async_buffered_multiwriter"))) {
          OB_LOG(WARN, "fail to close and free async buffered multiwriter!", KR(ret));
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid access_type!", K(access_type), KR(ret));
      }
    } else {
      // Handle sync IO
      if (OB_STORAGE_ACCESS_APPENDER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAppender>(ctx, appender_ctx_pool_, access_type, "appender"))) {
          OB_LOG(WARN, "fail to close and free appender!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_READER == access_type
          || OB_STORAGE_ACCESS_NOHEAD_READER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageReader>(ctx, reader_ctx_pool_, access_type, "reader"))) {
          OB_LOG(WARN, "fail to close and free reader!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_ADAPTIVE_READER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageAdaptiveReader>(ctx, adaptive_reader_ctx_pool_, access_type, "adaptive_reader"))) {
          OB_LOG(WARN, "fail to close and free adaptive reader!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_OVERWRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageWriter>(ctx, overwriter_ctx_pool_, access_type, "overwriter"))) {
          OB_LOG(WARN, "fail to close and free overwriter!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_MULTIPART_WRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageMultiPartWriter>(ctx, multipart_writer_ctx_pool_, access_type, "multipart_writer"))) {
          OB_LOG(WARN, "fail to close and free multipart writer!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageDirectMultiPartWriter>(ctx, direct_multiwriter_ctx_pool_, access_type, "direct_multipart_writer"))) {
          OB_LOG(WARN, "fail to close and free direct multipart writer!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type) {
        if (OB_FAIL(close_and_free_ctx_in_pool<ObStorageBufferedMultiPartWriter>(ctx, buffered_multiwriter_ctx_pool_, access_type, "buffered_multipart_writer"))) {
          OB_LOG(WARN, "fail to close and free buffered multipart writer!", KR(ret));
        }
      } else if (OB_STORAGE_ACCESS_UTIL == access_type) {
        ObStorageUtil *util = static_cast<ObStorageUtil*>(ctx);
        util->close();
        util_ctx_pool_.free(util);
      } else {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "invalid access_type!", K(access_type), KR(ret));
      }
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
  bool enable_async_io = false;
  //validate fd
  if (fd_mng_.validate_fd(fd, false)) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "fd should not be a valid one!", K(fd), K(ret));
  } else if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "pathname is null!", K(ret));
  } else if (OB_ISNULL(ObObjectStorageInfo::cluster_state_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cluster state mgr is null!", KR(ret));
  } else {
    enable_async_io = ObObjectStorageInfo::cluster_state_mgr_->is_enable_object_storage_async_io();
  }

  //handle open logical
  if (OB_SUCC(ret)) {
    if(OB_ISNULL(opts)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "for object device, opts should not be null!", K(ret));
    } else if (OB_FAIL(get_access_type(opts, access_type))) {
      OB_LOG(WARN, "fail to get access type!", KCSTRING(pathname), K(ret));
    } else {
      if (enable_async_io && (!is_valid_async_access_type(access_type) || !is_valid_async_storage_type(device_type_)) ) {
        enable_async_io = false;
      }

      if (enable_async_io) {
        if (OB_STORAGE_ACCESS_READER == access_type) {
          ret = open_for_async_reader(pathname, ctx, true/*head_meta*/);
        } else if (OB_STORAGE_ACCESS_NOHEAD_READER == access_type) {
          ret = open_for_async_reader(pathname, ctx, false/*head_meta*/);
        } else if (OB_STORAGE_ACCESS_OVERWRITER == access_type) {
          ret = open_for_async_writer(pathname, ctx);
        } else if (OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER == access_type) {
          ret = open_for_async_direct_multiwriter(pathname, ctx);
        } else if (OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER == access_type) {
          ret = open_for_async_buffered_multiwriter(pathname, ctx);
        } else {
          ret = OB_NOT_SUPPORTED;
          OB_LOG(WARN, "not supported async io!", KR(ret), K(access_type));
        }
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
        } else if (OB_STORAGE_ACCESS_UTIL == access_type) {
          ret = open_for_util(ctx);
        }
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
    } else {
      fd.is_object_storage_async_io_ = enable_async_io;
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type), K(fd));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncDirectMultiPartWriter, complete);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, complete);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a multipart writer fd!", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageMultiPartWriter, complete);
    } else if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageDirectMultiPartWriter, complete);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, complete);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a multipart writer fd!", KR(ret), K(op_type));
    }
  }
  return ret;
}

int ObObjectDevice::abort(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type), K(fd));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncDirectMultiPartWriter, abort);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, abort);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a multipart writer fd!", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageMultiPartWriter, abort);
    } else if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageDirectMultiPartWriter, abort);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, abort);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a multipart writer fd!", KR(ret), K(op_type));
    }
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = NULL;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fail to close fd. since fd is invalid!", K(ret) ,K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_FAIL(release_res(ctx, fd, (ObStorageAccessType)op_type))) {
    OB_LOG(WARN, "fail to release the resource!", K(ret), K(op_type));
  }
  dec_ref();
  return ret;
}

int ObObjectDevice::seal_for_adaptive(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = NULL;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (OB_STORAGE_ACCESS_APPENDER == op_type) {
    ObStorageAppender *appender = static_cast<ObStorageAppender*>(ctx);
    if (OB_FAIL(appender->seal_for_adaptive())) {
      OB_LOG(WARN, "fail to seal!", K(ret), K(op_type));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknown access type, not an appender fd!", KR(ret), K(op_type));
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
  common::ObString uri(pathname);
  if (OB_FAIL(util_.get_file_stat(uri, is_adaptive, statbuf))) {
    OB_LOG(WARN, "fail to get file stat!", K(ret), K(uri), K(is_adaptive));
  }
  return ret;
}

int ObObjectDevice::get_file_content_digest(
    const char *pathname, char *digest_buf, const int64_t digest_buf_len)
{
  int ret = OB_SUCCESS;
  const common::ObString uri(pathname);
  if (OB_FAIL(util_.get_file_content_digest(uri, digest_buf, digest_buf_len))) {
    OB_LOG(WARN, "fail to get file content digest!",
        K(ret), K(uri), KP(digest_buf), K(digest_buf_len));
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
  if (op.is_dir_scan()) {
    ret = util_.list_directories(uri, is_adaptive, op);
  } else {
    ret = util_.list_files(uri, is_adaptive, op);
  }

  if (OB_FAIL(ret)) {
    OB_LOG(WARN, "fail to do list/dir scan!", K(ret), KCSTRING(dir_name),
        "is_dir_scan", op.is_dir_scan());
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = NULL;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_READER || op_type == OB_STORAGE_ACCESS_NOHEAD_READER) {
      ObStorageAsyncReader *reader = static_cast<ObStorageAsyncReader *>(ctx);
      ObObjectDeviceAsyncContextForWait context;
      if (OB_FAIL(context.init())) {
        OB_LOG(WARN, "failed to init context", KR(ret), K(fd), K(offset), K(size));
      } else if (OB_FAIL(reader->pread(static_cast<char *>(buf), size, offset, ObObjectDeviceAsyncContextForWait::async_callback, &context))) {
        OB_LOG(WARN, "failed to spawn async io", KR(ret), K(fd), K(offset), K(size));
      } else if (OB_FAIL(context.wait())) {
        OB_LOG(WARN, "failed to wait async io", KR(ret), K(fd), K(offset), K(size));
      } else if (OB_UNLIKELY(!context.completed_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "wait async io success but context value is invalid", KR(ret), K(context));
      } else if (OB_UNLIKELY(context.ret_code_ != OB_SUCCESS)) {
        ret = context.ret_code_;
        OB_LOG(WARN, "wait async io success but io failed", KR(ret), K(context));
      } else {
        read_size = context.bytes_;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd is not a supported reader fd", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_READER || op_type == OB_STORAGE_ACCESS_NOHEAD_READER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageReader, pread, static_cast<char *>(buf), size, offset, read_size);
    } else if (op_type == OB_STORAGE_ACCESS_ADAPTIVE_READER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAdaptiveReader, pread, static_cast<char *>(buf), size, offset, read_size);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd is not a reader fd!", KR(ret), K(op_type));
    }
  }

  return ret;
}

/*since oss/cos donot support random write, so backup use write interface in most scenario
  only nfs can use pwrite interface
*/
int ObObjectDevice::write(const ObIOFd &fd, const void *buf, const int64_t size, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = NULL;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd), K(ret));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_OVERWRITER) {
      ObStorageAsyncWriter *writer = static_cast<ObStorageAsyncWriter *>(ctx);
      ObObjectDeviceAsyncContextForWait context;
      if (OB_FAIL(context.init())) {
        OB_LOG(WARN, "failed to init context", KR(ret), K(fd), K(size));
      } else if (OB_FAIL(writer->write(static_cast<const char *>(buf), size, context.async_callback, &context))) {
        OB_LOG(WARN, "failed to spawn async io", KR(ret), K(fd), K(size));
      } else if (OB_FAIL(context.wait())) {
        OB_LOG(WARN, "failed to wait async io", KR(ret), K(fd), K(size));
      } else if (OB_UNLIKELY(!context.completed_)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "wait async io success but context value is invalid", KR(ret), K(context));
      } else if (OB_UNLIKELY(context.ret_code_ != OB_SUCCESS)) {
        ret = context.ret_code_;
        OB_LOG(WARN, "wait async io success but io failed", KR(ret), K(context));
      } else {
        write_size = context.bytes_;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd is not a supported writer fd", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_OVERWRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageWriter, write, static_cast<const char *>(buf), size);
    } else if (op_type == OB_STORAGE_ACCESS_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageMultiPartWriter, write, static_cast<const char *>(buf), size);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = NULL;

  UNUSED(offset);

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret), K(fd));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd is not a supported writer fd", KR(ret), K(op_type));
  } else {
    if (OB_STORAGE_ACCESS_APPENDER == op_type) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAppender, pwrite, static_cast<const char *>(buf), size, offset);
    } else if (OB_STORAGE_ACCESS_MULTIPART_WRITER == op_type) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageMultiPartWriter, pwrite, static_cast<const char *>(buf), size, offset);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    ObObjectDeviceAsyncContextForWait context;
    if (OB_FAIL(context.init())) {
      OB_LOG(WARN, "failed to init context", KR(ret), K(fd), K(size), K(part_id));
    } else if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncDirectMultiPartWriter, upload_part, buf, size, part_id, context.async_callback, &context);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, upload_part, buf, size, part_id, context.async_callback, &context);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd is not a supported writer fd", KR(ret), K(op_type));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(context.wait())) {
      OB_LOG(WARN, "failed to wait async io", KR(ret), K(fd), K(size), K(part_id));
    } else if (OB_UNLIKELY(!context.completed_)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "wait async io success but context value is invalid", KR(ret), K(context));
    } else if (OB_UNLIKELY(context.ret_code_ != OB_SUCCESS)) {
      ret = context.ret_code_;
      OB_LOG(WARN, "wait async io success but io failed", KR(ret), K(context));
    } else {
      write_size = context.bytes_;
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageDirectMultiPartWriter, upload_part, static_cast<const char *>(buf), size, part_id);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, upload_part, static_cast<const char *>(buf), size, part_id);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
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
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncDirectMultiPartWriter, buf_append_part, static_cast<const char *>(buf), size, tenant_id, is_full);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, buf_append_part, static_cast<const char *>(buf), size, tenant_id, is_full);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageDirectMultiPartWriter, buf_append_part, static_cast<const char *>(buf), size, tenant_id, is_full);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, buf_append_part, static_cast<const char *>(buf), size, tenant_id, is_full);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
  }
  return ret;
}

int ObObjectDevice::get_part_id(
    const ObIOFd &fd, bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncDirectMultiPartWriter, get_part_id, is_exist, part_id);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, get_part_id, is_exist, part_id);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageDirectMultiPartWriter, get_part_id, is_exist, part_id);
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, get_part_id, is_exist, part_id);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
  }

  return ret;
}

int ObObjectDevice::get_part_size(const ObIOFd &fd, const int64_t part_id, int64_t &part_size)
{
  int ret = OB_SUCCESS;
  ObStorageAccessType op_type;
  void *ctx = nullptr;

  fd_mng_.get_fd_op_type(fd, op_type);
  if (!fd_mng_.validate_fd(fd, true)) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
  } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
  } else if (fd.is_object_storage_async_io_) {
    if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageAsyncBufferedMultiPartWriter, get_part_size, part_id, part_size);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknown access type, not a writable type!", KR(ret), K(op_type));
    }
  } else {
    if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      CALL_CTX_METHOD_WITH_ARGS(ctx, ObStorageBufferedMultiPartWriter, get_part_size, part_id, part_size);
    } else {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "unknow access type, not a writable type!", KR(ret), K(op_type));
    }
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
  int ret = OB_SUCCESS;
  return ret;
}

int ObObjectDevice::io_destroy(ObIOContext *io_context)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObObjectDevice::io_prepare_pwrite(const ObIOFd &fd, void *buf, size_t count,
                                      int64_t offset, ObIOCB *iocb, void *callback)
{
  int ret = OB_SUCCESS;
  ObObjectDeviceIOCB *object_iocb = nullptr;

  if (OB_UNLIKELY(!fd.is_valid()) || OB_ISNULL(iocb) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(buf), KP(iocb), K(fd));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_OBJECT_DEVICE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(object_iocb = static_cast<ObObjectDeviceIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object iocb is null", KR(ret), KP(iocb));
  } else {
    object_iocb->fd_ = fd;
    object_iocb->io_buf_ = buf;
    object_iocb->io_buf_size_ = count;
    object_iocb->io_offset_ = offset;
    object_iocb->ctx_ = callback;
  }
  return ret;
}

int ObObjectDevice::io_prepare_pread(const ObIOFd &fd, void *buf, size_t count,
                                     int64_t offset, ObIOCB *iocb, void *callback)
{
  int ret = OB_SUCCESS;
  ObObjectDeviceIOCB *object_iocb = nullptr;
  if (OB_UNLIKELY(!fd.is_valid()) || OB_ISNULL(buf) || OB_ISNULL(iocb) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(buf), KP(iocb), K(fd));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_OBJECT_DEVICE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(object_iocb = static_cast<ObObjectDeviceIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object iocb is null", KR(ret), KP(iocb));
  } else {
    object_iocb->fd_ = fd;
    object_iocb->io_buf_ = buf;
    object_iocb->io_buf_size_ = count;
    object_iocb->io_offset_ = offset;
    object_iocb->ctx_ = callback;
  }
  return ret;
}

int ObObjectDevice::io_prepare_upload_part(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t part_id,
    ObIOCB *iocb,
    void *callback)
{
  int ret = OB_SUCCESS;
  ObObjectDeviceIOCB *object_iocb = nullptr;
  // buf can be null
  if (OB_UNLIKELY(!fd.is_valid()) || OB_ISNULL(iocb) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(buf), KP(iocb), K(fd));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_OBJECT_DEVICE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(object_iocb = static_cast<ObObjectDeviceIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object iocb is null", KR(ret), KP(iocb));
  } else {
    object_iocb->fd_ = fd;
    object_iocb->io_buf_ = buf;
    object_iocb->io_buf_size_ = count;
    object_iocb->io_offset_ = 0;
    object_iocb->part_id = part_id;
    object_iocb->ctx_ = callback;
  }
  return ret;
}

int ObObjectDevice::io_submit(ObIOContext *io_context, ObIOCB *iocb)
{
  UNUSED(io_context);
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObjectDevice", 5000);
  ObObjectDeviceIOCB *object_iocb = nullptr;

  if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
    OB_LOG(INFO, "ObObjectDevice status", K(iocb_count_), K(async_context_count_), K(io_event_count_));
  }

  if (OB_ISNULL(iocb)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(iocb));
  } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_OBJECT_DEVICE != iocb->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
  } else if (OB_ISNULL(object_iocb = static_cast<ObObjectDeviceIOCB *>(iocb))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object iocb is null", KR(ret), KP(iocb));
  } else {
    ObIOFd fd = object_iocb->fd_;
    ObStorageAccessType op_type = ObStorageAccessType::OB_STORAGE_ACCESS_MAX_TYPE;
    void *ctx = nullptr;
    ObObjectDeviceAsyncContextForNoWait *async_context = nullptr;

    fd_mng_.get_fd_op_type(fd, op_type);
    if (!fd_mng_.validate_fd(fd, true)) {
      ret = OB_NOT_INIT;
      OB_LOG(WARN, "fd is not init!", K(fd.first_id_), K(fd.second_id_));
    } else if (OB_UNLIKELY(!fd.is_object_storage_async_io_)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd is not object storage async io!", K(fd.first_id_), K(fd.second_id_));
    } else if (OB_FAIL(fd_mng_.fd_to_ctx(fd, ctx))) {
      OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
    } else if (OB_ISNULL(ctx)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "fd ctx is null!", KR(ret), K(op_type));
    } else if (OB_FAIL(alloc_and_assign_async_context(iocb, async_context))) {
      OB_LOG(WARN, "failed to alloc and assign async context", KR(ret), KP(iocb));
    } else if (op_type == OB_STORAGE_ACCESS_READER || op_type == OB_STORAGE_ACCESS_NOHEAD_READER) {
      ObStorageAsyncReader *async_reader =
          static_cast<ObStorageAsyncReader *>(ctx);
      if (OB_FAIL(async_reader->pread(
              (char *)object_iocb->io_buf_, object_iocb->io_buf_size_,
              object_iocb->io_offset_, async_context->async_callback, async_context))) {
        OB_LOG(WARN, "failed to spwan async reader pread!", KR(ret),
               KPC(object_iocb));
      }
    } else if (op_type == OB_STORAGE_ACCESS_OVERWRITER) {
      ObStorageAsyncWriter *async_writer = static_cast<ObStorageAsyncWriter *>(ctx);
      const char *buf = object_iocb->io_buf_size_ == 0 ? "\0" : static_cast<const char *>(object_iocb->io_buf_);
      if (OB_FAIL(async_writer->write(buf, object_iocb->io_buf_size_, async_context->async_callback, async_context))) {
        OB_LOG(WARN, "failed to spwan async writer write!", KR(ret),
              KPC(object_iocb));
      }
    } else if (op_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER) {
      ObStorageAsyncBufferedMultiPartWriter *async_buffered_multiwriter =
          static_cast<ObStorageAsyncBufferedMultiPartWriter *>(ctx);
      if (OB_FAIL(async_buffered_multiwriter->upload_part(
              (char *)object_iocb->io_buf_, object_iocb->io_buf_size_,
              object_iocb->part_id, async_context->async_callback, async_context))) {
        OB_LOG(WARN, "failed to spwan async buffered multiwriter write!",
               KR(ret), KPC(object_iocb));
      }
    } else if (op_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER) {
      ObStorageAsyncDirectMultiPartWriter *async_direct_multiwriter =
          static_cast<ObStorageAsyncDirectMultiPartWriter *>(ctx);
      if (OB_FAIL(async_direct_multiwriter->upload_part(
              (char *)object_iocb->io_buf_, object_iocb->io_buf_size_,
              object_iocb->part_id, async_context->async_callback, async_context))) {
        OB_LOG(WARN, "failed to spwan async direct multiwriter write!", KR(ret),
               KPC(object_iocb));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "invalid access type in async io submit!", KR(ret), K(op_type));
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(async_context)) {
      free_async_context(async_context);
      async_context = nullptr;
    }
  }
  return ret;
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
  int ret = OB_SUCCESS;
  ObObjectDeviceIOEvents *io_events = nullptr;
  if (OB_ISNULL(events)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(events));
  } else if (OB_UNLIKELY(ObIOEventsType::IO_EVENTS_TYPE_OBJECT_DEVICE != events->get_type())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid events pointer", KR(ret), KP(events), "events_type", events->get_type());
  } else if (OB_ISNULL(io_events = static_cast<ObObjectDeviceIOEvents *>(events))) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "object io events is null", KR(ret), KP(events));
  } else if (FALSE_IT(io_events->clear())) {
  } else if (OB_UNLIKELY(!is_started_)) {
    // do nothing to forbid the io_getevents call when the object device is not started
  } else if (OB_ISNULL(timeout) || OB_UNLIKELY(min_nr <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(min_nr), KP(timeout));
  } else {
    const int64_t timeout_us = (timeout->tv_sec * 1000000000 + timeout->tv_nsec) / 1000;
    const int64_t start_time_us = ObTimeUtility::current_time();
    ObObjectDeviceIOEvent *io_event = nullptr;
    do {
      io_event = nullptr;
      if (OB_FAIL(pop_io_event(io_event))) {
        if (OB_LIKELY(ret == OB_ENTRY_NOT_EXIST)) {
          const int64_t current_time_us = ObTimeUtility::current_time();
          if (current_time_us - start_time_us < timeout_us) {
            const int64_t sleep_time_us = min(1000, timeout_us - (current_time_us - start_time_us));
            ::usleep(sleep_time_us);
          }
          if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
            int64_t cost_us = ObTimeUtility::current_time() - start_time_us;
            OB_LOG(INFO, "there is no enough io event to get", KR(ret), K(min_nr), K(io_events->complete_io_cnt_), K(start_time_us), K(cost_us));
          }
          ret = OB_SUCCESS;
        } else {
          OB_LOG(WARN, "fail to pop io event", KR(ret));
        }
      } else if (OB_ISNULL(io_event)) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "io event is null", KR(ret));
      } else if (OB_FAIL(io_events->io_events_.push_back(*io_event))) {
        OB_LOG(WARN, "fail to push io event", KR(ret), KP(io_event));
      } else {
        io_events->complete_io_cnt_++;
        free_io_event(io_event);
      }
    } while (OB_SUCC(ret)
             && io_events->complete_io_cnt_ < min_nr
             && io_events->complete_io_cnt_ < io_events->max_event_cnt_
             && ObTimeUtility::current_time() - start_time_us < timeout_us);
  }
  return ret;
}

ObIOCB *ObObjectDevice::alloc_iocb(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  ObObjectDeviceIOCB *iocb = nullptr;
  if (OB_LIKELY(is_started_)) {
    if (OB_ISNULL(iocb = OB_NEWx(ObObjectDeviceIOCB, &allocator_))) {
      OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory");
    } else {
      inc_ref();
      ATOMIC_INC(&iocb_count_);
    }
  }
  return iocb;
}

ObIOEvents *ObObjectDevice::alloc_io_events(const uint32_t max_events)
{
  int ret = OB_SUCCESS;
  ObObjectDeviceIOEvents *io_events = nullptr;
  ObMemAttr attr = SET_USE_500("ObjectDevice");
  if (max_events <= 0) {
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid argument", K(max_events));
  } else if (OB_ISNULL(io_events = OB_NEW(ObObjectDeviceIOEvents, attr.label_))) {
    OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory");
  } else if (FALSE_IT(io_events->io_events_.set_attr(attr))) {
  } else if (OB_FAIL(io_events->io_events_.prepare_allocate(max_events))) {
    OB_LOG(WARN, "fail to prepare allocate io events", KR(ret), K(max_events));
  } else {
    io_events->max_event_cnt_ = max_events;
    io_events->complete_io_cnt_ = 0;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(io_events)) {
    OB_DELETE(ObObjectDeviceIOEvents, attr.label_, io_events);
    io_events = nullptr;
  }
  return io_events;
}

void ObObjectDevice::free_iocb(ObIOCB *iocb)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_started_)) {
    ObObjectDeviceIOCB *object_iocb = nullptr;
    if (OB_ISNULL(iocb)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "iocb is null", KR(ret), KP(iocb));
    } else if (OB_UNLIKELY(ObIOCBType::IOCB_TYPE_OBJECT_DEVICE != iocb->get_type())) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "Invalid iocb pointer", KR(ret), KP(iocb), "iocb_type", iocb->get_type());
    } else if (OB_ISNULL(object_iocb = static_cast<ObObjectDeviceIOCB *>(iocb))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "object iocb is null", KR(ret), KP(iocb));
    } else {
      OB_DELETEx(ObObjectDeviceIOCB, &allocator_, object_iocb);
      ATOMIC_DEC(&iocb_count_);
      dec_ref();
    }
  }
}

void ObObjectDevice::free_io_events(ObIOEvents *io_event)
{
  ObMemAttr attr = SET_USE_500("ObjectDevice");
  if (OB_NOT_NULL(io_event)) {
    ObObjectDeviceIOEvents *object_io_events = static_cast<ObObjectDeviceIOEvents *>(io_event);
    OB_DELETE(ObObjectDeviceIOEvents, attr.label_, object_io_events);
  }
}

ObObjectDeviceIOEvent *ObObjectDevice::alloc_io_event()
{
  ObObjectDeviceIOEvent *io_event = nullptr;
  char *buf = nullptr;
  if (OB_UNLIKELY(!is_started_)) {
    OB_LOG_RET(WARN, OB_NOT_INIT, "not init", K(is_started_));
  } else if (OB_ISNULL(io_event = OB_NEWx(ObObjectDeviceIOEvent, &allocator_))) {
    OB_LOG_RET(WARN, OB_ALLOCATE_MEMORY_FAILED, "fail to alloc memory");
  } else {
    inc_ref();
    ATOMIC_INC(&io_event_count_);
  }
  return io_event;
}

void ObObjectDevice::free_io_event(ObObjectDeviceIOEvent *io_event)
{
  if (OB_UNLIKELY(!is_started_)) {
    OB_LOG_RET(WARN, OB_NOT_INIT, "not init", K(is_started_));
  } else {
    OB_DELETEx(ObObjectDeviceIOEvent, &allocator_, io_event);
    ATOMIC_DEC(&io_event_count_);
    dec_ref();
  }
}

int ObObjectDevice::alloc_and_assign_async_context(void *ctx, ObObjectDeviceAsyncContextForNoWait *&context)
{
  int ret = OB_SUCCESS;
  context = nullptr;
  if (OB_UNLIKELY(!is_started_)) {
    OB_LOG_RET(WARN, OB_NOT_INIT, "not init", K(is_started_));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(ctx));
  } else if (OB_ISNULL(context = OB_NEWx(ObObjectDeviceAsyncContextForNoWait, &allocator_, this, ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to alloc memory", KR(ret));
  } else {
    inc_ref();
    ATOMIC_INC(&async_context_count_);
  }
  return ret;
}

void ObObjectDevice::free_async_context(ObObjectDeviceAsyncContextForNoWait *&context)
{
  if (OB_UNLIKELY(!is_started_)) {
    OB_LOG_RET(WARN, OB_NOT_INIT, "not init", K(is_started_));
  } else {
    OB_DELETEx(ObObjectDeviceAsyncContextForNoWait, &allocator_, context);
    context = nullptr;
    ATOMIC_DEC(&async_context_count_);
    dec_ref();
  }
}

int ObObjectDevice::push_io_event(ObObjectDeviceIOEvent *io_event)
{
  return async_io_event_queue_.push(io_event);
}

int ObObjectDevice::pop_io_event(ObObjectDeviceIOEvent *&io_event)
{
  return async_io_event_queue_.pop(io_event);
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
