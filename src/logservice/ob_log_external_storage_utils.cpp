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
#include "ob_log_external_storage_utils.h"
#include "share/ob_device_manager.h"                          // ObDeviceManager
#include "share/backup/ob_backup_io_adapter.h"                // ObBackupIOAdapter
namespace oceanbase
{
namespace common
{
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}
using namespace common;
using namespace share;
namespace logservice
{
int get_and_init_io_device(const ObString &uri,
                           const ObString &storage_info,
                           const uint64_t storage_id,
                           ObIODevice *&io_device)
{
  int ret = OB_SUCCESS;
  ObIODOpts opts;
  ObIODOpt opt;
  opts.opts_ = &opt;
  opts.opt_cnt_ = 1;
  opt.key_ = "storage_info";
  opt.value_.value_str = storage_info.ptr();
  share::ObBackupStorageInfo tmp_storage_info;
  // TODO(zjf225077): 删除这一套ObLogExternalStorageIOTaskHandleAdapter，统一使用ObBackupIoAdapter
  ObStorageIdMod storage_id_mod(storage_id, ObStorageUsedMod::STORAGE_USED_CLOG);
  if (OB_FAIL(tmp_storage_info.set(uri.ptr(), storage_info.ptr()))) {
    CLOG_LOG(WARN, "set ObBackupStorageInfo failed", K(uri), KP(io_device));
  } else if (OB_FAIL(ObDeviceManager::get_instance().get_device(uri, tmp_storage_info, storage_id_mod, io_device))) {
    CLOG_LOG(WARN, "get_device from ObDeviceManager failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->start(opts))) {
    CLOG_LOG(WARN, "start io device failed", K(uri), KP(io_device));
  } else {
    CLOG_LOG(TRACE, "get_io_device success", K(uri), KP(io_device));
  }
  return ret;
}

void release_io_device(ObIODevice *&io_device)
{
  if (NULL != io_device) {
    (void)ObDeviceManager::get_instance().release_device(io_device);
  }
  io_device = NULL;
}

int convert_to_storage_access_type(const OPEN_FLAG &open_flag,
                                   ObStorageAccessType &storage_access_type)
{
  int ret = OB_SUCCESS;
  if (OPEN_FLAG::READ_FLAG == open_flag) {
    storage_access_type = ObStorageAccessType::OB_STORAGE_ACCESS_ADAPTIVE_READER;
  } else if (OPEN_FLAG::MULTI_UPLOAD_FLAG == open_flag) {
    storage_access_type = ObStorageAccessType::OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER;
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "not supported flag", K(open_flag));
  }
  return ret;
}

int open_io_fd(const ObString &uri,
               const OPEN_FLAG open_flag,
               ObIODevice *io_device,
               ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  ObIODOpt opt;
  ObIODOpts iod_opts;
  iod_opts.opts_ = &opt;
  iod_opts.opt_cnt_ = 1;
  // TODO by runlin: support write
  ObStorageAccessType access_type;
  if (OB_FAIL(convert_to_storage_access_type(open_flag, access_type))) {
    CLOG_LOG(WARN, "convert_to_storage_access_type failed", K(open_flag));
  } else if (FALSE_IT(opt.set("AccessType", OB_STORAGE_ACCESS_TYPES_STR[access_type]))) {
    // flag=-1 and mode=0 are invalid, because ObObjectDevice does not use flag and mode;
  } else if (OB_FAIL(io_device->open(uri.ptr(), -1, 0, io_fd, &iod_opts))) {
    CLOG_LOG(WARN, "open fd failed", K(uri), K(open_flag));
  } else {
    CLOG_LOG(TRACE, "open fd success", K(uri), K(open_flag));
  }
  return ret;
}

int close_io_fd(ObIODevice *io_device,
                const ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (NULL == io_device || !io_fd.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "io device is empty");
  } else if (OB_FAIL(io_device->close(io_fd))) {
    CLOG_LOG(WARN, "fail to close fd!", K(io_fd));
  } else {
    CLOG_LOG(TRACE, "close_io_fd success", KP(io_device), K(io_fd));
  }
  return ret;
}

ObLogExternalStorageCtxItem::ObLogExternalStorageCtxItem(
  const ObIOFd &io_fd,
  ObIODevice *io_device)
: io_handle_(), io_fd_(io_fd), io_device_(io_device) {}

ObLogExternalStorageCtxItem::ObLogExternalStorageCtxItem() {reset();}

ObLogExternalStorageCtxItem::~ObLogExternalStorageCtxItem()
{
  reset();
}

void ObLogExternalStorageCtxItem::reset()
{
  io_handle_.reset();
  io_fd_.reset();
  io_device_ = NULL;
}

bool ObLogExternalStorageCtxItem::is_valid() const
{
  // io_handle_ will init by async_pread or multi upload.
  return io_fd_.is_valid() && NULL != io_device_;
}

ObLogExternalStorageCtx::ObLogExternalStorageCtx()
: items_(NULL), count_(0), capacity_(0), is_inited_(false) {}

ObLogExternalStorageCtx::~ObLogExternalStorageCtx() { destroy(); }

int ObLogExternalStorageCtx::init(const ObString &uri,
                                  const ObString &storage_info,
                                  const uint64_t storage_id,
                                  const int64_t concurrency,
                                  const OPEN_FLAG &flag)
{
  int ret = OB_SUCCESS;
  ObLogExternalStorageCtxItem *tmp_ptr = NULL;
  ObIODevice *io_device = NULL;
  ObIOFd io_fd;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx has inited", KR(ret));
  } else if (uri.empty()
      || (!uri.prefix_match(OB_FILE_PREFIX) && storage_info.empty())
      || 0 >= concurrency) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(uri), K(storage_info), K(concurrency));
  } else if (NULL == (tmp_ptr = reinterpret_cast<ObLogExternalStorageCtxItem*>(mtl_malloc(
      concurrency * sizeof(ObLogExternalStorageCtxItem), "ObLogEXTHandler")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed", KR(ret));
  } else if (OB_FAIL(get_and_init_io_device(uri, storage_info, storage_id, io_device))) {
    CLOG_LOG(WARN, "get_and_init_io_device failed", KR(ret));
  } else if (OB_FAIL(open_io_fd(uri, flag, io_device, io_fd))) {
    CLOG_LOG(WARN, "open_io_fd failed", KR(ret));
  } else {
    for (int64_t i = 0; i < concurrency; i++) {
      new(tmp_ptr+i)ObLogExternalStorageCtxItem(io_fd, io_device);
    }
    count_ = 0;
    capacity_ = concurrency;
    items_ = tmp_ptr;
    io_fd_ = io_fd;
    io_device_ = io_device;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    if (io_fd.is_valid()) {
      close_io_fd(io_device, io_fd);
    }
    if (NULL != io_device) {
      release_io_device(io_device);
    }
    if (NULL != tmp_ptr) {
      mtl_free(tmp_ptr);
      tmp_ptr = NULL;
    }
  }
  return ret;
}

int ObLogExternalStorageCtx::wait(int64_t &out_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    ObLogExternalStorageCtxItem *item = NULL;
    int tmp_ret = OB_SUCCESS;
    // NB: wait until all item finished, we can not check OB_SUCC(ret) in for loop.
    //     record first errno in for loop.
    for (int64_t i = 0; i < count_; i++) {
      if (OB_TMP_FAIL(get_item(i, item))) {
        CLOG_LOG(WARN, "get_item failed", KR(ret), K(i), KPC(this));
      } else if (OB_UNLIKELY(!item->is_valid())) {
        tmp_ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "unexpected error, item is invalid", KR(ret), K(i), KPC(this), K(item));
      } else if (OB_TMP_FAIL(item->io_handle_.wait())) {
        CLOG_LOG(WARN, "wait io_handle failed", KR(ret), K(i), KPC(this));
      } else {
        out_size += item->io_handle_.get_data_size();
        CLOG_LOG(TRACE, "wait success", K(i), K(out_size), KPC(item));
      }
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
      }
    }
    count_ = 0;
  }
  return ret;
}

int ObLogExternalStorageCtx::get_item(const int64_t index,
                                      ObLogExternalStorageCtxItem *&item) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else if (index >= capacity_ || 0 > index) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument inited", KR(ret), K(index), KPC(this));
  } else {
    item = &items_[index];
  }
  return ret;
}

int ObLogExternalStorageCtx::get_io_fd(ObIOFd &io_fd) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    io_fd = io_fd_;
  }
  return ret;
}

int ObLogExternalStorageCtx::get_io_device(ObIODevice *&io_device) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else {
    io_device = io_device_;
  }
  return ret;
}

void ObLogExternalStorageCtx::destroy()
{
  is_inited_ = false;
  if (NULL != items_) {
    close_io_fd(io_device_, io_fd_);
    release_io_device(io_device_);
    for (int64_t i = 0; i < capacity_; i++) {
      ObLogExternalStorageCtxItem &item = items_[i];
      item.~ObLogExternalStorageCtxItem();
    }
    mtl_free(items_);
  }
  count_ = 0;
  capacity_ = 0;
  items_ = NULL;
}

int ObLogExternalStorageCtx::inc_count()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageCtx not inited", KR(ret));
  } else if (count_ >= capacity_) {
    ret = OB_SIZE_OVERFLOW;
    CLOG_LOG(ERROR, "size overflow", KR(ret), K_(count), K_(capacity));
  } else {
    count_++;
  }
  return ret;
}

int64_t ObLogExternalStorageCtx::get_count() const
{
  return count_;
}

bool ObLogExternalStorageCtx::is_valid() const
{
  return true == is_inited_ && NULL != items_ && count_ <= capacity_ && capacity_ > 0 && io_fd_.is_valid() && NULL != io_device_;
}

ObLogExternalStorageHandleAdapter::ObLogExternalStorageHandleAdapter() {}

ObLogExternalStorageHandleAdapter::~ObLogExternalStorageHandleAdapter() {}

int ObLogExternalStorageHandleAdapter::exist(const ObString &uri,
                                             const ObString &storage_info,
                                             bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  ObIODevice *io_device = NULL;
  uint64_t storage_id = OB_INVALID_ID;
  if (OB_FAIL(get_and_init_io_device(uri, storage_info, storage_id, io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->adaptive_exist(uri.ptr(), exist))) {
    CLOG_LOG(WARN, "exist failed", K(uri), KP(io_device), K(exist));
  } else {
    CLOG_LOG(TRACE, "exist success", K(uri), KP(io_device), K(exist));
  }
  release_io_device(io_device);
  return ret;
}

int ObLogExternalStorageHandleAdapter::get_file_size(const ObString &uri,
                                                     const ObString &storage_info,
                                                     int64_t &file_size)
{
  int ret = OB_SUCCESS;
  file_size = 0;
  ObIODFileStat file_stat;
  ObIODevice *io_device = NULL;
  uint64_t storage_id = OB_INVALID_ID;
  if (OB_FAIL(get_and_init_io_device(uri, storage_info, storage_id, io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", K(uri), KP(io_device));
  } else if (OB_FAIL(io_device->adaptive_stat(uri.ptr(), file_stat))) {
    CLOG_LOG(WARN, "stat io deveice failed", K(uri));
  } else {
    file_size = file_stat.size_;
    CLOG_LOG(TRACE, "get_file_size success", K(uri), KP(io_device), K(file_size));
  }
  release_io_device(io_device);
  return ret;
}

int ObLogExternalStorageHandleAdapter::async_pread(const int64_t offset,
                                                   char *buf,
                                                   const int64_t read_buf_size,
                                                   ObLogExternalStorageCtxItem &io_ctx)
{
  ObTimeGuard time_guard("storage pread", 100 * 1000);
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (0 > offset || NULL == buf || 0 >= read_buf_size || !io_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(offset), KP(buf), K(read_buf_size), K(io_ctx));
  } else if (OB_FAIL(io_adapter.async_pread(*io_ctx.io_device_, io_ctx.io_fd_, buf,
                                            offset, read_buf_size, io_ctx.io_handle_, common::ObIOModule::CLOG_READ_IO))) {
    CLOG_LOG(WARN, "pread failed", KR(ret), K(io_ctx), K(offset), K(read_buf_size),
             KP(buf), K(time_guard));
  } else if (FALSE_IT(time_guard.click("after async_pread"))) {
  } else {
    CLOG_LOG(TRACE, "pread success", K(time_guard), K(io_ctx), K(offset), K(read_buf_size), KP(buf));
  }
  return ret;
}

int ObLogExternalStorageHandleAdapter::async_pwrite(const int64_t offset,
                                                    const char *buf,
                                                    const int64_t write_buf_size,
                                                    ObLogExternalStorageCtxItem &io_ctx)
{
  ObTimeGuard time_guard("storage pread", 100 * 1000);
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  if (0 > offset || NULL == buf || 0 >= write_buf_size || !io_ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(offset), KP(buf), K(write_buf_size), K(io_ctx));
  } else if (OB_FAIL(io_adapter.async_upload_data(*io_ctx.io_device_, io_ctx.io_fd_, buf,
                                                   offset, write_buf_size, io_ctx.io_handle_, common::ObIOModule::CLOG_WRITE_IO))) {
    CLOG_LOG(WARN, "async_upload_data failed", KR(ret), K(io_ctx), K(offset), K(write_buf_size),
             KP(buf), K(time_guard));
  } else if (FALSE_IT(time_guard.click("after async_upload_data"))) {
  } else {
    CLOG_LOG(TRACE, "async_upload_data success", K(time_guard), K(io_ctx), K(offset), K(write_buf_size), KP(buf));
  }
  return ret;
}
} // end namespace logservice
} // end namespace oceanbase
