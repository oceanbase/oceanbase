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

#include "ob_log_external_storage_handler.h"
#include "share/backup/ob_backup_io_adapter.h"                                // ObBackupIoAdapter
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_shared_log_utils.h"                                          // ObSharedLogUtils
#include "share/ob_device_manager.h"
#endif
#include "ob_log_external_storage_utils.h"                                    // get_and_init_io_device

namespace oceanbase
{
using namespace common;

using namespace share;
namespace logservice
{
using namespace palf;
const int64_t ObLogExternalStorageHandler::CONCURRENCY_LIMIT = 128;
const int64_t ObLogExternalStorageHandler::DEFAULT_RETRY_INTERVAL = 2 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_TIME_GUARD_THRESHOLD = 2 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_PREAD_TIME_GUARD_THRESHOLD = 100 * 1000;
const int64_t ObLogExternalStorageHandler::DEFAULT_RESIZE_TIME_GUARD_THRESHOLD = 100 * 1000;
const int64_t ObLogExternalStorageHandler::CAPACITY_COEFFICIENT = 64;
int64_t ObLogExternalStorageHandler::SINGLE_TASK_MINIMUM_SIZE = 2 * 1024 * 1024;
const int64_t ObLogExternalStorageHandler::DEFAULT_PRINT_INTERVAL = 5 * 1000 * 1000;

ObLogExternalStorageHandler::ObLogExternalStorageHandler()
    : concurrency_(-1),
      capacity_(-1),
      resize_rw_lock_(common::ObLatchIds::LOG_EXTERNAL_STORAGE_HANDLER_RW_LOCK),
      construct_async_task_lock_(common::ObLatchIds::LOG_EXTERNAL_STORAGE_HANDLER_LOCK),
      handle_adapter_(NULL),
      read_size_("ReadSize", DEFAULT_PRINT_INTERVAL),
      read_cost_("ReadCost", DEFAULT_PRINT_INTERVAL),
      is_running_(false),
      is_inited_(false)
{}

ObLogExternalStorageHandler::~ObLogExternalStorageHandler()
{
  destroy();
}

int ObLogExternalStorageHandler::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler inited twice", KPC(this));
  } else if (NULL == (handle_adapter_ = MTL_NEW(ObLogExternalStorageHandleAdapter, "ObLogEXTHandler"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "allocate memory failed");
  } else {
    concurrency_ = 1;
    capacity_ = CAPACITY_COEFFICIENT;
    is_running_ = false;
    is_inited_ = true;
    CLOG_LOG(INFO, "ObLogExternalStorageHandler inits successfully", KPC(this));
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

int ObLogExternalStorageHandler::start(const int64_t concurrency)
{
  int ret = OB_SUCCESS;
  WLockGuard guard(resize_rw_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not inited", K(concurrency), KPC(this));
  } else if (is_running_) {
    CLOG_LOG(WARN, "ObLogExternalStorageHandler has run", K(concurrency), KPC(this));
  } else if (!is_valid_concurrency_(concurrency)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(concurrency), KPC(this));
  } else if (OB_FAIL(resize_(concurrency))) {
    CLOG_LOG(WARN, "resize_ failed", K(concurrency), KPC(this));
  } else {
    is_running_ = true;
  }
  return ret;
}

void ObLogExternalStorageHandler::stop()
{
  WLockGuard guard(resize_rw_lock_);
  is_running_ = false;
}

void ObLogExternalStorageHandler::wait()
{
}

void ObLogExternalStorageHandler::destroy()
{
  CLOG_LOG_RET(WARN, OB_SUCCESS, "ObLogExternalStorageHandler destroy");
  is_inited_ = false;
  stop();
  wait();
  concurrency_ = -1;
  capacity_ = -1;
  if (OB_NOT_NULL(handle_adapter_)) {
    MTL_DELETE(ObLogExternalStorageHandleAdapter, "ObLogEXTHandler", handle_adapter_);
  }
  handle_adapter_ = NULL;
}

int ObLogExternalStorageHandler::resize(const int64_t new_concurrency,
                                        const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("resize thread pool", DEFAULT_RESIZE_TIME_GUARD_THRESHOLD);
  time_guard.click("after hold lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not inited", KPC(this), K(new_concurrency), K(timeout_us));
  } else if (!is_valid_concurrency_(new_concurrency) || 0 >= timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid arguments", KPC(this), K(new_concurrency), K(timeout_us));
  } else if (!check_need_resize_(new_concurrency)) {
    CLOG_LOG(TRACE, "no need resize", KPC(this), K(new_concurrency));
  } else {
    WLockGuardTimeout guard(resize_rw_lock_, timeout_us, ret);
    // hold lock failed
    if (OB_FAIL(ret)) {
      CLOG_LOG(WARN, "hold lock failed", KPC(this), K(new_concurrency), K(timeout_us));
    } else if (!is_running_) {
      ret = OB_NOT_RUNNING;
      CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", KPC(this), K(new_concurrency), K(timeout_us));
    } else {
      do {
        ret = resize_(new_concurrency);
        if (OB_FAIL(ret)) {
          usleep(DEFAULT_RETRY_INTERVAL);
        }
      } while (OB_FAIL(ret));
      time_guard.click("after create new thread pool");
      CLOG_LOG(INFO, "ObLogExternalStorageHandler resize success", KPC(this), K(new_concurrency));
    }
  }
  return ret;
}

int ObLogExternalStorageHandler::pread(const common::ObString &uri,
                                       const common::ObString &storage_info,
                                       const uint64_t storage_id,
                                       const int64_t offset,
                                       char *buf,
                                       const int64_t read_buf_size,
                                       int64_t &real_read_size,
                                       palf::LogIOContext &io_ctx)
{
  int ret = OB_SUCCESS;
  int64_t async_task_count = 0;
  ObTimeGuard time_guard("slow pread", DEFAULT_PREAD_TIME_GUARD_THRESHOLD);
  int64_t file_size = 0;
  int64_t real_read_buf_size = 0;
  ObLogExternalStorageCtx run_ctx;
  real_read_size = 0;
  RLockGuard guard(resize_rw_lock_);
  time_guard.click("after hold by lock");
  CONSUMER_GROUP_FUNC_GUARD(io_ctx.get_function_type());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", K(uri), K(offset), KP(buf), K(read_buf_size));
    // when uri is NFS, storage_info is empty.
  } else if (uri.empty() || 0 > offset || NULL == buf || 0 >= read_buf_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler invalid argument", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (OB_FAIL(handle_adapter_->get_file_size(uri, storage_info, file_size))) {
    CLOG_LOG(WARN, "get_file_size failed", K(uri), K(offset), KP(buf), K(read_buf_size));
  } else if (offset > file_size) {
    ret = OB_FILE_LENGTH_INVALID;
    CLOG_LOG(WARN, "read position lager than file size, invalid argument", K(file_size), K(offset), K(uri));
  } else if (offset == file_size) {
    real_read_size = 0;
    CLOG_LOG(TRACE, "read position equal to file size, no need read data", K(file_size), K(offset), K(uri));
  } else if (FALSE_IT(time_guard.click("after get file size"))) {
    // NB: limit read size.
  } else if (FALSE_IT(real_read_buf_size = std::min(file_size - offset, read_buf_size))) {
  } else if (OB_FAIL(construct_async_pread_tasks_(
      uri, storage_info, storage_id, offset, buf, real_read_buf_size, run_ctx))) {
    CLOG_LOG(WARN, "construct_async_pread+task_ failed", K(uri),
             K(offset), KP(buf), K(read_buf_size));
  } else if (FALSE_IT(time_guard.click("after construct async tasks"))) {
  } else if (OB_FAIL(run_ctx.wait(real_read_size))) {
    CLOG_LOG(WARN, "wait async tasks finished failed", K(uri),
             K(offset), KP(buf), K(read_buf_size), K(run_ctx));
  } else if (FALSE_IT(time_guard.click("after wait async tasks"))) {
  } else {
    read_size_.stat(real_read_size);
    read_cost_.stat(time_guard.get_diff());
    CLOG_LOG(TRACE, "pread finished", K(time_guard), K(uri), K(offset), K(read_buf_size),
             K(real_read_size));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLogExternalStorageHandler::upload(const uint64_t tenant_id,
	                                      const int64_t palf_id,
	                                      const palf::block_id_t block_id,
	                                      const char *write_buf,
	                                      const int64_t write_buf_size)
{
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  uint64_t storage_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", K(tenant_id), K(palf_id), K(block_id), KP(write_buf), K(write_buf_size));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", K(tenant_id), K(palf_id), K(block_id), KP(write_buf), K(write_buf_size));
    // when uri is NFS, storage_info is empty.
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)
             || !is_valid_block_id(block_id)
             || OB_ISNULL(write_buf)
             || 0 >= write_buf_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id), K(block_id), KP(write_buf), K(write_buf_size), K(ret));
  } else if (ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, ObLSID(palf_id), block_id, uri, sizeof(uri), dest, storage_id)) {
    //ignore ret
    CLOG_LOG(WARN, "construct_external_storage_access_info failed", K(tenant_id), K(palf_id), K(block_id), KP(write_buf), K(write_buf_size), K(ret));
  } else if (OB_FAIL(do_upload_(uri, dest.get_storage_info(), storage_id, write_buf, write_buf_size))) {
    CLOG_LOG(WARN, "do_upload_ failed", K(tenant_id), K(palf_id), K(block_id), KP(write_buf), K(write_buf_size), K(ret));
  } else {
    CLOG_LOG(TRACE, "upload success", K(uri), K(dest), KP(write_buf), K(write_buf_size));
  }
  return ret;
}

int ObLogExternalStorageHandler::pread(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::block_id_t block_id,
  const int64_t offset,
  char *read_buf,
  const int64_t read_buf_size,
  int64_t &real_read_size,
  palf::LogIOContext &io_ctx)
{

  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = {'\0'};
  char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
  ObBackupDest dest;
  ObBackupStorageInfo *storage_info = NULL;
  ObObjectStorageInfo storage_info_base;
  uint64_t storage_id;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", K(tenant_id), K(palf_id), K(block_id), KP(read_buf), K(read_buf_size));
  } else if (!is_running_) {
    ret = OB_NOT_RUNNING;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not running", K(tenant_id), K(palf_id), K(block_id), KP(read_buf), K(read_buf_size));
    // when uri is NFS, storage_info is empty.
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)
             || !is_valid_block_id(block_id)
             || OB_ISNULL(read_buf)
             || 0 >= read_buf_size) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id), K(block_id), KP(read_buf), K(read_buf_size));
  } else if (OB_FAIL(ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, ObLSID(palf_id), block_id, uri, sizeof(uri), dest, storage_id))) {
    CLOG_LOG(WARN, "construct_external_storage_access_info failed", K(tenant_id), K(palf_id), K(block_id),
             KP(read_buf), K(read_buf_size));
  } else if (OB_ISNULL(storage_info = dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "storage info is null", K(ret));
  } else if (OB_FAIL(storage_info_base.assign(*storage_info))) {
    CLOG_LOG(WARN, "fail to assign storage info base!", K(ret), KP(storage_info));
  } else if (OB_FAIL(storage_info_base.get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
    // no need encrypt
    CLOG_LOG(WARN, "fail to get storage info str!", K(ret), KP(storage_info));
  } else if (OB_FAIL(pread(uri, storage_info_str, storage_id, offset, read_buf, read_buf_size, real_read_size, io_ctx))) {
    CLOG_LOG(WARN, "pread failed", K(tenant_id), K(palf_id), K(block_id),
             KP(read_buf), K(read_buf_size));
    ret = convert_ret_code_(ret);
  } else {}
  return ret;
}
#endif

int64_t ObLogExternalStorageHandler::get_recommend_concurrency_in_single_file() const
{
  return palf::PALF_PHY_BLOCK_SIZE / SINGLE_TASK_MINIMUM_SIZE;
}

bool ObLogExternalStorageHandler::is_valid_concurrency_(const int64_t concurrency) const
{
  return 0 <= concurrency;
}

int64_t ObLogExternalStorageHandler::get_async_task_count_(const int64_t total_size) const
{
  // TODO by runlin: consider free async thread num.
  int64_t minimum_async_task_size = SINGLE_TASK_MINIMUM_SIZE;
  int64_t minimum_async_task_count = 1;
  int64_t async_task_count = std::max(minimum_async_task_count,
                                      std::min(concurrency_,
                                               (total_size + minimum_async_task_size - 1)/minimum_async_task_size));
  return async_task_count;
}

int ObLogExternalStorageHandler::construct_async_pread_tasks_(
    const common::ObString &uri,
    const common::ObString &storage_info,
    const uint64_t storage_id,
    const int64_t offset,
    char *read_buf,
    const int64_t read_buf_size,
    ObLogExternalStorageCtx &run_ctx)
{
  int ret = OB_SUCCESS;
  int64_t async_task_count = get_async_task_count_(read_buf_size);
  int64_t async_task_size = (read_buf_size + async_task_count - 1) / async_task_count;
  int64_t remained_task_count = async_task_count;
  int64_t remained_total_size = read_buf_size;
  if (OB_FAIL(run_ctx.init(uri, storage_info, storage_id, async_task_count, OPEN_FLAG::READ_FLAG))) {
    CLOG_LOG(WARN, "init ObLogExternalStorageIOTaskCtx failed", K(run_ctx), K(async_task_count));
  } else {
    CLOG_LOG(TRACE, "begin construct async tasks", K(async_task_count), K(async_task_size),
             K(remained_task_count), K(remained_total_size));
    int64_t curr_read_offset = offset;
    int64_t curr_read_buf_pos = 0;
    int64_t curr_task_idx = 0;
    while (remained_task_count > 0 && OB_SUCC(ret)) {
      ObLogExternalStorageCtxItem *item = NULL;
      int64_t async_task_read_buf_size = std::min(remained_total_size, async_task_size);
      if (OB_FAIL(run_ctx.get_item(curr_task_idx, item))) {
        CLOG_LOG(WARN, "get_item failed", KR(ret), K(curr_task_idx));
      } else if (OB_FAIL(handle_adapter_->async_pread(curr_read_offset, read_buf + curr_read_buf_pos, async_task_read_buf_size, *item))) {
        CLOG_LOG(WARN, "async_pread failed", KR(ret), K(curr_task_idx));
      } else if (OB_FAIL(run_ctx.inc_count())) {
        CLOG_LOG(WARN, "inc count failed", KR(ret), K(curr_task_idx));
      } else {
        ++curr_task_idx;
        curr_read_offset += async_task_read_buf_size;
        curr_read_buf_pos += async_task_read_buf_size;
        remained_total_size -= async_task_read_buf_size;
        --remained_task_count;
      }
      CLOG_LOG(TRACE, "construct async tasks idx success", K(curr_task_idx), K(async_task_count), K(async_task_size),
               K(remained_task_count), K(remained_total_size));
    }
    if (OB_FAIL(ret)) {
      int64_t unused_read_size = 0;
      run_ctx.wait(unused_read_size);
    }
  }
  return ret;
}

int ObLogExternalStorageHandler::resize_(const int64_t new_concurrency)
{
  int ret = OB_SUCCESS;
  int64_t real_concurrency = MIN(new_concurrency, CONCURRENCY_LIMIT);
  concurrency_ = real_concurrency;
  capacity_ = CAPACITY_COEFFICIENT * real_concurrency;
  return ret;
}

bool ObLogExternalStorageHandler::check_need_resize_(const int64_t new_concurrency) const
{
  RLockGuard guard(resize_rw_lock_);
  return new_concurrency != concurrency_;
}

int ObLogExternalStorageHandler::convert_ret_code_(const int ret_code)
{
  int ret = ret_code;
  switch (ret_code) {
    case OB_OBJECT_STORAGE_PERMISSION_DENIED:
    case OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH:
      ret = OB_OBJECT_STORAGE_IO_ERROR;
      break;
    case OB_OBJECT_NOT_EXIST:
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      break;
    default:
      ret = OB_OBJECT_STORAGE_IO_ERROR;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
// =============================== begin multi upload ==============================
int ObLogExternalStorageHandler::init_multi_upload(
  const uint64_t tenant_id,
  const int64_t palf_id,
  const palf::block_id_t block_id,
  const int64_t part_count,
  ObLogExternalStorageCtx &run_ctx)
{
  ObIODevice *io_device = NULL;
  int ret = OB_SUCCESS;
  char uri[OB_MAX_URI_LENGTH] = {'\0'};
  ObBackupDest dest;
  ObBackupStorageInfo *storage_info = NULL;
  ObObjectStorageInfo storage_info_base;
  uint64_t storage_id;
  char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = {'\0'};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", K(tenant_id), K(palf_id), K(block_id));
    // when uri is NFS, storage_info is empty.
  } else if (!is_valid_tenant_id(tenant_id)
             || !is_valid_palf_id(palf_id)
             || !is_valid_block_id(block_id)
             || part_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id), K(block_id));
  } else if (ObSharedLogUtils::construct_external_storage_access_info(
      tenant_id, ObLSID(palf_id), block_id, uri, sizeof(uri), dest, storage_id)) {
    CLOG_LOG(WARN, "construct_external_storage_access_info failed", K(tenant_id), K(palf_id), K(block_id));
  } else if (OB_ISNULL(storage_info = dest.get_storage_info())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "storage info is null", K(ret));
  } else if (OB_FAIL(storage_info_base.assign(*storage_info))) {
    CLOG_LOG(WARN, "fail to assign storage info base!", K(ret), KP(storage_info));
  } else if (OB_FAIL(storage_info_base.get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
    // no need encrypt
    CLOG_LOG(WARN, "fail to assign storage info base!", K(ret), KP(storage_info));
  } else if (OB_FAIL(run_ctx.init(uri, storage_info_str, storage_id, part_count, OPEN_FLAG::MULTI_UPLOAD_FLAG))) {
  } else {
    CLOG_LOG(TRACE, "init run_ctx success!", K(ret), KP(storage_info), K(run_ctx));
  }
  return ret;
}

int ObLogExternalStorageHandler::upload_one_part(
  const char *write_buf,
  const int64_t write_buf_size,
  const int64_t offset,
  const int64_t part_id,
  ObLogExternalStorageCtx &run_ctx)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("upload_one_part", 100_ms);
  ObLogExternalStorageCtxItem *io_ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", KR(ret));
  } else if (NULL == write_buf
             || (0 > offset || offset >= PALF_PHY_BLOCK_SIZE)
             || !run_ctx.is_valid()
             || part_id != run_ctx.get_count()) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KR(ret), K(part_id), KP(write_buf), K(offset));
  } else if (OB_FAIL(run_ctx.get_item(part_id, io_ctx))) {
    CLOG_LOG(WARN, "get_item failed", KR(ret), K(part_id), KP(write_buf), K(offset));
  } else if (OB_FAIL(handle_adapter_->async_pwrite(offset, write_buf, write_buf_size, *io_ctx))) {
    CLOG_LOG(WARN, "pwrite failed", KR(ret), K(io_ctx), KP(write_buf), K(offset));
  } else if (OB_FAIL(run_ctx.inc_count())) {
    CLOG_LOG(WARN, "inc_count failed", KR(ret), K(io_ctx), KP(write_buf), K(offset));

  } else {
    CLOG_LOG(TRACE, "upload_one_part success", KR(ret), K(part_id), K(io_ctx), KP(write_buf), K(offset), K(write_buf_size));
  }
  return ret;
}

int ObLogExternalStorageHandler::complete_multi_upload(
  ObLogExternalStorageCtx &run_ctx)
{
  return finish_multi_upload_(EnumMultiUploadResult::COMPLETE, run_ctx);
}

int ObLogExternalStorageHandler::abort_multi_upload(
  ObLogExternalStorageCtx &run_ctx)
{
  return finish_multi_upload_(EnumMultiUploadResult::ABORT, run_ctx);
}

int ObLogExternalStorageHandler::finish_multi_upload_(
  const EnumMultiUploadResult enum_result,
  ObLogExternalStorageCtx &run_ctx)
{
  int ret = OB_SUCCESS;
  ObIODevice *io_device = NULL;
  ObIOFd io_fd;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObLogExternalStorageHandler not init", KR(ret));
  } else if (OB_FAIL(run_ctx.get_io_device(io_device))) {
    CLOG_LOG(WARN, "get_io_device failed", KR(ret));
  } else if (OB_FAIL(run_ctx.get_io_fd(io_fd))) {
    CLOG_LOG(WARN, "get_io_fd failed", KR(ret));
  } else if (enum_result == EnumMultiUploadResult::COMPLETE) {
    if (OB_FAIL(io_device->complete(io_fd))) {
      CLOG_LOG(WARN, "complete multi upload failed", KR(ret), K(io_fd));
    }
  } else if (enum_result == EnumMultiUploadResult::ABORT) {
    if (OB_FAIL(io_device->abort(io_fd))) {
      CLOG_LOG(WARN, "abort multi upload failed", KR(ret), K(io_fd));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "not supported", KR(ret), K(io_fd), K(enum_result));
  }
  return ret;
}

int ObLogExternalStorageHandler::do_upload_(const char *uri,
                                            const ObBackupStorageInfo *storage_info,
                                            const uint64_t storage_id,
                                            const char *write_buf,
                                            const int64_t write_buf_size)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  ObString uri_ob_string(uri);
  if (OB_FAIL(io_adapter.write_single_file(uri_ob_string, storage_info, write_buf, write_buf_size,
      ObStorageIdMod(storage_id, ObStorageUsedMod::STORAGE_USED_CLOG)))) {
    CLOG_LOG(WARN, "write_single_file failed", K(uri_ob_string), KP(storage_info), KP(write_buf), K(write_buf_size));
  } else {
  }
  return ret;
}
// =============================== end multi upload ==============================
#endif
} // end namespace logservice
} // end namespace oceanbase
