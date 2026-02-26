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

#define USING_LOG_PREFIX COMMON
#include "share/io/ob_io_manager.h"
#include "share/redolog/ob_log_file_handler.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_file_manager.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace blocksstable;
namespace common
{
bool ObNormalRetryWriteParam::match(const int ret_value) const
{
  bool b_ret = false;
  if (OB_UNLIKELY(!matched_ret_values_.created())) {
    // do nothing
  } else if (0 == matched_ret_values_.size()) {
    // if array is empty, that means array matches any ret value
    b_ret = true;
  } else if (OB_HASH_EXIST == matched_ret_values_.exist_refactored(ret_value)) {
    b_ret = true;
  }
  return b_ret;
}

ObLogFileHandler::ObLogFileHandler()
  : is_inited_(false),
    log_dir_(nullptr),
    file_id_(OB_INVALID_FILE_ID),
    io_fd_(),
    file_group_(),
    file_size_(0),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObLogFileHandler::~ObLogFileHandler()
{
  destroy();
}

int ObLogFileHandler::init(
    const char *log_dir,
    int64_t file_size,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(log_dir) || OB_UNLIKELY(0 == STRLEN(log_dir))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log dir", K(ret), KP(log_dir));
  } else {
    tenant_id_ = tenant_id;
    log_dir_ = log_dir;
    file_size_ = file_size;
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("fail to init policy", K(ret), K(log_dir));
  } else if (OB_FAIL(file_group_.init(log_dir))) {
    LOG_WARN("fail to init file group", K(ret), K(log_dir));
  } else {
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObLogFileHandler::destroy()
{
  log_dir_ = nullptr;
  file_id_ = OB_INVALID_FILE_ID;
  if (io_fd_.is_normal_file() && !GCTX.is_shared_storage_mode()) {
    LOCAL_DEVICE_INSTANCE.close(io_fd_);
  }
  io_fd_.reset();
  file_group_.destroy();
  file_size_ = 0;
  is_inited_ = false;
  LOG_DEBUG("log file handler destroyed");
}

int ObLogFileHandler::open(const int64_t file_id, const int flag)
{
  int ret = OB_SUCCESS;
  ObIOFd tmp_io_fd;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", K(ret), K(file_id));
  } else if (OB_FAIL(inner_open(flag, file_id, tmp_io_fd))) {
    LOG_WARN("fail to inner open", K(ret), K(file_id));
  } else {
    io_fd_ = tmp_io_fd;
    file_id_ = file_id;
    file_group_.update_max_file_id(file_id);
  }
  return ret;
}

int ObLogFileHandler::close()
{
  int ret = OB_SUCCESS;
  if (GCTX.is_shared_storage_mode()) { // nothing to do.
  } else if (OB_FAIL(inner_close(io_fd_))) {
    LOG_WARN("fail to close", K(ret), K_(log_dir), K_(file_id), K_(io_fd));
  } else {
    io_fd_.reset();
  }
  return ret;
}

int ObLogFileHandler::exist(const int64_t file_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  char file_path[MAX_PATH_SIZE] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", K(ret), K(file_id));
  } else if (OB_FAIL(format_file_path(file_path, sizeof(file_path), log_dir_, file_id))) {
    LOG_WARN("fail to format file path", K(ret), K_(log_dir), K(file_id));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
    blocksstable::ObStorageObjectOpt opt;
    opt.set_private_slog_opt(tenant_id_, tenant_epoch_id, file_id);
    if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, 0/*do not need ls_epoch*/, is_exist))) {
      LOG_WARN("fail to check slog checkpoint file exist", K(ret), K(opt));
    }
#endif
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.exist(file_path, is_exist))) {
    LOG_WARN("fail to check file exists", K(ret), K(file_path));
  }
  return ret;
}

int ObLogFileHandler::read(void *buf, int64_t count, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || count <= 0 || offset < 0 || offset > file_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(count), K(offset), K_(file_size));
  } else if (OB_FAIL(inner_read(io_fd_, buf, count, offset, read_size))) {
    LOG_WARN("fail to read", K(ret), KP(buf), K(count), K(offset), K(read_size));
  }
  return ret;
}

int ObLogFileHandler::write(void *buf, int64_t count, const int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || count <= 0 || offset < 0 || offset >= file_size_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(count), K(offset), K_(file_size));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(normal_retry_write_object(buf, count, offset))) {
      LOG_WARN("fail to write object", K(ret), KP(buf), K(count), K(offset));
    }
#endif
  } else if (OB_FAIL(normal_retry_write_file(buf, count, offset))) {
    LOG_WARN("fail to normal_retry_write", K(ret), KP(buf), K(count), K(offset));
  }
  return ret;
}

int ObLogFileHandler::delete_file(const int64_t file_id)
{
  int ret = OB_SUCCESS;
  char file_path[MAX_PATH_SIZE] = { 0 };
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid file id", K(ret), K(file_id));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
    blocksstable::MacroBlockId object_id;
    blocksstable::ObStorageObjectOpt opt;
    opt.set_private_slog_opt(tenant_id_, tenant_epoch_id, file_id);
    if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, object_id))) {
      LOG_WARN("fail to get slog object id", K(ret), K(opt));
    } else if (OB_FAIL(ObObjectManager::delete_object(object_id, 0/* needn't ls epoch id */))) {
      LOG_WARN("fail to delete object", K(ret), K(object_id), K(opt));
    }
#endif
  } else if (OB_FAIL(format_file_path(file_path, sizeof(file_path), log_dir_, file_id))) {
    LOG_WARN("fail to format file path", K(ret), K_(log_dir), K(file_id));
  } else if (OB_FAIL(unlink(file_path))) {
    LOG_WARN("inner unlink file fail ", K(ret), K(file_path));
  }
  return ret;
}

int ObLogFileHandler::inner_open(const int flag, const int64_t file_id, ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(do_open(flag, file_id, io_fd))) {
    LOG_WARN("fail to do open", K(ret), K(file_id), K(flag));
  }
  return ret;
}

int ObLogFileHandler::inner_close(const ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!io_fd.is_normal_file()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io fd is not normal file", K(ret), K(io_fd));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.close(io_fd))) {
    LOG_WARN("fail to close io fd", K(ret), K(io_fd));
  }
  return ret;
}

int ObLogFileHandler::inner_read(const ObIOFd &io_fd, void *buf, const int64_t size,
    const int64_t offset, int64_t &read_size, int64_t retry_cnt)
{
  int ret = OB_SUCCESS;
  int64_t read_sz = 0;
  read_size = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || size <= 0 || offset < 0 || retry_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments ", K(ret), K(buf), K(size), K(offset), K(retry_cnt));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(inner_read_object(offset, size, retry_cnt, buf, read_sz))) {
      LOG_WARN("fail to inner read object", K(ret), K(file_id_), K(read_sz));
    }
#endif
  } else if (OB_FAIL(inner_read_file(io_fd, offset, size, retry_cnt, buf, read_sz))) {
    LOG_WARN("fail to inner read file", K(ret), K(read_sz));
  }
  if (OB_SUCC(ret) && read_sz == size) {
    read_size = read_sz;
    ret = OB_SUCCESS;
  } else if (OB_DATA_OUT_OF_RANGE == ret) {
    read_size = read_sz;
    ret = OB_SUCCESS;
  } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
    LOG_WARN("underlying io memory not enough", K(ret), K(buf), K(read_sz), K(size), K(offset));
  } else {
    int tmp_ret = ret;
    ret = OB_IO_ERROR;
    LOG_ERROR("fail to read", K(ret), K(tmp_ret), K(buf), K(read_sz), K(size), K(offset), K(errno));
  }
  return ret;
}

int ObLogFileHandler::inner_read_file(
    const ObIOFd &io_fd,
    const int64_t offset,
    const int64_t size,
    const int64_t retry_cnt,
    void *buf,
    int64_t &read_sz)
{
  int ret = OB_SUCCESS;
  if (!io_fd.is_normal_file()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io fd is not normal file", K(ret), K(io_fd));
  } else {
    int cnt =0;
    while (OB_SUCC(ret) && read_sz < size && cnt++ < retry_cnt) {
      ObIOInfo io_info;
      io_info.tenant_id_ = tenant_id_;
      io_info.fd_ = io_fd;
      io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
      io_info.offset_ = offset + read_sz;
      io_info.size_ = size - read_sz;
      io_info.flag_.set_mode(ObIOMode::READ);
      io_info.flag_.set_sys_module_id(ObIOModule::SLOG_IO);
      io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      io_info.buf_ = nullptr;
      io_info.user_data_buf_ = reinterpret_cast<char*>(buf) + read_sz;
      io_info.callback_ = nullptr;
      io_info.timeout_us_ = GCONF._data_storage_io_timeout;

      ObIOHandle io_handle;
      io_handle.reset();
      ret = ObIOManager::get_instance().read(io_info, io_handle);
      if (OB_DATA_OUT_OF_RANGE == ret && io_handle.get_data_size() < io_info.size_) { // partial read
        read_sz += io_handle.get_data_size();
        break;
      } else if (OB_SUCCESS != ret) {
        LOG_WARN("fail to aio_read", K(ret), K(io_info));
      } else if (io_handle.get_data_size() > io_info.size_) {
        ret = OB_IO_ERROR;
        LOG_WARN("invalid io handle data size", K(ret),
            "data size", io_handle.get_data_size(), "left buffer size", io_info.size_);
      } else {
        read_sz += io_handle.get_data_size();
      }
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLogFileHandler::inner_read_object(
    const int64_t offset,
    const int64_t size,
    const int64_t retry_cnt,
    void *buf,
    int64_t &read_sz)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
  blocksstable::MacroBlockId object_id;
  blocksstable::ObStorageObjectOpt opt;
  opt.set_private_slog_opt(tenant_id_, tenant_epoch_id, file_id_);
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, object_id))) {
    LOG_WARN("fail to get slog object id", K(ret), K(opt));
  } else {
    int64_t cnt =0;
    while (OB_SUCC(ret) && read_sz < size && cnt++ < retry_cnt) {
      ObStorageObjectReadInfo read_info;
      read_info.mtl_tenant_id_ = tenant_id_;
      read_info.macro_block_id_ = object_id;
      read_info.offset_ = offset + read_sz;
      read_info.size_ = size - read_sz;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
      read_info.buf_ = reinterpret_cast<char*>(buf) + read_sz;
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;

      ObStorageObjectHandle handle;
      handle.reset();
      ret = ObObjectManager::read_object(read_info, handle);
      if (OB_DATA_OUT_OF_RANGE == ret && handle.get_data_size() < read_info.size_) { // partial read
        read_sz += handle.get_data_size();
        break;
      } else if (OB_SUCCESS != ret) {
        LOG_WARN("fail to aio_read", K(ret), K(read_info));
      } else if (handle.get_data_size() > read_info.size_) {
        ret = OB_IO_ERROR;
        LOG_WARN("invalid io handle data size", K(ret),
            "data size", handle.get_data_size(), "left buffer size", read_info.size_);
      } else {
        read_sz += handle.get_data_size();
      }
    }
  }
  return ret;
}
#endif

int ObLogFileHandler::unlink(const char* file_path)
{
  int ret = OB_SUCCESS;
  const int64_t UNLINK_RETRY_INTERVAL_US = 100L * 1000; // 100ms

  if (OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to format file path", K(ret), KP(file_path));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(LOCAL_DEVICE_INSTANCE.unlink(file_path)) && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
      LOG_WARN("unlink failed", K(ret), K(file_path));
      ob_usleep<ObWaitEventIds::SLOG_NORMAL_RETRY_SLEEP>(UNLINK_RETRY_INTERVAL_US);
      ret = OB_SUCCESS;
    } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
      ret = OB_SUCCESS;
      break;
    } else {
      break;
    }
  }
  return ret;
}

int ObLogFileHandler::normal_retry_write_file(void *buf, int64_t size, int64_t offset)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(buf) || size <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments ", K(ret), K(buf), K(size), K(offset));
  } else if (!io_fd_.is_normal_file()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io fd is not normal file", K(ret), K_(io_fd));
  } else {
    int64_t retry_cnt = 0;
    do {
      ObIOInfo io_info;
      io_info.flag_.set_write();
      io_info.tenant_id_ = tenant_id_;
      io_info.fd_ = io_fd_;
      io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
      io_info.offset_ = offset;
      io_info.size_ = size;
      io_info.flag_.set_sys_module_id(ObIOModule::SLOG_IO);
      io_info.flag_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      io_info.buf_ = reinterpret_cast<const char *>(buf);
      io_info.callback_ = nullptr;
      io_info.timeout_us_ = GCONF._data_storage_io_timeout;
      ObIOHandle io_handle;
      if (OB_FAIL(ObIOManager::get_instance().aio_write(io_info, io_handle))) {
        LOG_WARN("fail to aio_write", K(ret), K(io_info));
      } else if(OB_FAIL(io_handle.wait())) {
        LOG_WARN("failed to wait for aio_write", K(ret));
      }

      if (OB_FAIL(ret)) {
        retry_cnt ++;
        if (REACH_TIME_INTERVAL(LOG_INTERVAL_US)) {
          LOG_WARN("fail to aio_write", K(ret), K(io_info), K(retry_cnt));
        } else {
          ob_usleep<ObWaitEventIds::SLOG_NORMAL_RETRY_SLEEP>(SLEEP_TIME_US);
        }
      }
    } while (OB_FAIL(ret));
  }

  return ret;
}

int ObLogFileHandler::normal_retry_write_object(
    void *buf,
    const int64_t size,
    const int64_t offset)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
  blocksstable::ObStorageObjectOpt opt;
  opt.set_private_slog_opt(tenant_id_, tenant_epoch_id, file_id_);
  if (OB_ISNULL(buf) || size <= 0 || offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments ", K(ret), K(buf), K(size), K(offset));
  } else {
    int64_t retry_cnt = 0;
    do {
      ObStorageObjectWriteInfo write_info;
      write_info.mtl_tenant_id_ = tenant_id_;
      write_info.offset_ = offset;
      write_info.size_ = size;
      write_info.io_desc_.set_write();
      write_info.io_desc_.set_sys_module_id(ObIOModule::SLOG_IO);
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.buffer_ = reinterpret_cast<char*>(buf);
      write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      ObStorageObjectHandle handle;
      handle.reset();
      if (OB_FAIL(ObObjectManager::write_object(opt, write_info, handle))) {
        LOG_WARN("fail to write object", K(ret), K(write_info));
      }
      if (OB_FAIL(ret)) {
        retry_cnt ++;
        if (REACH_TIME_INTERVAL(LOG_INTERVAL_US)) {
          LOG_WARN("fail to aio_write", K(ret), K(write_info), K(retry_cnt));
        } else {
          ob_usleep<ObWaitEventIds::SLOG_NORMAL_RETRY_SLEEP>(SLEEP_TIME_US);
        }
      }
    } while (OB_FAIL(ret));
  }
  return ret;
}

int ObLogFileHandler::open(const char *file_path, const int flags, const mode_t mode, ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(file_path) || OB_UNLIKELY(0 == STRLEN(file_path))) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args", K(ret), K(file_path));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    // nothing to do.
#endif
  } else {
    const int64_t MAX_RETRY_TIME = 30 * 1000 * 1000;
    const int64_t start_time = ObTimeUtility::fast_current_time();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(file_path, flags, mode, io_fd))) {
        LOG_WARN("failed to open file", K(ret), K(file_path), K(errno), KERRMSG);
        if (OB_TIMEOUT == ret || OB_EAGAIN == ret || OB_SERVER_OUTOF_DISK_SPACE == ret) {
          ret = OB_SUCCESS;
          ob_usleep<ObWaitEventIds::SLOG_NORMAL_RETRY_SLEEP>(ObLogDefinition::RETRY_SLEEP_TIME_IN_US);
        }
      } else {
        break;
      }
    }

    if (OB_SUCC(ret)) {
      const int64_t total_retry_time = ObTimeUtility::fast_current_time() - start_time;
      if (total_retry_time > MAX_RETRY_TIME) {
        LOG_WARN_RET(OB_ERR_TOO_MUCH_TIME, "open file costs too much time", K(ret), K(total_retry_time), K(file_path), K(io_fd));
      }
    }
  }

  return ret;
}

int ObLogFileHandler::format_file_path(char *buf, const int64_t buf_size,
    const char *log_dir, const int64_t file_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || buf_size <= 0 || OB_ISNULL(log_dir) || !is_valid_file_id(file_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_size), KP(log_dir), K(file_id));
  } else if (STRLEN(log_dir) <= 0 || STRLEN(log_dir) >= buf_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log_dir", K(ret), K(buf_size), K(log_dir));
  } else {
    int pret = snprintf(buf, buf_size, "%s/%ld", log_dir, file_id);
    if (pret <= 0 || pret >= buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", K(ret), K(log_dir), K(file_id));
    }
  }
  return ret;
}

int ObLogFileHandler::do_open(const int flag, const int64_t file_id, ObIOFd &io_fd)
{
  int ret = OB_SUCCESS;
  char file_path[MAX_PATH_SIZE] = { 0 };
  if (OB_FAIL(format_file_path(file_path, sizeof(file_path), log_dir_, file_id))) {
    LOG_WARN("fail to format file path", K(ret), K_(log_dir), K(file_id));
  } else if (OB_FAIL(open(file_path, flag, ObLogDefinition::FILE_OPEN_MODE, io_fd))) {
    LOG_WARN("fail to do open", K(ret), K(flag),
      LITERAL_K(ObLogDefinition::FILE_OPEN_MODE), K(file_path), K(flag), K(io_fd));
  }
  return ret;
}

int ObLogFileHandler::get_total_used_size(int64_t &using_space) const
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(file_group_.get_total_used_size(using_space))) {
      LOG_WARN("fail to get total used size", K(ret));
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else {
    const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_slog_used_size(tenant_id_, tenant_epoch_id, using_space))) {
      LOG_WARN("fail to get slog used size", K(ret));
    }
#endif
  }
  return ret;
}

int ObLogFileHandler::get_file_id_range(int64_t &min_file_id, int64_t &max_file_id)
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(file_group_.get_file_id_range(min_file_id, max_file_id))) {
      LOG_WARN("fail to get file id range", K(ret));
    }
#ifdef OB_BUILD_SHARED_STORAGE
  } else {
    const int64_t tenant_epoch_id = OB_SERVER_TENANT_ID == tenant_id_ ? 0 : MTL_EPOCH_ID();
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_slog_id_range(tenant_id_, tenant_epoch_id, min_file_id, max_file_id))) {
      LOG_WARN("fail to get slog id range", K(ret));
    }
#endif
  }
  return ret;
}

}
}
