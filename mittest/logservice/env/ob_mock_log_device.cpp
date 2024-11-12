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

#include "ob_mock_log_device.h"
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include <unistd.h>
#include <linux/falloc.h>
#include "share/ob_errno.h"                      // errno
#include "lib/utility/ob_utility.h"              // ob_pread
#include "share/ob_define.h"
#include "logservice/palf/log_io_utils.h"
#include "logservice/ob_log_io_adapter.h"
#include "share/ob_local_device.h"
#define private public
#define protected public
#include "share/ob_device_manager.h"
#undef private
#undef protected

using namespace oceanbase::common;

namespace oceanbase {
namespace logservice {
// ===================== log store env =======================
ObMockLogDevice::ObMockLogDevice()
{
}

ObMockLogDevice::~ObMockLogDevice()
{
  destroy();
}

int ObMockLogDevice::init(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObMockLogDevice::reconfig(const common::ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::get_config(ObIODOpts &opts)
{
  UNUSED(opts);
  return OB_NOT_SUPPORTED;
}

// TODO by runlin shake
int ObMockLogDevice::start(const common::ObIODOpts &opts)
{
  int ret = OB_SUCCESS;
  return ret;
}

// TODO by runlin shre?
void ObMockLogDevice::destroy()
{
}

//file/dir interfaces
int ObMockLogDevice::open(const char *pathname,
                      const int flags,
                      const mode_t mode,
                      ObIOFd &fd,
                      common::ObIODOpts *opts)
{
  return LOG_IO_ADAPTER.get_local_device()->open(pathname, flags, mode, fd, opts);
}

int ObMockLogDevice::complete(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::abort(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::close(const ObIOFd &fd)
{
  return LOG_IO_ADAPTER.get_local_device()->close(fd);
}

int ObMockLogDevice::mkdir(const char *pathname, mode_t mode)
{
  return LOG_IO_ADAPTER.get_local_device()->mkdir(pathname, mode);
}

// TODO by nianguan:distinguish 'rm' from 'rm -r'
int ObMockLogDevice::rmdir(const char *pathname)
{
  return LOG_IO_ADAPTER.get_local_device()->rmdir(pathname);
}

int ObMockLogDevice::unlink(const char *pathname)
{
  return LOG_IO_ADAPTER.get_local_device()->unlink(pathname);
}

int ObMockLogDevice::rename(const char *oldpath, const char *newpath)
{
  return LOG_IO_ADAPTER.get_local_device()->rename(oldpath, newpath);
}

int ObMockLogDevice::seal_file(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  UNUSED(dir_name);
  UNUSED(func);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::is_tagging(const char *pathname, bool &is_tagging)
{
  UNUSED(pathname);
  UNUSED(is_tagging);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  return LOG_IO_ADAPTER.get_local_device()->scan_dir(dir_name, op);
}

int ObMockLogDevice::fsync(const ObIOFd &fd)
{
  return LOG_IO_ADAPTER.get_local_device()->fsync(fd);
}

int ObMockLogDevice::fdatasync(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::fallocate(const ObIOFd &fd,
                           mode_t mode,
                           const int64_t offset,
                           const int64_t len)
{
  return LOG_IO_ADAPTER.get_local_device()->fallocate(fd, mode, offset, len);
}

int ObMockLogDevice::lseek(const ObIOFd &fd,
                       const int64_t offset,
                       const int whence,
                       int64_t &result_offset)
{
  UNUSED(fd);
  UNUSED(offset);
  UNUSED(whence);
  UNUSED(result_offset);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::truncate(const char *pathname, const int64_t len)
{
  UNUSED(pathname);
  UNUSED(len);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::ftruncate(const ObIOFd &fd, const int64_t len)
{
  return LOG_IO_ADAPTER.get_local_device()->ftruncate(fd, len);
}

int ObMockLogDevice::exist(const char *pathname, bool &is_exist)
{
  UNUSED(pathname);
  UNUSED(is_exist);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::stat(const char *pathname, ObIODFileStat &statbuf)
{
  return LOG_IO_ADAPTER.get_local_device()->stat(pathname, statbuf);
}

int ObMockLogDevice::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  // if (IS_NOT_INIT) {
  //   ret = OB_NOT_INIT;
  // } else if (!is_running_) {
  //   ret = OB_STATE_NOT_MATCH;
  //   CLOG_LOG(WARN, "ObMockLogDevice has not been started", K(ret), K_(is_running));
  // } else if (!fd.is_valid()) {
  //   ret = OB_INVALID_ARGUMENT;
  //   CLOG_LOG(WARN, "invalid arguments.", K(fd), K(ret));
  // } else {
  //   FstatReq req;
  //   StatResp resp;
  //   req.set_epoch(epoch_);
  //   req.set_fd(fd.second_id_);

  //   if (OB_FAIL(grpc_adapter_.fstat(req, resp))) {
  //     CLOG_LOG(ERROR, "grpc call failed for fstat", K(ret));
  //   } else if (true == is_epoch_changed(resp)) {
  //     ret = OB_STATE_NOT_MATCH;
  //     CLOG_LOG(WARN, "epoch changed, maybe logstore process restart", K(ret));
  //   } else if (true == has_sys_errno(resp)) {
  //     ret = convert_sys_errno(resp.err_no());
  //     CLOG_LOG(WARN, "logstore encounters erros", K(ret), K(resp.err_no()));
  //   } else {
  //     CLOG_LOG(TRACE, "ObMockLogDevice fstat successfully");
  //   }
  // }

  return ret;
}

int ObMockLogDevice::del_unmerged_parts(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::adaptive_exist(const char *pathname, bool &is_exist)
{
  UNUSED(pathname);
  UNUSED(is_exist);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::adaptive_stat(const char *pathname, ObIODFileStat &statbuf)
{
  UNUSED(pathname);
  UNUSED(statbuf);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::adaptive_unlink(const char *pathname)
{
  UNUSED(pathname);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op)
{
  UNUSED(dir_name);
  UNUSED(op);
  return OB_NOT_SUPPORTED;
}

//block interfaces
int ObMockLogDevice::mark_blocks(ObIBlockIterator &block_iter)
{
  UNUSED(block_iter);
  return OB_NOT_SUPPORTED;
}


int ObMockLogDevice::alloc_block(const common::ObIODOpts *opts, ObIOFd &block_id)
{
  UNUSED(opts);
  UNUSED(block_id);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::alloc_blocks(
    const common::ObIODOpts *opts,
    const int64_t count,
    common::ObIArray<ObIOFd> &blocks)
{
  UNUSED(opts);
  UNUSED(count);
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

void ObMockLogDevice::free_block(const ObIOFd &block_id)
{
  UNUSED(block_id);
}

int ObMockLogDevice::fsync_block()
{
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::get_restart_sequence(uint32_t &restart_id) const
{
  UNUSED(restart_id);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::mark_blocks(const common::ObIArray<ObIOFd> &blocks)
{
  UNUSED(blocks);
  return OB_NOT_SUPPORTED;
}

//sync io interfaces
int ObMockLogDevice::pread(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker)
{
  return LOG_IO_ADAPTER.get_local_device()->pread(fd, offset, size, buf, read_size, checker);
}

int ObMockLogDevice::pwrite(
    const ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size)
{
  return LOG_IO_ADAPTER.get_local_device()->pwrite(fd, offset, size, buf, write_size);
}

int ObMockLogDevice::read(
  const ObIOFd &fd,
  void *buf,
  const int64_t size,
  int64_t &read_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(read_size);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::write(
  const ObIOFd &fd,
  const void *buf,
  const int64_t size,
  int64_t &write_size)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(size);
  UNUSED(write_size);
  return OB_NOT_SUPPORTED;
}

//async io interfaces
int ObMockLogDevice::io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context)
{
  UNUSED(max_events);
  UNUSED(io_context);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_destroy(common::ObIOContext *io_context)
{
  UNUSED(io_context);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_prepare_pread(
  const ObIOFd &fd,
  void *buf,
  size_t count,
  int64_t offset,
  ObIOCB *iocb,
  void *callback)
{
  UNUSED(fd);
  UNUSED(buf);
  UNUSED(count);
  UNUSED(offset);
  UNUSED(iocb);
  UNUSED(callback);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_submit(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_cancel(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb)
{
  UNUSED(io_context);
  UNUSED(iocb);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::io_getevents(
    common::ObIOContext *io_context,
    int64_t min_nr,
    common::ObIOEvents *events,
    struct timespec *timeout)
{
  UNUSED(io_context);
  UNUSED(min_nr);
  UNUSED(events);
  UNUSED(timeout);
  return OB_NOT_SUPPORTED;
}

common::ObIOCB* ObMockLogDevice::alloc_iocb()
{
  ob_assert(false);
  return nullptr;
}

common::ObIOEvents *ObMockLogDevice::alloc_io_events(const uint32_t max_events)
{
  ob_assert(false);
  UNUSED(max_events);
  return nullptr;
}

void ObMockLogDevice::free_iocb(common::ObIOCB *iocb)
{
  ob_assert(false);
  UNUSED(iocb);
}

void ObMockLogDevice::free_io_events(common::ObIOEvents *io_event)
{
  ob_assert(false);
  UNUSED(io_event);
}

int64_t ObMockLogDevice::get_total_block_size() const
{
  ob_assert(false);
  return 0;
}

int64_t ObMockLogDevice::get_free_block_count() const
{
  ob_assert(false);
  return 0;
}

int64_t ObMockLogDevice::get_reserved_block_count() const
{
  ob_assert(false);
  return 0;
}

int64_t ObMockLogDevice::get_max_block_count(int64_t reserved_size) const
{
  ob_assert(false);
  return 0;
}

int64_t ObMockLogDevice::get_max_block_size(int64_t reserved_size) const
{
  ob_assert(false);
  return 0;
}

int ObMockLogDevice::check_space_full(const int64_t required_size) const
{
  UNUSED(required_size);
  return OB_NOT_SUPPORTED;
}

int ObMockLogDevice::check_write_limited() const
{
  return OB_NOT_SUPPORTED;
}

#define DEVICE_MGR (ObDeviceManager::get_instance())
int add_mock_device_to_device_manager(ObDeviceManager::ObDeviceInsInfo*& device_info)
{
  int ret = OB_SUCCESS;
  int64_t last_no_ref_idx = -1;
  int64_t avai_idx = -1;
  ObIODevice* device_handle = NULL;
  const char *mock_device_info = "mock://";
  ObString storage_info(mock_device_info);

  ObStorageType device_type = OB_STORAGE_MAX_TYPE;
  void* mem = NULL;
  device_type = OB_STORAGE_LOG_STORE;
  mem = DEVICE_MGR.allocator_.alloc(sizeof(ObMockLogDevice));
  if (NULL != mem) {new(mem)ObMockLogDevice;} else {return OB_ALLOCATE_MEMORY_FAILED;}
  device_handle = static_cast<ObIODevice*>(mem);
  device_handle->device_type_ = device_type;

  //first validate the key(storage info)
  //find a device slot
  for (int i = 0; i < DEVICE_MGR.MAX_DEVICE_INSTANCE; i++) {
    if (0 == DEVICE_MGR.device_ins_[i].ref_cnt_ && NULL == DEVICE_MGR.device_ins_[i].device_) {
      avai_idx = i;
      break;
    } else if (0 == DEVICE_MGR.device_ins_[i].ref_cnt_ && NULL != DEVICE_MGR.device_ins_[i].device_) {
      last_no_ref_idx = i;
    }
  }

  if (-1 == avai_idx && -1 == last_no_ref_idx) {
    OB_LOG(WARN, "devices too mang!", K(DEVICE_MGR.MAX_DEVICE_INSTANCE));
    //cannot insert into device manager
    ret = OB_OUT_OF_ELEMENT;
  } else {
    //try to release one
    if (-1 == avai_idx && -1 != last_no_ref_idx) {
      //erase from map
      ObString old_key(DEVICE_MGR.device_ins_[last_no_ref_idx].storage_info_);
      if (OB_FAIL(DEVICE_MGR.device_map_.erase_refactored(old_key))) {
        OB_LOG(WARN, "fail to erase device from device map",
            KP(old_key.ptr()), K(ret));
      } else if (OB_FAIL(DEVICE_MGR.handle_map_.erase_refactored((int64_t)(DEVICE_MGR.device_ins_[last_no_ref_idx].device_)))) {
        OB_LOG(WARN, "fail to erase device from handle map", K(ret));
      } else {
        /*free the resource*/
        ObIODevice* del_device = DEVICE_MGR.device_ins_[last_no_ref_idx].device_;
        del_device->destroy();
        DEVICE_MGR.allocator_.free(del_device);
        DEVICE_MGR.device_ins_[last_no_ref_idx].device_ = NULL;
        DEVICE_MGR.device_count_--;
        avai_idx = last_no_ref_idx;
        OB_LOG(INFO, "release one device for realloc another!");
      }
    }

    if (OB_SUCCESS == ret) {
      //insert into map
      STRCPY(DEVICE_MGR.device_ins_[avai_idx].storage_info_, storage_info.ptr());
      ObString cur_key(DEVICE_MGR.device_ins_[avai_idx].storage_info_);
      if (OB_FAIL(DEVICE_MGR.device_map_.set_refactored(cur_key, &(DEVICE_MGR.device_ins_[avai_idx])))) {
        OB_LOG(WARN, "fail to set device to device map!", KR(ret));
      } else if (OB_FAIL(DEVICE_MGR.handle_map_.set_refactored((int64_t)(device_handle), &(DEVICE_MGR.device_ins_[avai_idx])))) {
        OB_LOG(WARN, "fail to set device to handle map!",
            K(ret), KP(storage_info.ptr()));
      } else {
        OB_LOG(INFO, "success insert into map!",
            KP(storage_info.ptr()));
      }
    }
  }


  if (OB_FAIL(ret)) {
    if (NULL != device_handle) {
      DEVICE_MGR.allocator_.free(device_handle);
    }
    device_info = NULL;
  } else {
    DEVICE_MGR.device_ins_[avai_idx].device_ = device_handle;
    DEVICE_MGR.device_ins_[avai_idx].ref_cnt_ = 0;
    DEVICE_MGR.device_count_++;
    OB_LOG(INFO, "alloc a new device!", KP(storage_info.ptr()));
    device_info = &(DEVICE_MGR.device_ins_[avai_idx]);
  }

  return ret;
}

} // namespace logservice
} // namespace oceanbase
