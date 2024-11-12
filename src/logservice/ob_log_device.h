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

#ifndef OCEANBASE_LOGSERVICE_LOG_DEVICE_
#define OCEANBASE_LOGSERVICE_LOG_DEVICE_

#include "common/storage/ob_io_device.h"
#include "share/ob_errno.h"        // errno
#include "ob_log_grpc_adapter.h"

namespace oceanbase {
namespace logservice {

// ========================== log store env =================
int init_log_store_env();
void fin_log_store_env();

class ObLogDevice : public common::ObIODevice {
public:
  ObLogDevice();
  virtual ~ObLogDevice();
  virtual int init(const common::ObIODOpts &opts) override;
  int init(const ObAddr &log_store_addr,
           const int64_t cluster_id);
  virtual int reconfig(const common::ObIODOpts &opts) override;
  virtual int get_config(common::ObIODOpts &opts) override;
  virtual void destroy() override;

  virtual int start(const common::ObIODOpts &opts) override;

  //file/dir interfaces
  //
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to open
  //  OB_ALLOCATE_DISK_SPACE_FAILED: log disk is full
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int open(const char *pathname,
                   const int flags,
                   const mode_t mode,
                   common::ObIOFd &fd,
                   common::ObIODOpts *opts = NULL) override;
  virtual int complete(const ObIOFd &fd) override;
  virtual int abort(const ObIOFd &fd) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to close
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: no file for fd
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int close(const common::ObIOFd &fd) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to mkdir
  //  OB_FILE_OR_DIRECTORY_EXIST: dir has already existed
  //  OB_ALLOCATE_DISK_SPACE_FAILED: log disk is full
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int mkdir(const char *pathname, mode_t mode) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to mkdir
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: dir does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int rmdir(const char *pathname) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to unlink
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file/dir does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int unlink(const char *pathname) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to rename
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file/dir does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int rename(const char *oldpath,
                     const char *newpath) override;
  virtual int seal_file(const common::ObIOFd &fd) override;
  virtual int scan_dir(const char *dir_name,
                       int (*func)(const dirent *entry)) override;
  virtual int scan_dir(const char *dir_name,
                       common::ObBaseDirEntryOperator &op) override;
  virtual int is_tagging(const char *pathname,
                         bool &is_tagging) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_STATE_NOT_MATCH: epoch changed, need reopen and retry
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to fsync
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file/dir does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int fsync(const common::ObIOFd &fd) override;
  virtual int fdatasync(const common::ObIOFd &fd) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_STATE_NOT_MATCH: epoch changed, need reopen and retry
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to fallocate
  //  OB_ALLOCATE_DISK_SPACE_FAILED: log disk is out of size
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file/dir does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int fallocate(const common::ObIOFd &fd,
                        mode_t mode,
                        const int64_t offset,
                        const int64_t len) override;
  virtual int lseek(const common::ObIOFd &fd,
                    const int64_t offset,
                    const int whence,
                    int64_t &result_offset) override;
  virtual int truncate(const char *pathname,
                       const int64_t len) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_STATE_NOT_MATCH: epoch changed, need reopen and retry
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to ftuncate
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int ftruncate(const common::ObIOFd &fd,
                        const int64_t len) override;
  virtual int exist(const char *pathname,
                    bool &is_exist) override;
  // @return :
  //  OB_SUCCESS
  //  OB_INVALID_ARGUMENT: invalid argument
  //  OB_NOT_RUNNING: log device is not started
  //  OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to stat
  //  OB_NO_SUCH_FILE_OR_DIRECTORY: file does not exist
  //  OB_ALLOCATE_MEMORY_FAILED: out of memory
  //  OB_IO_ERROR: other io error
  virtual int stat(const char *pathname,
                   common::ObIODFileStat &statbuf) override;
  virtual int fstat(const common::ObIOFd &fd,
                    common::ObIODFileStat &statbuf) override;

  //for object device, local device should not use these
  int del_unmerged_parts(const char *pathname);
  int adaptive_exist(const char *pathname, bool &is_exist);
  int adaptive_stat(const char *pathname, ObIODFileStat &statbuf);
  int adaptive_unlink(const char *pathname);
  int adaptive_scan_dir(const char *dir_name, ObBaseDirEntryOperator &op);

  //block interfaces
  virtual int mark_blocks(common::ObIBlockIterator &block_iter) override;
  virtual int alloc_block(const common::ObIODOpts *opts, common::ObIOFd &block_id) override;
  virtual int alloc_blocks(
    const common::ObIODOpts *opts,
    const int64_t count,
    common::ObIArray<common::ObIOFd> &blocks) override;
  virtual void free_block(const common::ObIOFd &block_id) override;
  virtual int fsync_block() override;
  virtual int mark_blocks(const common::ObIArray<common::ObIOFd> &blocks) override;
  virtual int get_restart_sequence(uint32_t &restart_id) const override;

  //sync io interfaces
  //  @return :
  //    OB_SUCCESS
  //    OB_INVALID_ARGUMENT: invalid argument
  //    OB_NOT_SUPPORT: fd is a normal file
  //    OB_NOT_RUNNING: log device is not started
  //    OB_STATE_NOT_MATCH: need to reopen and pread again
  //    OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to pread
  //    OB_NO_SUCH_FILE_OR_DIRECTORY: file does not exist
  //    OB_ALLOCATE_MEMORY_FAILED: out of memory
  //    OB_IO_ERROR: other io error
  virtual int pread(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker = nullptr) override;
  //  @return :
  //    OB_SUCCESS
  //    OB_INVALID_ARGUMENT: invalid argument
  //    OB_NOT_SUPPORT: fd is a normal file
  //    OB_NOT_RUNNING: log device is not started
  //    OB_STATE_NOT_MATCH: need to reopen and pread again
  //    OB_FILE_OR_DIRECTORY_PERMISSION_DENIED: is not allowed to pread
  //    OB_NO_SUCH_FILE_OR_DIRECTORY: file does not exist
  //    OB_ALLOCATE_MEMORY_FAILED: out of memory
  //    OB_IO_ERROR: other io error
  virtual int pwrite(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    const void *buf,
    int64_t &write_size) override;
  virtual int read(
    const common::ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size) override;
  virtual int write(
    const common::ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size) override;

  //async io interfaces
  virtual int io_setup(
    uint32_t max_events,
    common::ObIOContext *&io_context) override;
  virtual int io_destroy(common::ObIOContext *io_context) override;
  virtual int io_prepare_pwrite(
    const common::ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    common::ObIOCB *iocb,
    void *callback) override;
  virtual int io_prepare_pread(
    const common::ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    common::ObIOCB *iocb,
    void *callback) override;
  virtual int io_submit(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb) override;
  virtual int io_cancel(
    common::ObIOContext *io_context,
    common::ObIOCB *iocb) override;
  virtual int io_getevents(
    common::ObIOContext *io_context,
    int64_t min_nr,
    common::ObIOEvents *events,
    struct timespec *timeout) override;
  virtual common::ObIOCB *alloc_iocb() override;
  virtual common::ObIOEvents *alloc_io_events(const uint32_t max_events) override;
  virtual void free_iocb(common::ObIOCB *iocb) override;
  virtual void free_io_events(common::ObIOEvents *io_event) override;

  // space management interface
  virtual int64_t get_total_block_size() const override;
  virtual int64_t get_free_block_count() const override;
  virtual int64_t get_max_block_size(int64_t reserved_size) const override;
  virtual int64_t get_max_block_count(int64_t reserved_size) const override;
  virtual int64_t get_reserved_block_count() const override;
  virtual int check_space_full(const int64_t required_size) const override;
  virtual int check_write_limited() const override;

  // batch interface
  int batch_fallocate(const char *dir_name,
                      const int64_t block_count,
                      const int64_t block_size);
public:
  static const int64_t RESERVED_BLOCK_INDEX = 2; // the first 2 blocks is used for super block
  static constexpr int16_t LOG_COMPATIBLE_VERSION = 1;
  static constexpr int64_t INITIAL_LOG_STORE_EPOCH = 0;
private:
  int reload_epoch_();
  bool get_reloading_state_() const;
  void wait_if_epoch_changing();
  int convert_sys_errno(const int32_t err_no);
  template <typename GrpcReq, typename GrpcResp>
  void deal_with_epoch_changed(GrpcReq &req, GrpcResp &resp);
  template <typename GrpcResp>
  int get_resp_ret_code(const GrpcResp &resp);
  template <typename GrpcResp>
  bool has_sys_errno(const GrpcResp &resp) const;
private:
  mutable ObSpinLock lock_;
  static const int64_t DEFUALT_PRE_ALLOCATED_IOCB_COUNT = 32 * 512;// 32 thread * max_io_depth
  static const int64_t RETRY_INTERVAL = 10 * 1000L; // 10ms
  int64_t version_;
  int64_t epoch_;
  int64_t pwrite_seq_;
  ObLogGrpcAdapter grpc_adapter_;
  int64_t ref_;
  bool is_reloading_;
  bool is_running_;
  bool is_inited_;
  bool is_marked_;
};

} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_LOG_DEVICE_ */
