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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_DEVICE_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_STORAGE_DEVICE_H_
#include "common/storage/ob_io_device.h"
#include "lib/restore/ob_storage.h"
#include "common/storage/ob_fd_simulator.h"
#include "lib/allocator/ob_pooled_allocator.h"

namespace oceanbase
{
namespace common
{

/*
there are three write mode
------use write interface----
1、write single file, use write, truncate mode(USE util)
2、appender mode, application code use this mode in its most scenario (USE interface directly)
------use pwrite interface
3、random write, application set write position(USE interface directly)

in the current cos/oss code implement, we have appender&writer
now we define that:
1、write only for write single file
2、appender use in the scenario which need position(offset), 
   absolutely append mode & random mode are this scenario.

write single file->write->storage_writer
random write->pwrite->storage_appender->pwrite,(without append mode)
appender->write->storage_appender->write(with append mode)

there are two read mode, but alse use pread interface(only provide pread interface)
1、read single file (USE util)
2、read part file, application provide offset (USE util)
*/
class ObObjectDevice : public common::ObIODevice
{
public:
  ObObjectDevice();
  virtual ~ObObjectDevice();
 
  /*the interface need override*/
  virtual int init(const ObIODOpts &opts) override;
  virtual void destroy() override;
  virtual int start(const ObIODOpts &opts) override;
  //file/dir interfaces
  virtual int open(const char *pathname, const int flags, const mode_t mode, 
                   ObIOFd &fd, ObIODOpts *opts= NULL) override;
  virtual int close(const ObIOFd &fd) override;
  virtual int mkdir(const char *pathname, mode_t mode) override;
  virtual int rmdir(const char *pathname) override;
  virtual int unlink(const char *pathname) override;
  virtual int exist(const char *pathname, bool &is_exist) override;
  //sync io interfaces
  virtual int pread(const ObIOFd &fd, const int64_t offset, const int64_t size,
                    void *buf, int64_t &read_size, ObIODPreadChecker *checker = nullptr) override;
  virtual int pwrite(const ObIOFd &fd, const int64_t offset, const int64_t size,
                     const void *buf, int64_t &write_size) override;
  virtual int scan_dir(const char *dir_name, ObBaseDirEntryOperator &op) override;
  virtual int is_tagging(const char *pathname, bool &is_tagging) override;
  virtual int stat(const char *pathname, ObIODFileStat &statbuf) override;
  //add new
  virtual int get_config(ObIODOpts &opts) override;

public:
  common::ObFdSimulator& get_fd_mng() {return fd_mng_;}                 

private:
  int get_access_type(ObIODOpts *opts, ObStorageAccessType& access_type);
  int open_for_reader(const char *pathname, void*& ctx);
  int open_for_overwriter(const char *pathname, void*& ctx);
  int open_for_appender(const char *pathname, ObIODOpts *opts, void*& ctx);
  int release_res(void* ctx, const ObIOFd &fd, ObStorageAccessType access_type);
private:
  //maybe fd mng can be device level
  common::ObFdSimulator    fd_mng_;
  
  ObStorageUtil            util_;
  /*obj ctx pool: use to create fd ctx(reader/writer)*/
  common::ObPooledAllocator<ObStorageReader, ObMalloc, ObSpinLock> reader_ctx_pool_;
  common::ObPooledAllocator<ObStorageAppender, ObMalloc, ObSpinLock> appender_ctx_pool_;
  common::ObPooledAllocator<ObStorageWriter, ObMalloc, ObSpinLock> overwriter_ctx_pool_;
  common::ObObjectStorageInfo storage_info_;
  bool is_started_;
  char storage_info_str_[OB_MAX_URI_LENGTH];
  common::ObSpinLock lock_;

private:
  /*Object device will not use this interface, just return not support error code*/
  virtual int reconfig(const ObIODOpts &opts) override;
  virtual int rename(const char *oldpath, const char *newpath) override;
  virtual int seal_file(const ObIOFd &fd) override;
  virtual int scan_dir(const char *dir_name, int (*func)(const dirent *entry)) override;
  virtual int fsync(const ObIOFd &fd) override;
  virtual int fdatasync(const ObIOFd &fd) override;
  virtual int fallocate(const ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len) override;
  virtual int lseek(const ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset) override;
  virtual int truncate(const char *pathname, const int64_t len) override; 
  virtual int fstat(const ObIOFd &fd, ObIODFileStat &statbuf) override;
  //block interfaces
  virtual int mark_blocks(ObIBlockIterator &block_iter) override;
  virtual int alloc_block(const ObIODOpts *opts, ObIOFd &block_id) override;
  virtual int alloc_blocks(
    const ObIODOpts *opts,
    const int64_t count,
    ObIArray<ObIOFd> &blocks) override;
  virtual void free_block(const ObIOFd &block_id) override;
  virtual int fsync_block() override;
  virtual int mark_blocks(const ObIArray<ObIOFd> &blocks) override;
  virtual int get_restart_sequence(uint32_t &restart_id) const override;
  virtual int read(
    const ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size) override;
  virtual int write(
    const ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size) override;
  //async io interfaces
  virtual int io_setup(
    uint32_t max_events,
    ObIOContext *&io_context) override;
  virtual int io_destroy(ObIOContext *io_context) override;
  virtual int io_prepare_pwrite(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback) override;
  virtual int io_prepare_pread(
    const ObIOFd &fd,
    void *buf,
    size_t count,
    int64_t offset,
    ObIOCB *iocb,
    void *callback) override;
  virtual int io_submit(
    ObIOContext *io_context,
    ObIOCB *iocb) override;
  virtual int io_cancel(
    ObIOContext *io_context,
    ObIOCB *iocb) override;
  virtual int io_getevents(
    ObIOContext *io_context,
    int64_t min_nr,
    ObIOEvents *events,
    struct timespec *timeout) override;
  virtual ObIOCB *alloc_iocb() override;
  virtual ObIOEvents *alloc_io_events(const uint32_t max_events) override;
  virtual void free_iocb(ObIOCB *iocb) override;
  virtual void free_io_events(ObIOEvents *io_event) override;

  // space management interface
  virtual int64_t get_total_block_size() const override;
  virtual int64_t get_free_block_count() const override;
  virtual int64_t get_max_block_size(int64_t reserved_size) const override;
  virtual int64_t get_max_block_count(int64_t reserved_size) const override;
  virtual int64_t get_reserved_block_count() const override;
  virtual int check_space_full(const int64_t required_size) const override;
};



}
}
#endif