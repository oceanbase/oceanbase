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

#ifndef SRC_SHARE_OB_LOCAL_DEVICE_H_
#define SRC_SHARE_OB_LOCAL_DEVICE_H_

#include <libaio.h>
#include "lib/allocator/ob_fifo_allocator.h"
#include "common/storage/ob_io_device.h"

namespace oceanbase {
namespace share {

class ObLocalDevice;

class ObLocalIOCB : public common::ObIOCB
{
public:
  ObLocalIOCB() : iocb_() {}
  virtual ~ObLocalIOCB() {}
private:
  friend class ObLocalDevice;
  struct iocb iocb_;
};

class ObLocalIOContext : public common::ObIOContext
{
public:
  ObLocalIOContext() : io_context_() {}
  virtual ~ObLocalIOContext() {}
private:
  friend class ObLocalDevice;
  io_context_t io_context_;
};

class ObLocalIOEvents : public common::ObIOEvents
{
public:
  ObLocalIOEvents();
  virtual ~ObLocalIOEvents();
  virtual int64_t get_complete_cnt() const override;
  virtual int get_ith_ret_code(const int64_t i) const override;
  virtual int get_ith_ret_bytes(const int64_t i) const override;
  virtual void *get_ith_data(const int64_t i) const override;
private:
  friend class ObLocalDevice;
  int64_t complete_io_cnt_;
  struct io_event *io_events_;
};

class ObLocalDevice : public common::ObIODevice {
public:
  ObLocalDevice();
  virtual ~ObLocalDevice();
  virtual int init(const common::ObIODOpts &opts) override;
  virtual int reconfig(const common::ObIODOpts &opts) override;
  virtual int get_config(common::ObIODOpts &opts) override;
  virtual void destroy() override;

  virtual int start(const common::ObIODOpts &opts) override;

  //file/dir interfaces
  virtual int open(const char *pathname, const int flags, const mode_t mode, common::ObIOFd &fd, common::ObIODOpts *opts = NULL) override;
  virtual int close(const common::ObIOFd &fd) override;
  virtual int mkdir(const char *pathname, mode_t mode) override;
  virtual int rmdir(const char *pathname) override;
  virtual int unlink(const char *pathname) override;
  virtual int rename(const char *oldpath, const char *newpath) override;
  virtual int seal_file(const common::ObIOFd &fd) override;
  virtual int scan_dir(const char *dir_name, int (*func)(const dirent *entry)) override;
  virtual int scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op) override;
  virtual int is_tagging(const char *pathname, bool &is_tagging) override;
  virtual int fsync(const common::ObIOFd &fd) override;
  virtual int fdatasync(const common::ObIOFd &fd) override;
  virtual int fallocate(const common::ObIOFd &fd, mode_t mode, const int64_t offset, const int64_t len) override;
  virtual int lseek(const common::ObIOFd &fd, const int64_t offset, const int whence, int64_t &result_offset) override;
  virtual int truncate(const char *pathname, const int64_t len) override;
  virtual int exist(const char *pathname, bool &is_exist) override;
  virtual int stat(const char *pathname, common::ObIODFileStat &statbuf) override;
  virtual int fstat(const common::ObIOFd &fd, common::ObIODFileStat &statbuf) override;

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
  virtual int pread(
    const common::ObIOFd &fd,
    const int64_t offset,
    const int64_t size,
    void *buf,
    int64_t &read_size,
    common::ObIODPreadChecker *checker = nullptr) override;
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

public:
  static const int64_t RESERVED_BLOCK_INDEX = 2; // the first 2 blocks is used for super block

private:
  int get_block_file_size(
    const char *sstable_dir,
    const int64_t reserved_size,
    const int64_t block_size,
    const int64_t suggest_file_size,
    const int64_t disk_percentage,
    int64_t &block_file_size);
  int open_block_file(
    const char *store_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t file_size,
    const int64_t disk_percentage,
    const int64_t reserved_size,
    bool &is_exist);
  int resize_block_file(const int64_t new_size);
  int64_t get_block_file_offset(const common::ObIOFd &fd, const int64_t offset);
  int try_punch_hole(const int64_t block_index);
  static int pread_impl(const int64_t fd, void *buf, const int64_t size, const int64_t offset, int64_t &read_size);
  static int pwrite_impl(const int64_t fd, const void *buf, const int64_t size, const int64_t offset, int64_t &write_size);
  static int convert_sys_errno();
private:
  static const int64_t DEFUALT_PRE_ALLOCATED_IOCB_COUNT = 32 * 512;// 32 thread * max_io_depth

  bool is_inited_;
  bool is_marked_;
  int block_fd_;
  char store_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char sstable_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  char store_path_[common::OB_MAX_FILE_NAME_LENGTH];
  lib::ObMutex block_lock_;
  int64_t block_size_;
  int64_t block_file_size_;
  int64_t disk_percentage_;
  int64_t free_block_push_pos_;
  int64_t free_block_pop_pos_;
  int64_t free_block_cnt_;
  int64_t total_block_cnt_;
  int64_t *free_block_array_;
  bool *block_bitmap_;
  common::ObFIFOAllocator allocator_;
  ObIOCBPool<ObLocalIOCB> iocb_pool_;
  bool is_fs_support_punch_hole_;
};

OB_INLINE int64_t ObLocalDevice::get_block_file_offset(const common::ObIOFd &fd, const int64_t offset)
{
  return fd.second_id_ * block_size_ + offset;
}

} /* namespace share */
} /* namespace oceanbase */

#endif /* SRC_SHARE_OB_LOCAL_DEVICE_H_ */
