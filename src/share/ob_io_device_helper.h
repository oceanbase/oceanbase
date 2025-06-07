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

#ifndef OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_
#define OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_

#include <stdint.h>
#include "common/storage/ob_io_device.h"
#include "share/ob_local_device.h"
#include "share/config/ob_server_config.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/ob_ss_io_device_wrapper.h"
#endif

namespace oceanbase
{
namespace share
{

/*
 * ObGetFileIdRangeFunctor is used for geting slog min id and slog max id
 */
class ObGetFileIdRangeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObGetFileIdRangeFunctor(const char *dir)
    : dir_(dir),
      min_file_id_(OB_INVALID_FILE_ID),
      max_file_id_(OB_INVALID_FILE_ID)
  {
  }
  virtual ~ObGetFileIdRangeFunctor() = default;

  virtual int func(const dirent *entry) override;
  uint32_t get_min_file_id() const { return min_file_id_; }
  uint32_t get_max_file_id() const { return max_file_id_; }
private:
  const char *dir_;
  uint32_t min_file_id_;
  uint32_t max_file_id_;

  DISALLOW_COPY_AND_ASSIGN(ObGetFileIdRangeFunctor);
};

/*
 * ObGetFileSizeFunctor is used for geting slog file size
 */
class ObGetFileSizeFunctor : public common::ObBaseDirEntryOperator
{
public:
  ObGetFileSizeFunctor(const char* dir)
    : dir_(dir), total_size_(0)
  {
  }
  virtual ~ObGetFileSizeFunctor() = default;

  virtual int func(const dirent *entry) override;
  int64_t get_total_size() const { return total_size_; }
private:
  const char* dir_;
  int64_t total_size_;

  DISALLOW_COPY_AND_ASSIGN(ObGetFileSizeFunctor);
};

class ObScanDirOp : public common::ObBaseDirEntryOperator
{
public:
  ObScanDirOp(const char *dir = nullptr)
   : dir_(dir) {}
  virtual ~ObScanDirOp() = default;
  virtual int func(const dirent *entry) override;
  int set_dir(const char *dir);
  const char *get_dir() const { return dir_; }
  TO_STRING_KV(K_(dir));
private:
  const char *dir_;
  DISALLOW_COPY_AND_ASSIGN(ObScanDirOp);
};

/*this class seems like the adapter for local/ofs device*/
class ObSNIODeviceWrapper final
{
public:
  static ObSNIODeviceWrapper &get_instance();

  int init(
      const char *data_dir,
      const char *sstable_dir,
      const int64_t block_size,
      const int64_t data_disk_percentage,
      const int64_t data_disk_size);
  void destroy();

  ObIODevice &get_local_device() { abort_unless(NULL != local_device_); return *local_device_; }

  // just for unittest (mock_tenant_module_env)
  void set_local_device(ObLocalDevice *local_device)
  {
    local_device_ = local_device;
  }

private:
  ObSNIODeviceWrapper();
  ~ObSNIODeviceWrapper();
  int get_local_device_from_mgr(share::ObLocalDevice *&local_device);

private:
  ObLocalDevice *local_device_;
  bool is_inited_;
};

struct BlockFileAttr
{
public:
  BlockFileAttr(char *store_path, const char *block_sstable_dir_name,
    const char *block_sstable_file_name, int &block_fd, int64_t &block_file_size, int64_t &block_size,
    int64_t &total_block_cnt, int64_t *&free_block_array, bool *&block_bitmap, int64_t &free_block_cnt,
    int64_t &free_block_push_pos, int64_t &free_block_pop_pos, const char *device_name)
    : store_path_(store_path), block_sstable_dir_name_(block_sstable_dir_name),
      block_sstable_file_name_(block_sstable_file_name), block_fd_(block_fd),
      block_file_size_(block_file_size), block_size_(block_size), total_block_cnt_(total_block_cnt),
      free_block_array_(free_block_array), block_bitmap_(block_bitmap),
      free_block_cnt_(free_block_cnt), free_block_push_pos_(free_block_push_pos),
      free_block_pop_pos_(free_block_pop_pos), device_name_(device_name) {}
  virtual ~BlockFileAttr() {}
  TO_STRING_KV(K_(store_path), K_(block_sstable_dir_name), K_(block_sstable_file_name), K_(block_fd),
    K_(block_file_size), K_(block_size), K_(total_block_cnt), KP_(free_block_array), KP_(block_bitmap),
    K_(free_block_cnt), K_(free_block_push_pos), K_(free_block_pop_pos), K_(device_name));

public:
  char *store_path_;
  const char *block_sstable_dir_name_;
  const char *block_sstable_file_name_;
  int &block_fd_;
  int64_t &block_file_size_;
  int64_t &block_size_;
  int64_t &total_block_cnt_;
  int64_t *&free_block_array_;
  bool *&block_bitmap_;
  int64_t &free_block_cnt_;
  int64_t &free_block_push_pos_;
  int64_t &free_block_pop_pos_;
  const char *device_name_;
};

class ObIODeviceLocalFileOp
{
public:
  static int open(const char *pathname,
                  const int flags,
                  const mode_t mode,
                  common::ObIOFd &fd,
                  common::ObIODOpts *opts = NULL);
  static int close(const common::ObIOFd &fd);
  static int mkdir(const char *pathname, mode_t mode);
  static int rmdir(const char *pathname);
  static int unlink(const char *pathname);
  static int rename(const char *oldpath, const char *newpath);
  static int seal_file(const common::ObIOFd &fd);
  static int scan_dir(const char *dir_name, int (*func)(const dirent *entry));
  static int scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op);
  static int scan_dir_rec(const char *dir_name, ObScanDirOp &reg_op, ObScanDirOp &dir_op);
  static int is_tagging(const char *pathname, bool &is_tagging);
  static int fsync(const common::ObIOFd &fd);
  static int fdatasync(const common::ObIOFd &fd);
  static int fallocate(const common::ObIOFd &fd,
                       mode_t mode,
                       const int64_t offset,
                       const int64_t len);
  static int lseek(const common::ObIOFd &fd,
                   const int64_t offset,
                   const int whence,
                   int64_t &result_offset);
  static int truncate(const char *pathname, const int64_t len);
  static int exist(const char *pathname, bool &is_exist);
  static int stat(const char *pathname, common::ObIODFileStat &statbuf);
  static int fstat(const common::ObIOFd &fd, common::ObIODFileStat &statbuf);

  static int read(
    const common::ObIOFd &fd,
    void *buf,
    const int64_t size,
    int64_t &read_size);
  static int write(
    const common::ObIOFd &fd,
    const void *buf,
    const int64_t size,
    int64_t &write_size);

  static int pread_impl(const int64_t fd,
                        void *buf,
                        const int64_t size,
                        const int64_t offset,
                        int64_t &read_size);
  static int pwrite_impl(const int64_t fd,
                         const void *buf,
                         const int64_t size,
                         const int64_t offset,
                         int64_t &write_size);
  static int convert_sys_errno();
  static int convert_sys_errno(const int error_no);

  static int get_block_file_size(const char *sstable_dir,
                                 const int64_t reserved_size,
                                 const int64_t block_size,
                                 const int64_t suggest_file_size,
                                 const int64_t disk_percentage,
                                 int64_t &block_file_size);

  static int compute_block_file_size(const char *sstable_dir,
                                     const int64_t reserved_size,
                                     const int64_t block_size,
                                     const int64_t suggest_file_size,
                                     const int64_t disk_percentage,
                                     int64_t &block_file_size);

  static int check_disk_space_available(const char *sstable_dir,
                                        const int64_t data_disk_size,
                                        const int64_t reserved_size,
                                        const int64_t used_disk_size,
                                        const bool need_report_user_error);

  static int open_block_file(const char *store_dir,
                             const char *sstable_dir,
                             const int64_t block_size,
                             const int64_t file_size,
                             const int64_t disk_percentage,
                             const int64_t reserved_size,
                             bool &is_exist,
                             BlockFileAttr &block_file_attr);
};

#ifdef OB_BUILD_SHARED_STORAGE
#define ObIODeviceWrapper ObSSIODeviceWrapper
#else
#define ObIODeviceWrapper ObSNIODeviceWrapper
#endif

#define LOCAL_DEVICE_INSTANCE ::oceanbase::share::ObIODeviceWrapper::get_instance().get_local_device()
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_IO_DEVICE_HELPER_H_
