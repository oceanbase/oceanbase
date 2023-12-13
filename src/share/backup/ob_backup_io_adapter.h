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

#ifndef SRC_LIBRARY_SRC_LIB_RESTORE_OB_BACKUP_IO_ADAPTER_H_
#define SRC_LIBRARY_SRC_LIB_RESTORE_OB_BACKUP_IO_ADAPTER_H_

#include "common/storage/ob_io_device.h"
#include "common/storage/ob_device_common.h"
#include "lib/container/ob_array.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace common
{

class ObBackupIoAdapter
{
public:
  explicit ObBackupIoAdapter() {}
  virtual ~ObBackupIoAdapter() {}
  int is_exist(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &exist);
  //TODO (@shifangdan.sfd): refine repeated logics between normal interfaces and adaptive ones
  int adaptively_is_exist(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &exist);
  int get_file_length(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, int64_t &file_length);
  int adaptively_get_file_length(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, int64_t &file_length);
  int del_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  int adaptively_del_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  int del_unmerged_parts(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  int mkdir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  int mk_parent_dir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  int write_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, const char *buf, const int64_t size);
  int read_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf,const int64_t buf_size,
      int64_t &read_size);
  int adaptively_read_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf,const int64_t buf_size,
      int64_t &read_size);
  int read_single_text_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf, const int64_t buf_size);
  int adaptively_read_single_text_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf, const int64_t buf_size);
  int list_files(
      const common::ObString &dir_path,
      const share::ObBackupStorageInfo *storage_info,
      common::ObBaseDirEntryOperator &op);
  int adaptively_list_files(
      const common::ObString &dir_path,
      const share::ObBackupStorageInfo *storage_info,
      common::ObBaseDirEntryOperator &op);
  int read_part_file(
      const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
      char *buf, const int64_t buf_size, const int64_t offset,
      int64_t &read_size);
  int adaptively_read_part_file(
      const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
      char *buf, const int64_t buf_size, const int64_t offset,
      int64_t &read_size);
  int del_dir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info);
  /*backup logical related func*/
  int get_file_size(ObIODevice* device_handle, const ObIOFd &fd, int64_t &file_length);
  int delete_tmp_files(
      const common::ObString &uri,
      const share::ObBackupStorageInfo *storage_info);
  int is_empty_directory(const common::ObString &uri, 
      const share::ObBackupStorageInfo *storage_info, 
      bool &is_empty_directory);
  int open_with_access_type(ObIODevice*& device_handle, ObIOFd &fd, 
              const share::ObBackupStorageInfo *storage_info, const common::ObString &uri,
              ObStorageAccessType access_type);
  int list_directories(
      const common::ObString &uri,
      const share::ObBackupStorageInfo *storage_info,
      common::ObBaseDirEntryOperator &op);
  int is_tagging(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &is_tagging);
  int close_device_and_fd(ObIODevice*& device_handle, ObIOFd &fd);
  int get_and_init_device(
      ObIODevice*& dev_handle, 
      const share::ObBackupStorageInfo *storage_info,
      const common::ObString &storage_type_prefix);
  int set_access_type(ObIODOpts* opts, bool is_appender, int max_opt_num);
  int set_open_mode(ObIODOpts* opts, bool lock_mode, bool new_file, int max_opt_num);
  int set_append_strategy(ObIODOpts* opts, bool is_data_file, int64_t epoch, int max_opt_num);

private:
  int do_read_part_file(
      const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
      char *buf, const int64_t buf_size, const int64_t offset,
      int64_t &read_size);
  
  
  DISALLOW_COPY_AND_ASSIGN(ObBackupIoAdapter);
};

class ObCntFileListOp : public ObBaseDirEntryOperator
{
public:
  ObCntFileListOp() : file_count_(0) {}
  ~ObCntFileListOp() {}
  int func(const dirent *entry) 
  {
    UNUSED(entry);
    file_count_++;
    return OB_SUCCESS;
  }
  int64_t get_file_count() {return file_count_;}
private:
  int64_t file_count_;
};

class ObFileListArrayOp : public ObBaseDirEntryOperator
{
public: 
  ObFileListArrayOp(common::ObIArray <common::ObString>& name_array, common::ObIAllocator& array_allocator)
    : name_array_(name_array), allocator_(array_allocator) {}
  ~ObFileListArrayOp() {}
  int func(const dirent *entry) ;

private:
  common::ObIArray <common::ObString>& name_array_;
  common::ObIAllocator& allocator_;
};

class ObDirPrefixEntryNameFilter : public ObBaseDirEntryOperator
{
public:
  ObDirPrefixEntryNameFilter(common::ObIArray<ObIODirentEntry> &d_entrys)
      : is_inited_(false),
        d_entrys_(d_entrys)
  {
    filter_str_[0] = '\0';
  }
  virtual ~ObDirPrefixEntryNameFilter() = default;
  int init(const char *filter_str, const int32_t filter_str_len);
  virtual int func(const dirent *entry) override;
private:
  bool is_inited_;
  char filter_str_[common::MAX_PATH_SIZE];
  common::ObIArray<ObIODirentEntry> &d_entrys_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirPrefixEntryNameFilter);
};

}
}

#endif
