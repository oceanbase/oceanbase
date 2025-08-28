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

#ifndef STORAGE_LOG_STREAM_BACKUP_DEVICE_WRAPPER_H_
#define STORAGE_LOG_STREAM_BACKUP_DEVICE_WRAPPER_H_

#include "lib/restore/ob_object_device.h"
#include "share/backup/ob_backup_path.h"
#include "share/backup/ob_backup_struct.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace backup
{

static const int64_t BACKUP_WRAPPER_DEVICE_OPT_NUM = 6;

class ObBackupWrapperIODevice final : public common::ObObjectDevice
{
public:
  ObBackupWrapperIODevice();
  ~ObBackupWrapperIODevice();
  static int setup_io_storage_info(const share::ObBackupDest &backup_dest,
      char *buf, const int64_t len, common::ObIODOpts *iod_opts);
  static int setup_io_opts_for_backup_device(
      const int64_t backup_set_id, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
      const int64_t turn_id, const int64_t retry_id, const int64_t file_id, const ObBackupDeviceMacroBlockId::BlockType &block_type,
      const ObStorageAccessType &access_type, ObIODOpts *io_d_opts);
  virtual void destroy() override;
  virtual int open(const char *pathname, const int flags, const mode_t mode, 
                   ObIOFd &fd, ObIODOpts *opts = NULL) override;
  virtual int alloc_block(const ObIODOpts *opts, ObIOFd &block_id) override;
  virtual int pread(const ObIOFd &fd, const int64_t offset, const int64_t size,
                    void *buf, int64_t &read_size, ObIODPreadChecker *checker = nullptr) override;
  virtual int pwrite(const ObIOFd &fd, const int64_t offset, const int64_t size,
                     const void *buf, int64_t &write_size) override;
  virtual int close(const ObIOFd &fd) override;
  virtual void dec_ref() override;

public:
  int alloc_mem_block(const int64_t size, char *&buf);

public:
  int64_t simulated_fd_id() const { return simulated_fd_id_; }
  int64_t simulated_slot_version() const { return simulated_slot_version_; }

private:
  int parse_storage_device_type_(const common::ObString &storage_type_prefix);
  int check_need_realloc_(bool &need_realloc);
  int pre_alloc_block_array_();
  int realloc_block_array_();
  int get_min_unused_block_(int64_t &idx);
  int convert_block_id_to_addr_(const int64_t idx, ObIOFd &block_id);
  int get_offset_and_length_(const int64_t idx, int64_t &offset, int64_t &length);
  int parse_io_device_opts_(common::ObIODOpts *opts);
  int get_opt_value_(ObIODOpts *opts, const char* key, int64_t& value);

private:
  static const int64_t DEFAULT_BLOCK_COUNT = 4096;
  static const int64_t DEFAULT_ALLOC_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const char *FIRST_ID_STR;
  static const char *SECOND_ID_STR;
  static const char *THIRD_ID_STR;

private:
  bool is_inited_;
  bool is_opened_;
  lib::ObMutex mutex_;
  int64_t used_block_cnt_;
  int64_t total_block_cnt_;
  int64_t block_size_;
  int64_t *block_list_;
  int64_t max_alloced_block_idx_;
  int64_t backup_set_id_;
  share::ObBackupDataType backup_data_type_;
  int64_t ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  int64_t file_id_;
  ObBackupDeviceMacroBlockId::BlockType block_type_;
  ObIOFd io_fd_;
  int64_t simulated_fd_id_; // used for fd simulator
  int64_t simulated_slot_version_; // used for fd simulator
  ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupWrapperIODevice);
};

class ObBackupDeviceHelper
{
public:
  static int alloc_backup_device(const uint64_t tenant_id, ObBackupWrapperIODevice *&device);
  static void release_backup_device(ObBackupWrapperIODevice *&device);
  static int get_device_and_fd(const uint64_t tenant_id,
                               const int64_t first_id, 
                               const int64_t second_id, 
                               const int64_t third_id,
                               ObStorageIdMod &mod,
                               ObBackupWrapperIODevice *&device, 
                               ObIOFd &fd);
  static int close_device_and_fd(ObBackupWrapperIODevice *&device_handle, ObIOFd &fd);

private:
  static int get_companion_index_file_path_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, share::ObBackupPath &backup_path);
  static int get_backup_data_file_path_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, share::ObBackupPath &backup_path);
  static int setup_io_device_opts_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, common::ObIODOpts *io_d_opts);
  static int get_backup_dest_(const uint64_t tenant_id, const int64_t backup_set_id, share::ObBackupDest &backup_dest);
  static int get_backup_type_(const uint64_t tenant_id, const int64_t backup_set_id, share::ObBackupType &backup_type);
  static int get_restore_dest_id_(const uint64_t tenant_id, ObStorageIdMod &mod);

};

}
}

#endif

