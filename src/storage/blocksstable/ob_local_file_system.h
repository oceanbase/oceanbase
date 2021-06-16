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

#ifndef OB_LOCAL_FILE_SYSTEM_H_
#define OB_LOCAL_FILE_SYSTEM_H_

#include "ob_store_file_system.h"
#include "ob_super_block_buffer_holder.h"

namespace oceanbase {
namespace blocksstable {
class ObStoreFile;
class ObLocalFileSystem;

class ObLocalStorageFile : public ObStorageFile {
public:
  virtual ~ObLocalStorageFile() = default;

  virtual int init(const common::ObAddr& svr_addr, const FileType file_type) override;
  virtual int init(const common::ObAddr& svr_addr, const uint64_t tenant_id, const int64_t file_id,
      const FileType file_type) override;
  virtual void reset() override;

  virtual int open(const int flags) override;
  virtual int close() override;
  virtual int unlink() override;
  virtual int fsync() override;
  virtual int read_super_block(ObISuperBlock& super_block) override;
  virtual int write_super_block(const ObISuperBlock& super_block) override;

  virtual int async_read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle) override;
  virtual int async_write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle) override;
  virtual int write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle) override;
  virtual int read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle) override;

  virtual const char* get_path() const override
  {
    return "\0";
  }

  VIRTUAL_TO_STRING_KV(K(file_type_), K(tenant_id_), K(file_id_));

private:
  ObStoreFileSystem* file_system_;

  friend class ObLocalFileSystem;
  friend class ObRaidFileSystem;
  friend class ObStorageFileAllocator<ObLocalStorageFile>;
  ObLocalStorageFile();
  DISALLOW_COPY_AND_ASSIGN(ObLocalStorageFile);
};

class ObLocalFileSystem : public ObStoreFileSystem {
public:
  // The SUPER BLOCK OFFSET MUST NOT be modified
  static const int64_t MASTER_SUPER_BLOCK_OFFSET = 0;
  static const int64_t BACKUP_SUPER_BLOCK_OFFSET = common::OB_DEFAULT_MACRO_BLOCK_SIZE;

  ObLocalFileSystem();
  virtual ~ObLocalFileSystem();
  virtual int init(const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service) override;
  virtual void destroy() override;
  virtual int async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle) override;
  virtual int async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle) override;
  virtual int fsync() override;
  OB_INLINE virtual int64_t get_total_data_size() const override
  {
    return data_file_size_;
  }
  virtual int write_server_super_block(const ObServerSuperBlock& super_block) override;
  virtual int read_server_super_block(ObServerSuperBlock& super_block) override;
  OB_INLINE ObStoreFile& get_store_file()
  {
    return store_file_;
  }

  virtual int init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const override;
  virtual bool is_disk_full() const override;

  virtual int start() override;
  virtual void stop() override;
  virtual void wait() override;

  // storage 3.0 interface
  virtual int alloc_file(ObStorageFile*& file) override;
  virtual int free_file(ObStorageFile*& file) override;
  virtual ObStorageFileHandle& get_server_root_handle() override;

  // used in when block ref cnt in PG decreased to 0
  virtual int unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id) override;
  // used in replay
  virtual int link_block(const ObStorageFile& file, const MacroBlockId& macro_id) override;

  // used in report bad block.
  virtual int get_macro_block_info(const storage::ObTenantFileKey& file_key, const MacroBlockId& macro_block_id,
      ObMacroBlockInfo& macro_block_info) override;
  virtual int report_bad_block(const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg,
      const char* file_path) override;
  virtual int get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos) override;

  virtual int64_t get_free_macro_block_count() const override;
  virtual int get_marker_status(ObMacroBlockMarkerStatus& status) override;
  virtual int read_old_super_block(ObSuperBlockV2& super_block) override;
  virtual int get_super_block_version(int64_t& super_block_version) override;
  virtual int resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage) override;

  TO_STRING_KV("type", "LOCAL");

private:
  int open(bool& exist);
  // read/write super block to/from buffer holder
  template <typename SuperBlockClass>
  int inner_read_super_block(const int64_t offset, SuperBlockClass& super_block);
  int inner_write_super_block();
  int inner_get_super_block_version(const int64_t offset, int64_t& super_block_version);

private:
  bool is_opened_;
  common::ObDiskFd fd_;
  int64_t macro_block_size_;
  int64_t data_file_size_;
  int64_t datafile_disk_percentage_;
  ObSuperBlockBufferHolder super_block_buf_holder_;
  char store_path_[common::OB_MAX_FILE_NAME_LENGTH];
  ObStorageFileAllocator<ObLocalStorageFile> file_allocator_;
  ObStoreFile& store_file_;
  ObStorageFile* server_root_;
  DISALLOW_COPY_AND_ASSIGN(ObLocalFileSystem);
  const char* sstable_dir_;
  ObSuperBlockV2 old_super_block_;
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_LOCAL_FILE_SYSTEM_H_ */
