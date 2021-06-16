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

#ifndef OB_RAID_FILE_SYSTEM_H_
#define OB_RAID_FILE_SYSTEM_H_

#include <dirent.h>
#include "lib/ob_define.h"
#include "lib/io/ob_io_manager.h"
#include "ob_store_file_system.h"
#include "ob_local_file_system.h"
#include "lib/container/ob_se_array.h"
#include "ob_super_block_buffer_holder.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace blocksstable {
class ObRaidFileSystem;

struct ObRaidCommonHeader final {
  static const int64_t VERSION_1;
  static const int64_t HEADER_SIZE;

  int64_t magic_;
  int64_t version_;
  int64_t header_length_;
  int64_t header_checksum_;
  int64_t data_size_;
  int64_t data_checksum_;
  int64_t data_timestamp_;
  int64_t reserved_[505];  // reserved count must keep header size 4096

  ObRaidCommonHeader();
  bool is_valid() const;

  int64_t get_header_checksum() const;
  int set_checksum(const void* payload_buf, const int64_t len);
  int check_checksum(const void* payload_buf, const int64_t len) const;

  TO_STRING_KV(K_(magic), K_(version), K_(header_length), K_(header_checksum), K_(data_size), K_(data_checksum),
      K_(data_timestamp), "strip_size", header_length_ + data_size_);
};

struct ObScanDiskInfo final {
  int64_t disk_id_;
  bool has_sstable_file_;

  ObScanDiskInfo() : disk_id_(-1), has_sstable_file_(false)
  {}
  TO_STRING_KV(K_(disk_id), K_(has_sstable_file));
};

struct ObDiskFileInfo {
  static const int64_t OB_DISK_FILE_INFO_VERSION = 1;
  enum ObDiskStatus {
    INIT = 0,
    REBUILD = 1,
    NORMAL = 2,
    ERROR = 3,
    DROP = 4,
    MAX,
  };

  ObDiskID disk_id_;
  ObDiskStatus status_;
  int64_t create_ts_;
  int64_t rebuild_finish_ts_;
  ObString sstable_path_;
  ObString disk_name_;
  char sstable_path_buf_[common::MAX_PATH_SIZE];
  char disk_name_buf_[common::MAX_PATH_SIZE];

  ObDiskFileInfo();
  ObDiskFileInfo(const ObDiskFileInfo& other);

  bool is_valid() const;
  bool operator==(const ObDiskFileInfo& other) const;
  ObDiskFileInfo& operator=(const ObDiskFileInfo& r);
  int set_disk_path(const common::ObString& sstable_path, const common::ObString& disk_name);
  const char* get_status_str() const;
  TO_STRING_KV(
      KP(this), K_(disk_id), K_(status), K_(create_ts), K_(rebuild_finish_ts), K_(disk_name), K_(sstable_path));
  OB_UNIS_VERSION(OB_DISK_FILE_INFO_VERSION);
};

struct ObDiskFileStatus {
  ObDiskFileInfo info_;
  int32_t fd_;

  ObDiskFileStatus() : info_(), fd_(OB_INVALID_FD)
  {}
  bool is_valid() const;
  int get_disk_fd(ObDiskFd& fd) const;
  TO_STRING_KV(K_(fd), K_(info));
};

// must keep this struct operator =() won't fail
struct ObRaidFileInfo final {
  static const int64_t OB_RAID_FILE_INFO_VERSION = 1;
  static const int64_t OB_RAID_FILE_FORMAT_VERSION_1 = 1;

  int64_t format_version_;
  int64_t rebuild_seq_;
  int64_t rebuild_ts_;
  int64_t bootstrap_ts_;
  int64_t blocksstable_size_;
  int64_t strip_size_;
  int64_t src_data_num_;
  int64_t parity_num_;
  int64_t disk_count_;  // TODO(): new add
  int64_t total_macro_block_count_;
  int64_t macro_block_size_;

  ObRaidFileInfo();
  bool is_valid() const;
  void reset();
  OB_INLINE int64_t get_strip_payload_size() const
  {
    return strip_size_ - ObRaidCommonHeader::HEADER_SIZE;
  }
  bool equals(const ObRaidFileInfo& other) const;

  TO_STRING_KV(K_(format_version), K_(rebuild_seq), K_(rebuild_ts), K_(bootstrap_ts), K_(blocksstable_size),
      K_(strip_size), "strip_payload_size", get_strip_payload_size(), K_(src_data_num), K_(parity_num),
      K_(macro_block_size));
  OB_UNIS_VERSION(OB_RAID_FILE_INFO_VERSION);
};

// must keep this struct operator =() won't fail
struct ObDiskFileSuperBlock {
  static const int64_t OB_DISK_FILE_SUPER_BLOCK_VERSION = 1;
  int64_t disk_idx_;
  ObRaidFileInfo info_;
  common::ObSEArray<ObDiskFileInfo, common::OB_MAX_DISK_NUMBER> disk_info_;

  ObDiskFileSuperBlock();
  bool is_valid() const;
  bool equals(const ObDiskFileSuperBlock& other) const;
  void reset();
  TO_STRING_KV(K_(disk_idx), K_(info), K_(disk_info));
  OB_UNIS_VERSION(OB_DISK_FILE_SUPER_BLOCK_VERSION);
};

// must keep this struct operator =() won't fail
struct ObRaidFileStatus {
  bool is_inited_;
  ObRaidFileInfo info_;
  common::ObSEArray<ObDiskFileStatus, common::OB_MAX_DISK_NUMBER> disk_status_;

  ObRaidFileStatus();

  int64_t get_write_num() const
  {
    return info_.src_data_num_ + info_.parity_num_;
  }
  int64_t get_disk_count() const
  {
    return info_.disk_count_;
  }
  int get_read_fd(const int64_t idx, ObDiskFd& fd) const;
  int get_fd(const int64_t idx, ObDiskFd& fd) const;
  int get_fd(const ObString& disk_name, ObDiskFd& fd) const;
  int get_disk_status(const ObString& disk_name, ObDiskFileStatus*& disk_status);
  int get_disk_status(const ObString& disk_name, const ObDiskFileStatus*& disk_status) const;
  int get_disk_super_block(const int64_t idx, ObDiskFileSuperBlock& super_block) const;
  int load_raid(const ObDiskFileSuperBlock& super_block);

  int init_raid(const ObStorageEnv& storage_env, const common::ObIArray<ObScanDiskInfo>& disk_status,
      const int64_t min_total_space, const int64_t min_free_space);
  int cal_raid_param(const ObStorageEnv::REDUNDANCY_LEVEL& redundancy_level, const int64_t disk_count,
      const common::ObDiskType& disk_type, const int64_t macro_block_size, int64_t& src_data_num, int64_t& parity_num,
      int64_t& strip_size);

  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(is_inited), K_(info), K_(disk_status));
};

class ObRaidStripLocation {
public:
  ObRaidStripLocation();

  int init(
      const int64_t block_index, const int64_t block_offset, const ObRaidFileInfo& info, const bool include_header);

  int next_strip(int64_t& strip_idx, int64_t& disk_id, int64_t& offset, int64_t& strip_left_size);
  TO_STRING_KV(K_(block_index), K_(block_offset), K_(strip_num), K_(strip_size), K_(strip_payload_size), K_(disk_count),
      K_(blocksstable_size), K_(include_header), K_(is_inited), K_(strip_skewing_step), K_(cur_idx), K_(has_next),
      K_(strip_infos));

private:
  struct StripInfo final {
    StripInfo();
    int64_t disk_idx_;
    int64_t offset_;
    int64_t strip_idx_;
    TO_STRING_KV(K_(disk_idx), K_(offset), K_(strip_idx));
  };

  bool is_inited_;
  int64_t block_index_;
  int64_t block_offset_;
  int64_t strip_num_;
  int64_t strip_size_;
  int64_t strip_payload_size_;
  int64_t disk_count_;
  int64_t blocksstable_size_;

  bool include_header_;
  int64_t strip_skewing_step_;
  int64_t cur_idx_;
  bool has_next_;
  ObSEArray<StripInfo, OB_MAX_DISK_NUMBER> strip_infos_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidStripLocation);
};

struct ObRaidDiskUtil {
  static int is_raid(const char* sstable_dir, bool& is_raid);
  static int scan_store_dir(const char* sstable_dir, common::ObIArray<ObScanDiskInfo>& disk_status, bool& has_free_disk,
      bool& is_formated, int64_t& min_total_space, int64_t& min_free_space);
  static int build_sstable_dir(const char* sstable_dir, const int64_t dir_name, char* buf, int64_t buf_len);
  static int build_sstable_file_path(const char* sstable_dir, const int64_t dir_name, char* buf, int64_t buf_len);
  static int disk_name_filter(const struct ::dirent* d);
  static int is_local_file_system_exist(const char* sstable_dir, bool& is_exist);
};

class ObRaidIOErrorHandler final : public ObIIOErrorHandler {
public:
  ObRaidIOErrorHandler();
  virtual ~ObRaidIOErrorHandler()
  {}
  int init(ObRaidFileSystem* raid_file_system, const ObStoreFileReadInfo& read_info);
  virtual int64_t get_deep_copy_size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIIOErrorHandler*& handler) const;
  virtual int init_recover_io_master(
      const ObBitSet<OB_MAX_DISK_NUMBER>& recover_disk_idx_set, ObIAllocator* allocator, ObIOMaster* io_master);
  virtual int set_read_io_buf(char* io_buf, const int64_t io_buf_size, const int64_t aligned_offset);
  virtual int get_recover_request_num(int64_t& recover_request_num) const;
  TO_STRING_KV(K_(is_inited), K_(macro_block_ctx), K_(offset), K_(size), K_(io_desc), KP_(out_io_buf),
      K_(out_io_buf_size), K_(aligned_offset));

private:
  bool is_inited_;
  ObMacroBlockCtx macro_block_ctx_;
  int64_t offset_;  // offset of read_info
  int64_t size_;    // size of read_info
  common::ObIODesc io_desc_;
  char* out_io_buf_;
  int64_t out_io_buf_size_;  // aligned size
  int64_t aligned_offset_;   // offset in macro block
  ObRaidFileSystem* raid_file_system_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidIOErrorHandler);
};

class ObRaidRecoverIOCallback : public ObIOCallback {
public:
  struct RecoverParam {
    common::ObSEArray<int64_t, OB_MAX_DISK_NUMBER> input_index_;
    common::ObSEArray<int64_t, OB_MAX_DISK_NUMBER> recover_index_;
    int64_t data_count_;
    int64_t parity_count_;
    int64_t strip_size_;
    int64_t block_index_;

    RecoverParam();
    int assign(const RecoverParam& param);
    bool is_valid() const;
    TO_STRING_KV(K_(data_count), K_(parity_count), K_(strip_size), K_(input_index), K_(recover_index), K_(block_index));
  };
  ObRaidRecoverIOCallback();
  virtual ~ObRaidRecoverIOCallback();
  int init(
      common::ObIAllocator* allocator, char* out_buf, const int64_t out_buf_size, const int64_t offset_of_macro_block);
  int set_recover_param(const RecoverParam& param);
  virtual int64_t size() const;
  virtual int alloc_io_buf(char*& io_buf, int64_t& io_buf_size, int64_t& aligned_offset);
  virtual int inner_process(const bool is_success);
  virtual int inner_deep_copy(char* buf, const int64_t buf_len, ObIOCallback*& callback) const;
  virtual const char* get_data();
  TO_STRING_KV(K_(is_inited), KP_(allocator), KP_(buf), KP_(io_buf_size), K_(io_buf_size), K_(recover_param),
      K_(out_offset), KP_(out_io_buf), K_(out_io_buf_size));

private:
  bool is_inited_;
  common::ObIAllocator* allocator_;  // TODO(): replace it
  char* buf_;                        // whole buf
  char* io_buf_;                     // aligned buf for io
  int64_t io_buf_size_;
  RecoverParam recover_param_;

  int64_t out_offset_;  // offset in macro block
  char* out_io_buf_;
  int64_t out_io_buf_size_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidRecoverIOCallback);
};

class ObRaidFileWriteMgr final : public common::ObIOWriteFinishCallback {
public:
  ObRaidFileWriteMgr();
  ~ObRaidFileWriteMgr();
  int init();
  void destroy();
  int record_start(const int64_t block_index);  // if old request to same block not finish, will wait
  int notice_finish(const int64_t block_index);

private:
  bool is_inited_;
  ObThreadCond cond_;
  hash::ObHashSet<int64_t, hash::NoPthreadDefendMode> block_set_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidFileWriteMgr);
};

class ObRaidRebuildTask final : public common::ObTimerTask {
public:
  ObRaidRebuildTask(ObRaidFileSystem& file_system);
  virtual ~ObRaidRebuildTask();
  void runTimerTask();

private:
  ObRaidFileSystem& file_system_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidRebuildTask);
};

class ObRaidFileSystem : public ObStoreFileSystem {
public:
  ObRaidFileSystem();
  virtual ~ObRaidFileSystem();

  virtual int init(const ObStorageEnv& storage_env, storage::ObPartitionService& partition_service);
  virtual void destroy();
  virtual int async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle);
  virtual int async_read(const ObStoreFileReadInfo& read_info, common::ObIOHandle& io_handle);
  virtual int write_server_super_block(const ObServerSuperBlock& super_block) override;
  virtual int read_server_super_block(ObServerSuperBlock& super_block) override;
  virtual int fsync();
  virtual int64_t get_total_data_size() const;
  virtual int init_file_ctx(const ObStoreFileType& file_type, blocksstable::ObStoreFileCtx& file_ctx) const;
  virtual bool is_disk_full() const override;
  int init_recover_io_master(const ObMacroBlockCtx& macro_block_ctx, const int64_t aligned_offset,
      const int64_t out_io_buf_size, const ObIODesc& io_desc, const ObBitSet<OB_MAX_DISK_NUMBER>& recover_disk_idx_set,
      ObRaidRecoverIOCallback& callback, ObIOMaster* io_master);
  virtual int add_disk(const ObString& diskgroup_name, const ObString& disk_path, const ObString& alias_name);
  virtual int drop_disk(const ObString& diskgroup_name, const ObString& alias_name);
  int do_rebuild_task();
  virtual int get_disk_status(ObDiskStats& disk_stats) override;
  int64_t get_raid_src_data_num() const;

  virtual int start();
  virtual void stop();
  virtual void wait();

  // used in report bad block.
  virtual int get_macro_block_info(const storage::ObTenantFileKey& file_key, const MacroBlockId& macro_block_id,
      ObMacroBlockInfo& macro_block_info) override;
  virtual int report_bad_block(const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg,
      const char* file_path) override;
  virtual int get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos) override;
  virtual int get_marker_status(ObMacroBlockMarkerStatus& status) override;
  virtual int64_t get_free_macro_block_count() const override;
  virtual ObStorageFileHandle& get_server_root_handle() override;
  virtual int alloc_file(ObStorageFile*& file) override;
  virtual int free_file(ObStorageFile*& file) override;
  virtual int get_super_block_version(int64_t& version) override;
  // used in storage file ref cnt.
  virtual int unlink_block(const ObStorageFile& file, const MacroBlockId& macro_id) override;
  virtual int link_block(const ObStorageFile& file, const MacroBlockId& macro_id) override;

  TO_STRING_KV("type", "RAID");

private:
  int open(bool& is_formated);
  int init_raid(
      const int64_t min_total_space, const int64_t min_free_disk_space, common::ObIArray<ObScanDiskInfo>& disk_status);
  int write_raid_super_block(const ObRaidFileStatus& raid_status);
  int write_disk_super_block(const bool is_master_block, const ObRaidFileStatus& raid_status, const int64_t idx);
  int load_raid(const common::ObIArray<ObScanDiskInfo>& disk_status);
  int load_raid_super_block(const common::ObIArray<ObScanDiskInfo>& disk_status, const bool is_master_super_block,
      ObDiskFileSuperBlock& disk_super_block);
  int build_raid_status(const ObDiskFileSuperBlock& disk_super_block, ObRaidFileStatus& raid_status);
  int load_disk_super_block(
      const int64_t disk_id, const bool is_master_super_block, ObDiskFileSuperBlock& disk_super_block);
  int do_async_write(const ObStoreFileWriteInfo& write_info, common::ObIOHandle& io_handle);
  int write_super_block(const bool is_master, const ObServerSuperBlock& super_block);
  int read_super_block(const bool is_master, ObServerSuperBlock& super_block);

  int find_need_disk_idx(int64_t& new_disk_idx);
  int open_new_disk(const int64_t blocksstable_size, ObDiskFileStatus& disk_status);
  int close_disk(ObDiskFileStatus& disk_status);

  // rebuild
  int get_need_build_disk(ObDiskID& disk_id);
  int rebuild_disk(const ObDiskID& disk_id);
  int rebuild_macro_block(const ObDiskID& disk_id, const int64_t block_index);
  int cal_rebuild_read_io_info(const ObDiskID& rebuild_disk_id, const int64_t block_index, bool& need_rebuild,
      ObIOInfo& info, common::ObIArray<int64_t>& input_index, common::ObIArray<int64_t>& recover_index,
      int64_t& recover_offset);
  int write_strip_data(const ObDiskID& disk_id, const int64_t offset, const char* strip_buf, const int64_t strip_size);
  int finish_rebuild_disk(const ObDiskID& disk_id, const int64_t rebuild_macro_count);

private:
  bool is_opened_;
  mutable lib::ObMutex mutex_;       // TODO(): add latch id later
  mutable common::SpinRWLock lock_;  // TODO(): add latch id later
  const ObStorageEnv* storage_env_;
  ObRaidFileStatus raid_status_;
  ObSuperBlockBufferHolder super_block_buf_holder_;  // used for reserved memory for write super block
  ObRaidFileWriteMgr write_mgr_;
  ObTimer timer_;
  ObRaidRebuildTask rebuild_task_;
  ObStorageFileAllocator<ObLocalStorageFile> file_allocator_;
  ObStorageFile* server_root_;
  DISALLOW_COPY_AND_ASSIGN(ObRaidFileSystem);
};

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_RAID_FILE_SYSTEM_H_ */
