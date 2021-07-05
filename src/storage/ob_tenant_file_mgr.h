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

#ifndef OB_SERVER_TENANT_FILE_MGR_H_
#define OB_SERVER_TENANT_FILE_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "blocksstable/slog/ob_base_storage_logger.h"
#include "blocksstable/ob_store_file_system.h"
#include "storage/ob_tenant_file_struct.h"

namespace oceanbase {
namespace storage {

class ObTenantFileMgr final {
public:
  ObTenantFileMgr();
  ~ObTenantFileMgr();
  int init(const uint64_t tenant_id, common::ObConcurrentFIFOAllocator& allocator);
  void destroy();
  int alloc_file(const bool is_sys_table, blocksstable::ObStorageFileHandle& file_handle, const bool write_slog = true);
  int open_file(const ObTenantFileKey& file_key, blocksstable::ObStorageFileHandle& file_handle);
  int replay_alloc_file(const ObTenantFileInfo& file_info);
  int replay_open_files(const common::ObAddr& server);
  int update_tenant_file_meta_blocks(
      const ObTenantFileKey& file_key, const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int update_tenant_file_meta_info(const ObTenantFileCheckpointEntry& file_meta, const bool write_checkpoint_slog);
  int update_file_status(const ObTenantFileKey& file_key, ObTenantFileStatus status);
  int get_tenant_file(const ObTenantFileKey& file_key, blocksstable::ObStorageFileHandle& file_handle);
  int get_all_tenant_file(blocksstable::ObStorageFilesHandle& files_handle);
  int get_tenant_file_info(const ObTenantFileKey& tenant_file_key, ObTenantFileInfo& tenant_file_info);
  int get_tenant_file_meta_blocks(
      const ObTenantFileKey& tenant_file_key, common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int get_macro_meta_replay_map(const ObTenantFileKey& tenant_file_key, ObMacroMetaReplayMap*& replay_map);
  int get_all_tenant_file_infos(
      common::ObIAllocator& allocator, common::ObIArray<ObTenantFileInfo*>& tenant_file_infos);
  int replay_update_tenant_file_super_block(const ObTenantFileKey& file_key, const ObTenantFileSuperBlock& super_block);
  int replay_remove_tenant_file_super_block(const ObTenantFileKey& file_key, const bool delete_file);
  int replay_update_tenant_file_info(const ObTenantFileInfo& file_info);
  int recycle_file();
  OB_INLINE uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  int write_add_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int add_pg(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int write_remove_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int remove_pg(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int clear_replay_map();
  int is_from_svr_ckpt(const ObTenantFileKey& file_key, bool& from_svr_skpt) const;

public:
  static const int64_t START_FILE_ID = 1L;

private:
  static const int64_t TENANT_MAX_FILE_CNT = 1000;
  static const int64_t TENANT_MIN_FILE_CNT = 10;
  typedef common::hash::ObHashMap<ObTenantFileKey, ObTenantFileValue*> TENANT_FILE_MAP;
  int create_new_tenant_file(const bool write_slog, const bool create_sys_table, const int64_t file_cnt);
  int choose_tenant_file(const bool is_sys_table, const bool need_create_file, TENANT_FILE_MAP& tenant_file_map,
      ObTenantFileValue*& value);
  int alloc_exist_file(const ObTenantFileInfo& tenant_file_info, const bool write_slog, const bool open_file,
      const bool is_owner, const bool from_svr_ckpt);
  int write_update_slog(const ObTenantFileKey& tenant_key, const bool in_slog_trans,
      const ObTenantFileSuperBlock& tenant_file_super_block);
  int write_remove_slog(const ObTenantFileKey& tenant_key, const bool remove_file);
  int write_update_file_info_slog(const ObTenantFileKey& file_key, const ObTenantFileSuperBlock& super_block);
  int update_tenant_file_super_block_in_map(
      const ObTenantFileKey& tenant_key, const ObTenantFileSuperBlock& tenant_file_super_block);
  int generate_unique_file_id(int64_t& file_id);
  int update_tenant_file_meta_blocks_impl(
      const ObTenantFileKey& file_key, const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);

private:
  common::TCRWLock lock_;
  TENANT_FILE_MAP tenant_file_map_;
  common::ObConcurrentFIFOAllocator* allocator_;
  uint64_t tenant_id_;
  bool is_inited_;
};

class ObBaseFileMgr : public blocksstable::ObIRedoModule {
public:
  ObBaseFileMgr();
  virtual ~ObBaseFileMgr();
  int init();
  void destroy();
  int alloc_file(const uint64_t tenant_id, const bool is_sys_table, blocksstable::ObStorageFileHandle& handle,
      const bool write_slog = true);
  int open_file(const ObTenantFileKey& tenant_file_key, blocksstable::ObStorageFileHandle& file_handle);
  int replay_alloc_file(const ObTenantFileInfo& file_info);
  int replay_open_files(const common::ObAddr& server);
  int update_tenant_file_meta_blocks(
      const ObTenantFileKey& file_key, const common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int update_tenant_file_meta_info(const ObTenantFileCheckpointEntry& file_entry, const bool skip_write_file_slog);
  int update_tenant_file_status(const ObTenantFileKey& file_key, ObTenantFileStatus status);
  int get_tenant_file(const ObTenantFileKey& file_key, blocksstable::ObStorageFileHandle& file_handle);
  int get_all_tenant_file(blocksstable::ObStorageFilesHandle& files_handle);
  int get_tenant_file_info(const ObTenantFileKey& tenant_file_key, ObTenantFileInfo& tenant_file_info);
  int get_all_tenant_file_infos(
      common::ObIAllocator& allocator, common::ObIArray<ObTenantFileInfo*>& tenant_file_infos);
  int get_tenant_file_meta_blocks(
      const ObTenantFileKey& file_key, common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int get_macro_meta_replay_map(const ObTenantFileKey& tenant_file_key, ObMacroMetaReplayMap*& replay_map);
  virtual int replay(const blocksstable::ObRedoModuleReplayParam& param) override;
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;
  int replay_over();
  int recycle_file();
  int write_add_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int add_pg(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int write_remove_pg_slog(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int remove_pg(const ObTenantFileKey& file_key, const common::ObPGKey& pg_key);
  int is_from_svr_ckpt(const ObTenantFileKey& file_key, bool& from_svr_skpt);

private:
  typedef common::hash::ObHashMap<int64_t, ObTenantFileMgr*> TENANT_MGR_MAP;
  int get_tenant_mgr(const uint64_t tenant_id, ObTenantFileMgr*& tenant_mgr);
  int replay_update_tenant_file_super_block(const char* buf, const int64_t buf_len);
  int replay_remove_tenant_file_super_block(const char* buf, const int64_t buf_len);
  int replay_add_pg_to_file(const char* buf, const int64_t len);
  int replay_remove_pg_from_file(const char* buf, const int64_t len);
  int replay_update_tenant_file_info(const char* buf, const int64_t buf_len);

private:
  static const int64_t MAX_TENANT_CNT = 1000;
  static const int64_t MEM_LIMIT = 10 * 1024 * 1024 * 1024L;
  lib::ObMutex lock_;
  TENANT_MGR_MAP tenant_mgr_map_;
  common::ObConcurrentFIFOAllocator allocator_;

protected:
  bool is_inited_;
};

class ObFileRecycleTask : public common::ObTimerTask {
public:
  static const int64_t RECYCLE_CYCLE_US = 1000L * 1000L * 10L;  // 10s
public:
  explicit ObFileRecycleTask(ObBaseFileMgr& file_mgr) : file_mgr_(file_mgr)
  {}
  virtual ~ObFileRecycleTask()
  {}
  virtual void runTimerTask() override;

private:
  ObBaseFileMgr& file_mgr_;
};

class ObServerFileMgr : public ObBaseFileMgr {
public:
  static ObServerFileMgr& get_instance();
  int init();
  void destroy();
  int start();
  void stop();
  void wait();

private:
  ObServerFileMgr() : recycle_timer_(), recycle_task_(*this)
  {}
  virtual ~ObServerFileMgr()
  {
    destroy();
  }

private:
  common::ObTimer recycle_timer_;
  ObFileRecycleTask recycle_task_;
};

class ObTenantFileSLogFilter : public blocksstable::ObISLogFilter {
public:
  ObTenantFileSLogFilter() = default;
  virtual ~ObTenantFileSLogFilter() = default;
  virtual int filter(const ObISLogFilter::Param& param, bool& is_filtered) const override;
};

}  // end namespace storage
}  // end namespace oceanbase

#define OB_SERVER_FILE_MGR (oceanbase::storage::ObServerFileMgr::get_instance())

#endif  // OB_SERVER_TENANT_FILE_MGR_H_
