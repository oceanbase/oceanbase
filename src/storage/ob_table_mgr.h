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

#ifndef SRC_STORAGE_OB_TABLE_MGR_H_
#define SRC_STORAGE_OB_TABLE_MGR_H_

#include "ob_i_table.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_pre_alloc_link_hashmap.h"
#include "blocksstable/slog/ob_base_storage_logger.h"
#include "blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace storage {
class ObCreateSSTableParamWithTable;
class ObCreateSSTableParamWithPartition;
class ObPartitionComponentFactory;
class ObOldSSTable;

class ObTableMgrGCTask : public common::ObTimerTask {
public:
  ObTableMgrGCTask();
  virtual ~ObTableMgrGCTask();
  virtual void runTimerTask() override;
};

class ObTableMgr : public blocksstable::ObIRedoModule {
public:
  static const int64_t DEFAULT_HASH_MAP_BUCKETS_COUNT = 100000;  // 10w
  static const int64_t GC_INTERVAL_US = 60 * 1000 * 1000;
  struct CompletedTableNode;
  struct AllTableNode;
  typedef common::hash::ObPreAllocLinkHashMap<ObITable::TableKey, ObITable, CompletedTableNode, ObTableProtector>
      CompletedTableMap;
  typedef common::hash::ObPreAllocLinkHashMap<uint64_t, ObITable, AllTableNode, ObTableProtector> AllTableMap;

  struct CompletedTableNode : public common::hash::ObPreAllocLinkHashNode<ObITable::TableKey, ObITable> {
  public:
    explicit CompletedTableNode(ObITable& item) : ObPreAllocLinkHashNode(item), next_(NULL)
    {}
    virtual ~CompletedTableNode()
    {
      next_ = NULL;
    }
    virtual OB_INLINE const ObITable::TableKey& get_key() const override
    {
      return item_.get_key();
    }
    static uint64_t hash(const ObITable::TableKey& key)
    {
      return key.hash();
    }
    INHERIT_TO_STRING_KV("ObPreAllocLinkHashNode", ObPreAllocLinkHashNode, KP_(next));
    CompletedTableNode* next_;
  };

  class CompletedTableGetFunctor : public CompletedTableMap::GetFunctor {
  public:
    explicit CompletedTableGetFunctor(ObTableHandle& handle) : handle_(handle)
    {}
    virtual ~CompletedTableGetFunctor()
    {}
    virtual int operator()(ObITable& table) override;

  private:
    ObTableHandle& handle_;
    DISALLOW_COPY_AND_ASSIGN(CompletedTableGetFunctor);
  };

  class UnneedCompletedTableFinder : public CompletedTableMap::ForeachFunctor {
  public:
    UnneedCompletedTableFinder();
    virtual ~UnneedCompletedTableFinder();
    int init(const int64_t max_batch_count);
    virtual int operator()(ObITable& table, bool& is_full) override;
    int get_tables(common::ObIArray<ObITable*>& del_sstables, common::ObIArray<ObITable*>& del_memtables);
    common::ObIArray<ObITable*>& get_all_tables()
    {
      return del_tables_;
    }
    bool is_full() const
    {
      return is_full_;
    }

  private:
    bool is_inited_;
    int64_t max_batch_count_;
    bool is_full_;
    common::ObArray<ObITable*> del_tables_;
    DISALLOW_COPY_AND_ASSIGN(UnneedCompletedTableFinder);
  };

  struct AllTableNode : public common::hash::ObPreAllocLinkHashNode<uint64_t, ObITable> {
    explicit AllTableNode(ObITable& item) : ObPreAllocLinkHashNode(item), next_(NULL)
    {
      key_ = reinterpret_cast<uint64_t>(&item_);
    }
    virtual ~AllTableNode()
    {
      key_ = 0;
      next_ = NULL;
    }
    virtual OB_INLINE const uint64_t& get_key() const override
    {
      return key_;
    }
    static uint64_t hash(const uint64_t& key)
    {
      return common::murmurhash(&key, sizeof(key), 0);
    }
    INHERIT_TO_STRING_KV("ObPreAllocLinkHashNode", ObPreAllocLinkHashNode, K_(key), KP_(next));
    uint64_t key_;
    AllTableNode* next_;
  };

  class UnneedAllTableFinder : public AllTableMap::ForeachFunctor {
  public:
    UnneedAllTableFinder()
    {}
    virtual ~UnneedAllTableFinder()
    {}
    virtual int operator()(ObITable& table, bool& is_full) override;
    common::ObIArray<ObITable*>& get_tables()
    {
      return del_tables_;
    }

  private:
    common::ObArray<ObITable*> del_tables_;
    DISALLOW_COPY_AND_ASSIGN(UnneedAllTableFinder);
  };

  class AllTableEraseChecker : public AllTableMap::EraseChecker {
  public:
    virtual int operator()(ObITable& table) override;
  };

  static ObTableMgr& get_instance();

  int init();
  int schedule_gc_task();
  virtual int enable_write_log() override;
  void destroy();
  void stop();

  int acquire_old_table(const ObITable::TableKey& table_key, ObTableHandle& handle);
  int create_memtable(ObITable::TableKey& table_key, ObTableHandle& handle);
  int release_table(ObITable* table);
  int clear_unneed_tables();
  int load_sstable(const char* buf, int64_t buf_len, int64_t& pos);
  int replay_add_old_sstable(ObSSTable* sstable);
  int replay_del_old_sstable(const ObITable::TableKey& table_key);
  virtual int replay(const blocksstable::ObRedoModuleReplayParam& param) override;
  virtual int parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream) override;
  int check_sstables();
  int check_tenant_sstable_exist(const uint64_t tenant_id, bool& is_exist);
  int free_all_sstables();
  int get_all_tables(ObTablesHandle& handle);

private:
  ObTableMgr();
  virtual ~ObTableMgr();
  int complete_memtable(memtable::ObMemtable* memtable, const int64_t snapshot_version, const int64_t freeze_log_ts);
  int complete_sstables(ObTablesHandle& handle, const bool use_inc_macro_block_slog = false);
  int complete_sstable(ObTableHandle& handle, const bool use_inc_macro_block_slog = false);
  int get_completed_sstables(ObTablesHandle& handle);
  int free_del_tables();
  template <class T>
  int alloc_table(T*& table, const int64_t tenant_id = common::OB_SERVER_TENANT_ID);
  void free_table(ObITable* table);
  //  int write_create_sstable_log(ObSSTable &sstable);
  int write_delete_sstable_log(const ObITable::TableKey& table_key);
  int write_complete_sstable_log(ObSSTable& sstable, const bool use_inc_macro_block_slog);
  int replay_create_sstable(const char* buf, const int64_t buf_len);
  int replay_complete_sstable(const char* buf, const int64_t buf_len);
  int replay_delete_sstable(const char* buf, const int64_t buf_len);
  int replay_delete_sstable(const ObITable::TableKey& table_key);
  int schedule_release_task();
  int check_can_complete_sstables(ObTablesHandle& handle);
  int remove_completed_sstables(common::ObIArray<ObITable*>& write_slog_tables);
  int remove_completed_memtables(common::ObIArray<ObITable*>& memtables);
  int clear_unneed_completed_tables();
  int clear_unneed_completed_tables(const int64_t max_del_count, bool& need_next);
  int clear_unneed_all_tables();
  int remove_unused_callback_for_uncommited_txn_(memtable::ObMemtable* mt);
  int add_replay_sstable_to_map(const bool is_replay, ObOldSSTable& tmp_sstable);
  int lock_multi_bucket_lock(common::ObMultiBucketLockGuard& guard, common::ObIArray<ObITable*>& tables);
  void free_all_tables();

private:
  bool is_inited_;
  CompletedTableMap completed_table_map_;
  AllTableMap all_table_map_;

  mutable common::ObBucketLock sstable_bucket_lock_;
  ObTableMgrGCTask gc_task_;
  ObTableMgrGCTask fast_gc_task_;
  bool is_gc_started_;
  DISALLOW_COPY_AND_ASSIGN(ObTableMgr);
};

}  // namespace storage
}  // namespace oceanbase

#endif /* SRC_STORAGE_OB_TABLE_MGR_H_ */
