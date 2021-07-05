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

#ifndef OB_PG_SSTABLE_MGR_H_
#define OB_PG_SSTABLE_MGR_H_

#include "lib/lock/ob_spin_lock.h"
#include "lib/hash/ob_pre_alloc_link_hashmap.h"
#include "blocksstable/slog/ob_base_storage_logger.h"
#include "blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_table.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
namespace storage {

enum ObRedoLogSubType {
  CHANGE_MACRO_BLOCK_META_V2 = 1,
};

class ObPGSSTableMgr {
public:
  ObPGSSTableMgr();
  virtual ~ObPGSSTableMgr();
  int init(const common::ObPGKey& pg_key);
  int create_sstable(storage::ObCreateSSTableParamWithTable& param, ObTableHandle& handle);
  int create_sstable(storage::ObCreateSSTableParamWithPartition& param, ObTableHandle& handle);
  int create_sstable(const ObITable::TableKey& table_key, blocksstable::ObSSTableBaseMeta& meta, ObTableHandle& handle);
  void free_sstable(ObSSTable* sstable);
  int acquire_sstable(const ObITable::TableKey& table_key, ObTableHandle& handle);
  int get_all_sstables(ObTablesHandle& handle);
  int add_sstables(const bool in_slog_trans, ObTablesHandle& tables_handle);
  int add_sstable(const bool in_slog_trans, ObTableHandle& table_handle);
  int recycle_unused_sstables(const int64_t max_recycle_cnt, int64_t& recycled_cnt);
  int serialize(common::ObIAllocator& allocator, char*& buf, int64_t& serialize_size, ObTablesHandle* tables_handle);
  int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  int replay_add_sstable(ObSSTable& replay_sstable);
  int replay_remove_sstable(const ObITable::TableKey& table_key);
  int replay_add_old_sstable(ObOldSSTable& replay_old_sstable);
  OB_INLINE void set_pg_key(ObPGKey pg_key)
  {
    pg_key_ = pg_key;
  }
  int check_all_sstable_unused(bool& all_unused);
  int enable_write_log();
  OB_INLINE int set_storage_file_handle(blocksstable::ObStorageFileHandle& file_handle)
  {
    return file_handle_.assign(file_handle);
  }
  int get_clean_out_log_ts(int64_t& clean_out_log_ts);

private:
  static const int64_t DEFAULT_HASH_BUCKET_COUNT = 100;
  static const int64_t DEFAULT_MAX_RECYCLE_COUNT = 1000;
  struct TableNode : public common::hash::ObPreAllocLinkHashNode<ObITable::TableKey, ObITable> {
  public:
    explicit TableNode(ObITable& item) : ObPreAllocLinkHashNode(item), next_(NULL)
    {}
    virtual ~TableNode()
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
    TableNode* next_;
  };
  typedef common::hash::ObPreAllocLinkHashMap<ObITable::TableKey, ObITable, TableNode, ObTableProtector> TableMap;
  class TableGetFunctor : public TableMap::GetFunctor {
  public:
    explicit TableGetFunctor(ObTableHandle& table_handle) : table_handle_(table_handle)
    {}
    virtual ~TableGetFunctor() = default;
    virtual int operator()(ObITable& table) override;

  private:
    ObTableHandle& table_handle_;
  };
  class UnusedTableFunctor : public TableMap::ForeachFunctor {
  public:
    UnusedTableFunctor() : is_inited_(false), is_full_(false), max_recycle_cnt_(0), tables_()
    {}
    virtual ~UnusedTableFunctor() = default;
    int init(const int64_t max_recycle_count);
    virtual int operator()(ObITable& table, bool& is_full) override;
    int get_tables(common::ObIArray<ObITable*>& tables);
    common::ObIArray<ObITable*>& get_all_tables()
    {
      return tables_;
    }
    bool is_full() const
    {
      return is_full_;
    }

  private:
    bool is_inited_;
    bool is_full_;
    int64_t max_recycle_cnt_;
    common::ObArray<ObITable*> tables_;
  };
  class CheckAllTableUnusedFunctor : public TableMap::ForeachFunctor {
  public:
    CheckAllTableUnusedFunctor() : is_all_unused_(true)
    {}
    virtual ~CheckAllTableUnusedFunctor() = default;
    virtual int operator()(ObITable& table, bool& is_full) override;
    bool is_all_unused()
    {
      return is_all_unused_;
    }

  private:
    bool is_all_unused_;
  };
  class GetCleanOutLogIdFunctor : public TableMap::ForeachFunctor {
  public:
    GetCleanOutLogIdFunctor() : clean_out_log_ts_(INT64_MAX)
    {}
    virtual ~GetCleanOutLogIdFunctor() = default;
    int64_t get_clean_out_log_ts() const
    {
      return clean_out_log_ts_;
    }
    virtual int operator()(ObITable& table, bool& is_full) override;

  private:
    int64_t clean_out_log_ts_;
  };
  void destroy();
  int write_add_sstable_log(ObSSTable& sstable);
  int write_remove_sstable_log(const ObITable::TableKey& table_key);
  int check_can_add_sstables(ObTablesHandle& tables_handle);
  int lock_multi_bucket(common::ObMultiBucketLockGuard& guard, common::ObIArray<ObITable*>& tables);
  int add_sstable_to_map(ObSSTable& sstable, const bool overwrite);
  int alloc_sstable(const int64_t tenant_id, ObSSTable*& sstable);
  int64_t get_serialize_size();

private:
  bool is_inited_;
  ObPGKey pg_key_;
  common::ObBucketLock bucket_lock_;
  TableMap table_map_;
  blocksstable::ObStorageFileHandle file_handle_;
  bool enable_write_log_;
  DISALLOW_COPY_AND_ASSIGN(ObPGSSTableMgr);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_PG_SSTABLE_MGR_H_
