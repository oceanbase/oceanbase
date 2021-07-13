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

#ifndef OCEANBASE_STORAGE_OB_PG_PARTITION
#define OCEANBASE_STORAGE_OB_PG_PARTITION

#include "lib/hash/ob_link_hashmap.h"
#include "share/ob_partition_modify.h"
#include "storage/memtable/ob_memtable_context.h"
#include "storage/ob_partition_schema_recorder.h"
#include "storage/ob_i_partition_storage.h"
#include "storage/ob_replay_status.h"
#include "storage/transaction/ob_trans_ctx.h"

namespace oceanbase {

namespace common {
class ObPartitionKey;
}

namespace storage {
class ObIPartitionComponentFactory;
class ObPartitionStorage;
class ObPGPartitionArrayGuard;
class ObPGMemtableMgr;
typedef common::LinkHashNode<common::ObPartitionKey> ObPGPartitionHashNode;
typedef common::LinkHashValue<common::ObPartitionKey> ObPGPartitionHashValue;
class ObPGPartition : public ObPGPartitionHashValue {
public:
  ObPGPartition();
  virtual ~ObPGPartition();
  void destroy();
  int init(const common::ObPartitionKey& pkey, ObIPartitionComponentFactory* cp_fty,
      share::schema::ObMultiVersionSchemaService* schema_service, transaction::ObTransService* txs,
      ObIPartitionGroup* pg, ObPGMemtableMgr& pg_memtable_mgr);
  int check_init(void* cp, const char* cp_name) const;
  bool is_inited() const;

  // partition management
  int serialize(ObArenaAllocator& allocator, char*& new_buf, int64_t& serialize_size);
  int deserialize(const ObReplicaType replica_type, const char* buf, const int64_t buf_len, ObIPartitionGroup* pg,
      int64_t& pos, bool& is_old_meta, ObPartitionStoreMeta& old_meta);
  virtual ObIPartitionStorage* get_storage();
  const common::ObPartitionKey& get_partition_key() const
  {
    return pkey_;
  }
  const ObPartitionSplitInfo& get_split_info();
  void set_merge_status(bool merge_success);
  bool can_schedule_merge();
  ObIPartitionGroup* get_pg()
  {
    return pg_;
  }
  int set_gc_starting();
  int get_gc_start_ts(int64_t& gc_start_ts);

  int replay_schema_log(const char* buf, const int64_t size, const int64_t log_id);

  // Transaction I/O operations
  int delete_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int delete_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObNewRow& row);

  int put_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int insert_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      ObNewRowIterator* row_iter, int64_t& affected_rows);

  int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
      const common::ObNewRow& row);

  int insert_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const common::ObIArray<uint64_t>& column_ids,
      const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
      int64_t& affected_rows, common::ObNewRowIterator*& duplicated_rows);

  int fetch_conflict_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param,
      const ObIArray<uint64_t>& in_column_ids, const ObIArray<uint64_t>& out_column_ids,
      ObNewRowIterator& check_row_iter, ObIArray<ObNewRowIterator*>& dup_row_iters);

  int update_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObIArray<uint64_t>& updated_column_ids, ObNewRowIterator* row_iter, int64_t& affected_rows);

  int update_row(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const ObIArray<uint64_t>& column_ids,
      const ObIArray<uint64_t>& updated_column_ids, const ObNewRow& old_row, const ObNewRow& new_row);

  int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      ObNewRowIterator* row_iter, const ObLockFlag lock_flag, int64_t& affected_rows);

  int lock_rows(const ObStoreCtx& ctx, const ObDMLBaseParam& dml_param, const int64_t abs_lock_timeout,
      const ObNewRow& row, const ObLockFlag lock_flag);

  int table_scan(ObTableScanParam& param, const int64_t data_max_schema_version, ObNewRowIterator*& result);

  int table_scan(ObTableScanParam& param, const int64_t data_max_schema_version, ObNewIterIterator*& result);

  int join_mv_scan(ObTableScanParam& left_param, ObTableScanParam& right_param,
      const int64_t left_data_max_schema_version, const int64_t right_data_max_schema_version,
      ObPGPartition& right_part, common::ObNewRowIterator*& result);

  int add_sstable_for_merge(storage::ObSSTable* table, const int64_t max_kept_major_version_number);

  int feedback_scan_access_stat(const ObTableScanParam& param);
  // During the indexing process, after the new version of the schema version is refreshed,
  //  a clog needs to be persisted
  int update_build_index_schema_info(
      const int64_t schema_version, const int64_t schema_refreshed_ts, const uint64_t log_id, const int64_t log_ts);
  int get_refreshed_schema_info(
      int64_t& schema_version, int64_t& refreshed_schema_ts, uint64_t& log_id, int64_t& log_ts);
  TO_STRING_KV(K_(pkey), KP_(cp_fty), KP_(storage));

private:
  bool is_gc_starting_() const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPGPartition);
  static const int64_t DELAY_SCHEDULE_TIME_UNIT = 1000 * 1000 * 1;       // 1s
  static const int64_t MAX_DELAY_SCHEDULE_TIME_UNIT = 1000 * 1000 * 60;  // 60s
private:
  bool is_inited_;
  common::ObPartitionKey pkey_;
  ObIPartitionComponentFactory* cp_fty_;
  ObPartitionStorage* storage_;
  ObPartitionSchemaRecorder schema_recorder_;
  ObIPartitionGroup* pg_;

  // merge statistic
  bool merge_successed_;
  int64_t merge_timestamp_;
  int64_t merge_failed_cnt_;
  int64_t gc_start_ts_;

  // build index schema version
  int64_t build_index_schema_version_;
  int64_t build_index_schema_version_refreshed_ts_;
  uint64_t schema_version_change_log_id_;
  int64_t schema_version_change_log_ts_;
};

// Memory Alloc
class PGPartitionInfoAlloc {
public:
  static ObPGPartition* alloc_value()
  {
    return op_alloc(ObPGPartition);
  }
  static void free_value(ObPGPartition* p)
  {
    if (NULL != p) {
      op_free(p);
      p = NULL;
    }
  }
  static ObPGPartitionHashNode* alloc_node(ObPGPartition* p)
  {
    UNUSED(p);
    return op_alloc(ObPGPartitionHashNode);
  }
  static void free_node(ObPGPartitionHashNode* node)
  {
    if (NULL != node) {
      op_free(node);
      node = NULL;
    }
  }
};

static const int64_t SHRINK_THRESHOLD = 128;
typedef common::ObLinkHashMap<common::ObPartitionKey, ObPGPartition, PGPartitionInfoAlloc> ObPGPartitionMap;

// thread safe
class ObPartitionKeyList {
private:
  class Node : public ObDLinkBase<Node> {
  public:
    Node(const common::ObPartitionKey& key) : key_(key)
    {}
    const common::ObPartitionKey key_;
  };
  typedef common::ObDList<Node> List;

public:
  ObPartitionKeyList() : lock_(), list_()
  {}

  inline int size()
  {
    return list_.get_size();
  }

  int add(const common::ObPartitionKey& pkey, const lib::ObLabel& label)
  {
    int ret = OB_SUCCESS;
    ObMemAttr attr(pkey.get_tenant_id(), label);
    void* ptr = ob_malloc(sizeof(Node), attr);
    if (OB_UNLIKELY(NULL == ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "alloc node memory fail fail", K(pkey));
    } else {
      Node* n = new (ptr) Node(pkey);
      TCWLockGuard guard(lock_);
      if (!list_.add_last(n)) {
        ob_free(n);
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "add node to list_ fail", K(pkey));
      }
    }
    return ret;
  }

  // remove latest inserted pkey
  // NOTICE : _assumption_ : no concurrent remove of same `pkey`
  bool remove_latest(const common::ObPartitionKey& pkey)
  {
    Node* n = NULL;

    {
      TCRLockGuard guard(lock_);
      DLIST_FOREACH_BACKWARD_X(curr, list_, NULL == n)
      {
        if (curr->key_ == pkey) {
          n = curr;
        }
      }
    }

    if (NULL != n) {
      {
        TCWLockGuard guard(lock_);
        n->unlink();
      }
      ob_free(n);
    }

    return NULL != n;
  }

  template <typename Fn>
  int for_each(Fn& fn)
  {
    int ret = OB_SUCCESS;
    TCRLockGuard guard(lock_);
    DLIST_FOREACH(cur, list_)
    {
      ret = fn(cur->key_);
    }
    return ret;
  }

  template <typename Fn>
  void remove_all(Fn& fn)
  {
    TCWLockGuard guard(lock_);
    DLIST_REMOVE_ALL_NORET(cur, list_)
    {
      cur->unlink();
      fn(cur->key_);
      ob_free(cur);
    }
  }

private:
  TCRWLock lock_;
  List list_;
};

class ObPGPartitionGuard {
public:
  ObPGPartitionGuard() : pg_partition_(NULL), map_(NULL)
  {}
  ObPGPartitionGuard(const common::ObPartitionKey& pkey, const ObPGPartitionMap& map) : pg_partition_(NULL), map_(NULL)
  {
    (void)set_pg_partition(pkey, map);
  }

  int set_pg_partition(const common::ObPartitionKey& pkey, const ObPGPartitionMap& map)
  {
    int ret = OB_SUCCESS;

    if (!pkey.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey));
    } else if (OB_FAIL(const_cast<ObPGPartitionMap&>(map).get(pkey, pg_partition_))) {
      STORAGE_LOG(WARN, "partition map get error", K(ret), K(pkey), K(lbt()));
    } else {
      pkey_ = pkey;
      map_ = &const_cast<ObPGPartitionMap&>(map);
    }

    return ret;
  }

  int set_pg_partition(const common::ObPartitionKey& pkey, ObPGPartition* pg_partition)
  {
    int ret = OB_SUCCESS;

    if (OB_ISNULL(pg_partition)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), KP(pg_partition));
    } else {
      pkey_ = pkey;
      pg_partition_ = const_cast<ObPGPartition*>(pg_partition);
    }

    return ret;
  }

  ~ObPGPartitionGuard()
  {
    if (NULL != pg_partition_ && NULL != map_) {
      (void)map_->revert(pg_partition_);
      pg_partition_ = NULL;
    }
  }

  ObPGPartition* get_pg_partition()
  {
    return pg_partition_;
  }
  TO_STRING_KV(KP_(pg_partition), K_(pkey), KP_(map));

private:
  ObPGPartition* pg_partition_;
  common::ObPartitionKey pkey_;
  ObPGPartitionMap* map_;
};

class ObPGPartitionArrayGuard {
public:
  ObPGPartitionArrayGuard(ObPGPartitionMap& map) : pg_partition_map_(map)
  {}
  ~ObPGPartitionArrayGuard();
  int push_back(ObPGPartition* pg_partition);
  int64_t count()
  {
    return partitions_.count();
  }
  ObPGPartition* at(int64_t i)
  {
    return partitions_.at(i);
  }
  ObPGPartitionMap& get_partition_map()
  {
    return pg_partition_map_;
  }

private:
  common::ObSEArray<ObPGPartition*, OB_DEFAULT_PARTITION_KEY_COUNT> partitions_;
  ObPGPartitionMap& pg_partition_map_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PG_PARTITION
