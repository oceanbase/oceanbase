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

#ifndef OCEANBASE_MEMTABLE_OB_MEMTABLE_INTERFACE_
#define OCEANBASE_MEMTABLE_OB_MEMTABLE_INTERFACE_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_id_map.h"
#include "lib/allocator/ob_lf_fifo_allocator.h"
#include "lib/profile/ob_active_resource_list.h"

#include "common/row/ob_row.h"  //for ObNewRow

#include "storage/ob_i_store.h"  //for ObStoreRow and ObStoreRowIterator
#include "storage/ob_i_table.h"
#include "storage/memtable/mvcc/ob_mvcc.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace common {
class ObVersion;
class ObExtStoreRange;
}  // namespace common
namespace storage {
class ObIPartitionGroup;
}
namespace transaction {
class ObTransID;
}
namespace memtable {

// Interfaces of Memtable
////////////////////////////////////////////////////////////////////////////////////////////////////

class ObIMemtable;
class ObIMemtableCtx : public ObIMvccCtx {
public:
  ObIMemtableCtx() : ObIMvccCtx(), is_standalone_(false)
  {}
  virtual ~ObIMemtableCtx()
  {}

public:
  virtual void set_read_only() = 0;
  virtual void inc_ref() = 0;
  virtual void dec_ref() = 0;
  virtual int trans_begin() = 0;
  virtual int sub_trans_begin(const int64_t snaptshot, const int64_t abs_expired_time,
      const bool is_safe_snapshot = false, const int64_t trx_lock_timeout = -1) = 0;
  virtual int sub_trans_end(const bool commit) = 0;
  virtual int sub_stmt_begin() = 0;
  virtual int sub_stmt_end(const bool commit) = 0;
  virtual int fast_select_trans_begin(
      const int64_t snapshot, const int64_t abs_expired_time, const int64_t trx_lock_wait_expired_time) = 0;
  virtual int trans_end(const bool commit, const int64_t trans_version) = 0;
  virtual int trans_clear() = 0;
  virtual int elr_trans_preparing() = 0;
  virtual int trans_kill() = 0;
  virtual int trans_publish() = 0;
  virtual int trans_replay_begin() = 0;
  virtual int trans_replay_end(const bool commit, const int64_t trans_version, const uint64_t checksum = 0) = 0;
  virtual uint64_t calc_checksum(const int64_t trans_version) = 0;
  virtual uint64_t calc_checksum2(const int64_t trans_version) = 0;
  // method called when leader takeover
  virtual int replay_to_commit() = 0;
  // method called when leader revoke
  virtual int commit_to_replay() = 0;
  virtual int get_trans_status() const = 0;
  virtual void set_trans_ctx(transaction::ObTransCtx* ctx) = 0;
  virtual transaction::ObTransCtx* get_trans_ctx() = 0;
  virtual bool is_multi_version_range_valid() const = 0;
  virtual const ObVersionRange& get_multi_version_range() const = 0;
  virtual int stmt_data_relocate(const int relocate_data_type, ObMemtable* memtable, bool& relocated) = 0;
  virtual void inc_relocate_cnt() = 0;
  virtual void inc_truncate_cnt() = 0;
  virtual int64_t get_relocate_cnt() = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual int set_memtable_for_cur_log(ObIMemtable* memtable) = 0;
  virtual ObIMemtable* get_memtable_for_cur_log() = 0;
  virtual void clear_memtable_for_cur_log() = 0;
  virtual bool has_read_elr_data() const = 0;
  virtual transaction::ObTransStateTableGuard* get_trans_table_guard() = 0;
  VIRTUAL_TO_STRING_KV("", "");

public:
  // return OB_AGAIN/OB_SUCCESS
  virtual int fill_redo_log(char* buf, const int64_t buf_len, int64_t& buf_pos) = 0;
  virtual int undo_fill_redo_log() = 0;
  virtual int audit_partition(const enum transaction::ObPartitionAuditOperator op, const int64_t count) = 0;
  common::ActiveResource resource_link_;
  bool is_standalone_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObMergePriorityInfo {
  ObMergePriorityInfo()
      : tenant_id_(0),
        last_freeze_timestamp_(0),
        handle_id_(0),
        emergency_(false),
        protection_clock_(0),
        partition_(nullptr)
  {}
  void set_low_priority()
  {
    tenant_id_ = INT64_MAX;
  }
  bool is_low_priority()
  {
    return INT64_MAX == tenant_id_;
  }
  bool compare(const ObMergePriorityInfo& other) const
  {
    int64_t cmp = 0;
    if (emergency_ != other.emergency_) {
      if (emergency_) {
        cmp = -1;
      }
    } else if (tenant_id_ != other.tenant_id_ && std::min(tenant_id_, other.tenant_id_) < 1000) {
      // sys tenant has higher priority
      cmp = tenant_id_ - other.tenant_id_;
    } else if (last_freeze_timestamp_ != other.last_freeze_timestamp_) {
      cmp = last_freeze_timestamp_ - other.last_freeze_timestamp_;
    } else if (handle_id_ != other.handle_id_) {
      cmp = handle_id_ - other.handle_id_;
    } else {
      cmp = protection_clock_ - other.protection_clock_;
    }
    return cmp < 0;
  }
  TO_STRING_KV(
      K_(tenant_id), K_(last_freeze_timestamp), K_(handle_id), K_(emergency), K_(protection_clock), KP_(partition));
  int64_t tenant_id_;
  int64_t last_freeze_timestamp_;
  int64_t handle_id_;
  bool emergency_;
  int64_t protection_clock_;
  storage::ObIPartitionGroup* partition_;
};

class ObIMemtable : public storage::ObITable {
public:
  ObIMemtable()
  {}
  virtual ~ObIMemtable()
  {}
  virtual int fake(const ObIMemtable& mt) = 0;

  virtual int get(const storage::ObTableIterParam& param, storage::ObTableAccessContext& context,
      const common::ObExtStoreRowkey& rowkey, storage::ObStoreRow& row) = 0;

  //
  // estimates costs on query operation, for query plan optimize.
  //
  // @param [in] query_flag in cache, daily merge or something indicate query operations.
  // @param [in] table_id query table (which is or not index query).
  // @param [in] rowkeys, multi-get rowkeys
  // @param [in] column_ids, output columns
  // @param [out] cost_metrics
  //
  // @return OB_SUCCESS
  //
  virtual int estimate_get_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObIArray<common::ObExtStoreRowkey>& rowkeys, storage::ObPartitionEst& part_est) = 0;

  virtual int estimate_scan_row_count(const common::ObQueryFlag query_flag, const uint64_t table_id,
      const common::ObExtStoreRange& key_range, storage::ObPartitionEst& part_est) = 0;
  //
  // Insert/Delete/Update row(s)
  //
  // @param [in] ctx, transaction
  // @param [in] table_id
  // @param [in] rowkey_size, length of rowkey columns
  // @param [in] column_ids, input columns
  // @param [in] row_iter, input iterator
  //
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& columns, storage::ObStoreRowIterator& row_iter) = 0;
  //
  // Insert/Delete/Update row
  //
  // @param [in] ctx, transaction
  // @param [in] table_id
  // @param [in] rowkey_size, length of rowkey columns
  // @param [in] column_ids, input columns
  // @param [in] row, row to be set
  //
  virtual int set(const storage::ObStoreCtx& ctx, const uint64_t table_id, const int64_t rowkey_size,
      const common::ObIArray<share::schema::ObColDesc>& columns, const storage::ObStoreRow& row) = 0;
  //
  // Lock rows
  //
  // @param [in] ctx, transaction
  // @param [in] table_id
  // @param [in] row_iter, input iterator
  // @param [in] lock_flag, operation flag
  //
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, common::ObNewRowIterator& row_iter) = 0;
  //
  // Lock single row
  //
  // @param [in] ctx, transaction
  // @param [in] table_id
  // @param [in] row, row to be locked
  // @param [in] lock_flag, operation flag
  //
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObNewRow& row) = 0;
  //
  // Lock single row
  //
  // @param [in] ctx, transaction
  // @param [in] table_id
  // @param [in] rowkey, row to be locked
  //
  virtual int lock(const storage::ObStoreCtx& ctx, const uint64_t table_id,
      const common::ObIArray<share::schema::ObColDesc>& columns, const common::ObStoreRowkey& rowkey) = 0;
  virtual int64_t get_frozen_trans_version()
  {
    return 0;
  }
  virtual int major_freeze(const common::ObVersion& version)
  {
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  virtual int minor_freeze(const common::ObVersion& version)
  {
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  virtual void inc_pending_lob_count()
  {}
  virtual void dec_pending_lob_count()
  {}
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObIMemtableCtxFactory {
public:
  ObIMemtableCtxFactory()
  {}
  virtual ~ObIMemtableCtxFactory()
  {}

public:
  virtual ObIMemtableCtx* alloc(const uint64_t tenant_id = OB_SERVER_TENANT_ID) = 0;
  virtual void free(ObIMemtableCtx* ctx) = 0;
};

class ObMemtableCtxFactory : public ObIMemtableCtxFactory {
public:
  enum {
    CTX_ALLOC_FIX = 1,
    CTX_ALLOC_VAR = 2,
  };
  typedef common::ObFixedQueue<ObIMemtableCtx> FreeList;
  typedef common::ObIDMap<ObIMemtableCtx, uint32_t> IDMap;
  typedef common::ObLfFIFOAllocator DynamicAllocator;
  static const int64_t OBJ_ALLOCATOR_PAGE = 1L << 22;                                            // 4MB
  static const int64_t DYNAMIC_ALLOCATOR_PAGE = common::OB_MALLOC_NORMAL_BLOCK_SIZE * 8 - 1024;  // 64k
  static const int64_t DYNAMIC_ALLOCATOR_PAGE_NUM = common::OB_MAX_CPU_NUM;
  static const int64_t MAX_CTX_HOLD_COUNT = 10000;
  static const int64_t MAX_CTX_COUNT = 3000000;

public:
  ObMemtableCtxFactory();
  ~ObMemtableCtxFactory();

public:
  ObIMemtableCtx* alloc(const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  void free(ObIMemtableCtx* ctx);
  IDMap& get_id_map()
  {
    return id_map_;
  }
  DynamicAllocator& get_allocator()
  {
    return ctx_dynamic_allocator_;
  }
  common::ObIAllocator& get_malloc_allocator()
  {
    return malloc_allocator_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableCtxFactory);

private:
  bool is_inited_;
  common::ModulePageAllocator mod_;
  common::ModuleArena ctx_obj_allocator_;
  DynamicAllocator ctx_dynamic_allocator_;
  common::ObMalloc malloc_allocator_;
  FreeList free_list_;
  IDMap id_map_;
  int64_t alloc_count_;
  int64_t free_count_;
};

class ObMemtableFactory {
public:
  ObMemtableFactory();
  ~ObMemtableFactory();

public:
  static ObMemtable* alloc(const uint64_t tenant_id);
  static void free(ObMemtable* mt);

private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableFactory);

private:
  static int64_t alloc_count_;
  static int64_t free_count_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_OB_MEMTABLE_INTERFACE_
