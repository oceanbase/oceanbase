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

#include "common/row/ob_row.h" //for ObNewRow
#include "storage/access/ob_table_access_context.h"
#include "storage/ob_i_table.h"
#include "storage/memtable/mvcc/ob_mvcc_ctx.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace common
{
class ObVersion;
}
namespace storage
{
class ObIPartitionGroup;
}
namespace transaction
{
class ObTransID;
}
namespace memtable
{

class ObRedoLogSubmitHelper;
class ObMemtableCtxCbAllocator;

// Interfaces of Memtable
////////////////////////////////////////////////////////////////////////////////////////////////////

class ObIMemtable;
class ObIMultiSourceDataUnit;
class ObIMemtableCtx : public ObIMvccCtx
{
public:
  ObIMemtableCtx(ObMemtableCtxCbAllocator &cb_allocator) : ObIMvccCtx(cb_allocator) {}
  virtual ~ObIMemtableCtx() {}
public:
  virtual void set_read_only() = 0;
  virtual void inc_ref() = 0;
  virtual void dec_ref() = 0;
  virtual int trans_begin() = 0;
  virtual int trans_end(const bool commit, const share::SCN trans_version, const share::SCN final_scn) = 0;
  virtual int trans_clear() = 0;
  virtual int elr_trans_preparing() = 0;
  virtual int trans_kill() = 0;
  virtual int trans_publish() = 0;
  virtual int trans_replay_begin() = 0;
  virtual int trans_replay_end(const bool commit,
                               const share::SCN trans_version,
                               const share::SCN final_scn,
                               const uint64_t log_cluster_version = 0,
                               const uint64_t checksum = 0) = 0;
  virtual void print_callbacks() = 0;
  //method called when leader takeover
  virtual int replay_to_commit(const bool is_resume) = 0;
  //method called when leader revoke
  virtual int commit_to_replay() = 0;
  virtual void set_trans_ctx(transaction::ObPartTransCtx *ctx) = 0;
  virtual void inc_truncate_cnt() = 0;
  virtual uint64_t get_tenant_id() const = 0;
  virtual storage::ObTxTableGuard *get_tx_table_guard() = 0;
  virtual int get_conflict_trans_ids(common::ObIArray<transaction::ObTransIDAndAddr> &array) = 0;
  VIRTUAL_TO_STRING_KV("", "");
public:
  // return OB_AGAIN/OB_SUCCESS
  virtual int fill_redo_log(char *buf,
                            const int64_t buf_len,
                            int64_t &buf_pos,
                            ObRedoLogSubmitHelper &helper,
                            const bool log_for_lock_node = true) = 0;
  common::ActiveResource resource_link_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct ObMergePriorityInfo
{
  ObMergePriorityInfo()
    : tenant_id_(0),
      last_freeze_timestamp_(0),
      handle_id_(0),
      emergency_(false),
      protection_clock_(0),
      partition_(nullptr) {}
  void set_low_priority()
  {
    tenant_id_ = INT64_MAX;
  }
  bool is_low_priority()
  {
    return INT64_MAX == tenant_id_;
  }
  bool compare(const ObMergePriorityInfo &other) const
  {
    int64_t cmp = 0;
    if (emergency_ != other.emergency_) {
      if (emergency_) {
        cmp = -1;
      }
    } else if (tenant_id_ != other.tenant_id_
               && std::min(tenant_id_, other.tenant_id_) < 1000) {
      // sys tenant has higher priority
      cmp = tenant_id_ - other.tenant_id_;
    } else if (last_freeze_timestamp_ != other.last_freeze_timestamp_) {
      cmp = last_freeze_timestamp_ - other.last_freeze_timestamp_;
    } else if (handle_id_ != other.handle_id_){
      cmp = handle_id_ - other.handle_id_;
    } else {
      cmp = protection_clock_ - other.protection_clock_;
    }
    return cmp < 0;
  }
  TO_STRING_KV(K_(tenant_id), K_(last_freeze_timestamp), K_(handle_id), K_(emergency),
    K_(protection_clock), KP_(partition));
  int64_t tenant_id_;
  int64_t last_freeze_timestamp_;
  int64_t handle_id_;
  bool emergency_;
  int64_t protection_clock_;
  storage::ObIPartitionGroup *partition_;
};

class ObIMemtable: public storage::ObITable
{
public:
  ObIMemtable() : ls_id_(), snapshot_version_(share::SCN::max_scn())
  {}
  virtual ~ObIMemtable() {}
  virtual share::ObLSID &get_ls_id() { return ls_id_;}
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const blocksstable::ObDatumRowkey &rowkey,
      blocksstable::ObDatumRow &row) = 0;

  virtual int64_t get_frozen_trans_version() { return 0; }
  virtual int major_freeze(const common::ObVersion &version)
  { UNUSED(version); return common::OB_SUCCESS; }
  virtual int minor_freeze(const common::ObVersion &version)
  { UNUSED(version); return common::OB_SUCCESS; }

  virtual void inc_pending_lob_count() {}
  virtual void dec_pending_lob_count() {}
  virtual int on_memtable_flushed() { return common::OB_SUCCESS; }
  virtual bool can_be_minor_merged() { return false; }
  void set_snapshot_version(const share::SCN snapshot_version) { snapshot_version_  = snapshot_version; }
  virtual int64_t get_snapshot_version() const override { return snapshot_version_.get_val_for_tx(); }
  virtual share::SCN get_snapshot_version_scn() const { return snapshot_version_; }
  virtual int64_t get_upper_trans_version() const override
  { return OB_NOT_SUPPORTED; }
  virtual int64_t get_max_merged_trans_version() const override
  { return OB_NOT_SUPPORTED; }

  virtual int64_t get_serialize_size() const override
  { return OB_NOT_SUPPORTED; }

  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override
  { return OB_NOT_SUPPORTED; }

  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos) override
  { return OB_NOT_SUPPORTED; }

  virtual bool can_be_minor_merged() const { return false; }
  virtual int64_t inc_write_ref() { return OB_INVALID_COUNT; }
  virtual int64_t dec_write_ref() { return OB_INVALID_COUNT; }
  virtual int64_t get_write_ref() const { return OB_INVALID_COUNT; }
  virtual uint32_t get_freeze_flag() { return 0; }
  virtual bool get_is_tablet_freeze() { return false; }
  virtual int64_t get_max_schema_version() const { return 0; }
  virtual int64_t get_occupied_size() const { return 0; }
  virtual int estimate_phy_size(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, int64_t& total_bytes, int64_t& total_rows)
  {
    UNUSEDx(start_key, end_key);
    total_bytes = 0;
    total_rows = 0;
    return OB_SUCCESS;
  }
  virtual int get_split_ranges(const ObStoreRowkey* start_key, const ObStoreRowkey* end_key, const int64_t part_cnt, common::ObIArray<common::ObStoreRange> &range_array)
  {
    UNUSEDx(start_key, end_key);
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(part_cnt != 1)) {
      ret = OB_NOT_SUPPORTED;
    } else {
      ObStoreRange merge_range;
      merge_range.set_start_key(ObStoreRowkey::MIN_STORE_ROWKEY);
      merge_range.set_end_key(ObStoreRowkey::MAX_STORE_ROWKEY);
      if (OB_FAIL(range_array.push_back(merge_range))) {
        TRANS_LOG(ERROR, "push back to range array failed", K(ret));
      }
    }
    return ret;
  }
  virtual bool is_empty() const override
  {
    return false;
  }
  virtual bool get_is_force_freeze()
  {
    return false;
  }
protected:
  share::ObLSID ls_id_;
  share::SCN snapshot_version_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class ObMemtableFactory
{
public:
  ObMemtableFactory();
  ~ObMemtableFactory();
public:
  static ObMemtable *alloc(const uint64_t tenant_id);
  static void free(ObMemtable *mt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObMemtableFactory);
private:
  static int64_t alloc_count_;
  static int64_t free_count_;
};

}
}

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_INTERFACE_

