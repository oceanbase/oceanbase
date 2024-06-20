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
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace common
{
class ObVersion;
}

namespace memtable
{
class ObTxFillRedoCtx;
class ObMemtableCtxCbAllocator;

class ObIMemtableCtx : public ObIMvccCtx
{
public:
  ObIMemtableCtx() : ObIMvccCtx() {}
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
  virtual int get_conflict_trans_ids(common::ObIArray<transaction::ObTransIDAndAddr> &array) = 0;
  VIRTUAL_TO_STRING_KV("", "");
public:
  // return OB_AGAIN/OB_SUCCESS
  virtual int fill_redo_log(ObTxFillRedoCtx &ctx) = 0;
  common::ActiveResource resource_link_;
};

}  // namespace memtable

namespace storage {

class ObIMemtable : public storage::ObITable {
public:
  ObIMemtable()
    : ls_id_(),
      snapshot_version_(share::SCN::max_scn()),
      trace_id_(checkpoint::INVALID_TRACE_ID)
  {}
  virtual ~ObIMemtable() {}
  void reset()
  {
    ObITable::reset();
    ls_id_.reset();
    snapshot_version_.set_max();
    reset_trace_id();
  }
  int get_ls_id(share::ObLSID &ls_id);
  share::ObLSID get_ls_id() const;
  virtual ObTabletID get_tablet_id() const = 0;
  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) = 0;
  virtual int64_t get_frozen_trans_version() { return 0; }
  virtual int major_freeze(const common::ObVersion &version)
  {
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  virtual int minor_freeze(const common::ObVersion &version)
  {
    UNUSED(version);
    return common::OB_SUCCESS;
  }
  virtual void inc_pending_lob_count() {}
  virtual void dec_pending_lob_count() {}
  virtual int on_memtable_flushed() { return common::OB_SUCCESS; }
  virtual bool can_be_minor_merged() { return false; }
  void set_snapshot_version(const share::SCN snapshot_version) { snapshot_version_ = snapshot_version; }
  virtual int64_t get_snapshot_version() const override { return snapshot_version_.get_val_for_tx(); }
  virtual share::SCN get_snapshot_version_scn() const { return snapshot_version_; }
  virtual int64_t get_upper_trans_version() const override { return OB_NOT_SUPPORTED; }
  virtual int64_t get_max_merged_trans_version() const override { return OB_NOT_SUPPORTED; }

  virtual int64_t get_serialize_size() const override { return OB_NOT_SUPPORTED; }

  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const override { return OB_NOT_SUPPORTED; }

  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos) override { return OB_NOT_SUPPORTED; }

  virtual int64_t inc_write_ref() { return OB_INVALID_COUNT; }
  virtual int64_t dec_write_ref() { return OB_INVALID_COUNT; }
  virtual int64_t get_write_ref() const { return OB_INVALID_COUNT; }
  virtual uint32_t get_freeze_flag() { return 0; }
  virtual int64_t get_occupied_size() const { return 0; }
  virtual int estimate_phy_size(const ObStoreRowkey *start_key,
                                const ObStoreRowkey *end_key,
                                int64_t &total_bytes,
                                int64_t &total_rows)
  {
    UNUSEDx(start_key, end_key);
    total_bytes = 0;
    total_rows = 0;
    return OB_SUCCESS;
  }

  virtual int get_split_ranges(const ObStoreRange &input_range,
                               const int64_t part_cnt,
                               ObIArray<ObStoreRange> &range_array)
  {
    UNUSEDx(input_range);
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(part_cnt != 1)) {
      ret = OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "split a single range is not supported", KR(ret), K(input_range), K(part_cnt));
    } else if (OB_FAIL(range_array.push_back(input_range))) {
      STORAGE_LOG(WARN, "push back to range array failed", K(ret));
    }
    return ret;
  }

  virtual bool is_empty() const override { return false; }

  virtual int64_t dec_ref()
  {
    int64_t ref_cnt = ObITable::dec_ref();
    checkpoint::ObCheckpointDiagnoseMgr *cdm = MTL(checkpoint::ObCheckpointDiagnoseMgr*);
    if (0 == ref_cnt) {
      if (get_tablet_id().is_ls_inner_tablet()) {
        REPORT_CHECKPOINT_DIAGNOSE_INFO(update_start_gc_time_for_checkpoint_unit, this)
      }
    }
    return ref_cnt;
  }

  void set_trace_id(const int64_t trace_id)
  {
    if (get_tablet_id().is_ls_inner_tablet()) {
      ADD_CHECKPOINT_DIAGNOSE_INFO_AND_SET_TRACE_ID(checkpoint::ObCheckpointUnitDiagnoseInfo, trace_id);
    } else {
      ADD_CHECKPOINT_DIAGNOSE_INFO_AND_SET_TRACE_ID(checkpoint::ObMemtableDiagnoseInfo, trace_id);
    }
  }
  void reset_trace_id() { ATOMIC_STORE(&trace_id_, checkpoint::INVALID_TRACE_ID); }
  int64_t get_trace_id() const { return ATOMIC_LOAD(&trace_id_); }
protected:
  share::ObLSID ls_id_;
  share::SCN snapshot_version_;
  // a round tablet freeze identifier for checkpoint diagnose
  int64_t trace_id_;
};
}  // namespace storage

}  // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_OB_MEMTABLE_INTERFACE_

