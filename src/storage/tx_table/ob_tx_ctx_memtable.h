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

#ifndef OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE
#define OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE

#include "storage/memtable/ob_memtable_interface.h"
#include "storage/tx_table/ob_tx_ctx_table.h"
#include "storage/tx_table/ob_tx_table_iterator.h"
#include "storage/checkpoint/ob_common_checkpoint.h"

namespace oceanbase
{
namespace storage
{
//
// We need to maintain the filter location and the playback site
// separately to modularly manage different table structures, remove
// dependencies between them, and minor merge independently.
//
// So we need:
//
// 1, Adaptation is required for a management insituation named CheckpointMgr[1] as a
// design prototype, the goal is a unified abstraction of the recovery module.
//
// 2, Adaptation is required for a management insituation like LSM mgr[2] as a
// design prototype, the goal is a unified abstraction of the merge module.
//
// [1]:
// [2]:
class ObTxCtxMemtable : public ObIMemtable, public checkpoint::ObCommonCheckpoint
{
public:
  ObTxCtxMemtable();
  ~ObTxCtxMemtable();

  int init(const ObITable::TableKey &table_key, const share::ObLSID &ls_id);

  void reset();
  int on_memtable_flushed() override;
  bool is_frozen_memtable() override;
  bool is_active_memtable() override;

  // ================ INHERITED FROM ObIMemtable ===============
  // We need to inherient the memtable method for merge process to iterate the
  // ls_ctx_mgr for dumping the tx ctx table.
  virtual int scan(const ObTableIterParam &param,
                   ObTableAccessContext &context,
                   const blocksstable::ObDatumRange &key_range,
                   ObStoreRowIterator *&row_iter) override;

  transaction::ObLSTxCtxMgr *get_ls_tx_ctx_mgr();
  virtual bool can_be_minor_merged() override;

  // TODO : @handora.qc implement it
  virtual int64_t get_occupied_size() const override { return 0; }

  // ================ INHERITED FROM ObCommonCheckpoint ===============
  virtual share::SCN get_rec_scn();
  virtual int flush(share::SCN recycle_scn, const int64_t trace_id, bool need_freeze = true);

  virtual ObTabletID get_tablet_id() const override;
  virtual bool is_flushing() const override;

  // ================ NOT SUPPORTED INTERFACE ===============

  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  blocksstable::ObDatumRow &row) override;

  virtual int get(const storage::ObTableIterParam &param,
                  storage::ObTableAccessContext &context,
                  const blocksstable::ObDatumRowkey &rowkey,
                  ObStoreRowIterator *&row_iter) override;

  virtual int multi_get(const ObTableIterParam &param,
                        ObTableAccessContext &context,
                        const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
                        ObStoreRowIterator *&row_iter) override;

  virtual int multi_scan(const ObTableIterParam &param,
                         ObTableAccessContext &context,
                         const common::ObIArray<blocksstable::ObDatumRange> &ranges,
                         ObStoreRowIterator *&row_iter) override;

  virtual int get_frozen_schema_version(int64_t &schema_version) const override;

  INHERIT_TO_STRING_KV("ObITable", ObITable, KP(this), K_(snapshot_version),
                       K_(ls_id), K_(is_frozen));

private:
  bool is_inited_;
  bool is_frozen_;
  ObTxCtxTable ls_ctx_mgr_guard_;
  common::ObSpinLock flush_lock_;
};

}  // namespace storage
}  // namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_TX_CTX_MEMTABLE
