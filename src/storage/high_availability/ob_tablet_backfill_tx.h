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

#ifndef OCEABASE_STORAGE_TABLET_BACKFILL_TX
#define OCEABASE_STORAGE_TABLET_BACKFILL_TX

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_storage_ha_dag.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/multi_data_source/ob_tablet_mds_merge_ctx.h"

namespace oceanbase
{
namespace storage
{
class ObBackfillTabletsTableMgr;
class ObBackfillMdsTableHandle;

struct ObBackfillTXCtx
{
public:
  ObBackfillTXCtx();
  virtual ~ObBackfillTXCtx();
  int get_tablet_info(ObTabletBackfillInfo &tablet_info);
  bool is_valid() const;
  void reset();
  int build_backfill_tx_ctx(
      const share::ObTaskId &task_id,
      const share::ObLSID &ls_id,
      const share::SCN backfill_scn,
      const common::ObIArray<ObTabletBackfillInfo> &tablet_info_array);
  bool is_empty() const;
  int check_is_same(
      const ObBackfillTXCtx &backfill_tx_ctx,
      bool &is_same) const;
  int get_tablet_info_array(common::ObIArray<ObTabletBackfillInfo> &tablet_info_array) const;
  int64_t hash() const;

  VIRTUAL_TO_STRING_KV(
      K_(task_id),
      K_(ls_id),
      K_(backfill_scn),
      K_(tablet_info_index),
      K_(tablet_info_array));
public:
  share::ObTaskId task_id_;
  share::ObLSID ls_id_;
  share::SCN backfill_scn_;
private:
  bool inner_is_valid_() const;
private:
  common::SpinRWLock lock_;
  int64_t tablet_info_index_;
  common::ObArray<ObTabletBackfillInfo> tablet_info_array_;
  DISALLOW_COPY_AND_ASSIGN(ObBackfillTXCtx);
};

struct ObTabletBackfillMergeCtx : public compaction::ObTabletMergeCtx
{
public:
  ObTabletBackfillMergeCtx(
      compaction::ObTabletMergeDagParam &param,
      common::ObArenaAllocator &allocator);
  virtual ~ObTabletBackfillMergeCtx();
  int init(
      const SCN &backfill_scn,
      const int64_t ls_rebuild_seq,
      ObTabletHandle &tablet_handle,
      storage::ObTableHandleV2 &backfill_table_handle);
protected:
  virtual int get_ls_and_tablet() override;
  virtual int get_merge_tables(ObGetMergeTablesResult &get_merge_table_result) override;
  virtual int prepare_schema() override;
private:
  bool is_inited_;
  storage::ObTableHandleV2 backfill_table_handle_;
  share::SCN backfill_scn_;
  int64_t ls_rebuild_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackfillMergeCtx);
};

class ObTabletBackfillTXDag : public ObStorageHADag
{
public:
  ObTabletBackfillTXDag();
  virtual ~ObTabletBackfillTXDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  ObBackfillTXCtx *get_backfill_tx_ctx() { return backfill_tx_ctx_; }
  ObBackfillTabletsTableMgr *get_backfill_tablets_table_mgr() { return tablets_table_mgr_; }
  virtual int inner_reset_status_for_retry() override;

  int init(
      const share::ObTaskId &dag_net_id,
      const share::ObLSID &ls_id,
      const storage::ObTabletBackfillInfo &tablet_info,
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObBackfillTXCtx *backfill_tx_ctx,
      ObBackfillTabletsTableMgr *tablets_table_mgr);
  virtual int generate_next_dag(share::ObIDag *&dag);
  int get_tablet_handle(ObTabletHandle &tablet_handle);
  int init_tablet_handle();

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  bool is_inited_;
  share::ObTaskId dag_net_id_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObTabletHandle tablet_handle_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackfillTXDag);
};

class ObTabletBackfillTXTask : public share::ObITask
{
public:
  ObTabletBackfillTXTask();
  virtual ~ObTabletBackfillTXTask();
  int init(
      const share::ObTaskId &dag_net_id,
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_), K_(tablet_info));
private:
  int generate_backfill_tx_task_();
  int generate_table_backfill_tx_task_(
      share::ObITask *replace_task,
      common::ObIArray<ObTableHandleV2> &table_array,
      share::ObITask *child);
  int get_backfill_tx_memtables_(
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &table_array);
  int get_all_backfill_tx_tables_(
      const ObTablesHandleArray &sstable_handles,
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &table_array);
  int get_all_sstable_handles_(
      const ObTablet *tablet,
      ObTablesHandleArray &sstable_handles);
  int check_major_sstable_(
      const ObTablet *tablet,
      const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper);
  int init_tablet_table_mgr_();
  int split_sstable_array_by_backfill_(
      const ObTablesHandleArray &sstable_handles,
      common::ObIArray<ObTableHandleV2> &non_backfill_sstable,
      common::ObIArray<ObTableHandleV2> &backfill_sstable,
      share::SCN &max_end_major_scn);
  int add_ready_sstable_into_table_mgr_(
      const share::SCN &max_major_end_scn,
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &non_backfill_sstable);
  int add_sstable_into_handles_(
      ObTableStoreIterator &sstable_iter,
      ObTablesHandleArray &sstable_handles);
  int generate_mds_table_backfill_task_(
      share::ObITask *finish_task,
      share::ObITask *&child);
  int wait_memtable_frozen_();
  int init_tablet_handle_();

  int get_diagnose_support_info_(share::ObLSID &dest_ls_id, share::SCN &backfill_scn) const;
  void process_transfer_perf_diagnose_(
      const int64_t timestamp,
      const int64_t start_ts,
      const bool is_report,
      const ObStorageHACostItemName name,
      const int result) const;
private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackfillTXTask);
};

class ObTabletTableBackfillTXTask : public share::ObITask
{
public:
  ObTabletTableBackfillTXTask();
  virtual ~ObTabletTableBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObTabletHandle &tablet_handle,
      ObTableHandleV2 &table_handle,
      share::ObITask *child);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));
private:
  int check_need_merge_(bool &need_merge);
  int generate_merge_task_();

  int get_diagnose_support_info_(share::ObLSID &dest_ls_id, share::SCN &log_sync_scn) const;
  void process_transfer_perf_diagnose_(
      const int64_t timestamp,
      const int64_t start_ts,
      const bool is_report,
      const ObStorageHACostItemType type,
      const ObStorageHACostItemName name,
      const int result) const;
private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  share::ObITask *child_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableBackfillTXTask);
};

class ObTabletTableFinishBackfillTXTask : public share::ObITask
{
public:
  ObTabletTableFinishBackfillTXTask();
  virtual ~ObTabletTableFinishBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObTabletHandle &tablet_handle,
      ObTableHandleV2 &table_handle,
      share::ObITask *child);
  virtual int process() override;
  compaction::ObTabletBackfillMergeCtx &get_tablet_merge_ctx() { return tablet_merge_ctx_; }

  VIRTUAL_TO_STRING_KV(K("ObTabletTableFinishBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));
private:
  int prepare_merge_ctx_();
  int update_merge_sstable_();

private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
  compaction::ObTabletMergeDagParam param_;
  common::ObArenaAllocator allocator_;
  ObTabletBackfillMergeCtx tablet_merge_ctx_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  share::ObITask *child_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableFinishBackfillTXTask);
};

class ObFinishBackfillTXDag : public ObStorageHADag
{
public:
  ObFinishBackfillTXDag();
  virtual ~ObFinishBackfillTXDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int create_first_task() override;

  int init(
      const share::ObTaskId &task_id,
      const share::ObLSID &ls_id,
      const share::SCN &log_sync_scn,
      common::ObArray<ObTabletBackfillInfo> &tablet_info_array,
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObBackfillTabletsTableMgr *tablets_table_mgr);
  ObBackfillTXCtx *get_backfill_tx_ctx() { return &backfill_tx_ctx_; }
  ObBackfillTabletsTableMgr *get_tablets_table_mgr() { return tablets_table_mgr_; }
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));

protected:
  bool is_inited_;
  ObBackfillTXCtx backfill_tx_ctx_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishBackfillTXDag);
};

class ObFinishBackfillTXTask : public share::ObITask
{
public:
  ObFinishBackfillTXTask();
  virtual ~ObFinishBackfillTXTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));

private:
  bool is_inited_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishBackfillTXTask);
};

class ObTabletMdsTableBackfillTXTask : public share::ObITask
{
public:
  ObTabletMdsTableBackfillTXTask();
  virtual ~ObTabletMdsTableBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletBackfillInfo &tablet_info,
      ObTabletHandle &tablet_handle);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletMdsTableBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));
private:
  int do_backfill_mds_table_(ObTableHandleV2 &mds_sstable);
  int prepare_mds_table_merge_ctx_(
      compaction::ObTabletMergeCtx &tablet_merge_ctx);
  int build_mds_table_to_sstable_(
      compaction::ObTabletMergeCtx &tablet_merge_ctx,
      ObTableHandleV2 &table_handle);
  int prepare_backfill_mds_sstables_(
      const ObTableHandleV2 &mds_sstable,
      common::ObIArray<ObTableHandleV2> &mds_sstable_array);
  int do_backfill_mds_sstables_(
      const common::ObIArray<ObTableHandleV2> &mds_sstable_array,
      ObTableHandleV2 &table_handle);
  int prepare_mds_sstable_merge_ctx_(
      const common::ObIArray<ObTableHandleV2> &mds_sstable_array,
      ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx);
  int build_mds_sstable_(
      compaction::ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx,
      ObTableHandleV2 &table_handle);
  int update_merge_sstable_(
      compaction::ObTabletCrossLSMdsMinorMergeCtx &tablet_merge_ctx);

private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObTabletHandle tablet_handle_;
  common::ObArenaAllocator allocator_;
  compaction::ObLocalArena merger_arena_;
  ObBackfillTabletsTableMgr *tablets_table_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletMdsTableBackfillTXTask);
};

}
}
#endif
