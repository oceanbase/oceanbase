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

#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_storage_ha_dag.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"

namespace oceanbase
{
namespace storage
{

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
      const share::SCN log_sync_scn,
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
      K_(log_sync_scn),
      K_(tablet_info_index),
      K_(tablet_info_array));
public:
  share::ObTaskId task_id_;
  share::ObLSID ls_id_;
  share::SCN log_sync_scn_;
private:
  bool inner_is_valid_() const;
private:
  common::SpinRWLock lock_;
  int64_t tablet_info_index_;
  common::ObArray<ObTabletBackfillInfo> tablet_info_array_;
  DISALLOW_COPY_AND_ASSIGN(ObBackfillTXCtx);
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
  int init(
      const share::ObTaskId &dag_net_id,
      const share::ObLSID &ls_id,
      const storage::ObTabletBackfillInfo &tablet_info,
      ObIHADagNetCtx *ha_dag_net_ctx,
      ObBackfillTXCtx *backfill_tx_ctx);
  virtual int generate_next_dag(share::ObIDag *&dag);
  int get_tablet_handle(ObTabletHandle &tablet_handle);

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  bool is_inited_;
  share::ObTaskId dag_net_id_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObTabletHandle tablet_handle_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackfillTXDag);
};

class ObFinishTabletBackfillTXTask;
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
      ObFinishTabletBackfillTXTask *finish_tablet_backfill_tx_task,
      common::ObIArray<ObTableHandleV2> &table_array);
  int get_backfill_tx_memtables_(
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &table_array);
  int get_backfill_tx_minor_sstables_(
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &minor_sstables);
  int get_all_backfill_tx_tables_(
      ObTablet *tablet,
      common::ObIArray<ObTableHandleV2> &table_array);
private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  ObTabletBackfillInfo tablet_info_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletBackfillTXTask);
};

class ObTabletTableBackfillTXTask : public share::ObITask
{
public:
  ObTabletTableBackfillTXTask();
  virtual ~ObTabletTableBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      ObTableHandleV2 &table_handle);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));
private:
  int prepare_merge_ctx_();
  int check_need_merge_(bool &need_merge);
  int do_backfill_tx_();
  int prepare_partition_merge_();
  int update_merge_sstable_();

private:
  bool is_inited_;
  ObBackfillTXCtx *backfill_tx_ctx_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
  ObTableHandleV2 table_handle_;
  compaction::ObTabletMergeDagParam param_;
  common::ObArenaAllocator allocator_;
  compaction::ObTabletMergeCtx tablet_merge_ctx_;
  compaction::ObPartitionMerger *merger_;
  int64_t transfer_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletTableBackfillTXTask);
};

class ObFinishTabletBackfillTXTask : public share::ObITask
{
public:
  ObFinishTabletBackfillTXTask();
  virtual ~ObFinishTabletBackfillTXTask();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletBackfillTXTask"), KP(this), KPC(ha_dag_net_ctx_));

private:
  bool is_inited_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishTabletBackfillTXTask);
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
      ObIHADagNetCtx *ha_dag_net_ctx);
  ObBackfillTXCtx *get_backfill_tx_ctx() { return &backfill_tx_ctx_; }
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));

protected:
  bool is_inited_;
  ObBackfillTXCtx backfill_tx_ctx_;
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




}
}
#endif
