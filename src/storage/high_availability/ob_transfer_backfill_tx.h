/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEABASE_STORAGE_TRANSFER_BACKFILL_TX_
#define OCEABASE_STORAGE_TRANSFER_BACKFILL_TX_

#include "ob_storage_ha_struct.h"
#include "ob_storage_ha_dag.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

struct ObTransferBackfillTXParam: public share::ObIDagInitParam
{
public:
  ObTransferBackfillTXParam();
  virtual ~ObTransferBackfillTXParam() {}
  virtual bool is_valid() const override;
  void reset();
  VIRTUAL_TO_STRING_KV(K_(task_id), K_(src_ls_id), K_(dest_ls_id), K_(backfill_scn), K_(tablet_infos));
  uint64_t tenant_id_;
#ifdef ERRSIM
  ObErrsimTransferBackfillPoint errsim_point_info_;
#endif
  share::ObTaskId task_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN backfill_scn_;
  common::ObArray<ObTabletBackfillInfo> tablet_infos_;
};

class ObTransferWorkerMgr final
{
public:
  ObTransferWorkerMgr();
  ~ObTransferWorkerMgr();
  int init(ObLS *dest_ls);
  int process();
  int cancel_dag_net();
  void reset_task_id();
  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(task_id), KP_(dest_ls));
private:
  int check_task_exist_(const share::ObTaskId &task_id, bool &is_exist);
  void update_task_id_();
  int do_transfer_backfill_tx_(const ObTransferBackfillTXParam &param);
  int get_need_backfill_tx_tablets_(ObTransferBackfillTXParam &param);
  // Only the minor data is exist and transfer start commit, then the tablet can do backfill.
  int check_source_tablet_ready_(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObTabletTransferInfo &transfer_info,
      bool &is_ready,
      ObTabletHAStatus &ha_status /* source tablet ha status */) const;
  void set_errsim_backfill_point_();
private:
  bool is_inited_;
#ifdef ERRSIM
  ObErrsimTransferBackfillPoint errsim_point_info_;
#endif
  uint64_t tenant_id_;
  share::ObTaskId task_id_;
  ObLS *dest_ls_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferWorkerMgr);
};

struct ObTransferBackfillTXCtx : public ObIHADagNetCtx
{
public:
  ObTransferBackfillTXCtx();
  virtual ~ObTransferBackfillTXCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual bool is_valid() const;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::TRANSFER_BACKFILL_TX; }
public:
  uint64_t tenant_id_;
#ifdef ERRSIM
  ObErrsimTransferBackfillPoint errsim_point_info_;
#endif
  share::ObTaskId task_id_;
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN backfill_scn_;
  common::ObArray<ObTabletBackfillInfo> tablet_infos_;
  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(tenant_id),
      K_(task_id),
      K_(src_ls_id),
      K_(backfill_scn));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransferBackfillTXCtx);
};

class ObTransferBackfillTXDagNet: public share::ObIDagNet
{
public:
  ObTransferBackfillTXDagNet();
  virtual ~ObTransferBackfillTXDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx();
  virtual int deal_with_cancel() override;
  virtual bool is_ha_dag_net() const override { return true; }

  ObTransferBackfillTXCtx *get_ctx() { return &ctx_; }
  const share::ObLSID &get_ls_id() const { return ctx_.src_ls_id_; }
  const share::SCN &get_backfill_scn() const { return ctx_.backfill_scn_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));
private:
  int start_running_for_backfill_();

private:
  bool is_inited_;
  ObTransferBackfillTXCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferBackfillTXDagNet);
};


class ObBaseTransferBackfillTXDag : public ObStorageHADag
{
public:
  explicit ObBaseTransferBackfillTXDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObBaseTransferBackfillTXDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  int prepare_ctx(share::ObIDagNet *dag_net);
#ifdef ERRSIM
  virtual common::ObErrsimModuleType::TYPE get_module_type() { return ObErrsimModuleType::ERRSIM_MODULE_TRANSFER; }
#endif

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
private:
  DISALLOW_COPY_AND_ASSIGN(ObBaseTransferBackfillTXDag);
};

class ObStartTransferBackfillTXDag : public ObBaseTransferBackfillTXDag
{
public:
  ObStartTransferBackfillTXDag();
  virtual ~ObStartTransferBackfillTXDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTransferBackfillTXDag);
};

class ObStartTransferBackfillTXTask : public share::ObITask
{
public:
  ObStartTransferBackfillTXTask();
  virtual ~ObStartTransferBackfillTXTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartTransferBackfillTXTask"), KP(this), KPC(ctx_));
private:
  int generate_transfer_backfill_tx_dags_();

private:
  bool is_inited_;
  ObTransferBackfillTXCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTransferBackfillTXTask);
};

class ObTransferReplaceTableDag : public ObBaseTransferBackfillTXDag
{
public:
  ObTransferReplaceTableDag();
  virtual ~ObTransferReplaceTableDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool check_can_retry();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferReplaceTableDag);
};

class ObTransferReplaceTableTask : public share::ObITask
{
public:
  ObTransferReplaceTableTask();
  virtual ~ObTransferReplaceTableTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTransferReplaceTableTask"), KP(this), KPC(ctx_));
private:
  int do_replace_logical_tables_(ObLS *ls);
  int transfer_replace_tables_(
      ObLS *ls,
      const ObTabletBackfillInfo &tablet_info,
      const ObTablet *tablet);
  int get_source_tablet_tables_(
      const ObTablet *dest_tablet,
      const ObTabletBackfillInfo &tablet_info,
      ObTableStoreIterator &sstable_iter,
      ObTabletHandle &tablet_handle,
      ObTabletRestoreStatus::STATUS &restore_status,
      common::ObArenaAllocator &allocator,
      ObTablesHandleArray &tables_handle);
  int get_all_sstable_handles_(
      const ObTablet *tablet,
      const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      ObTableStoreIterator &sstable_iter,
      ObTablesHandleArray &sstable_handles);
  int check_src_tablet_sstables_(const ObTablet *tablet, ObTablesHandleArray &tables_handle);
  int check_source_minor_end_scn_(
      const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      const ObTablet *dest_tablet,
      bool &need_fill_minor);
  int fill_empty_minor_sstable(
      ObTablet *tablet,
      bool need_fill_minor,
      const share::SCN &end_scn,
      const ObTabletMemberWrapper<ObTabletTableStore> &wrapper,
      common::ObArenaAllocator &allocator,
      ObTablesHandleArray &tables_handle);
  int check_src_memtable_is_empty_(ObTablet *tablet, const share::SCN &transfer_scn);
  int build_migration_param_(
      const ObTablet *tablet,
      ObTabletHandle &src_tablet_handle,
      ObMigrationTabletParam &param);
  int check_major_sstable_(
      const ObTablet *tablet,
      const ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper);
private:
  bool is_inited_;
  ObTransferBackfillTXCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferReplaceTableTask);
};

}
}
#endif
