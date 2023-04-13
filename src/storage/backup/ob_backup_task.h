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

#ifndef STORAGE_LOG_STREAM_BACKUP_TASK_H_
#define STORAGE_LOG_STREAM_BACKUP_TASK_H_

#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/function/ob_function.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/backup/ob_archive_struct.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/backup/ob_backup_ctx.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/backup/ob_backup_index_store.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_reader.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "share/backup/ob_backup_data_store.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase {
namespace share
{
class SCN;
}
namespace backup {

class ObLSBackupDagInitParam;

struct ObLSBackupDagNetInitParam : public share::ObIDagInitParam {
  ObLSBackupDagNetInitParam();
  virtual ~ObLSBackupDagNetInitParam();
  virtual bool is_valid() const override;
  int assign(const ObLSBackupDagNetInitParam &param);
  int convert_to(ObLSBackupParam &param);
  int convert_to(ObLSBackupDagInitParam &init_param);
  bool operator==(const ObLSBackupDagNetInitParam &other) const;
  VIRTUAL_TO_STRING_KV(K_(backup_dest), K_(tenant_id), K_(dest_id), K_(backup_set_desc), K_(ls_id), K_(turn_id),
    K_(retry_id), K_(dest_id), K_(backup_data_type));
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  ObBackupReportCtx report_ctx_;
  share::SCN start_scn_;               // for backup meta
  share::ObBackupDataType backup_data_type_;   // for build index
  share::SCN compl_start_scn_;         // for complemnt log
  share::SCN compl_end_scn_;           // for complemet log
};

struct ObLSBackupDagInitParam : public share::ObIDagInitParam {
  ObLSBackupDagInitParam();
  virtual ~ObLSBackupDagInitParam();
  virtual bool is_valid() const override;
  bool operator==(const ObLSBackupDagInitParam &other) const;
  VIRTUAL_TO_STRING_KV(K_(job_desc), K_(backup_dest), K_(tenant_id), K_(backup_set_desc), K_(ls_id), K_(turn_id),
      K_(retry_id), K_(backup_stage), K_(dest_id));
  int convert_to(const share::ObBackupDataType &backup_data_type, ObLSBackupDataParam &param);
  int assign(const ObLSBackupDagInitParam &param);
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  ObLSBackupStage backup_stage_;
  int64_t dest_id_;
};

enum ObBackupDagNetSubType : int64_t {
  LOG_STREAM_BACKUP_META_DAG_NET = 0,
  LOG_STREAM_BACKUP_DAG_DAG_NET = 1,
  LOG_STREAM_BACKUP_BUILD_INDEX_DAG_NET = 2,
  LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG_NET = 3,
};

enum ObBackupDagSubType : int64_t {
  LOG_STREAM_BACKUP_META_DAG = 0,
  LOG_STREAM_BACKUP_PREPARE_DAG = 1,
  LOG_STREAM_BACKUP_PREFETCH_DAG = 2,
  LOG_STREAM_BACKUP_DATA_DAG = 3,
  LOG_STREAM_BACKUP_BUILD_INDEX_DAG = 4,
  LOG_STREAM_BACKUP_COMPLEMENT_LOG_DAG = 5,
  LOG_STREAM_BACKUP_FINISH_DAG = 6,
};

class ObBackupDagNet : public share::ObIDagNet
{
public:
  explicit ObBackupDagNet(const ObBackupDagNetSubType &sub_type);
  virtual ~ObBackupDagNet();
  ObBackupDagNetSubType get_sub_type() const { return sub_type_; };
  INHERIT_TO_STRING_KV("ObIDagNet", ObIDagNet, K_(sub_type));
protected:
  ObBackupDagNetSubType sub_type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDagNet);
};

class ObLSBackupMetaDagNet : public ObBackupDagNet {
public:
  ObLSBackupMetaDagNet();
  virtual ~ObLSBackupMetaDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual bool is_valid() const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(param));

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  share::SCN start_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupMetaDagNet);
};

class ObLSBackupDataDagNet : public ObBackupDagNet {
public:
  ObLSBackupDataDagNet();
  virtual ~ObLSBackupDataDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual bool is_valid() const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  share::ObBackupDataType get_backup_data_type() const
  {
    return backup_data_type_;
  }
  void set_backup_data_type(const share::ObBackupDataType &type)
  {
    backup_data_type_ = type;
  }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(param));

private:
  int inner_init_before_run_();
  int get_batch_size_(int64_t &batch_size);
  int prepare_backup_tablet_provider_(const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type,
      ObLSBackupCtx &ls_backup_ctx, ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy,
      ObIBackupTabletProvider *&provider);

private:
  bool is_inited_;
  ObLSBackupStage start_stage_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupDagNetInitParam param_;
  ObLSBackupCtx ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;  // owned by dag net
  ObBackupMacroBlockTaskMgr task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupDataDagNet);
};

class ObBackupBuildTenantIndexDagNet : public ObBackupDagNet {
public:
  ObBackupBuildTenantIndexDagNet();
  virtual ~ObBackupBuildTenantIndexDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual bool is_valid() const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(param));

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  share::ObBackupDataType backup_data_type_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupBuildTenantIndexDagNet);
};

class ObLSBackupComplementLogDagNet : public ObBackupDagNet {
public:
  ObLSBackupComplementLogDagNet();
  virtual ~ObLSBackupComplementLogDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual bool is_valid() const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(param), K_(compl_start_scn), K_(compl_end_scn));

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  share::SCN compl_start_scn_;
  share::SCN compl_end_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupComplementLogDagNet);
};

class ObBackupDag : public share::ObIDag
{
public:
  explicit ObBackupDag(const ObBackupDagSubType &sub_type);
  virtual ~ObBackupDag();
  ObBackupDagSubType get_sub_type() const { return sub_type_; };
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(sub_type));
protected:
  ObBackupDagSubType sub_type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDag);
};

class ObLSBackupMetaDag : public ObBackupDag {
public:
  ObLSBackupMetaDag();
  virtual ~ObLSBackupMetaDag();
  int init(const share::SCN &start_scn, const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  share::SCN start_scn_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupMetaDag);
};

class ObLSBackupPrepareDag : public ObBackupDag {
public:
  ObLSBackupPrepareDag();
  virtual ~ObLSBackupPrepareDag();
  int init(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
      const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
      ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  int get_concurrency_count_(const share::ObBackupDataType &backup_data_type, int64_t &concurrency);

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupPrepareDag);
};

class ObLSBackupFinishDag : public ObBackupDag {
public:
  ObLSBackupFinishDag();
  virtual ~ObLSBackupFinishDag();
  int init(const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx, ObLSBackupCtx &backup_ctx,
      ObBackupIndexKVCache &index_kv_cache);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int64_t hash() const override;
  virtual bool check_can_schedule() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited));

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupIndexKVCache *index_kv_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupFinishDag);
};

class ObLSBackupDataDag : public ObBackupDag {
public:
  ObLSBackupDataDag(const ObBackupDagSubType &sub_type = ObBackupDagSubType::LOG_STREAM_BACKUP_DATA_DAG);
  virtual ~ObLSBackupDataDag();
  int init(const int64_t task_id, const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
      const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
      ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache, share::ObIDag *index_rebuild_dag);
  int provide(const common::ObIArray<ObBackupProviderItem> &items);
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param));

protected:
  bool is_inited_;
  int64_t task_id_;
  ObLSBackupDagInitParam param_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupReportCtx report_ctx_;
  ObArray<ObBackupProviderItem> backup_items_;
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
  share::ObIDag *index_rebuild_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupDataDag);
};

class ObPrefetchBackupInfoDag : public ObLSBackupDataDag {
public:
  ObPrefetchBackupInfoDag();
  virtual ~ObPrefetchBackupInfoDag();
  virtual int create_first_task() override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPrefetchBackupInfoDag);
};

class ObLSBackupIndexRebuildDag : public ObBackupDag {
public:
  ObLSBackupIndexRebuildDag();
  virtual ~ObLSBackupIndexRebuildDag();
  int init(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
      const ObBackupIndexLevel &index_level, const ObBackupReportCtx &report_ctx, ObBackupMacroBlockTaskMgr *task_mgr,
      ObIBackupTabletProvider *provider, ObBackupIndexKVCache *index_kv_cache, ObLSBackupCtx *ctx);
  virtual int create_first_task() override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

private:
  int get_file_id_list_(common::ObIArray<int64_t> &file_id_list);

private:
  bool is_inited_;
  ObBackupReportCtx report_ctx_;
  ObLSBackupDagInitParam param_;
  ObBackupIndexLevel index_level_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObIBackupTabletProvider *provider_;
  ObBackupIndexKVCache *index_kv_cache_;
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupIndexRebuildDag);
};

class ObLSBackupComplementLogDag : public ObBackupDag {
public:
  ObLSBackupComplementLogDag();
  virtual ~ObLSBackupComplementLogDag();
  int init(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id, const int64_t turn_id,
      const int64_t retry_id, const share::SCN &start_scn, const share::SCN &end_scn, const ObBackupReportCtx &report_ctx);
  virtual int create_first_task() override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool operator==(const ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override;
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }

private:
  bool is_inited_;
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  int64_t retry_id_;
  share::SCN compl_start_scn_;
  share::SCN compl_end_scn_;
  ObBackupReportCtx report_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObLSBackupComplementLogDag);
};

class ObLSBackupMetaTask : public share::ObITask {
public:
  ObLSBackupMetaTask();
  virtual ~ObLSBackupMetaTask();
  int init(const share::SCN &start_scn, const ObLSBackupDagInitParam &param, const ObBackupReportCtx &report_ctx);
  virtual int process() override;

private:
  int advance_checkpoint_by_flush_(const uint64_t tenant_id, const share::ObLSID &ls_id, const share::SCN &start_scn);
  int get_backup_meta_ctx_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      ObBackupLSMetaInfo &ls_meta_info, common::ObArray<common::ObTabletID> &tablet_id_list);
  int backup_ls_meta_package_(const ObBackupLSMetaInfo &ls_meta_info);
  int backup_ls_tablet_list_(
      const share::SCN &scn, const share::ObLSID &ls, const common::ObIArray<common::ObTabletID> &tablet_id_list);
  int build_backup_data_ls_tablet_desc_(const share::ObLSID &ls_id, const share::SCN &scn,
      const common::ObIArray<common::ObTabletID> &tablet_id, share::ObBackupDataTabletToLSDesc &info);


private:
  bool is_inited_;
  share::SCN start_scn_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupMetaTask);
};

class ObLSBackupPrepareTask : public share::ObITask {
public:
  ObLSBackupPrepareTask();
  virtual ~ObLSBackupPrepareTask();
  int init(const int64_t concurrency, const ObLSBackupDagInitParam &param,
      const share::ObBackupDataType &backup_data_type, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
      ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache, const ObBackupReportCtx &report_ctx);
  virtual int process() override;

private:
  int may_need_advance_checkpoint_();
  int fetch_cur_ls_rebuild_seq_(int64_t &rebuild_seq);
  int fetch_backup_ls_meta_(share::SCN &clog_checkpoint_scn);
  int check_tx_data_can_explain_user_data_();
  int get_backup_tx_data_table_filled_tx_scn_(share::SCN &filled_tx_scn);
  int prepare_meta_index_store_(ObBackupMetaIndexStore &meta_index_store);
  int get_sys_ls_retry_id_(int64_t &retry_id);
  int prepare_meta_index_store_param_(const int64_t retry_id, ObBackupIndexStoreParam &param);
  int get_cur_ls_min_filled_tx_scn_(share::SCN &min_filled_tx_scn);
  int get_tablet_min_filled_tx_scn_(ObTabletHandle &tablet_handle,
      share::SCN &min_filled_tx_scn, bool &has_minor_sstable);

private:
  bool is_inited_;
  int64_t concurrency_;
  ObLSBackupDagInitParam param_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupPrepareTask);
};

class ObPrefetchBackupInfoTask : public share::ObITask {
public:
  ObPrefetchBackupInfoTask();
  virtual ~ObPrefetchBackupInfoTask();
  int init(const ObLSBackupDagInitParam &param, const share::ObBackupDataType &backup_data_type,
      const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider,
      ObBackupMacroBlockTaskMgr &task_mgr, ObBackupIndexKVCache &index_kv_cache, share::ObIDag *index_rebuild_dag);
  virtual int process() override;

private:
  int inner_process_();
  int check_backup_items_valid_(const common::ObIArray<ObBackupProviderItem> &items);
  int get_prev_backup_set_desc_(const uint64_t tenant_id, const int64_t dest_id, const share::ObBackupSetDesc &cur_backup_set_desc,
      share::ObBackupSetDesc &prev_backup_set_desc);
  int get_tenant_macro_index_retry_id_(const share::ObBackupDest &backup_dest, const share::ObBackupSetDesc &prev_backup_set_desc,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, int64_t &retry_id);
  int get_need_copy_item_list_(const common::ObIArray<ObBackupProviderItem> &list,
      common::ObIArray<ObBackupProviderItem> &need_copy_list, common::ObIArray<ObBackupProviderItem> &no_need_copy_list,
      common::ObIArray<ObBackupPhysicalID> &no_need_copy_macro_index_list);
  int check_backup_item_need_copy_(
      const ObBackupProviderItem &item, bool &need_copy, ObBackupMacroBlockIndex &macro_index);
  int generate_next_prefetch_dag_();
  int generate_backup_dag_(const int64_t task_id, const common::ObIArray<ObBackupProviderItem> &items);

private:
  bool is_inited_;
  ObLSBackupDagInitParam param_;
  ObBackupReportCtx report_ctx_;
  share::ObBackupDataType backup_data_type_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupMacroBlockIndexStore macro_index_store_;
  share::ObIDag *index_rebuild_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObPrefetchBackupInfoTask);
};

class ObLSBackupDataTask : public share::ObITask {
public:
  ObLSBackupDataTask();
  virtual ~ObLSBackupDataTask();
  int init(const int64_t task_id, const share::ObBackupDataType &backup_data_type,
      const common::ObIArray<ObBackupProviderItem> &backup_items, const ObLSBackupDataParam &param,
      const ObBackupReportCtx &report_ctx, common::ObInOutBandwidthThrottle &bandwidth_throttle,
      ObLSBackupCtx &ls_backup_ctx, ObIBackupTabletProvider &provider, ObBackupMacroBlockTaskMgr &task_mgr,
      ObBackupIndexKVCache &kv_cache, share::ObIDag *index_rebuild_dag);
  virtual int process() override;

private:
  int may_inject_simulated_error_();
  int report_tablet_skipped_(const common::ObTabletID &tablet_id,
      const share::ObBackupSkippedType &skipped_type);

private:
  int build_backup_file_header_(ObBackupFileHeader &file_header);
  int do_write_file_header_();
  int do_backup_macro_block_data_();
  int do_backup_meta_data_();
  int get_tablet_meta_info_(
      const ObBackupProviderItem &item, ObTabletMetaReaderType &reader_type, ObBackupMetaType &meta_type);
  int finish_task_in_order_();
  int report_ls_backup_task_info_(const ObLSBackupStat &stat);
  int update_task_stat_(const share::ObBackupStats &old_backup_stat, const ObLSBackupStat &ls_stat,
      share::ObBackupStats &new_backup_stat);
  int update_task_info_stat_(const ObBackupLSTaskInfo &task_info, const ObLSBackupStat &stat, ObLSBackupStat &new_stat);
  int do_generate_next_task_();
  int check_disk_space_();
  int get_macro_block_id_list_(common::ObIArray<ObBackupMacroBlockId> &list);
  int get_meta_item_list_(common::ObIArray<ObBackupProviderItem> &list);
  int prepare_macro_block_reader_(
      const common::ObIArray<ObBackupMacroBlockId> &list, ObMultiMacroBlockBackupReader *&reader);
  int prepare_tablet_meta_reader_(const common::ObTabletID &tablet_id, const ObTabletMetaReaderType &reader_type,
      storage::ObTabletHandle &tablet_handle, ObITabletMetaBackupReader *&reader);
  int get_next_macro_block_data_(ObMultiMacroBlockBackupReader *reader, blocksstable::ObBufferReader &buffer_reader,
      blocksstable::ObLogicMacroBlockId &logic_id);
  int check_macro_block_data_(const blocksstable::ObBufferReader &data);
  int write_macro_block_data_(const blocksstable::ObBufferReader &data, const blocksstable::ObLogicMacroBlockId &logic_id,
      ObBackupMacroBlockIndex &macro_index);
  int write_backup_meta_(const blocksstable::ObBufferReader &data, const common::ObTabletID &tablet_id,
      const ObBackupMetaType &meta_type, ObBackupMetaIndex &meta_index);
  int get_tablet_handle_(const common::ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle);
  int release_tablet_handle_(const common::ObTabletID &tablet_id);
  int check_backup_finish_(bool &finish);
  int do_generate_next_backup_dag_();
  int get_max_file_id_(int64_t &max_file_id);
  bool is_change_turn_error_(const int64_t error_code) const;
  void record_server_event_(const int64_t cost_us) const;
  int mark_backup_item_finished_(const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id);
  int get_backup_item_(const blocksstable::ObLogicMacroBlockId &logic_id, ObBackupProviderItem &item);
  int finish_backup_items_();
  int backup_secondary_metas_(ObBackupTabletStat *tablet_stat);
  int may_fill_reused_backup_items_(
      const common::ObTabletID &tablet_id, ObBackupTabletStat *tablet_stat);
  int check_macro_block_need_reuse_when_recover_(const blocksstable::ObLogicMacroBlockId &logic_id,
      const common::ObArray<blocksstable::ObLogicMacroBlockId> &logic_id_list, bool &need_reuse);
  int release_tablet_handles_(ObBackupTabletStat *tablet_stat);

private:
  static const int64_t CHECK_DISK_SPACE_INTERVAL = 5 * 1000 * 1000;  // 5s;

private:
  bool is_inited_;
  int64_t task_id_;
  ObLSBackupStat backup_stat_;
  ObLSBackupDataParam param_;
  ObBackupReportCtx report_ctx_;
  share::ObBackupDataType backup_data_type_;
  ObBackupDataCtx backup_data_ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  ObBackupDiskChecker disk_checker_;
  common::ObArenaAllocator allocator_;
  common::ObArray<ObBackupProviderItem> backup_items_;
  common::ObArray<common::ObTabletID> finished_tablet_list_;
  share::ObIDag *index_rebuild_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupDataTask);
};

class ObBackupIndexRebuildTask : public share::ObITask {
public:
  ObBackupIndexRebuildTask();
  virtual ~ObBackupIndexRebuildTask();
  int init(const ObLSBackupDataParam &param, const ObBackupIndexLevel &index_level, ObLSBackupCtx *ls_backup_ctx,
      ObIBackupTabletProvider *provider, ObBackupMacroBlockTaskMgr *task_mgr, ObBackupIndexKVCache *kv_cache,
      common::ObInOutBandwidthThrottle &bandwidth_throttle, const ObBackupReportCtx &report_ctx);
  virtual int process() override;

private:
  int check_all_tablet_released_();
  int mark_ls_task_final_();
  bool need_build_index_() const;
  int merge_macro_index_();
  int merge_meta_index_(const bool is_sec_meta);
  int generate_next_phase_dag_();
  void record_server_event_(const int64_t cost_us) const;
  int report_check_tablet_info_event_();

private:
  bool is_inited_;
  ObLSBackupDataParam param_;
  ObBackupIndexLevel index_level_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObIBackupTabletProvider *provider_;
  ObBackupMacroBlockTaskMgr *task_mgr_;
  ObBackupIndexKVCache *index_kv_cache_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupIndexRebuildTask);
};

class ObLSBackupFinishTask : public share::ObITask {
public:
  ObLSBackupFinishTask();
  virtual ~ObLSBackupFinishTask();
  int init(const ObLSBackupDataParam &param, const ObBackupReportCtx &report_ctx, ObLSBackupCtx &ctx,
      ObBackupIndexKVCache &index_kv_cache);
  virtual int process() override;

private:
  bool is_inited_;
  ObLSBackupDataParam param_;
  ObBackupReportCtx report_ctx_;
  ObLSBackupCtx *ls_backup_ctx_;
  ObBackupIndexKVCache *index_kv_cache_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupFinishTask);
};

class ObLSBackupComplementLogTask : public share::ObITask {
  struct BackupPieceFile {
    BackupPieceFile();
    void reset();
    int set(const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id, const int64_t ls_id,
        const int64_t file_id);
    TO_STRING_KV(K_(tenant_id), K_(round_id), K_(piece_id), K_(ls_id), K_(file_id));
    uint64_t tenant_id_;
    int64_t round_id_;
    int64_t piece_id_;
    int64_t ls_id_;
    int64_t file_id_;
  };

  class BackupPieceOp : public ObBaseDirEntryOperator {
  public:
    BackupPieceOp();
    virtual int func(const dirent *entry) override;
    int get_file_id_list(common::ObIArray<int64_t> &files) const;
    TO_STRING_KV(K_(file_id_list));

  private:
    int parse_log_file_id_(const common::ObString &file_name, int64_t &piece_id);

  private:
    ObArray<int64_t> file_id_list_;
  };

  struct CompareArchivePiece {
    bool operator()(const share::ObTenantArchivePieceAttr &lhs, const share::ObTenantArchivePieceAttr &rhs) const;
  };

public:
  ObLSBackupComplementLogTask();
  virtual ~ObLSBackupComplementLogTask();
  int init(const ObBackupJobDesc &job_desc, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id, const share::SCN &start_scn,
      const share::SCN &end_scn, const int64_t turn_id, const int64_t retry_id, const ObBackupReportCtx &report_ctx);
  virtual int process() override;

private:
  int get_complement_log_dir_path_(share::ObBackupPath &backup_path);
  int calc_backup_file_range_(common::ObIArray<BackupPieceFile> &file_list);
  int check_pieces_continue_(const common::ObIArray<share::ObTenantArchivePieceAttr> &rounds);
  int get_piece_id_by_ts_(const uint64_t tenant_id, const share::SCN &scn, int64_t &piece_id);
  int get_all_pieces_(const uint64_t tenant_id, const int64_t start_piece_id, const int64_t end_piece_id,
      common::ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int get_all_piece_file_list_(const uint64_t tenant_id,
      const common::ObIArray<share::ObTenantArchivePieceAttr> &piece_list, const share::SCN &start_scn, const share::SCN &end_scn,
      common::ObIArray<BackupPieceFile> &piece_file_list);
  int inner_get_piece_file_list_(
      const int64_t round_id, const int64_t piece_id, common::ObIArray<BackupPieceFile> &piece_file_list);
  int locate_archive_file_id_by_ts_(const uint64_t tenant_id, const int64_t round_id, const int64_t piece_id,
      const share::SCN &scn, const bool is_upper_bound, int64_t &file_id);
  int filter_file_id_smaller_than_(const int64_t file_id, common::ObIArray<BackupPieceFile> &list);
  int filter_file_id_larger_than_(const int64_t file_id, common::ObIArray<BackupPieceFile> &list);
  int get_src_backup_piece_dir_(const int64_t round_id, const int64_t piece_id, share::ObBackupPath &backup_path);
  int get_src_backup_file_path_(const BackupPieceFile &piece_file, share::ObBackupPath &backup_path);
  int get_dst_backup_file_path_(const BackupPieceFile &piece_file, share::ObBackupPath &backup_path);
  int backup_complement_log_(const common::ObIArray<BackupPieceFile> &path);
  int inner_backup_complement_log_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path);
  int transfer_clog_file_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path);
  int inner_transfer_clog_file_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path);
  int get_transfer_length_(const int64_t delta_len, int64_t &transfer_len);
  int get_file_length_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, int64_t &length);
  int post_rpc_result_(const int64_t result);

private:
  bool is_inited_;
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  share::SCN compl_start_scn_;
  share::SCN compl_end_scn_;
  int64_t turn_id_;
  int64_t retry_id_;

  ObBackupReportCtx report_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSBackupComplementLogTask);
};

}  // namespace backup
}  // namespace oceanbase

#endif
