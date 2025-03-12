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

#ifndef STORAGE_LOG_STREAM_BACKUP_COMPLEMENT_LOG_H_
#define STORAGE_LOG_STREAM_BACKUP_COMPLEMENT_LOG_H_

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_archive_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/ob_rs_mgr.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_iterator.h"
#include "storage/backup/ob_backup_operator.h"
#include "storage/backup/ob_backup_utils.h"
#include "storage/backup/ob_backup_restore_util.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "logservice/archiveservice/ob_archive_file_utils.h"
#include "share/ls/ob_ls_table_operator.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ob_storage_rpc.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_archive_path.h"
#include "share/backup/ob_archive_store.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/backup/ob_backup_task.h"
#include <algorithm>

namespace oceanbase {
namespace backup {

struct ObBackupPieceFile {
  ObBackupPieceFile();
  void reset();
  int set(const int64_t dest_id, const int64_t round_id, const int64_t piece_id, const share::ObLSID &ls_id,
      const int64_t file_id, const share::SCN &start_scn, const share::SCN &checkpoint_scn, const ObBackupPathString &path);
  TO_STRING_KV(K_(dest_id), K_(round_id), K_(piece_id), K_(ls_id), K_(file_id), K_(start_scn), K_(checkpoint_scn), K_(path));
  int64_t dest_id_;
  int64_t round_id_;
  int64_t piece_id_;
  share::ObLSID ls_id_;
  int64_t file_id_;
  share::SCN start_scn_;
  share::SCN checkpoint_scn_;
  ObBackupPathString path_;
};

struct ObBackupComplementLogCtx final {
  ObBackupJobDesc job_desc_;
  share::ObBackupDest backup_dest_;
  uint64_t tenant_id_;
  int64_t dest_id_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  share::SCN compl_start_scn_;
  share::SCN compl_end_scn_;
  int64_t turn_id_;
  int64_t retry_id_;
  ObBackupReportCtx report_ctx_;
  bool is_only_calc_stat_;

  bool is_valid() const;
  bool operator==(const ObBackupComplementLogCtx &other) const;
  uint64_t calc_hash(uint64_t seed) const;

  TO_STRING_KV(K_(backup_dest), K_(tenant_id), K_(dest_id), K_(backup_set_desc), K_(ls_id), K_(turn_id));

public:
  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int get_result(int32_t &result);
  int check_allow_retry(bool &allow_retry);

private:
  mutable lib::ObMutex mutex_;
  ObStorageHAResultMgr result_mgr_;
};

class ObBackupPieceOp : public ObBaseDirEntryOperator {
public:
  ObBackupPieceOp();
  virtual int func(const dirent *entry) override;
  int get_file_id_list(common::ObIArray<int64_t> &files) const;
  TO_STRING_KV(K_(file_id_list));

private:
  ObArray<int64_t> file_id_list_;
};

struct CompareArchivePiece {
  bool operator()(const share::ObTenantArchivePieceAttr &lhs, const share::ObTenantArchivePieceAttr &rhs) const;
};

class ObBackupComplementLogDagNet : public ObBackupDagNet {
public:
  ObBackupComplementLogDagNet();
  virtual ~ObBackupComplementLogDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int start_running() override;
  virtual bool operator==(const share::ObIDagNet &other) const override;
  virtual bool is_valid() const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));

private:
  bool is_inited_;
  ObBackupComplementLogCtx ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupComplementLogDagNet);
};

class ObBackupLSLogGroupDag : public share::ObIDag {
public:
  ObBackupLSLogGroupDag();
  virtual ~ObBackupLSLogGroupDag();
  int init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx, common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }

protected:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogGroupDag);
};

class ObBackupLSLogGroupTask : public share::ObITask {
public:
  ObBackupLSLogGroupTask();
  virtual ~ObBackupLSLogGroupTask();
  int init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx, common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual int process() override;

private:
  int get_next_ls_id_(share::ObLSID &ls_id);
  int generate_ls_dag_();
  int record_server_event_();

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  int64_t current_idx_;
  common::ObArray<share::ObLSID> ls_ids_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogGroupTask);
};

class ObBackupLSLogDag : public share::ObIDag {
public:
  ObBackupLSLogDag();
  virtual ~ObBackupLSLogDag();
  int init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx,
      common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual lib::Worker::CompatMode get_compat_mode() const { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }

protected:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogDag);
};

class ObBackupLSLogTask : public share::ObITask {
public:
  ObBackupLSLogTask();
  virtual ~ObBackupLSLogTask();
  int init(const share::ObLSID &ls_id, ObBackupComplementLogCtx *ctx, common::ObInOutBandwidthThrottle *bandwidth_throttle);
  virtual int process() override;

private:
  int get_active_round_dest_id_(const uint64_t tenant_id, int64_t &dest_id);
  int inner_process_(const int64_t archive_dest_id, const share::ObLSID &ls_id);
  int deal_with_piece_meta_(const common::ObIArray<ObTenantArchivePieceAttr> &piece_list);
  int inner_deal_with_piece_meta_(const ObTenantArchivePieceAttr &piece_attr);
  int generate_ls_copy_task_(const bool is_only_calc_stat, const share::ObLSID &ls_id);
  int get_complement_log_dir_path_(share::ObBackupPath &backup_path);

private:
  int get_ls_replay_start_scn_if_not_newly_created_(const share::ObLSID &ls_id, share::SCN &start_scn);
  int calc_backup_file_range_(const int64_t dest_id, const share::ObLSID &ls_id,
      common::ObArray<ObTenantArchivePieceAttr> &piece_list, common::ObIArray<ObBackupPieceFile> &file_list);
  int update_ls_task_stat_(const share::ObBackupStats &old_backup_stat,
      const int64_t compl_log_file_count, share::ObBackupStats &new_backup_stat);
  int report_complement_log_stat_(const common::ObIArray<ObBackupPieceFile> &file_list);
  int get_piece_id_by_scn_(const uint64_t tenant_id, const int64_t dest_id, const share::SCN &scn, int64_t &piece_id);
  int get_all_pieces_(const uint64_t tenant_id, const int64_t dest_id, const int64_t start_piece_id, const int64_t end_piece_id,
      common::ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int wait_pieces_frozen_(const common::ObArray<share::ObTenantArchivePieceAttr> &piece_list);
  int wait_piece_frozen_(const share::ObTenantArchivePieceAttr &piece);
  int check_piece_frozen_(const share::ObTenantArchivePieceAttr &piece, bool &is_frozen);
  int get_all_piece_file_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObIArray<share::ObTenantArchivePieceAttr> &piece_list, const share::SCN &start_scn, const share::SCN &end_scn,
      common::ObIArray<ObBackupPieceFile> &piece_file_list);
  int inner_get_piece_file_list_(const share::ObLSID &ls_id, const ObTenantArchivePieceAttr &piece_attr, common::ObIArray<ObBackupPieceFile> &piece_file_list);
  int locate_archive_file_id_by_scn_(const ObTenantArchivePieceAttr &piece_attr, const share::ObLSID &ls_id, const SCN &scn, int64_t &file_id);
  int get_file_in_between_(const int64_t start_file_id, const int64_t end_file_id, common::ObIArray<ObBackupPieceFile> &list);
  int filter_file_id_smaller_than_(const int64_t file_id, common::ObIArray<ObBackupPieceFile> &list);
  int filter_file_id_larger_than_(const int64_t file_id, common::ObIArray<ObBackupPieceFile> &list);
  int get_src_backup_piece_dir_(const share::ObLSID &ls_id, const ObTenantArchivePieceAttr &piece_attr, share::ObBackupPath &backup_path);

private:
  int write_format_file_();
  int generate_format_desc_(share::ObBackupFormatDesc &format_desc);
  int transform_and_copy_meta_file_(const ObTenantArchivePieceAttr &piece_attr);
  // ls_file_info
  int copy_ls_file_info_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // piece_file_info
  int copy_piece_file_info_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // single_piece_info
  int copy_single_piece_info_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // tenant_archive_piece_infos
  int copy_tenant_archive_piece_infos_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // checkpoint_info
  int copy_checkpoint_info_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // round_start
  int copy_round_start_file_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // round_end
  int copy_round_end_file_(const ObTenantArchivePieceAttr &piece_attr, const share::ObArchiveStore &src_store, const share::ObArchiveStore &dest_store);
  // piece_start
  int copy_piece_start_file_(const ObTenantArchivePieceAttr &piece_attr, const share::ObBackupDest &src, const share::ObBackupDest &dest);
  // piece_end
  int copy_piece_end_file_(const ObTenantArchivePieceAttr &piece_file, const share::ObBackupDest &src, const share::ObBackupDest &dest);
  int get_archive_backup_dest_(const ObBackupPathString &path, share::ObBackupDest &archive_dest);
  int get_copy_src_and_dest_(const ObTenantArchivePieceAttr &piece_file, share::ObBackupDest &src, share::ObBackupDest &dest);
  int record_server_event_();

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  common::ObArray<ObBackupPieceFile> file_list_;
  share::ObBackupDest archive_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogTask);
};

class ObBackupLSLogFinishTask;
class ObBackupLSLogFileTask : public share::ObITask
{
public:
  ObBackupLSLogFileTask();
  virtual ~ObBackupLSLogFileTask();
  int init(const share::ObLSID &ls_id, common::ObInOutBandwidthThrottle *bandwidth_throttle,
      ObBackupComplementLogCtx *ctx, ObBackupLSLogFinishTask *finish_task);
  virtual int process() override;
  virtual int generate_next_task(ObITask *&next_task) override;

private:
  int build_backup_piece_file_info_(ObBackupLSLogFinishTask *finish_task);
  int inner_process_(const ObBackupPieceFile &piece_file);
  int inner_backup_complement_log_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path);
  int get_src_backup_file_path_(const ObBackupPieceFile &piece_file, share::ObBackupPath &backup_path);
  int get_dst_backup_file_path_(const ObBackupPieceFile &piece_file, share::ObBackupPath &backup_path);
  int transfer_clog_file_(const share::ObBackupPath &src_path, const share::ObBackupPath &dst_path);
  int inner_transfer_clog_file_(const ObBackupPath &src_path, const ObBackupPath &dst_path,
      ObIODevice *&device_handle, ObIOFd &fd, const int64_t dst_len, int64_t &transfer_len);
  int get_transfer_length_(const int64_t delta_len, int64_t &transfer_len);
  int get_file_length_(const common::ObString &path, const share::ObBackupStorageInfo *storage_info, int64_t &length);
  int get_archive_backup_dest_(const ObBackupPathString &path, share::ObBackupDest &archive_dest);
  int record_server_event_();
  int report_progress_();

private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  ObBackupLSLogFinishTask *finish_task_;
  ObBackupPieceFile backup_piece_file_;
  int64_t last_active_time_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  share::ObBackupDest archive_dest_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogFileTask);
};

class ObBackupLSLogFinishTask : public share::ObITask
{
public:
  ObBackupLSLogFinishTask();
  virtual ~ObBackupLSLogFinishTask();
  int init(const share::ObLSID &ls_id, const common::ObIArray<ObBackupPieceFile> &file_list,
      ObBackupComplementLogCtx *ctx);
  virtual int process() override;
  int check_is_iter_end(bool &is_iter_end);
  int get_copy_file_info(ObBackupPieceFile &piece_file);

private:
  int report_task_result_();
  int record_server_event_();

private:
  bool is_inited_;
  ObMutex mutex_;
  int64_t idx_;
  share::ObLSID ls_id_;
  ObBackupComplementLogCtx *ctx_;
  common::ObArray<ObBackupPieceFile> file_list_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogFinishTask);
};

class ObBackupLSLogGroupFinishDag : public share::ObIDag
{
public:
  ObBackupLSLogGroupFinishDag();
  virtual ~ObBackupLSLogGroupFinishDag();
  int init(ObBackupComplementLogCtx *ctx);
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool is_ha_dag() const override { return true; }

protected:
  bool is_inited_;
  ObBackupComplementLogCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogGroupFinishDag);
};

class ObBackupLSLogGroupFinishTask : public share::ObITask
{
public:
  ObBackupLSLogGroupFinishTask();
  virtual ~ObBackupLSLogGroupFinishTask();
  int init(ObBackupComplementLogCtx *ctx);
  virtual int process() override;

private:
  int report_task_result_();
  int record_server_event_();

private:
  bool is_inited_;
  ObBackupComplementLogCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupLSLogGroupFinishTask);
};

}  // namespace backup
}  // namespace oceanbase

#endif
