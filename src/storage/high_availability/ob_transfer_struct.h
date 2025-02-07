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

#ifndef OCEABASE_STORAGE_TRANSFER_STRUCT_
#define OCEABASE_STORAGE_TRANSFER_STRUCT_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/transfer/ob_transfer_info.h"
#include "share/ob_balance_define.h"
#include "share/scn.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{

struct ObTXStartTransferOutInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTXStartTransferOutInfo();
  ~ObTXStartTransferOutInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTXStartTransferOutInfo &start_transfer_out_info);
  bool empty_tx() { return filter_tx_need_transfer_ && move_tx_ids_.count() == 0; }
  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
  share::ObTransferTaskID task_id_;
  share::SCN data_end_scn_;
  int64_t transfer_epoch_;
  uint64_t data_version_;
  bool filter_tx_need_transfer_;
  common::ObSEArray<transaction::ObTransID, 1> move_tx_ids_;
  DISALLOW_COPY_AND_ASSIGN(ObTXStartTransferOutInfo);
};

struct ObTXStartTransferInInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTXStartTransferInInfo();
  ~ObTXStartTransferInInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTXStartTransferInInfo &start_transfer_in_info);

  int get_tablet_id_list(common::ObIArray<common::ObTabletID> &tablet_id_list) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN start_scn_;
  common::ObSArray<ObMigrationTabletParam> tablet_meta_list_;
  share::ObTransferTaskID task_id_;
  uint64_t data_version_;

  DISALLOW_COPY_AND_ASSIGN(ObTXStartTransferInInfo);
};

struct ObTXFinishTransferOutInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTXFinishTransferOutInfo();
  ~ObTXFinishTransferOutInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTXFinishTransferOutInfo &finish_transfer_out_info);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN finish_scn_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
  share::ObTransferTaskID task_id_;
  uint64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObTXFinishTransferOutInfo);
};

struct ObTXFinishTransferInInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTXFinishTransferInInfo();
  ~ObTXFinishTransferInInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTXFinishTransferInInfo &finish_transfer_in_info);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN start_scn_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
  share::ObTransferTaskID task_id_;
  uint64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObTXFinishTransferInInfo);
};

struct ObTXTransferInAbortedInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObTXTransferInAbortedInfo();
  ~ObTXTransferInAbortedInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTXTransferInAbortedInfo &transfer_in_info);
  int64_t to_string(char *buf, const int64_t buf_len) const;

  share::ObLSID dest_ls_id_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
  uint64_t data_version_;
  DISALLOW_COPY_AND_ASSIGN(ObTXTransferInAbortedInfo);
};

struct ObTransferEventRecorder final
{
  static void record_transfer_task_event(
      const share::ObTransferTaskID &task_id,
      const char *event_name,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id);
  static void record_ls_transfer_event(
      const char *event_name,
      const transaction::NotifyType &type,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const bool for_replay,
      const share::SCN &scn,
      const int64_t result);
  static void record_tablet_transfer_event(
      const char *event_name,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t transfer_seq,
      const ObTabletStatus &tablet_status,
      const int64_t result);
  static void record_advance_transfer_status_event(
      const uint64_t tenant_id,
      const share::ObTransferTaskID &task_id,
      const share::ObLSID &src_ls_id,
      const share::ObLSID &dest_ls_id,
      const share::ObTransferStatus &next_status,
      const int64_t result);
};

struct ObTXTransferUtils
{
  static int get_tablet_status(
      const bool get_commit,
      ObTabletHandle &tablet_handle,
      ObTabletCreateDeleteMdsUserData &user_data);
  static int get_tablet_status(
      const bool get_commit,
      const ObTablet *tablet,
      ObTabletCreateDeleteMdsUserData &user_data);
  static int create_empty_minor_sstable(
      const common::ObTabletID &tablet_id,
      const share::SCN start_scn,
      const share::SCN end_scn,
      const ObStorageSchema &table_schema,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &table_handle);
  static int set_tablet_freeze_flag(storage::ObLS &ls, ObTablet *tablet);
  static int traverse_trans_to_submit_redo_log_with_retry(
    storage::ObLS &ls,
    const int64_t timeout);

private:
  static int get_tablet_status_(
      const bool get_commit,
      const ObTablet *tablet,
      ObTabletCreateDeleteMdsUserData &user_data);
  static int build_empty_minor_sstable_param_(
      const share::SCN start_scn,
      const share::SCN end_scn,
      const ObStorageSchema &table_schema,
      const common::ObTabletID &tablet_id,
      ObTabletCreateSSTableParam &param);
};

struct ObTransferLockStatus final
{
public:
  enum STATUS : uint8_t
  {
    START = 0,
    DOING = 1,
    ABORTED = 2,
    MAX_STATUS
  };
public:
  ObTransferLockStatus() : status_(MAX_STATUS) {}
  ~ObTransferLockStatus() = default;
  explicit ObTransferLockStatus(const STATUS &status) : status_(status) {}

  bool is_valid() const { return START <= status_ && status_ < MAX_STATUS; }
  void reset() { status_ = MAX_STATUS; }
  const char *str() const;
  int parse_from_str(const ObString &str);
  STATUS get_status() const { return status_; }

  TO_STRING_KV(K_(status), "status", str());
private:
  STATUS status_;
};

struct ObTransferLockInfoRowKey final {
public:
  ObTransferLockInfoRowKey();
  ~ObTransferLockInfoRowKey() = default;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObTransferTaskLockInfo final {
public:
  ObTransferTaskLockInfo();
  ~ObTransferTaskLockInfo() = default;
  void reset();
  bool is_valid() const;
  int assign(const ObTransferTaskLockInfo &other);
  int set(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id, const ObTransferLockStatus &status,
      const int64_t lock_owner, const common::ObString &comment);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(task_id), K_(status), K_(lock_owner), K_(comment));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t task_id_;
  ObTransferLockStatus status_;
  int64_t lock_owner_;
  ObSqlString comment_;
};

class ObTransferRelatedInfo final
{
public:
  ObTransferRelatedInfo();
  ~ObTransferRelatedInfo();
  int init();
  bool is_valid() const;
  void reset();
  void destroy();
  int set_info(
      const share::ObTransferTaskID &task_id,
      const share::SCN &start_scn);
  int record_error_diagnose_info_in_replay(
      const share::ObTransferTaskID &task_id,
      const share::ObLSID &dest_ls_id,
      const int result_code,
      const bool clean_related_info,
      const share::ObStorageHADiagTaskType type,
      const share::ObStorageHACostItemName result_msg);
  int record_error_diagnose_info_in_backfill(
      const share::SCN &log_sync_scn,
      const share::ObLSID &dest_ls_id,
      const int result_code,
      const ObTabletID &tablet_id,
      const ObMigrationStatus &migration_status,
      const share::ObStorageHACostItemName result_msg);

  int record_perf_diagnose_info_in_replay(
      const share::ObStorageHAPerfDiagParams &params,
      const int result,
      const uint64_t timestamp,
      const int64_t start_ts,
      const bool is_report);

  int record_perf_diagnose_info_in_backfill(
      const share::ObStorageHAPerfDiagParams &params,
      const share::SCN &log_sync_scn,
      const int result_code,
      const ObMigrationStatus &migration_status,
      const uint64_t timestamp,
      const int64_t start_ts,
      const bool is_report);
  int get_related_info_task_id(share::ObTransferTaskID &task_id) const;
  int reset(const share::ObTransferTaskID &task_id);
  TO_STRING_KV(K_(task_id), K_(start_scn), K_(start_out_log_replay_num),
      K_(start_in_log_replay_num), K_(finish_out_log_replay_num), K_(finish_in_log_replay_num));
  typedef hash::ObHashMap<common::ObTabletID, int64_t, hash::NoPthreadDefendMode> TxBackfillStatMap;

private:
  void reset_();
  int inc_tx_backfill_retry_num_(const ObTabletID &id, int64_t &retry_num);
  const share::ObTransferTaskID &get_task_id_() const;
  const share::SCN &get_start_scn_() const;
  int get_replay_retry_num_(
      const share::ObStorageHADiagTaskType type, const bool inc_retry_num, int64_t &retry_num);
  int construct_perf_diag_info_(
      const share::ObStorageHAPerfDiagParams &params,
      const uint64_t timestamp,
      const int64_t retry_num,
      const share::ObTransferTaskID &task_id,
      const int result,
      const int64_t start_ts,
      const bool is_report);

private:
  bool is_inited_;
  share::ObTransferTaskID task_id_;
  share::SCN start_scn_;
  int64_t start_out_log_replay_num_;
  int64_t start_in_log_replay_num_;
  int64_t finish_out_log_replay_num_;
  int64_t finish_in_log_replay_num_;
  TxBackfillStatMap map_;
  SpinRWLock lock_;

  DISALLOW_COPY_AND_ASSIGN(ObTransferRelatedInfo);
};

struct ObTransferBuildTabletInfoCtx final
{
public:
  ObTransferBuildTabletInfoCtx();
  ~ObTransferBuildTabletInfoCtx();
  int build_transfer_tablet_info(
      const share::ObLSID &dest_ls_id,
      const common::ObIArray<share::ObTransferTabletInfo> &tablet_info_array,
      const common::ObCurTraceId::TraceId &task_id,
      const uint64_t data_version);
  void reuse();
  int get_next_tablet_info(share::ObTransferTabletInfo &tablet_info);
  bool is_valid() const;
  void inc_child_task_num();
  void dec_child_task_num();
  int64_t get_child_task_num();
  int64_t get_total_tablet_count();
  bool is_build_tablet_finish() const;
  common::ObCurTraceId::TraceId &get_task_id() { return task_id_; }
  bool is_failed() const;
  void set_result(const int32_t result);
  share::ObLSID &get_dest_ls_id() { return dest_ls_id_; }
  uint64_t get_data_version() { return data_version_; }
  int32_t get_result();

  int add_tablet_info(const ObMigrationTabletParam &param);
  int get_tablet_info(const int64_t index, const ObMigrationTabletParam *&param);
  int64_t get_tablet_info_num() const;
  int build_storage_schema_info(
      const share::ObTransferTaskInfo &task_info,
      ObTimeoutCtx &timeout_ctx);
  TO_STRING_KV(K_(index), K_(tablet_info_array), K_(child_task_num), K_(total_tablet_count),
      K_(result), K_(data_version), K_(task_id));
private:
  bool is_valid_() const;

private:
  class ObTransferStorageSchemaMgr final
  {
  public:
    ObTransferStorageSchemaMgr();
    ~ObTransferStorageSchemaMgr();
    int init(const int64_t bucket_num);
    int build_storage_schema(
        const share::ObTransferTaskInfo &task_info,
        ObTimeoutCtx &timeout_ctx);
    int get_storage_schema(
        const ObTabletID &tablet_id,
        ObStorageSchema *&storage_schema);
    void reset();
  private:
    int build_latest_storage_schema_(
        const share::ObTransferTaskInfo &task_info,
        ObTimeoutCtx &timeout_ctx);
    int build_tablet_storage_schema_(
        const share::ObTransferTaskInfo &task_info,
        const ObTabletID &tablet_id,
        const int64_t schema_version,
        ObLS *ls,
        ObMultiVersionSchemaService &schema_service);
  private:
    bool is_inited_;
    common::ObArenaAllocator allocator_;
    hash::ObHashMap<ObTabletID, ObStorageSchema *> storage_schema_map_;
    DISALLOW_COPY_AND_ASSIGN(ObTransferStorageSchemaMgr);
  };

  struct ObTransferTabletInfoMgr final
  {
  public:
    ObTransferTabletInfoMgr();
    ~ObTransferTabletInfoMgr();
    int add_tablet_info(const ObMigrationTabletParam &param);
    int64_t get_tablet_info_num() const;
    int get_tablet_info(const int64_t index, const ObMigrationTabletParam *&param);
    void reuse();
    int build_storage_schema(
        const share::ObTransferTaskInfo &task_info,
        ObTimeoutCtx &timeout_ctx);
    TO_STRING_KV(K_(tablet_info_array));
  private:
    common::SpinRWLock lock_;
    common::ObArray<ObMigrationTabletParam> tablet_info_array_;
    ObTransferStorageSchemaMgr storage_schema_mgr_;
    DISALLOW_COPY_AND_ASSIGN(ObTransferTabletInfoMgr);
  };
private:
  common::SpinRWLock lock_;
  share::ObLSID dest_ls_id_;
  int64_t index_;
  common::ObArray<share::ObTransferTabletInfo> tablet_info_array_;
  int64_t child_task_num_;
  int64_t total_tablet_count_;
  int32_t result_;
  uint64_t data_version_;
  common::ObCurTraceId::TraceId task_id_;
  ObTransferTabletInfoMgr mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTransferBuildTabletInfoCtx);
};




}
}
#endif
