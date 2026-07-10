/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace storage
{

class ObLS;

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
  static int create_empty_mini_minor_sstable(
      const common::ObTabletID &tablet_id,
      const share::SCN start_scn,
      const share::SCN end_scn,
      const ObStorageSchema &table_schema,
      const ObITable::TableType &table_type,
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
  static int build_empty_mini_minor_sstable_param_(
      const share::SCN start_scn,
      const share::SCN end_scn,
      const ObStorageSchema &table_schema,
      const common::ObTabletID &tablet_id,
      const ObITable::TableType &table_type,
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
  int get_related_info_task_id(share::ObTransferTaskID &task_id) const;
  int reset(const share::ObTransferTaskID &task_id);
  TO_STRING_KV(K_(task_id), K_(start_scn), K_(start_out_log_replay_num),
      K_(start_in_log_replay_num), K_(finish_out_log_replay_num), K_(finish_in_log_replay_num));
  typedef hash::ObHashMap<common::ObTabletID, int64_t, hash::NoPthreadDefendMode> TxBackfillStatMap;

private:
  void reset_();
  const share::ObTransferTaskID &get_task_id_() const;
  const share::SCN &get_start_scn_() const;
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
    int check_all_tables_exist_(
        const int64_t schema_version,
        const common::ObIArray<ObTabletID> &tablet_ids,
        const common::ObIArray<uint64_t> &table_ids);
    int get_schema_guard_(
        const uint64_t tenant_id,
        const int64_t schema_version,
        const uint64_t table_id,
        ObMultiVersionSchemaService &schema_service,
        share::schema::ObSchemaGetterGuard &schema_guard);
    int build_tablet_storage_schema_(
        const uint64_t tenant_id,
        const ObTabletID &tablet_id,
        const uint64_t table_id,
        const uint64_t compat_version,
        ObLS *ls,
        share::schema::ObSchemaGetterGuard &schema_guard);
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

class ObTransferBuildTabletInfoHelper
{
public:
  static int build_tablet_info(
      ObLS *ls,
      const share::ObTransferTabletInfo &tablet_info,
      ObTransferBuildTabletInfoCtx &ctx);
  static int loop_to_build_tablet_infos(
      ObLS *ls,
      common::ObTimeoutCtx &timeout_ctx,
      ObTransferBuildTabletInfoCtx &ctx);
};

struct ObTransferLSInfo final
{
public:
  ObTransferLSInfo() : src_is_duplicate_ls_(false), dest_is_duplicate_ls_(false) {}
  ~ObTransferLSInfo() = default;
  void reset();
  bool allow_learner_list_not_same() const { return src_is_duplicate_ls_ != dest_is_duplicate_ls_; }
  bool need_check_r_replica() const { return src_is_duplicate_ls_ && dest_is_duplicate_ls_; }
  bool must_get_learner_list_from_src_ls() const { return dest_is_duplicate_ls_ && !src_is_duplicate_ls_; }
  bool must_get_learner_list_from_dest_ls() const { return src_is_duplicate_ls_ && !dest_is_duplicate_ls_; }
  TO_STRING_KV(K_(src_is_duplicate_ls), K_(dest_is_duplicate_ls));

  bool src_is_duplicate_ls_;
  bool dest_is_duplicate_ls_;
};

}
}
#endif
