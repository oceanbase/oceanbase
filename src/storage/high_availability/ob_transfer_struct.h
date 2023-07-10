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

  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(tablet_list));

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
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

  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(start_scn), K_(tablet_meta_list));

  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN start_scn_;
  common::ObSArray<ObMigrationTabletParam> tablet_meta_list_;
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

  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(finish_scn), K_(tablet_list));
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN finish_scn_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
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

  TO_STRING_KV(K_(src_ls_id), K_(dest_ls_id), K_(start_scn), K_(tablet_list));
  share::ObLSID src_ls_id_;
  share::ObLSID dest_ls_id_;
  share::SCN start_scn_;
  common::ObSArray<share::ObTransferTabletInfo> tablet_list_;
  DISALLOW_COPY_AND_ASSIGN(ObTXFinishTransferInInfo);
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

}
}
#endif
