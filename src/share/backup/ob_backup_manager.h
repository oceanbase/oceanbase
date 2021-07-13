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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_MANAGER_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_MANAGER_H_

#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "ob_backup_struct.h"

namespace oceanbase {
namespace share {

class ObBackupItemTransUpdater;
class ObIBackupLeaseService;

struct ObBackupInfoItem : public common::ObDLinkBase<ObBackupInfoItem> {
public:
  typedef common::ObFixedLengthString<common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH> Value;
  typedef common::ObDList<ObBackupInfoItem> ItemList;

  ObBackupInfoItem(ItemList& list, const char* name, const char* info);
  ObBackupInfoItem(const ObBackupInfoItem& item);
  ObBackupInfoItem();
  ObBackupInfoItem& operator=(const ObBackupInfoItem& item);

  bool is_valid() const
  {
    return NULL != name_;
  }
  TO_STRING_KV(K_(name), K_(value));
  const char* get_value_ptr() const
  {
    return value_.ptr();
  }
  int get_int_value(int64_t& value) const;
  int set_value(const int64_t value);
  int set_value(const char* buf);

  // update %value_
  int update(common::ObISQLClient& sql_client, const uint64_t tenant_id);
  // update %value_ will be rollback if transaction rollback or commit failed.
  int update(ObBackupItemTransUpdater& updater, const uint64_t tenant_id);
  // insert
  int insert(common::ObISQLClient& sql_client, const uint64_t tenant_id);

public:
  const char* name_;
  Value value_;
};

// Update item in transaction, if transaction rollback or commit failed the item value
// value will be rollback too.
class ObBackupItemTransUpdater {
public:
  ObBackupItemTransUpdater();
  ~ObBackupItemTransUpdater();

  int start(common::ObMySQLProxy& sql_proxy);
  int update(const uint64_t tenant_id, const ObBackupInfoItem& item);
  int load(const uint64_t tenant_id, ObBackupInfoItem& item, const bool need_lock = true);
  int end(const bool commit);
  common::ObMySQLTransaction& get_trans()
  {
    return trans_;
  }

private:
  const static int64_t PTR_OFFSET = sizeof(void*);

  bool started_;
  bool success_;
  common::ObMySQLTransaction trans_;
};

struct ObBaseBackupInfo {
public:
  ObBaseBackupInfo();
  ObBaseBackupInfo(const ObBaseBackupInfo& other);
  ObBaseBackupInfo& operator=(const ObBaseBackupInfo& other);
  void reset();
  bool is_valid() const;
  bool is_empty() const;
  DECLARE_TO_STRING;

  inline int64_t get_item_count() const
  {
    return list_.get_size();
  }
  int get_backup_info_status();

public:
  uint64_t tenant_id_;
  ObBackupInfoItem::ItemList list_;
  // base data backup info
  ObBackupInfoItem backup_dest_;
  ObBackupInfoItem backup_status_;
  ObBackupInfoItem backup_type_;
  ObBackupInfoItem backup_snapshot_version_;
  ObBackupInfoItem backup_schema_version_;
  ObBackupInfoItem backup_data_version_;
  ObBackupInfoItem backup_set_id_;
  ObBackupInfoItem incarnation_;
  ObBackupInfoItem backup_task_id_;
  // region info
  ObBackupInfoItem detected_backup_region_;
  ObBackupInfoItem backup_encryption_mode_;
  ObBackupInfoItem backup_passwd_;
};

class ObBackupInfoManager {
public:
  // friend class FakeZoneMgr;
  ObBackupInfoManager();
  virtual ~ObBackupInfoManager();
  void reset();
  int init(const common::ObIArray<uint64_t>& tenant_ids, common::ObMySQLProxy& proxy);
  int init(const uint64_t tenant_id, common::ObMySQLProxy& proxy);

  bool is_inited()
  {
    return inited_;
  }
  int get_tenant_count(int64_t& tenant_count) const;
  int get_backup_info(const uint64_t tenant_id, ObBaseBackupInfo& info);  // get backup info with tenant_id
  int get_backup_info(ObBaseBackupInfo& info);                            // get backup info with info
  int get_backup_info(const ObBackupInfoStatus::BackupStatus& status, common::ObIArray<ObBaseBackupInfo>& infos);
  int get_backup_info(const ObLogArchiveStatus::STATUS& status, common::ObIArray<ObBaseBackupInfo>& infos);
  int get_backup_info(common::ObIArray<ObBaseBackupInfo>& infos);
  int get_backup_info(common::ObIArray<ObBaseBackupInfoStruct>& infos);
  int get_backup_info(const uint64_t tenant_id, ObBackupItemTransUpdater& updater, ObBaseBackupInfoStruct& info);
  int update_backup_info(
      const uint64_t tenant_id, const ObBaseBackupInfoStruct& info, ObBackupItemTransUpdater& updater);
  int get_tenant_ids(common::ObIArray<uint64_t>& tenant_ids);
  int add_backup_info(const ObBaseBackupInfo& info);
  int add_backup_info(const uint64_t tenant_id);
  int cancel_backup();
  int check_can_update(const ObBaseBackupInfoStruct& src_info, const ObBaseBackupInfoStruct& dest_info);
  int get_backup_info_without_trans(const uint64_t tenant_id, ObBaseBackupInfoStruct& info);
  int get_detected_region(const uint64_t tenant_id, common::ObIArray<common::ObRegion>& detected_region);
  int get_backup_status(const uint64_t tenant_id, common::ObISQLClient& trans, ObBackupInfoStatus& status);
  int get_backup_scheduler_leader(
      const uint64_t tenant_id, common::ObISQLClient& trans, common::ObAddr& scheduler_leader, bool& has_leader);
  int update_backup_scheduler_leader(
      const uint64_t tenant_id, const common::ObAddr& scheduler_leader, common::ObISQLClient& trans);
  int clean_backup_scheduler_leader(const uint64_t tenant_id, const common::ObAddr& scheduler_leader);
  int insert_restore_tenant_base_backup_version(const uint64_t tenant_id, const int64_t major_version);
  int get_job_id(int64_t& job_id);
  int get_base_backup_version(const uint64_t tenant_id, common::ObISQLClient& trans, int64_t& base_backup_version);
  int is_backup_started(bool& is_started);
  int get_last_delete_expired_data_snapshot(
      const uint64_t tenant_id, common::ObISQLClient& trans, int64_t& last_delete_expired_data_snapshot);
  int update_last_delete_expired_data_snapshot(
      const uint64_t tenant_id, const int64_t last_delete_expired_data_snapshot, common::ObISQLClient& trans);

private:
  int convert_info_to_struct(const ObBaseBackupInfo& info, ObBaseBackupInfoStruct& info_struct);
  int convert_struct_to_info(const ObBaseBackupInfoStruct& info_struct, ObBaseBackupInfo& info);
  int find_tenant_id(const uint64_t tenant_id);
  int get_backup_info(const uint64_t tenant_id, ObBackupItemTransUpdater& updater, ObBaseBackupInfo& info);
  int update_backup_info(const uint64_t tenant_id, const ObBaseBackupInfo& info, ObBackupItemTransUpdater& updater);
  int check_can_update_(
      const ObBackupInfoStatus::BackupStatus& src_status, const ObBackupInfoStatus::BackupStatus& dest_status);

private:
  bool inited_;
  common::ObArray<uint64_t> tenant_ids_;
  common::ObMySQLProxy* proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupInfoManager);
};

struct ObBackupInfoSimpleItem {
  ObBackupInfoSimpleItem();
  ObBackupInfoSimpleItem(const char* name, const char* value);
  const char* name_;
  const char* value_;
  TO_STRING_KV(K_(name), K_(value));
};

class ObBackupInfoChecker final {
public:
  ObBackupInfoChecker();
  ~ObBackupInfoChecker();

  int init(common::ObMySQLProxy* sql_proxy);
  int check(const common::ObIArray<uint64_t>& tenant_ids, share::ObIBackupLeaseService& backup_lease_service);
  int check(const uint64_t tenant_id);

private:
  int get_item_count_(const uint64_t tenant_id, int64_t& item_count);
  int get_status_line_count_(const uint64_t tenant_id, int64_t& status_count);
  int get_new_items_(common::ObISQLClient& trans, const uint64_t tenant_id, const bool for_update,
      common::ObIArray<ObBackupInfoSimpleItem>& items);
  int insert_new_items_(
      common::ObMySQLTransaction& trans, const uint64_t tenant_id, common::ObIArray<ObBackupInfoSimpleItem>& items);
  int insert_new_item_(common::ObMySQLTransaction& trans, const uint64_t tenant_id, ObBackupInfoSimpleItem& item);
  int insert_log_archive_status_(common::ObMySQLTransaction& trans, const uint64_t tenant_id);

private:
  bool is_inited_;
  common::ObMySQLProxy* sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupInfoChecker);
};

}  // namespace share
}  // namespace oceanbase
#endif /* SRC_SHARE_BACKUP_OB_BACKUP_INFO_H_ */
