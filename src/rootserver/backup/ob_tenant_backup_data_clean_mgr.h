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

#ifndef OCEANBASE_ROOTSERVER_BACKUP_OB_TENANT_BACKUP_DATA_CLEAN_H_
#define OCEANBASE_ROOTSERVER_BACKUP_OB_TENANT_BACKUP_DATA_CLEAN_H_

#include "share/ob_define.h"
#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_manager.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/backup/ob_tenant_backup_clean_info_updater.h"
#include "share/backup/ob_extern_backup_info_mgr.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "archive/ob_archive_path.h"
#include "ob_backup_data_mgr.h"
#include "ob_backup_lease_service.h"

namespace oceanbase {
namespace common {
class ModulePageArena;
class ObServerConfig;
}  // namespace common
namespace share {
class ObPartitionInfo;
class ObPartitionTableOperator;
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObBackupDataClean;

enum ObBackupDataCleanMode { CLEAN = 0, TOUCH = 1, MAX };

struct ObBackupSetId {
  ObBackupSetId();
  virtual ~ObBackupSetId() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(backup_set_id), K_(clean_mode));
  int64_t backup_set_id_;
  ObBackupDataCleanMode clean_mode_;
};

struct ObLogArchiveRound {
  ObLogArchiveRound();
  virtual ~ObLogArchiveRound() = default;
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(log_archive_round), K_(log_archive_status), K_(start_ts), K_(checkpoint_ts));
  int64_t log_archive_round_;
  ObLogArchiveStatus::STATUS log_archive_status_;
  int64_t start_ts_;
  int64_t checkpoint_ts_;
};

struct ObBackupDataCleanElement {
  ObBackupDataCleanElement()
      : cluster_id_(OB_INVALID_CLUSTER_ID),
        incarnation_(0),
        backup_dest_(),
        backup_set_id_array_(),
        log_archive_round_array_()
  {}
  virtual ~ObBackupDataCleanElement()
  {}
  bool is_valid() const;
  void reset();
  bool is_same_element(
      const int64_t cluster_id, const int64_t incarnation, const share::ObBackupDest& backup_dest) const;
  int set_backup_set_id(const ObBackupSetId& backup_set_id);
  int set_log_archive_round(const ObLogArchiveRound& log_archive_round);

  TO_STRING_KV(K_(cluster_id), K_(incarnation), K_(backup_dest), K_(backup_set_id_array), K_(log_archive_round_array));
  int64_t cluster_id_;
  int64_t incarnation_;
  share::ObBackupDest backup_dest_;
  ObArray<ObBackupSetId> backup_set_id_array_;
  ObArray<ObLogArchiveRound> log_archive_round_array_;
};

struct ObSimpleBackupDataCleanTenant {
  ObSimpleBackupDataCleanTenant() : tenant_id_(OB_INVALID_ID), is_deleted_(false)
  {}
  virtual ~ObSimpleBackupDataCleanTenant()
  {}
  bool is_valid() const;
  void reset();
  uint64_t hash() const;

  TO_STRING_KV(K_(tenant_id), K_(is_deleted));
  uint64_t tenant_id_;
  bool is_deleted_;
};

struct ObBackupDataCleanTenant {
  ObBackupDataCleanTenant()
      : simple_clean_tenant_(), backup_element_array_(), clog_data_clean_point_(), clog_gc_snapshot_(0)
  {}
  virtual ~ObBackupDataCleanTenant()
  {}
  bool is_valid() const;
  void reset();
  int set_backup_clean_backup_set_id(const int64_t cluster_id, const int64_t incarnation,
      const share::ObBackupDest& backup_dest, const ObBackupSetId backup_set_id);
  int set_backup_clean_archive_round(const int64_t cluster_id, const int64_t incarnation,
      const share::ObBackupDest& backup_dest, const ObLogArchiveRound& archive_round);
  bool has_clean_backup_set(const int64_t backup_set_id) const;

  TO_STRING_KV(K_(simple_clean_tenant), K_(backup_element_array), K_(clog_data_clean_point), K_(clog_gc_snapshot));
  ObSimpleBackupDataCleanTenant simple_clean_tenant_;
  common::ObArray<ObBackupDataCleanElement> backup_element_array_;
  ObTenantBackupTaskInfo clog_data_clean_point_;
  int64_t clog_gc_snapshot_;
};

struct ObBackupDataCleanStatics {
  ObBackupDataCleanStatics();
  virtual ~ObBackupDataCleanStatics() = default;
  void reset();
  ObBackupDataCleanStatics& operator+=(const ObBackupDataCleanStatics& clean_statics);

  int64_t touched_base_data_files_;
  int64_t deleted_base_data_files_;
  int64_t touched_clog_files_;
  int64_t deleted_clog_files_;
  int64_t touched_base_data_files_ts_;
  int64_t deleted_base_data_files_ts_;
  int64_t touched_clog_files_ts_;
  int64_t deleted_clog_files_ts_;
  TO_STRING_KV(K_(touched_base_data_files), K_(deleted_base_data_files), K_(touched_clog_files), K_(deleted_clog_files),
      K_(touched_base_data_files_ts), K_(deleted_base_data_files_ts), K_(touched_clog_files_ts),
      K_(deleted_clog_files_ts));
};

class ObBackupDataCleanUtil {
public:
  static int get_backup_path_info(const ObBackupDest& backup_dest, const int64_t incarnation, const uint64_t tenant_id,
      const int64_t full_backup_set_id, const int64_t inc_backup_set_id, ObBackupBaseDataPathInfo& path_info);
  static int touch_backup_dir_files(const ObBackupPath& path, const char* storage_info,
      const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
      share::ObIBackupLeaseService& lease_service);
  static int delete_backup_dir_files(const ObBackupPath& path, const char* storage_info,
      const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
      share::ObIBackupLeaseService& lease_service);
  static int touch_clog_dir_files(const ObBackupPath& path, const char* storage_info, const uint64_t file_id,
      const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
      share::ObIBackupLeaseService& lease_service);
  static int delete_clog_dir_files(const ObBackupPath& path, const char* storage_info, const uint64_t file_id,
      const common::ObStorageType& device_type, ObBackupDataCleanStatics& clean_statics,
      share::ObIBackupLeaseService& lease_service);
  static int delete_backup_dir(
      const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type);
  static int delete_backup_file(
      const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type);
  static int touch_backup_file(
      const ObBackupPath& path, const char* storage_info, const common::ObStorageType& device_type);
  static int delete_tmp_files(const ObBackupPath& path, const char* storage_info);

private:
  static int check_can_delete(const common::ObStorageType& device_type, bool& can_delete);
  static int check_can_touch(const common::ObStorageType& device_type, bool& can_touch);
  static int get_file_id(const ObString& file_name, int64_t& file_id);
};

class ObTenantBackupDataCleanMgr {
public:
  ObTenantBackupDataCleanMgr();
  virtual ~ObTenantBackupDataCleanMgr();
  int init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean);
  int do_clean();

private:
  bool is_inited_;
  ObBackupDataCleanTenant clean_tenant_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupDataCleanMgr);
};

class ObTenantBackupBaseDataCleanTask {
public:
  ObTenantBackupBaseDataCleanTask();
  virtual ~ObTenantBackupBaseDataCleanTask();
  int init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean);
  int do_clean();
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int do_inner_clean(
      const ObSimpleBackupDataCleanTenant& simple_clean_tenant, const ObBackupDataCleanElement& clean_element);
  int clean_backup_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id);
  int get_tenant_backup_set_infos(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
      ObIArray<ObExternBackupSetInfo>& extern_backup_set_infos);
  int clean_backp_set(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
      const ObExternBackupSetInfo& extern_backup_set_info);

  int clean_backup_set_meta(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
      const ObExternBackupSetInfo& extern_backup_set_info);
  int touch_backup_set_meta(const ObBackupDataCleanElement& clean_element, const ObBackupPath& path);
  int delete_backup_set_meta(const ObBackupDataCleanElement& clean_element, const ObBackupPath& path);
  int get_table_id_list(
      const storage::ObPhyRestoreMetaIndexStore::MetaIndexMap& index_map, hash::ObHashSet<int64_t>& table_id_set);

  // clean backup set
  int try_clean_backup_set_dir(
      const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id);
  int try_clean_backup_set_info(
      const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id);
  int try_clean_backup_set_data_dir(
      const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id);
  int try_clean_full_backup_set_dir(
      const uint64_t tenant_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id);

private:
  bool is_inited_;
  ObBackupDataCleanTenant clean_tenant_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupBaseDataCleanTask);
};

class ObTenantBackupClogDataCleanTask {
public:
  ObTenantBackupClogDataCleanTask();
  virtual ~ObTenantBackupClogDataCleanTask();
  int init(const ObBackupDataCleanTenant& clean_tenant, ObBackupDataClean* data_clean);
  int do_clean();
  static int try_clean_table_clog_data_dir(const ObClusterBackupDest& backup_dest, const uint64_t tenant_id,
      const int64_t log_archive_round, const int64_t table_id, const char* storage_info,
      const common::ObStorageType& device_type);
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int do_inner_clean(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObTenantBackupTaskInfo& clog_data_clean_point);
  int clean_clog_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObTenantBackupTaskInfo& clog_data_clean_point,
      const ObLogArchiveRound& log_archive_round, const common::ObIArray<int64_t>& table_id_array,
      ObBackupDataMgr& backup_data_mgr);
  int do_clean_table_clog_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObTenantBackupTaskInfo& clog_data_clean_point,
      const ObLogArchiveRound& log_archive_round, const common::ObIArray<int64_t>& table_id,
      ObBackupDataMgr& backup_data_mgr);
  int set_partition_into_set(const common::ObIArray<ObBackupMetaIndex>& meta_index_array);
  int check_and_delete_clog_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& backup_clean_element, const int64_t clog_gc_snapshot);
  int check_and_delete_clog_data_with_round(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObClusterBackupDest& cluster_backup_dest, const ObLogArchiveRound& log_archive_round,
      const int64_t max_clean_clog_snapshot);
  int get_clog_pkey_list_not_in_base_data(const ObClusterBackupDest& cluster_backup_dest,
      const int64_t log_archive_round, const uint64_t tenant_id, common::ObIArray<ObPartitionKey>& pkey_list);
  int clean_interrputed_clog_data(const ObSimpleBackupDataCleanTenant& simple_clean_tenant,
      const ObBackupDataCleanElement& clean_element, const ObLogArchiveRound& log_archive_round);
  int try_clean_clog_data_dir(const ObClusterBackupDest& cluster_backup_dest, const uint64_t tenant_id,
      const int64_t log_archive_round, const char* storage_info, const common::ObStorageType& device_type);

private:
  static const int MAX_BUCKET_NUM = 2048;
  bool is_inited_;
  ObBackupDataCleanTenant clean_tenant_;
  hash::ObHashSet<common::ObPartitionKey> pkey_set_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantBackupClogDataCleanTask);
};

class ObTableBaseDataCleanMgr {
public:
  ObTableBaseDataCleanMgr();
  virtual ~ObTableBaseDataCleanMgr();
  int init(const int64_t table_id, const ObBackupDataCleanElement& clean_element, const ObBackupSetId& backup_set_id,
      const int64_t inc_backup_set_id, const common::ObIArray<ObBackupMetaIndex>& meta_index_array,
      ObBackupDataClean& data_clean);
  int do_clean();
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int clean_partition_backup_data(const ObBackupMetaIndex& meta_index);
  int try_clean_backup_table_dir();

private:
  bool is_inited_;
  int64_t table_id_;
  ObBackupDataCleanElement clean_element_;
  ObBackupSetId backup_set_id_;
  int64_t inc_backup_set_id_;
  common::ObArray<ObBackupMetaIndex> meta_index_array_;
  ObBackupBaseDataPathInfo path_info_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableBaseDataCleanMgr);
};

class ObTableClogDataCleanMgr {
public:
  ObTableClogDataCleanMgr();
  virtual ~ObTableClogDataCleanMgr();
  int init(const int64_t table_id, const ObBackupDataCleanElement& clean_element,
      const ObLogArchiveRound& log_archive_round, const ObTenantBackupTaskInfo& clog_data_clean_point,
      const common::ObIArray<ObBackupMetaIndex>& meta_index_array, ObBackupDataClean& data_clean);
  int do_clean();
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int clean_partition_clog_backup_data(const ObBackupMetaIndex& meta_index);
  int try_clean_backup_table_clog_dir();
  int get_partition_meta(const ObBackupMetaIndex& meta_index, storage::ObPartitionGroupMeta& pg_meta);

private:
  bool is_inited_;
  int64_t table_id_;
  ObBackupDataCleanElement clean_element_;
  ObLogArchiveRound log_archive_round_;
  ObTenantBackupTaskInfo clog_data_clean_point_;
  common::ObArray<ObBackupMetaIndex> meta_index_array_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableClogDataCleanMgr);
};

class ObPartitionClogDataCleanMgr {
public:
  ObPartitionClogDataCleanMgr();
  virtual ~ObPartitionClogDataCleanMgr();
  int init(const ObClusterBackupDest& cluster_backup_dest, const ObLogArchiveRound& log_archive_round,
      const ObPartitionKey& pkey, const uint64_t data_file_id, const uint64_t index_file_id,
      ObBackupDataClean& data_clean);
  int touch_clog_backup_data();
  int clean_clog_backup_data();
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int touch_clog_data_();
  int touch_clog_meta_();
  int clean_clog_data_();
  int clean_clog_meta_();
  int set_need_delete_clog_dir(
      const ObClusterBackupDest& cluster_backup_dest, const ObLogArchiveRound& log_arcvhie_round);

private:
  bool is_inited_;
  ObClusterBackupDest cluster_backup_dest_;
  ObLogArchiveRound log_archive_round_;
  ObPartitionKey pkey_;
  uint64_t data_file_id_;
  uint64_t index_file_id_;
  bool need_clean_dir_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;
};

class ObPartitionDataCleanMgr {
public:
  ObPartitionDataCleanMgr();
  virtual ~ObPartitionDataCleanMgr();
  int init(const ObPartitionKey& pkey, const ObBackupDataCleanElement& clean_element,
      const ObBackupSetId& backup_set_id, const int64_t inc_backup_set_id, ObBackupDataClean& data_clean);
  int do_clean();
  int get_clean_statics(ObBackupDataCleanStatics& clean_statics);

private:
  int touch_backup_data();
  int clean_backup_data();

private:
  bool is_inited_;
  ObPartitionKey pkey_;
  ObBackupSetId backup_set_id_;
  int64_t inc_backup_set_id_;
  ObBackupBaseDataPathInfo path_info_;
  ObBackupDataCleanStatics clean_statics_;
  ObBackupDataClean* data_clean_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionDataCleanMgr);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_BACKUP_OB_TENANT_BACKUP_DATA_CLEAN_H_
