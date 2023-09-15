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

#ifndef __OB_PHYSICAL_RESTORE_INFO_H__
#define __OB_PHYSICAL_RESTORE_INFO_H__

#include "share/backup/ob_backup_struct.h"
#include "share/restore/ob_restore_type.h"//ObRestoreType
#include "share/ob_tenant_info_proxy.h"//ObTenantRole
#include "share/restore/ob_restore_persist_helper.h"//ObRestoreJobPersistKey

namespace oceanbase
{
namespace common
{
  class ObMySQLProxy;
}
namespace obrpc
{
struct ObTableItem;
}
namespace share
{
enum PhysicalRestoreMod
{
  PHYSICAL_RESTORE_MOD_RS = 0,
  PHYSICAL_RESTORE_MOD_CLOG = 1,
  PHYSICAL_RESTORE_MOD_STORAGE = 2,
  PHYSICAL_RESTORE_MOD_MAX_NUM
};

/* physical restore related */
enum PhysicalRestoreStatus
{
  PHYSICAL_RESTORE_CREATE_TENANT = 0,          // restore tenant schema
  PHYSICAL_RESTORE_PRE = 1,                    // set parameters
  PHYSICAL_RESTORE_CREATE_INIT_LS = 2,         // create init ls
  PHYSICAL_RESTORE_WAIT_CONSISTENT_SCN = 3,    // wait clog recover to consistent scn
  PHYSICAL_RESTORE_WAIT_LS = 4,                // check all ls restore finish and sync ts is restore
  PHYSICAL_RESTORE_POST_CHECK = 5,             // check tenant is in restore, set tenant to normal
  PHYSICAL_RESTORE_UPGRADE = 6,                // upgrade post
  PHYSICAL_RESTORE_SUCCESS = 7,                // restore success
  PHYSICAL_RESTORE_FAIL = 8,                   // restore fail
  PHYSICAL_RESTORE_WAIT_TENANT_RESTORE_FINISH = 9, //sys tenant wait user tenant restore finish
  PHYSICAL_RESTORE_MAX_STATUS
};

class ObPhysicalRestoreWhiteList
{
public:
  ObPhysicalRestoreWhiteList();
  virtual ~ObPhysicalRestoreWhiteList();

  int assign(const ObPhysicalRestoreWhiteList &other);
  // str without '\0'
  int assign_with_hex_str(const common::ObString &str);
  void reset();

  int add_table_item(const obrpc::ObTableItem &item);
  // length without '\0'
  int64_t get_format_str_length() const;
  // str without '\0'
  int get_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  // str without '\0'
  int get_hex_str(common::ObIAllocator &allocator, common::ObString &str) const;
  const common::ObSArray<obrpc::ObTableItem> &get_table_white_list() const { return table_items_; }
  DECLARE_TO_STRING;
private:
  ObArenaAllocator allocator_;
  common::ObSArray<obrpc::ObTableItem> table_items_;
};

struct ObSimplePhysicalRestoreJob;
#define Property_declare_int(variable_type, variable_name)\
private:\
  variable_type variable_name##_;\
public:\
  variable_type get_##variable_name() const\
  { return variable_name##_;}\
  void set_##variable_name(variable_type other)\
  { variable_name##_ = other;}

#define Property_declare_ObString(variable_name)\
private:\
  ObString variable_name##_;\
public:\
  const ObString &get_##variable_name() const\
  { return variable_name##_;}\
  int set_##variable_name(const ObString &str)\
  { return deep_copy_ob_string(allocator_, str, variable_name##_);}\


struct ObPhysicalRestoreJob final
{
public:
  ObPhysicalRestoreJob();
  ~ObPhysicalRestoreJob() {}
  int assign(const ObPhysicalRestoreJob &other);
  int copy_to(ObSimplePhysicalRestoreJob &restore_info) const;
  bool is_valid() const;
  void reset();

  DECLARE_TO_STRING;
  
  ObPhysicalRestoreBackupDestList& get_multi_restore_path_list()
  {
    return multi_restore_path_list_;
  }
  const ObPhysicalRestoreBackupDestList& get_multi_restore_path_list() const
  {
    return multi_restore_path_list_;
  }

  ObPhysicalRestoreWhiteList& get_white_list()
  {
    return white_list_;
  }
  const ObPhysicalRestoreWhiteList& get_white_list() const
  {
    return white_list_;
  }
  int init_restore_key(const uint64_t tenant_id, const int64_t job_id);
  const ObRestoreJobPersistKey& get_restore_key() const
  {
    return restore_key_;
  }
  int64_t get_job_id() const
  {
    return restore_key_.job_id_;
  } 
  bool is_valid_status_to_recovery() const
  {
    return PHYSICAL_RESTORE_CREATE_INIT_LS != status_
          && PHYSICAL_RESTORE_FAIL != status_
          && PHYSICAL_RESTORE_PRE != status_;
  }

  //job_id in other tenant
  Property_declare_int(int64_t,  initiator_job_id)
  Property_declare_int(uint64_t,  initiator_tenant_id)
//restore tenant, in sys tenant, tenant_id is user_tenant
//in meta tenant, tenant_id is equal to tenant_id of restore_key
  Property_declare_ObString(backup_tenant_name)
  Property_declare_ObString(backup_cluster_name)
  Property_declare_int(uint64_t, tenant_id)
  Property_declare_int(uint64_t, backup_tenant_id)
  Property_declare_int(ObRestoreType, restore_type)

  Property_declare_int(PhysicalRestoreStatus, status)

  Property_declare_ObString(comment)

  Property_declare_int(int64_t, restore_start_ts)
  Property_declare_int(share::SCN, restore_scn)
  Property_declare_int(share::SCN, consistent_scn)
  Property_declare_int(uint64_t, post_data_version)
  Property_declare_int(uint64_t, source_cluster_version)
  Property_declare_int(uint64_t, source_data_version)
  //from cmd
  Property_declare_ObString(restore_option)
  Property_declare_ObString(backup_dest)
  Property_declare_ObString(description)

  //for create tenant from cmd
  Property_declare_ObString(tenant_name)
  Property_declare_ObString(pool_list)
  Property_declare_ObString(locality)
  Property_declare_ObString(primary_zone)
  Property_declare_int(lib::Worker::CompatMode, compat_mode)
  Property_declare_int(int64_t, compatible)

  //for kms
  Property_declare_ObString(kms_info)
  Property_declare_int(bool, kms_encrypt)
  Property_declare_ObString(encrypt_key)
  Property_declare_ObString(kms_dest)
  Property_declare_ObString(kms_encrypt_key)
  Property_declare_ObString(passwd_array)
  Property_declare_int(int64_t, concurrency)
  Property_declare_int(bool, recover_table)

private:
  //job_id and tenant_id in __all_restore_job primary_key
  ObRestoreJobPersistKey restore_key_;
  //for restore
  ObPhysicalRestoreBackupDestList multi_restore_path_list_;
  //for table recovery
  ObPhysicalRestoreWhiteList white_list_;
  //after recovery, restore will change to standby_tenant or primary tenant
  ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreJob);
};
/* physical restore related end */


struct ObSimplePhysicalRestoreJob final
{
public:
  ObPhysicalRestoreInfo restore_info_;
  int64_t restore_data_version_;
  int64_t snapshot_version_;
  int64_t schema_version_;
  int64_t job_id_;

  ObSimplePhysicalRestoreJob();
  virtual ~ObSimplePhysicalRestoreJob() = default;
  bool is_valid() const;
  int assign(const ObSimplePhysicalRestoreJob &other);
  int copy_to(share::ObPhysicalRestoreInfo &restore_info) const;
  TO_STRING_KV(K_(restore_info), K_(restore_data_version), K_(snapshot_version), K_(schema_version), K_(job_id));
  DISALLOW_COPY_AND_ASSIGN(ObSimplePhysicalRestoreJob);
};

struct ObRestoreProgressInfo {
public:
  ObRestoreProgressInfo() { reset(); }
  ~ObRestoreProgressInfo() {}
  void reset();
  TO_STRING_KV(K_(total_pg_cnt), K_(finish_pg_cnt), K_(total_partition_cnt), K_(finish_partition_cnt));
public:
  int64_t total_pg_cnt_;         // standalone pg cnt
  int64_t finish_pg_cnt_;
  int64_t total_partition_cnt_;  // partition cnt (sql view)
  int64_t finish_partition_cnt_;
};
}
}
#endif /* __OB_PHYSICAL_RESTORE_INFO_H__ */
//// end of header file


