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

#ifndef OCEANBASE_SHARE_OB_MAX_ID_FETCHER_H_
#define OCEANBASE_SHARE_OB_MAX_ID_FETCHER_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObISQLClient;
class ObMySQLProxy;
class ObString;
}
namespace share
{
// represent the different max_used_xxx_id type in __all_sys_stat table
enum ObMaxIdType
{
  OB_MAX_USED_TENANT_ID_TYPE,
  OB_MAX_USED_UNIT_CONFIG_ID_TYPE,
  OB_MAX_USED_UNIT_ID_TYPE,
  OB_MAX_USED_RESOURCE_POOL_ID_TYPE,
  OB_MAX_USED_SERVER_ID_TYPE,
  OB_MAX_USED_DDL_TASK_ID_TYPE,
  OB_MAX_USED_UNIT_GROUP_ID_TYPE,
  OB_MAX_USED_NORMAL_ROWID_TABLE_TABLET_ID_TYPE, /* used for tablet_id */
  OB_MAX_USED_EXTENDED_ROWID_TABLE_TABLET_ID_TYPE,     /* used for tablet_id */
  OB_MAX_USED_LS_ID_TYPE,
  OB_MAX_USED_LS_GROUP_ID_TYPE,
  OB_MAX_USED_SYS_PL_OBJECT_ID_TYPE, /* used for sys package object id */
  OB_MAX_USED_OBJECT_ID_TYPE,        /* used for all kinds of user schema objects */
  OB_MAX_USED_LOCK_OWNER_ID_TYPE,
  OB_MAX_USED_REWRITE_RULE_VERSION_TYPE,
  OB_MAX_USED_TTL_TASK_ID_TYPE,

  /* the following ObMaxIdType will be changed to OB_MAX_USED_OBJECT_ID_TYPE and won't be persisted. */
  OB_MAX_USED_TABLE_ID_TYPE,
  OB_MAX_USED_DATABASE_ID_TYPE,
  OB_MAX_USED_USER_ID_TYPE,
  OB_MAX_USED_TABLEGROUP_ID_TYPE,
  OB_MAX_USED_SEQUENCE_ID_TYPE,
  OB_MAX_USED_OUTLINE_ID_TYPE,
  OB_MAX_USED_CONSTRAINT_ID_TYPE,
  OB_MAX_USED_SYNONYM_ID_TYPE,
  OB_MAX_USED_UDF_ID_TYPE,
  OB_MAX_USED_UDT_ID_TYPE,
  OB_MAX_USED_ROUTINE_ID_TYPE,
  OB_MAX_USED_PACKAGE_ID_TYPE,
  OB_MAX_USED_KEYSTORE_ID_TYPE,
  OB_MAX_USED_MASTER_KEY_ID_TYPE,
  OB_MAX_USED_LABEL_SE_POLICY_ID_TYPE,
  OB_MAX_USED_LABEL_SE_COMPONENT_ID_TYPE,
  OB_MAX_USED_LABEL_SE_LABEL_ID_TYPE,
  OB_MAX_USED_LABEL_SE_USER_LEVEL_ID_TYPE,
  OB_MAX_USED_TABLESPACE_ID_TYPE,
  OB_MAX_USED_TRIGGER_ID_TYPE,
  OB_MAX_USED_PROFILE_ID_TYPE,
  OB_MAX_USED_AUDIT_ID_TYPE,
  OB_MAX_USED_DBLINK_ID_TYPE,
  OB_MAX_USED_DIRECTORY_ID_TYPE,
  OB_MAX_USED_CONTEXT_ID_TYPE,
  OB_MAX_USED_PARTITION_ID_TYPE,
  OB_MAX_USED_RLS_POLICY_ID_TYPE,
  OB_MAX_USED_RLS_GROUP_ID_TYPE,
  OB_MAX_USED_RLS_CONTEXT_ID_TYPE,
  OB_MAX_USED_SERVICE_NAME_ID_TYPE,
  OB_MAX_ID_TYPE,
};

class ObMaxIdFetcher
{
public:
  explicit ObMaxIdFetcher(common::ObMySQLProxy &proxy);
  explicit ObMaxIdFetcher(common::ObMySQLProxy &proxy, const int32_t group_id);
  virtual ~ObMaxIdFetcher();

  // For generate new object_ids
  int fetch_new_max_id(const uint64_t tenant_id, ObMaxIdType id_type,
                       uint64_t &max_id, const uint64_t initial = UINT64_MAX,
                       const int64_t size = 1);
  // For generate new tablet_ids
  int fetch_new_max_ids(const uint64_t tenant_id, ObMaxIdType id_type,
                        uint64_t &max_id, uint64_t size);
  int update_max_id(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                    ObMaxIdType max_id_type, const uint64_t max_id);
  // return OB_ENTRY_NOT_EXIST for %id_type not exist in __all_sys_table
  static int fetch_max_id(common::ObISQLClient &sql_client, const uint64_t tenant_id,
                   ObMaxIdType id_type, uint64_t &max_id);
  static const char *get_max_id_name(const ObMaxIdType max_id_type);
  static const char *get_max_id_info(const ObMaxIdType max_id_type);
  static int str_to_uint(const common::ObString &str, uint64_t &value);
private:
  static bool valid_max_id_type(const ObMaxIdType max_id_type)
  { return max_id_type >= 0 && max_id_type < OB_MAX_ID_TYPE; }
  // insert ignore into __all_sys_stat table
  int insert_initial_value(common::ObISQLClient &sql_client, uint64_t tenant_id,
      ObMaxIdType max_id_type, const uint64_t initial_value);

private:
  static const char *max_id_name_info_[OB_MAX_ID_TYPE][2];
  int convert_id_type(const ObMaxIdType &src, ObMaxIdType &dst);

  static const int64_t MAX_TENANT_MUTEX_BUCKET_CNT = 4096;
private:
  common::ObMySQLProxy &proxy_;
  static lib::ObMutex mutex_bucket_[MAX_TENANT_MUTEX_BUCKET_CNT];
  int32_t group_id_;

  DISALLOW_COPY_AND_ASSIGN(ObMaxIdFetcher);
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_MAX_ID_FETCHER_H_
