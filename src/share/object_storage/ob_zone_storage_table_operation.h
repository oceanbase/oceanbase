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

#ifndef OCEANBASE_SHARE_OBJECT_STORAGE_OB_ZONE_STORAGE_TABLE_OPERATION_H_
#define OCEANBASE_SHARE_OBJECT_STORAGE_OB_ZONE_STORAGE_TABLE_OPERATION_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/object_storage/ob_object_storage_struct.h"
namespace oceanbase
{
namespace share
{

class ObBackupDest;

class ObStorageInfoOperator
{
public:
  ObStorageInfoOperator() = default;
  virtual ~ObStorageInfoOperator() = default;
  static int insert_storage(common::ObISQLClient &proxy, const share::ObBackupDest &storage_dest,
                            const ObStorageUsedType::TYPE &used_for, const common::ObZone &zone,
                            const ObZoneStorageState::STATE op_type, const uint64_t storage_id,
                            const uint64_t op_id, const int64_t iops, const int64_t bandwidth);
  static int remove_storage_info(common::ObISQLClient &proxy, const common::ObZone &zone,
                                 const common::ObString &storage_path,
                                 const ObStorageUsedType::TYPE used_for);
  static int remove_storage_info(common::ObISQLClient &proxy, const common::ObZone &zone);
  static int update_storage_authorization(common::ObISQLClient &proxy, const common::ObZone &zone,
                                          const share::ObBackupDest &storage_dest,
                                          const uint64_t op_id,
                                          const ObStorageUsedType::TYPE used_for);
  static int update_storage_iops(common::ObISQLClient &proxy, const common::ObZone &zone,
                                 const share::ObBackupDest &storage_dest,
                                 const uint64_t op_id,
                                 const ObStorageUsedType::TYPE used_for,
                                 const int64_t max_iops);
  static int update_storage_bandwidth(common::ObISQLClient &proxy, const common::ObZone &zone,
                                      const share::ObBackupDest &storage_dest,
                                      const uint64_t op_id,
                                      const ObStorageUsedType::TYPE used_for,
                                      const int64_t max_bandwidth);
  static int update_storage_extension(common::ObISQLClient &proxy, const common::ObZone &zone,
                                      const share::ObBackupDest &storage_dest,
                                      const uint64_t op_id,
                                      const ObStorageUsedType::TYPE used_for,
                                      const char *extension);
  static int update_storage_state(common::ObISQLClient &proxy, const common::ObZone &zone,
                                  const share::ObBackupDest &storage_dest,
                                  const ObStorageUsedType::TYPE used_for, const uint64_t op_id,
                                  const ObZoneStorageState::STATE state);
  static int update_storage_state(common::ObISQLClient &proxy, const common::ObZone &zone,
                                  const common::ObString &storage_path,
                                  const ObStorageUsedType::TYPE used_for, const uint64_t op_id,
                                  const ObZoneStorageState::STATE state);
  static int update_storage_op_id(common::ObISQLClient &proxy, const common::ObZone &zone,
                                  const share::ObBackupDest &storage_dest,
                                  const ObStorageUsedType::TYPE used_for,
                                  const uint64_t old_op_id,
                                  const uint64_t op_id);
  static int update_storage_op_id(common::ObISQLClient &proxy, const common::ObZone &zone,
                                  const common::ObString &storage_path,
                                  const ObStorageUsedType::TYPE used_for,
                                  const uint64_t old_op_id,
                                  const uint64_t op_id);
  static int zone_storage_dest_exist(common::ObISQLClient &proxy, const common::ObZone &zone,
                                     const share::ObBackupDest &storage_dest,
                                     const ObStorageUsedType::TYPE used_for, bool &is_exist);
  static int update_last_check_time(common::ObISQLClient &proxy, const common::ObZone &zone,
                                    const share::ObBackupDest &storage_dest,
                                    const ObStorageUsedType::TYPE used_for,
                                    const int64_t last_check_time);
  static int get_zone_storage_table_info(common::ObISQLClient &proxy,
                                         ObArray<ObZoneStorageTableInfo> &storage_table_infos);
  static int get_zone_storage_table_dest(common::ObISQLClient &proxy,
                                         ObArray<ObStorageDestAttr> &dest_attrs);
  static int get_storage_state(common::ObISQLClient &proxy, const common::ObZone &zone,
                               const share::ObBackupDest &storage_dest,
                               const ObStorageUsedType::TYPE used_for,
                               ObZoneStorageState::STATE &state);
  static int get_storage_state(common::ObISQLClient &proxy, const common::ObZone &zone,
                               const common::ObString &storage_path,
                               const ObStorageUsedType::TYPE used_for,
                               ObZoneStorageState::STATE &state);
  static int get_storage_id(common::ObISQLClient &proxy, const common::ObZone &zone,
                            const common::ObString &storage_path,
                            const ObStorageUsedType::TYPE used_for, uint64_t &storage_id);
  static int fetch_new_storage_id(ObMySQLProxy &sql_proxy, uint64_t &new_storage_id);
  static int fetch_new_storage_op_id(ObMySQLProxy &sql_proxy, uint64_t &new_storage_op_id);
  // get ObZoneStorageTableInfo ordered by op_id from __all_zone_storage whose zone = @zone
  static int get_ordered_zone_storage_infos(
    common::ObISQLClient &proxy, const common::ObZone &zone,
    common::ObIArray<ObZoneStorageTableInfo> &storage_table_infos);
  // get ObZoneStorageTableInfo ordered by op_id from __all_zone_storage whose zone = @zone
  // and sub_op_id as max(sub_op_id) from __all_zone_storage_operation whose op_id=__all_zone_storage.op_id
  static int get_ordered_zone_storage_infos_with_sub_op_id(
    common::ObISQLClient &proxy, const common::ObZone &zone,
    common::ObIArray<ObZoneStorageTableInfo> &storage_table_infos);
  // get ObZoneStorageTableInfo from __all_zone_storage whose op_id = @op_id
  static int get_zone_storage_info(common::ObISQLClient &proxy, const uint64_t op_id,
                                   ObZoneStorageTableInfo &zone_storage_info);
  static int parse_storage_path(const char *storage_path, char *root_path, int64_t path_len,
                                char *endpoint, int64_t endpoint_len);
  static int select_for_update(common::ObMySQLTransaction &trans, const common::ObZone &zone);
  static int get_total_shared_data_size(int64_t &total_size);
  static int get_total_disk_size(int64_t &total_size);
  static int get_ls_total_disk_size(const uint64_t tenant_id, const int64_t ls_id, const common::ObAddr &server, int64_t &total_size);
  static int get_unit_data_disk_size(const uint64_t tenant_id, const common::ObAddr &server, int64_t &total_size);
  static int get_table_total_data_size(const uint64_t tenant_id, const common::ObAddr &server, int64_t &total_size);
  static int get_tmp_file_data_size(const uint64_t tenant_id, const common::ObAddr &server, int64_t &total_size);
  static int get_ls_leader_addr(const uint64_t tenant_id, const int64_t ls_id, common::ObAddr &server);
private:
  static int extract_storage_table_info(const sqlclient::ObMySQLResult &result,
      share::ObZoneStorageTableInfo &storage_table_info);
};

class ObStorageOperationOperator
{
public:
  ObStorageOperationOperator() = default;
  virtual ~ObStorageOperationOperator() = default;
  static int insert_storage_operation(common::ObISQLClient &proxy, const uint64_t storage_id,
                                      const uint64_t op_id, const uint64_t sub_op_id, const ObZone &zone,
                                      const ObZoneStorageState::STATE op_type, const char *op_info);
  // get ObZoneStorageOperationTableInfo with max(sub_op_id) from __all_zone_storage_operation
  // whose op_id = @op_id
  static int get_max_sub_op_info(ObISQLClient &proxy, const uint64_t op_id,
                                 ObZoneStorageOperationTableInfo &storage_op_info);
  // get ObZoneStorageOperationTableInfo with min(ob_op, sub_op_id) larger than (@ob_op, @sub_op_id)
  // from __all_zone_storage_operation whose zone = @zone
  static int get_min_op_info_greater_than(ObISQLClient &proxy, const common::ObZone &zone,
                                          const uint64_t last_op_id, const uint64_t last_sub_op_id,
                                          ObZoneStorageOperationTableInfo &storage_op_info);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_OBJECT_STORAGE_OB_ZONE_STORAGE_TABLE_OPERATION_H_ */