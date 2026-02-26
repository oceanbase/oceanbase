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

#ifndef OCEANBASE_ROOTSERVER_OB_STORAGE_MANAGER_H_
#define OCEANBASE_ROOTSERVER_OB_STORAGE_MANAGER_H_

#include "lib/container/ob_iarray.h"
#include "lib/thread/ob_async_task_queue.h"
#include "share/ob_cluster_role.h"
#include "share/ob_iserver_trace.h"
#include "share/object_storage/ob_object_storage_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "src/share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace obrpc
{
class ObAdminStorageArg;
class ObSrvRpcProxy;
class ObCommonRpcProxy;
} // namespace obrpc
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObBackupDest;
}
namespace rootserver
{
class ObZoneStorageManagerBase
{
public:
  ObZoneStorageManagerBase();
  virtual ~ObZoneStorageManagerBase();
  void reset_zone_storage_infos();
  int init(common::ObMySQLProxy &proxy, obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  bool is_inited() { return inited_; }
  bool is_reload() { return loaded_; }
  virtual int check_inner_stat() const;
  virtual int get_zone_storage_count(int64_t &zone_storage_count) const;
  virtual int add_storage(const common::ObString &storage_path, const common::ObString &access_info,
                          const common::ObString &attribute, const share::ObStorageUsedType::TYPE &use_for,
                          const common::ObZone &zone, const bool &wait_type, common::ObMySQLTransaction &trans);
  virtual int drop_storage(const common::ObString &storage_path,
                           const share::ObStorageUsedType::TYPE &use_for,
                           const common::ObZone &zone, const bool &force_type,
                           const bool &wait_type);
  virtual int alter_storage(const common::ObString &storage_path, const common::ObString &access_info,
                            const common::ObString &attribute, const bool &wait_type);
  virtual int reload();
  virtual int check_storage_operation_state();
  virtual int get_zone_storage_with_zone(const common::ObZone &zone, const share::ObStorageUsedType::TYPE used_for,
                                         share::ObBackupDest &storage_dest, bool &is_exist);    // get zone storage with zone_name and used_for_type
  virtual int check_zone_storage_with_region_scope(const common::ObRegion &region, const share::ObStorageUsedType::TYPE used_for,
                                                   const share::ObBackupDest &storage_dest, common::ObMySQLTransaction &trans);    // get zone storage with region_name and used_for_type

  virtual int get_storage_infos_by_zone(const ObZone &zone,
      ObIArray<share::ObZoneStorageTableInfo> &storage_infos) const;
  virtual int check_zone_storage_exist(const ObZone &zone, bool &storage_exist) const;
private:
  int add_storage_operation(const share::ObBackupDest &storage_dest,
                            const share::ObStorageUsedType::TYPE &used_for,
                            const common::ObZone &zone, const bool &wait_type,
                            const int64_t max_iops, const int64_t max_bandwidth,
                            common::ObMySQLTransaction &trans);
  int drop_storage_operation(const common::ObString &storage_path,
                             const share::ObStorageUsedType::TYPE &use_for,
                             const common::ObZone &zone, const bool &wait_type);
  int alter_storage_authorization(const share::ObBackupDest &storage_dest, const bool &wait_type);
  int alter_storage_attribute(const common::ObString &storage_path, const bool &wait_type,
                              const int64_t max_iops, const int64_t max_bandwidth, const ObStorageChecksumType &checksum_type);
  int check_zone_storage_exist(const common::ObZone &zone, const share::ObBackupDest &storage_dest,
                               const share::ObStorageUsedType::TYPE used_for, int64_t &idx) const;
  int check_zone_storage_exist(const common::ObZone &zone, const ObString &storage_path,
                               const share::ObStorageUsedType::TYPE used_for, int64_t &idx) const;
  int check_add_storage_access_info_equal(const share::ObBackupDest &storage_dest) const;
  int get_zone_storage_list_by_zone(const common::ObZone &zone,
                                    common::ObArray<int64_t> &drop_zone_storage_list) const;
  int update_zone_storage_table_state(const int64_t idx);
  int parse_attribute_str(const common::ObString &attribute, int64_t &max_iops, int64_t &max_bandwidth, ObStorageChecksumType &checksum_type);
  int check_need_fetch_storage_id(const share::ObBackupDest &storage_dest, bool &is_need_fetch, uint64_t &storage_id);

protected:
  common::SpinRWLock lock_;
  static int copy_infos(ObZoneStorageManagerBase &dest, const ObZoneStorageManagerBase &src);

private:
  bool inited_;
  bool loaded_;
  common::ObArray<share::ObZoneStorageTableInfo> zone_storage_infos_;
  common::ObMySQLProxy *proxy_;
  obrpc::ObSrvRpcProxy *srv_rpc_proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneStorageManagerBase);
};

class ObZoneStorageManager : public ObZoneStorageManagerBase
{
public:
  ObZoneStorageManager();
  virtual ~ObZoneStorageManager();
  int init(common::ObMySQLProxy &proxy, obrpc::ObSrvRpcProxy &srv_rpc_proxy);
  virtual int reload();

  virtual int add_storage(const common::ObString &storage_path, const common::ObString &access_info,
                          const common::ObString &attribute, const share::ObStorageUsedType::TYPE &use_for,
                          const common::ObZone &zone, const bool &wait_type, common::ObMySQLTransaction &trans);
  virtual int drop_storage(const common::ObString &storage_path,
                           const share::ObStorageUsedType::TYPE &use_for,
                           const common::ObZone &zone, const bool &force_type,
                           const bool &wait_type);
  virtual int alter_storage(const common::ObString &storage_path, const common::ObString &access_info,
                            const common::ObString &attribute, const bool &wait_type);
  virtual int check_storage_operation_state();
  virtual int get_zone_storage_with_zone(const common::ObZone &zone, const share::ObStorageUsedType::TYPE used_for,
                                         share::ObBackupDest &storage_dest, bool &is_exist);    // get zone storage with zone_name and used_for_type
  virtual int check_zone_storage_exist(const ObZone &zone, bool &storage_exist);

public:
  class ObZoneStorageManagerShadowGuard
  {
  public:
    ObZoneStorageManagerShadowGuard(const common::SpinRWLock &lock,
                                    ObZoneStorageManagerBase &storage_mgr,
                                    ObZoneStorageManagerBase &shadow, int &ret);
    ~ObZoneStorageManagerShadowGuard();

  private:
    common::SpinRWLock &lock_;
    ObZoneStorageManagerBase &storage_mgr_;
    ObZoneStorageManagerBase &shadow_;
    int &ret_;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObZoneStorageManagerShadowGuard);
  };

private:
  common::SpinRWLock write_lock_;
  ObZoneStorageManagerBase shadow_;
  // this proxy is not inited and
  // used to prevent somebody from trying to directly use the set-interfaces in
  // ObZoneStorageManagerBase
  common::ObMySQLProxy uninit_proxy_;
  obrpc::ObSrvRpcProxy uninit_rpc_proxy_;
};

} // namespace rootserver
} // namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_STORAGE_MANAGER_H_
