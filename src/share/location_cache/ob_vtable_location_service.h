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

#ifndef OCEANBASE_SHARE_OB_VTABLE_LOCATION_SERVICE
#define OCEANBASE_SHARE_OB_VTABLE_LOCATION_SERVICE

#include "share/partition_table/ob_partition_location.h" // ObPartitionLocation
#include "share/location_cache/ob_location_struct.h" // ObVTableLocationCacheKey/Value
#include "share/location_cache/ob_location_update_task.h" // ObVTableLocUpdateTask

namespace oceanbase
{
namespace obrpc
{
class ObCommonRpcProxy;
}

namespace share
{
class ObRsMgr;
class ObIAliveServerTracer;

// ObVtableLocationType is divided by distributed execution method
class ObVtableLocationType
{
  OB_UNIS_VERSION(1);
public:
  enum ObVtableType
  {
    INVALID_TYPE = 0,
    ONLY_LOCAL,
    ONLY_RS,
    TENANT_DISTRIBUTED,
    CLUSTER_DISTRIBUTED
  };
  ObVtableLocationType() : tenant_id_(OB_INVALID_TENANT_ID), type_(INVALID_TYPE) {}
  virtual ~ObVtableLocationType() {}
  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    type_ = INVALID_TYPE;
  }
  bool is_valid() const
  {
    return is_valid_tenant_id(tenant_id_)
        && type_ != INVALID_TYPE
        && type_ <= CLUSTER_DISTRIBUTED;
  }
  bool is_only_local() const { return ONLY_LOCAL == type_; }
  bool is_only_rs() const { return ONLY_RS == type_; }
  bool is_tenant_distributed() const { return TENANT_DISTRIBUTED == type_; }
  bool is_cluster_distributed() const { return CLUSTER_DISTRIBUTED == type_; }
  int gen_by_tenant_id_and_table_id(const uint64_t tenant_id, const uint64_t table_id);
  uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id), K_(type));
private:
  uint64_t tenant_id_;
  ObVtableType type_;
};

// ObVTableLocationService is used to get location for virtual table.
class ObVTableLocationService
{
public:
  ObVTableLocationService();
  virtual ~ObVTableLocationService() {}
  int init(ObRsMgr &rs_mgr);
  int vtable_get(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const int64_t expire_renew_time,
      bool &is_cache_hit,
      common::ObIArray<common::ObAddr> &locations);
  int vtable_nonblock_renew(
      const uint64_t tenant_id,
      const uint64_t table_id);
  int add_update_task(const ObVTableLocUpdateTask &task);
  int batch_process_tasks(
      const common::ObIArray<ObVTableLocUpdateTask> &tasks,
      bool &stopped);
  int process_barrier(const ObVTableLocUpdateTask &task, bool &stopped);
  int check_inner_stat_() const;
  void stop();
  void wait();
  int destroy();
  int reload_config();
private:
  /*
    get cached information,
    rs machine location information is obtained from rs_mgr_;
    cluster machine location information is obtained from all_server_tracer;
    tenant machine location information is obtained from all_server_tracer;

    @param[in] tenant_id:      Tenant for virtual table.
    @param[in] table_id:       Virtual table id.
    @param[out] locations:     Array of virtual table locations.
    @param[out] renew_time:    Refresh time of table locations.
    @return
      - OB_SUCCESS:             successfully
      - OB_ENTRY_NOT_EXIST      cache lacks the required information.
      - other:                  other failures
  */
  int get_from_vtable_cache_(
      const uint64_t tenant_id,
      const uint64_t table_id,
      common::ObIArray<common::ObAddr> &locations,
      int64_t &renew_time) const;
  int renew_vtable_location_(
      const uint64_t tenant_id,
      const uint64_t table_id,
      common::ObIArray<common::ObAddr> &locations);
  int get_rs_locations_(common::ObIArray<common::ObAddr> &server, int64_t &renew_time) const;
  int get_cluster_locations_(common::ObIArray<common::ObAddr> &servers, int64_t &renew_time) const;
  int get_tenant_locations_(const uint64_t tenant_id, common::ObIArray<common::ObAddr> &servers, int64_t &renew_time) const;

  typedef observer::ObUniqTaskQueue<ObVTableLocUpdateTask,
    ObVTableLocationService> ObVTableLocUpdateQueue;

  int inited_;
  ObVTableLocUpdateQueue update_queue_;
  ObRsMgr *rs_mgr_;
};

} // end namespace share
} // end namespace oceanbase
#endif
