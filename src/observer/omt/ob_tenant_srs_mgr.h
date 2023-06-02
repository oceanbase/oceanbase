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

#ifndef OCEANBASE_OBSERVER_OMT_OB_TENANT_SRS_MGR_H_
#define OCEANBASE_OBSERVER_OMT_OB_TENANT_SRS_MGR_H_

#include "ob_tenant_srs.h"
#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/list/ob_list.h"
#include "lib/allocator/page_arena.h"
#include "lib/lock/ob_drw_lock.h"
#include "lib/task/ob_timer.h"
#include "lib/geo/ob_srs_wkt_parser.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace omt
{

class ObTenantSrsMgr;

class ObTenantSrsMgr
{
  template <class Key, class Value, int num>
  class ObSrsContainer
      : public common::hash::ObHashMap<Key, Value *, common::hash::NoPthreadDefendMode>
  {
  public:
    ObSrsContainer()
    {
      this->create(num, "TenantSrsMgr","TenantSrs");
    }
    virtual ~ObSrsContainer() {}

  private:
    DISALLOW_COPY_AND_ASSIGN(ObSrsContainer);
  };

class AddTenantTask : public common::ObTimerTask
{
public:
  AddTenantTask(ObTenantSrsMgr *tenant_srs_mgr)
    : tenant_srs_mgr_(tenant_srs_mgr) {}
  virtual ~AddTenantTask() {}
  AddTenantTask(const AddTenantTask &) = delete;
  AddTenantTask &operator=(const AddTenantTask &) = delete;
  void runTimerTask(void) override;
  int add_tenants_to_map(common::ObIArray<uint64_t> &latest_tenant_ids);

private:
  static const uint64_t DEFAULT_PERIOD = 5000000;
  static const uint64_t BOOTSTRAP_PERIOD = 500000;
  ObTenantSrsMgr *tenant_srs_mgr_;
};

class DeleteTenantTask : public common::ObTimerTask
{
public:
  DeleteTenantTask(ObTenantSrsMgr *tenant_srs_mgr)
    : tenant_srs_mgr_(tenant_srs_mgr) {}
  virtual ~DeleteTenantTask() {}
  DeleteTenantTask(const DeleteTenantTask &) = delete;
  DeleteTenantTask &operator=(const DeleteTenantTask &) = delete;
  void runTimerTask(void) override;

private:
  static const uint64_t DEFAULT_PERIOD = 5000000;
  ObTenantSrsMgr *tenant_srs_mgr_;
};

  friend ObTenantSrs;
public:
  using TenantSrsMap = ObSrsContainer<uint64_t, ObTenantSrs, common::OB_MAX_SERVER_TENANT_CNT>;

  virtual ~ObTenantSrsMgr();
  static ObTenantSrsMgr &get_instance();
  int init(common::ObMySQLProxy *sql_proxy, const common::ObAddr &server,
           share::schema::ObMultiVersionSchemaService *schema_service);
  int add_tenant_srs(const uint64_t tenant_id);
  int get_tenant_srs_guard(uint64_t tenant_id, ObSrsCacheGuard &srs_guard);
  void stop();
  void wait();
  void destroy();
  bool is_sys_load_completed() { return is_sys_load_completed_; }
  void set_sys_load_completed() { is_sys_load_completed_ = true; }
  bool is_sys_schema_ready();
  int get_srs_bounds(uint64_t srid, const common::ObSrsItem *srs_item,
                     const common::ObSrsBoundsItem *&bounds_item);

private:
  ObTenantSrsMgr();
  int refresh_tenant_srs(uint64_t tenant_id);
  int get_tenant_guard_inner(uint64_t tenant_id, ObSrsCacheGuard &srs_guard);
  int add_tenants_srs(const common::ObIArray<uint64_t> &new_tenants_id);
  int try_to_add_new_tenants();
  int remove_nonexist_tenants();
  int move_tenant_srs_to_nonexist_list(uint64_t tenant_id);
  int delete_nonexist_tenants_srs();

  common::ObArenaAllocator allocator_;
  common::ObMySQLProxy *sql_proxy_;
  common::ObAddr self_;
  // protect tenant_srs_map_
  common::DRWLock rwlock_;
  TenantSrsMap tenant_srs_map_;
  AddTenantTask add_tenant_task_;
  DeleteTenantTask  del_tenant_task_;
  bool is_inited_;
  bool is_sys_load_completed_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObList<ObTenantSrs *, common::ObArenaAllocator> nonexist_tenant_srs_;
  common::ObSrsBoundsItem infinite_plane_;

  DISALLOW_COPY_AND_ASSIGN(ObTenantSrsMgr);
};

}  // namespace omt
}  // namespace oceanbase

#define OTSRS_MGR (::oceanbase::omt::ObTenantSrsMgr::get_instance())

#endif
