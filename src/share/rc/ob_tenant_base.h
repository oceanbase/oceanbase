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

#ifndef OB_TENANT_BASE_H_
#define OB_TENANT_BASE_H_

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_context.h"
#include "share/ob_worker.h"

namespace oceanbase {
namespace common {
class ObLDHandle;
}
namespace omt {
class ObPxPools;
}
namespace obmysql {
class ObMySQLRequestManager;
}
namespace sql {
namespace dtl {
class ObTenantDfc;
}
class ObPxPoolStat;
class ObTenantSqlMemoryManager;
class ObPlanMonitorNodeList;
}  // namespace sql
namespace storage {
class ObTenantStorageInfo;
}
namespace transaction {
class ObTenantWeakReadService;  // Tenant weakly consistent read service
class ObTransAuditRecordMgr;    // Transaction monitoring table
}  // namespace transaction
namespace share {

// Here are the types of tenant local variables that need to be added, and the tenant will create an instance for each
// type. The initialization and destruction logic of the instance is specified by the MTL_BIND interface. Use the
// MTL_GET interface to get the instance.
#define MTL_MEMBERS                          \
  MTL_LIST(sql::dtl::ObTenantDfc*,           \
      omt::ObPxPools*,                       \
      sql::ObPxPoolStat*,                    \
      ObWorker::CompatMode,                  \
      obmysql::ObMySQLRequestManager*,       \
      transaction::ObTenantWeakReadService*, \
      storage::ObTenantStorageInfo*,         \
      transaction::ObTransAuditRecordMgr*,   \
      sql::ObTenantSqlMemoryManager*,        \
      sql::ObPlanMonitorNodeList*)

// Specify the initialization and destruction functions of a specific instance.
//
// For example, to bind the corresponding function of type omt::ObPxPools*, first define two global functions (also can
// be static functions)
//   int mtl_px_pool_init(omt::ObPxPools *&px_pool);
//   void mtl_px_pool_destroy(omt::ObPxPools *&px_pool);
// Then call the MTL_BIND(mtl_px_pool_init, mtl_px_pool_destroy) method to bind.
//
// Note that the MTL_BIND call needs to be before the tenant is created, otherwise the bound function cannot be called
// when the tenant is created.
#define MTL_BIND(INIT, DESTORY) share::ObTenantBase::mtl_bind_init_and_destory_func(INIT, DESTORY);

// Get a local instance of the tenant
//
// It needs to be used in conjunction with the tenant context to obtain a local instance of the tenant of the specified
// type. For example, MTL_GET(ObPxPools*) can get the PX pool of the current tenant.
#define MTL_GET(TYPE) share::ObTenantBase::mtl_get<TYPE>()

// Auxiliary function
#define MTL_LIST(...) __VA_ARGS__

class ObTenantSpace;
// Tenant classes exposed to each module, the interfaces that need to be exposed are placed here (tenant-level service,
// mgr, etc.)
class ObTenantBase {
  // When get_tenant, omt will add a read lock to the tenant.
  // ObTenantSpaceFetcher needs to be unlocked when it is destructed,
  // therefore, the unlock interface is exposed to ObTenantSpaceFetcher
  friend class ObTenantSpaceFetcher;
  friend class ObTableSpace;

  template <class T>
  struct Identity {};

public:
  explicit ObTenantBase(const int64_t id);

  int init();
  void destory();
  uint64_t id() const
  {
    return id_;
  }

  template <class T>
  T get()
  {
    return inner_get(Identity<T>());
  }

  template <class T>
  static T mtl_get()
  {
    T obj = T();
    ObTenantSpace& ctx = CURRENT_ENTITY(TENANT_SPACE).entity();
    if (ctx.get_tenant() != nullptr) {
      obj = ctx.get_tenant()->get<T>();
    }
    return obj;
  }

private:
  ObTenantBase();

#define MEMBER(TYPE, IDX)                                                         \
public:                                                                           \
  typedef int (*init_m##IDX##_func_name)(TYPE&);                                  \
  typedef void (*destory_m##IDX##_func_name)(TYPE&);                              \
  static void mtl_bind_init_and_destory_func(                                     \
      init_m##IDX##_func_name init_func, destory_m##IDX##_func_name destory_func) \
  {                                                                               \
    init_m##IDX##_func = init_func;                                               \
    destory_m##IDX##_func = destory_func;                                         \
  }                                                                               \
                                                                                  \
private:                                                                          \
  TYPE inner_get(Identity<TYPE>)                                                  \
  {                                                                               \
    return m##IDX##_;                                                             \
  }                                                                               \
  TYPE m##IDX##_;                                                                 \
  static init_m##IDX##_func_name init_m##IDX##_func;                              \
  static destory_m##IDX##_func_name destory_m##IDX##_func;

  LST_DO2(MEMBER, (), MTL_MEMBERS);

protected:
  virtual int unlock(common::ObLDHandle& handle) = 0;

protected:
  // tenant id
  const uint64_t id_;
  bool inited_;
};

}  // end of namespace share
}  // end of namespace oceanbase

#endif  // OB_TENANT_BASE_H_
