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

#ifndef _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_H_
#define _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_H_

#include <functional>
#include "lib/container/ob_vector.h"
#include "ob_worker_pool.h"
#include "ob_token_calcer.h"
#include "ob_tenant_node_balancer.h"

namespace oceanbase {

namespace common {
class ObMySQLProxy;
class ObServerConfig;
}  // namespace common

namespace rpc {
class ObRequest;
}

namespace omt {

struct ObCtxMemConfig {
  ObCtxMemConfig() : ctx_id_(0), idle_size_(0)
  {}
  uint64_t ctx_id_;
  int64_t idle_size_;
  TO_STRING_KV(K_(ctx_id), K_(idle_size));
};

class ObICtxMemConfigGetter {
public:
  virtual int get(common::ObIArray<ObCtxMemConfig>& configs) = 0;
};

class ObCtxMemConfigGetter : public ObICtxMemConfigGetter {
public:
  virtual int get(common::ObIArray<ObCtxMemConfig>& configs);
};

// Forward declearation
class ObTenant;

// Type alias
typedef common::ObSortedVector<ObTenant*> TenantList;
typedef TenantList::iterator TenantIterator;
typedef common::ObVector<uint64_t> TenantIdList;

// This is the entry class of OMT module.
class ObMultiTenant : public share::ObThreadPool {
public:
  const static int64_t DEFAULT_TIMES_OF_WORKERS = 10;
  const static int64_t TIME_SLICE_PERIOD = 10000;
  constexpr static double DEFAULT_NODE_QUOTA = 16.;
  constexpr static double DEFAULT_QUOTA2THREAD = 2.;

public:
  explicit ObMultiTenant(ObIWorkerProcessor& procor);

  int init(common::ObAddr myaddr, double node_quota = DEFAULT_NODE_QUOTA,
      int64_t times_of_workers = DEFAULT_TIMES_OF_WORKERS, common::ObMySQLProxy* sql_proxy = NULL);

  int start();
  void stop();
  void wait();
  void destroy();

  int add_tenant(const uint64_t tenant_id, const double min_cpu, const double max_cpu);
  int del_tenant(const uint64_t tenant_id, const bool wait = true);
  int modify_tenant(const uint64_t tenant_id, const double min_cpu, const double max_cpu);
  int get_tenant(const uint64_t tenant_id, ObTenant*& tenant) const;
  int get_tenant_with_tenant_lock(const uint64_t tenant_id, common::ObLDHandle& handle, ObTenant*& tenant) const;
  int update_tenant(uint64_t tenant_id, std::function<int(ObTenant&)>&& func);
  int recv_request(const uint64_t tenant_id, rpc::ObRequest& req);

  inline TenantList& get_tenant_list();
  int for_each(std::function<int(ObTenant&)> func);
  void get_tenant_ids(TenantIdList& id_list);

  inline double get_node_quota() const;
  inline void set_quota2token(double num);
  inline double get_quota2token() const;
  inline double get_token2quota() const;
  inline double get_attenuation_factor() const;
  inline int64_t get_times_of_workers() const;
  int get_tenant_cpu_usage(const uint64_t tenant_id, double& usage) const;
  int get_tenant_cpu(const uint64_t tenant_id, double& min_cpu, double& max_cpu) const;

  inline int lock_tenant_list(bool write = false);
  inline int unlock_tenant_list(bool write = false);

  bool has_tenant(uint64_t tenant_id) const;

  inline void set_cpu_dump();
  inline void unset_cpu_dump();

  inline void set_synced();
  inline bool has_synced() const;

  void set_workers_per_cpu(int64_t v);

protected:
  void run1();
  int get_tenant_unsafe(const uint64_t tenant_id, ObTenant*& tenant) const;

protected:
  common::SpinRWLock lock_;
  double quota2token_;
  ObWorkerPool worker_pool_;
  TenantList tenants_;
  ObTokenCalcer token_calcer_;
  double node_quota_;
  int64_t times_of_workers_;
  ObTenantNodeBalancer* balancer_;
  common::ObAddr myaddr_;
  bool cpu_dump_;
  bool has_synced_;
  static ObICtxMemConfigGetter* mcg_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiTenant);
};  // end of class ObMultiTenant

// Inline function implementation
TenantList& ObMultiTenant::get_tenant_list()
{
  return tenants_;
}

double ObMultiTenant::get_node_quota() const
{
  return node_quota_;
}

void ObMultiTenant::set_quota2token(double num)
{
  quota2token_ = num;
}

double ObMultiTenant::get_quota2token() const
{
  return quota2token_;
}

double ObMultiTenant::get_token2quota() const
{
  return 1 / quota2token_;
}

int64_t ObMultiTenant::get_times_of_workers() const
{
  return times_of_workers_;
}

int ObMultiTenant::lock_tenant_list(bool write)
{
  int ret = common::OB_SUCCESS;
  if (write) {
    ret = lock_.wrlock();
  } else {
    ret = lock_.rdlock();
  }
  return ret;
}

int ObMultiTenant::unlock_tenant_list(bool write)
{
  UNUSED(write);
  return lock_.unlock();
}

void ObMultiTenant::set_cpu_dump()
{
  cpu_dump_ = true;
}

void ObMultiTenant::unset_cpu_dump()
{
  cpu_dump_ = false;
}

void ObMultiTenant::set_synced()
{
  has_synced_ = true;
}

bool ObMultiTenant::has_synced() const
{
  return has_synced_;
}

}  // end of namespace omt
}  // end of namespace oceanbase

#endif /* _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_H_ */
