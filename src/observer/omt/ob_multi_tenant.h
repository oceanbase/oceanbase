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
#include "lib/lock/ob_bucket_lock.h"    // ObBucketLock
#include "ob_tenant_node_balancer.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogger;
}
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
}
namespace rpc
{
class ObRequest;
}
namespace omt
{
class ObTenantConfig;

struct ObCtxMemConfig
{
  ObCtxMemConfig()
    : ctx_id_(0), idle_size_(0), limit_(INT64_MAX) {}
  uint64_t ctx_id_;
  int64_t idle_size_;
  int64_t limit_;
  TO_STRING_KV(K_(ctx_id), K_(idle_size), K_(limit));
};

class ObICtxMemConfigGetter
{
public:
  virtual int get(int64_t tenant_id, int64_t tenant_limit, common::ObIArray<ObCtxMemConfig> &configs) = 0;
};

class ObCtxMemConfigGetter : public ObICtxMemConfigGetter
{
public:
  virtual int get(int64_t tenant_id, int64_t tenant_limit, common::ObIArray<ObCtxMemConfig> &configs);
};

// Forward declearation
class ObTenant;
class ObTenantHandle;
class ObTenantMeta;

// Type alias
typedef common::ObSortedVector<ObTenant*> TenantList;
typedef TenantList::iterator TenantIterator;
typedef common::ObVector<uint64_t> TenantIdList;

// This is the entry class of OMT module.
class ObMultiTenant
    : public share::ObThreadPool
{
public:
  const     static int64_t TIME_SLICE_PERIOD        = 10000;

public:
  explicit ObMultiTenant();

  int init(common::ObAddr myaddr,
           common::ObMySQLProxy *sql_proxy = NULL,
           bool mtl_bind_flag = true);

  int start();
  void stop();
  void wait();
  void destroy();

  int create_hidden_sys_tenant();
  int update_hidden_sys_tenant();
  int convert_hidden_to_real_sys_tenant(const share::ObUnitInfoGetter::ObTenantConfig &unit, const int64_t abs_timeout_us = INT64_MAX);
  int create_tenant_without_unit(const uint64_t tenant_id, const double min_cpu, const double max_cpu);
  int create_tenant(const ObTenantMeta &meta, bool write_slog, const int64_t abs_timeout_us = INT64_MAX);
  int update_tenant_unit(const share::ObUnitInfoGetter::ObTenantConfig &unit);

  int get_tenant_unit(const uint64_t tenant_id, share::ObUnitInfoGetter::ObTenantConfig &unit);
  int get_unit_id(const uint64_t tenant_id, uint64_t &unit_id);
  int get_tenant_units(share::TenantUnits &units, bool include_hidden_sys);
  int get_tenant_metas(common::ObIArray<ObTenantMeta> &metas);
  int get_tenant_metas_for_ckpt(common::ObIArray<ObTenantMeta> &metas);
  int get_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &compat_mode);
  int mark_del_tenant(const uint64_t tenant_id);
  int del_tenant(const uint64_t tenant_id);
  int convert_real_to_hidden_sys_tenant();
  int update_tenant_cpu(const uint64_t tenant_id, const double min_cpu, const double max_cpu);
  int update_tenant_memory(const uint64_t tenant_id, const int64_t mem_limit, int64_t &allowed_mem_limit);
  int update_tenant_memory(const share::ObUnitInfoGetter::ObTenantConfig &unit,
                           const int64_t extra_memory = 0);
  int update_tenant_log_disk_size(const uint64_t tenant_id,
                                  const int64_t old_log_disk_size,
                                  const int64_t new_log_disk_size,
                                  int64_t &allowed_log_disk_size);
  int modify_tenant_io(const uint64_t tenant_id, const share::ObUnitConfig &unit_config);
  int update_tenant_config(uint64_t tenant_id);
  int update_palf_config();
  int update_tenant_dag_scheduler_config();
  int get_tenant(const uint64_t tenant_id, ObTenant *&tenant) const;
  int get_tenant_with_tenant_lock(const uint64_t tenant_id, common::ObLDHandle &handle, ObTenant *&tenant) const;
  int get_active_tenant_with_tenant_lock(const uint64_t tenant_id, common::ObLDHandle &handle, ObTenant *&tenant) const;
  int update_tenant(uint64_t tenant_id, std::function<int(ObTenant&)> &&func);
  int recv_request(const uint64_t tenant_id, rpc::ObRequest &req);
  int update_tenant_freezer_mem_limit(const uint64_t tenant_id,
                                      const int64_t tenant_min_mem,
                                      const int64_t tenant_max_mem);

  inline TenantList &get_tenant_list();
  int for_each(std::function<int(ObTenant &)> func);
  // NB: access MTL safely
  int operate_in_each_tenant(const std::function<int()> &func, bool skip_virtual_tenant = true);
  int operate_each_tenant_for_sys_or_self(const std::function<int()> &func, bool skip_virtual_tenant = true);
  void get_tenant_ids(TenantIdList &id_list);
  int get_mtl_tenant_ids(ObIArray<uint64_t> &tenant_ids);

  inline double get_node_quota() const;
  inline double get_attenuation_factor() const;
  inline int64_t get_times_of_workers() const;
  int get_tenant_cpu_usage(const uint64_t tenant_id, double &usage) const;
  int get_tenant_worker_time(const uint64_t tenant_id, int64_t &worker_time) const;
  int get_tenant_cpu_time(const uint64_t tenant_id, int64_t &rusage_time) const;
  int get_tenant_cpu(
      const uint64_t tenant_id,
      double &min_cpu, double &max_cpu) const;

  inline int lock_tenant_list(bool write=false);
  inline int unlock_tenant_list(bool write=false);

  bool has_tenant(uint64_t tenant_id) const;
  bool is_available_tenant(uint64_t tenant_id) const;
  int check_if_hidden_sys(const uint64_t tenant_id, bool &is_hidden_sys);
  inline void set_cpu_dump();
  inline void unset_cpu_dump();

  inline void set_synced();
  inline bool has_synced() const;

  void set_workers_per_cpu(int64_t v);
  int write_create_tenant_abort_slog(uint64_t tenant_id);
  int write_delete_tenant_commit_slog(uint64_t tenant_id);
  int clear_persistent_data(const uint64_t tenant_id);
  int check_if_unit_id_exist(const uint64_t unit_id, bool &exist);

protected:
  void run1();
  int get_tenant_unsafe(const uint64_t tenant_id, ObTenant *&tenant) const;

  int write_create_tenant_prepare_slog(const ObTenantMeta &meta);
  int write_create_tenant_commit_slog(uint64_t tenant_id);
  int write_delete_tenant_prepare_slog(uint64_t tenant_id);
  int write_update_tenant_unit_slog(const share::ObUnitInfoGetter::ObTenantConfig &unit);
  int construct_meta_for_hidden_sys(ObTenantMeta &meta);
  int construct_meta_for_virtual_tenant(const uint64_t tenant_id,
                                        const double min_cpu,
                                        const double max_cpu,
                                        const int64_t mem_limit,
                                        ObTenantMeta &meta);
  int create_virtual_tenants();
  int remove_tenant(const uint64_t tenant_id, bool &remove_tenant_succ);
  uint32_t get_tenant_lock_bucket_idx(const uint64_t tenant_id);
  int update_tenant_unit_no_lock(const share::ObUnitInfoGetter::ObTenantConfig &unit);
  int construct_allowed_unit_config(const int64_t allowed_log_disk_size,
                                    const share::ObUnitInfoGetter::ObTenantConfig &expected_unit_config,
                                    share::ObUnitInfoGetter::ObTenantConfig &allowed_unit);

private:
  int update_tenant_freezer_config_();
protected:
      static const int DEL_TRY_TIMES = 30;
      enum class ObTenantCreateStep {
        STEP_BEGIN = 0, // begin
        STEP_CTX_MEM_CONFIG_SETTED = 1, // set_tenant_ctx_idle succ
        STEP_LOG_DISK_SIZE_PINNED = 2,  // pin log disk size succ
        STEP_TENANT_NEWED = 3, // new tenant succ
        STEP_WRITE_PREPARE_SLOG = 4, // write_prepare_create_tenant_slog succ
        STEP_FINISH,
      };

  bool is_inited_;
  storage::ObStorageLogger *server_slogger_;

  // prevent concurrent creating or deleteing tenant and exclusive with checkpoint,
  // if use wrlock, the creating tenant rpc may be timeout but the creating will be executed
  // when the lock is acquired. so here we use the try_wrlock.
  common::ObBucketLock bucket_lock_;

  mutable common::SpinRWLock lock_; // protect tenant list
  TenantList tenants_;
  ObTenantNodeBalancer *balancer_;
  common::ObAddr myaddr_;
  bool cpu_dump_;
  bool has_synced_;
  static ObICtxMemConfigGetter *mcg_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMultiTenant);
}; // end of class ObMultiTenant

// Inline function implementation
TenantList &ObMultiTenant::get_tenant_list()
{
  return tenants_;
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

class ObSharedTimer
{
public:
  ObSharedTimer() : tg_id_(-1) {}
  static int mtl_init(ObSharedTimer *&st);
  static int mtl_start(ObSharedTimer *&st);
  static void mtl_stop(ObSharedTimer *&st);
  static void mtl_wait(ObSharedTimer *&st);
  void destroy();
  int get_tg_id() const { return tg_id_; }
private:
  int tg_id_;
};

} // end of namespace omt
} // end of namespace oceanbase


#endif /* _OCEABASE_OBSERVER_OMT_OB_MULTI_TENANT_H_ */
