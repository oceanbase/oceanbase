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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/resource/ob_table_load_resource_manager.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace lib;
using namespace share::schema;
using namespace table;
using namespace omt;

/**
 * ObRefreshAndCheckTask
 */
int ObTableLoadResourceManager::ObRefreshAndCheckTask::init(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObRefreshAndCheckTask init twice", KR(ret), KP(this));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }

  return ret;
}

void ObTableLoadResourceManager::ObRefreshAndCheckTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(manager_.init_resource())) {
    LOG_WARN("fail to init_resource", KR(ret));
  } else if (OB_FAIL(manager_.refresh_and_check())) {
    LOG_WARN("fail to refresh_and_check", KR(ret));
  }
}

/**
 * ObTableLoadResourceManager
 */
ObTableLoadResourceManager::ObTableLoadResourceManager()
  : refresh_and_check_task_(*this),
    is_stop_(false),
    resource_inited_(false),
    is_inited_(false)
{
}

ObTableLoadResourceManager::~ObTableLoadResourceManager()
{
}

int ObTableLoadResourceManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  const int64_t bucket_num = 1024;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadResourceManager init twice", KR(ret), KP(this));
  } else if (OB_FAIL(resource_pool_.create(bucket_num, "TLD_ResourceMgr", "TLD_ResourceMgr", tenant_id))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
  } else if (OB_FAIL(assigned_tasks_.create(bucket_num, "TLD_AssignedMgr", "TLD_AssignedMgr", tenant_id))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
  } else if (OB_FAIL(refresh_and_check_task_.init(tenant_id))) {
    LOG_WARN("fail to init check task", KR(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTableLoadResourceManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_INFO("ObTableLoadResourceManager init twice", KR(ret), KP(this));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 refresh_and_check_task_,
                                 REFRESH_AND_CHECK_TASK_INTERVAL,
                                 true))) {
    LOG_WARN("fail to schedule refresh_and_check task", KR(ret));
  }
  LOG_INFO("ObTableLoadResourceManager::start", KR(ret));

  return ret;
}

void ObTableLoadResourceManager::stop()
{
  if (OB_LIKELY(refresh_and_check_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
  }
  {
    lib::ObMutexGuard guard(mutex_);
    is_stop_ = true;
  }
}

int ObTableLoadResourceManager::wait()
{
  if (OB_LIKELY(refresh_and_check_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
  }

  return release_all_resource();
}

void ObTableLoadResourceManager::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    if (refresh_and_check_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
          refresh_and_check_task_.is_inited_ = false;
        }
      }
    }
    is_inited_ = false;
  }
}

int ObTableLoadResourceManager::resume()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(refresh_and_check_task_.init(MTL_ID()))) {
    LOG_WARN("fail to init check task", KR(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 refresh_and_check_task_,
                                 REFRESH_AND_CHECK_TASK_INTERVAL,
                                 true))) {
    LOG_WARN("fail to schedule resource check task", KR(ret));
  } else if (OB_FAIL(resource_pool_.clear())) {
    LOG_WARN("fail to clear resource_pool_", KR(ret));
  } else if (OB_FAIL(assigned_tasks_.clear())) {
    LOG_WARN("fail to clear assigned_tasks_", KR(ret));
  }

  return ret;
}

void ObTableLoadResourceManager::pause()
{
  int ret = OB_SUCCESS;
  if (refresh_and_check_task_.is_inited_) {
    bool is_exist = true;
    if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_, is_exist))) {
      if (is_exist) {
        TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
        TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), refresh_and_check_task_);
        refresh_and_check_task_.is_inited_ = false;
      }
    }
  }
  resource_inited_ = false;
}

int ObTableLoadResourceManager::apply_resource(ObDirectLoadResourceApplyArg &arg, ObDirectLoadResourceOpRes &res)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadResourceManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else if (OB_UNLIKELY(resource_inited_ == false)) {
    ret = OB_EAGAIN;
    LOG_WARN("ObTableLoadResourceManager is initializing", KR(ret));
  } else {
    ObResourceAssigned assigned_arg;
    lib::ObMutexGuard guard(mutex_);
    if (is_stop_) {
      ret = OB_EAGAIN;
      LOG_WARN("ObTableLoadResourceManager is stop", KR(ret));
    } else if (OB_FAIL(assigned_tasks_.get_refactored(arg.task_key_, assigned_arg))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_SUCCESS;
        ObResourceCtx ctx;
        for (int64_t i = 0; OB_SUCC(ret) && i < arg.apply_array_.count(); i++) {
          ObDirectLoadResourceUnit &apply_unit = arg.apply_array_[i];
          if (OB_FAIL(resource_pool_.get_refactored(apply_unit.addr_, ctx))) {
            LOG_WARN("fail to get refactored", KR(ret), K(apply_unit.addr_));
            if (ret == OB_HASH_NOT_EXIST) {
              // 第一次切主需要初始化，通过内部sql查询ACTIVE状态的observer可能不完整，期间若有导入任务进来时需要重试
              ret = OB_EAGAIN;
            }
          } else if (apply_unit.thread_count_ > ctx.thread_remain_ || apply_unit.memory_size_ > ctx.memory_remain_) {
            ret = OB_EAGAIN;
          }
	      }
        if (OB_SUCC(ret)) {
          ObResourceAssigned assigned(arg);
          if (OB_FAIL(assigned_tasks_.set_refactored(arg.task_key_, assigned))) {
            LOG_WARN("fail to set refactored assigned_tasks_", KR(ret), K(arg));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < arg.apply_array_.count(); i++) {
              if (OB_FAIL(resource_pool_.get_refactored(arg.apply_array_[i].addr_, ctx))) {
                LOG_WARN("fail to get refactored", K(arg.apply_array_[i].addr_));
              } else {
                ctx.thread_remain_ -= arg.apply_array_[i].thread_count_;
                ctx.memory_remain_ -= arg.apply_array_[i].memory_size_;
                if (OB_FAIL(resource_pool_.set_refactored(arg.apply_array_[i].addr_, ctx, 1))) {
                  LOG_WARN("fail to set refactored", K(arg.apply_array_[i].addr_));
                } else {
                  LOG_INFO("resource remain", K(arg.apply_array_[i]), K(ctx.thread_remain_), K(ctx.memory_remain_));
                }
              }
            }
            LOG_INFO("ObTableLoadResourceManager::apply_resource", K(arg));
          }
        }
      } else {
        LOG_WARN("fail to get refactored", KR(ret), K(arg.task_key_));
      }
    } else {
      LOG_INFO("resource has been assigned", K(arg.task_key_));
    }
  }
  res.error_code_ = ret;

  return ret;
}

int ObTableLoadResourceManager::release_resource(ObDirectLoadResourceReleaseArg &arg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadResourceManager not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    ObResourceAssigned assigned_arg;
    lib::ObMutexGuard guard(mutex_);
    if (is_stop_) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("ObTableLoadResourceManager is stop", KR(ret));
    } else if (OB_FAIL(assigned_tasks_.get_refactored(arg.task_key_, assigned_arg))) {
      if (ret == OB_HASH_NOT_EXIST) {
        LOG_INFO("resource has been released", K(arg.task_key_));
      } else {
        LOG_WARN("fail to get refactored", K(arg.task_key_));
      }
    } else {
      ObResourceCtx ctx;
      common::ObSArray<ObDirectLoadResourceUnit> &apply_array = assigned_arg.apply_arg_.apply_array_;
      for (int64_t i = 0; OB_SUCC(ret) && i < apply_array.count(); i++) {
        if (OB_FAIL(resource_pool_.get_refactored(apply_array[i].addr_, ctx))) {
          LOG_WARN("fail to get refactored", K(apply_array[i].addr_));
        } else {
          ctx.thread_remain_ += apply_array[i].thread_count_;
          ctx.memory_remain_ += apply_array[i].memory_size_;
          if (OB_FAIL(resource_pool_.set_refactored(apply_array[i].addr_, ctx, 1))) {
            LOG_WARN("fail to set refactored", K(apply_array[i].addr_));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(assigned_tasks_.erase_refactored(arg.task_key_))) {
          LOG_WARN("fail to erase refactored", K(arg.task_key_));
        } else {
          LOG_INFO("ObTableLoadResourceManager::release_resource", K(arg));
        }
      }
    }
  }

  return ret;
}

int ObTableLoadResourceManager::update_resource(ObDirectLoadResourceUpdateArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg));
  } else {
    ObArray<ObAddr> new_addrs;
    const int64_t bucket_num = 1024;
    typedef common::hash::ObHashMap<ObAddr, bool, common::hash::NoPthreadDefendMode> AddrMap;
    AddrMap addrs_map;
    new_addrs.set_tenant_id(MTL_ID());
    if (OB_FAIL(addrs_map.create(bucket_num, "TLD_ResourceMgr", "TLD_ResourceMgr", arg.tenant_id_))) {
      LOG_INFO("fail to create hashmap", KR(ret), K(bucket_num));
    } else {
      ObResourceCtx ctx;
      lib::ObMutexGuard guard(mutex_);
      for (ResourceCtxMap::iterator iter = resource_pool_.begin(); OB_SUCC(ret) && iter != resource_pool_.end(); iter++) {
        if (iter->second.thread_remain_ + arg.thread_count_ - iter->second.thread_total_ < 0 ||
            iter->second.memory_remain_ + arg.memory_size_ - iter->second.memory_total_ < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("the resource has been used and cannot be reduced", KR(ret), K(arg));
        } else if (OB_FAIL(addrs_map.set_refactored(iter->first, true))) {
          LOG_WARN("fail to set refactored", KR(ret), K(iter->first));
        }
      }
      // Record the newly added observer
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.addrs_.count(); i++) {
        if (OB_FAIL(resource_pool_.get_refactored(arg.addrs_[i], ctx))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
            if (OB_FAIL(new_addrs.push_back(arg.addrs_[i]))) {
              LOG_WARN("fail to push_back", KR(ret), K(arg.addrs_[i]));
            }
          } else {
            LOG_WARN("fail to get refactored", K(arg.addrs_[i]));
          }
        } else if (OB_FAIL(addrs_map.set_refactored(arg.addrs_[i], false, 1))) {
          LOG_WARN("fail to set refactored", KR(ret), K(arg.addrs_[i]));
        }
      }
      // If there are still tasks executing on the deleted observer node, a failure is returned
      for (AddrMap::iterator iter = addrs_map.begin(); OB_SUCC(ret) && iter != addrs_map.end(); iter++) {
        if (iter->second) {
          if (OB_FAIL(resource_pool_.get_refactored(iter->first, ctx))) {
            LOG_WARN("fail to get refactored", K(iter->first));
          } else if (ctx.thread_remain_ != ctx.thread_total_ || ctx.memory_remain_ != ctx.memory_total_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("there are still tasks executing on the deleted observer node", KR(ret));
          } else if (OB_FAIL(resource_pool_.erase_refactored(iter->first))) {
            LOG_WARN("fail to erase refactored", K(iter->first));
          } else {
            LOG_INFO("update resource delete observer", K(arg.tenant_id_), K(iter->first));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < new_addrs.count(); i++) {
        ObResourceCtx ctx(arg.thread_count_, arg.memory_size_, arg.thread_count_, arg.memory_size_);
        if (OB_FAIL(resource_pool_.set_refactored(new_addrs[i], ctx))) {
          LOG_WARN("fail to set refactored", K(new_addrs[i]));
        } else {
          LOG_INFO("update resource add observer", K(arg.tenant_id_), K(new_addrs[i]), K(ctx));
        }
      }
      for (ResourceCtxMap::iterator iter = resource_pool_.begin(); OB_SUCC(ret) && iter != resource_pool_.end(); iter++) {
        if (OB_FAIL(resource_pool_.get_refactored(iter->first, ctx))) {
          LOG_WARN("fail to get refactored", K(iter->first));
        } else {
          if (ctx.thread_total_ != arg.thread_count_ || ctx.memory_total_ != arg.memory_size_) {
            LOG_INFO("update resource thread and memory", K(arg.tenant_id_), K(iter->first), K(ctx.thread_total_), K(ctx.memory_total_), K(arg.thread_count_), K(arg.memory_size_));
          }
          ctx.thread_remain_ += arg.thread_count_ - ctx.thread_total_;
          ctx.memory_remain_ += arg.memory_size_ - ctx.memory_total_;
          ctx.thread_total_ = arg.thread_count_;
          ctx.memory_total_ = arg.memory_size_;
          if (OB_FAIL(resource_pool_.set_refactored(iter->first, ctx, 1))) {
            LOG_WARN("fail to set refactored", K(iter->first));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLoadResourceManager::gen_update_arg(ObDirectLoadResourceUpdateArg &update_arg)
{
  int ret = OB_SUCCESS;
  ObObj value;
  int64_t pctg = 0;
  ObSchemaGetterGuard schema_guard;
  const ObSysVarSchema *var_schema = NULL;
  ObTenant *tenant = nullptr;
  uint64_t tenant_id = MTL_ID();
  ObSqlString sql;

  SMART_VAR(common::ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt("select svr_ip, svr_port from %s where status = 'ACTIVE'", OB_ALL_SERVER_TNAME))) {
      LOG_WARN("failed to assign sql", KR(ret));
    } else if (OB_ISNULL(GCTX.sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql_proxy is null", KR(ret), K(GCTX.sql_proxy_));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret));
    } else {
      while (OB_SUCC(result->next())) {
        ObString svr_ip;
        int32_t svr_port = -1;
        ObAddr addr;
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "svr_ip", svr_ip);
        EXTRACT_INT_FIELD_MYSQL(*result, "svr_port", svr_port, int32_t);
        if (OB_SUCC(ret)) {
          if (false == addr.set_ip_addr(svr_ip, svr_port)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("failed to set_ip_addr", KR(ret), K(svr_ip), K(svr_port));
          } else if (OB_FAIL(update_arg.addrs_.push_back(addr))) {
            LOG_WARN("failed to push back obj", KR(ret));
          }
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        LOG_WARN("read dependency info failed", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
      LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObTableLoadService::get_memory_limit(update_arg.memory_size_))) {
      LOG_WARN("fail to get memory_limit", KR(ret), K(tenant_id));
    } else {
      update_arg.tenant_id_ = tenant_id;
      update_arg.thread_count_ = (int64_t)tenant->unit_max_cpu() * 2;
    }
  }

  return ret;
}

int ObTableLoadResourceManager::gen_check_res(bool first_check, ObDirectLoadResourceUpdateArg &update_arg, common::ObArray<ObDirectLoadResourceOpRes> &check_res)
{
  int ret = OB_SUCCESS;
  ObDirectLoadResourceCheckArg check_arg;
  check_arg.tenant_id_ = update_arg.tenant_id_;
  check_arg.first_check_ = first_check;
  if (OB_FAIL(check_res.reserve(update_arg.addrs_.size()))) {
    LOG_WARN("fail to reserve check_res", KR(ret));
  } else {
    for (int64_t i = 0; i < update_arg.addrs_.count(); i++) {
      ObDirectLoadResourceOpRes op_res;
      check_arg.avail_memory_ = update_arg.memory_size_;
      ObAddr &addr = update_arg.addrs_[i];
      if (ObTableLoadUtils::is_local_addr(addr)) {
        if (OB_FAIL(ObTableLoadService::refresh_and_check_resource(check_arg, op_res))) {
          LOG_WARN("fail to refresh_and_check_resource", KR(ret), K(addr));
        }
      } else {
        TABLE_LOAD_RESOURCE_RPC_CALL(check_resource, addr, check_arg, op_res);
      }
      if (OB_FAIL(check_res.push_back(op_res))) {
        LOG_WARN("fail to push back check_res", K(ret));
      }
    }
  }

  return ret;
}

void ObTableLoadResourceManager::check_assigned_task(common::ObArray<ObDirectLoadResourceOpRes> &check_res)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  uint64_t tenant_id = MTL_ID();
  typedef common::hash::ObHashMap<ObTableLoadUniqueKey, bool, common::hash::NoPthreadDefendMode> UniqueKeyMap;
  UniqueKeyMap refreshed_assigned_tasks;
  common::ObArray<int64_t> need_track_array;
  common::ObArray<ObTableLoadUniqueKey> need_release_array;
  need_track_array.set_tenant_id(MTL_ID());
  need_release_array.set_tenant_id(MTL_ID());
  if (OB_FAIL(refreshed_assigned_tasks.create(bucket_num, "TLD_ResourceMgr", "TLD_ResourceMgr", tenant_id))) {
    LOG_WARN("fail to create hashmap", KR(ret), K(bucket_num));
  } else {
    ObResourceAssigned assigned_arg;
    lib::ObMutexGuard guard(mutex_);
    for (ResourceAssignedMap::iterator iter = assigned_tasks_.begin(); OB_SUCC(ret) && iter != assigned_tasks_.end(); iter++) {
      if (OB_FAIL(refreshed_assigned_tasks.set_refactored(iter->first, false))) {
        LOG_WARN("fail to set refactored", KR(ret), K(iter->first));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < check_res.count(); i++) {
      for (int64_t j = 0; OB_SUCC(ret) && j < check_res[i].assigned_array_.count(); j++) {
        if (OB_FAIL(assigned_tasks_.get_refactored(check_res[i].assigned_array_[j].task_key_, assigned_arg))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
            if (OB_FAIL(need_track_array.push_back(i)) || OB_FAIL(need_track_array.push_back(j))) {
              LOG_WARN("fail to push back", K(i), K(j));
            }
          }
        } else {
          if (OB_FAIL(refreshed_assigned_tasks.set_refactored(check_res[i].assigned_array_[j].task_key_, true, 1))) {
            LOG_WARN("fail to set refactored", K(check_res[i].assigned_array_[j].task_key_));
          } else {
            assigned_arg.miss_counts_ = 0;
            if (OB_FAIL(assigned_tasks_.set_refactored(check_res[i].assigned_array_[j].task_key_, assigned_arg, 1))) {
              LOG_WARN("fail to set refactored", K(check_res[i].assigned_array_[j].task_key_));
            }
          }
        }
      }
    }
    for (UniqueKeyMap::iterator iter = refreshed_assigned_tasks.begin(); OB_SUCC(ret) && iter != refreshed_assigned_tasks.end(); iter++) {
      if (iter->second == false) {
        if (OB_FAIL(assigned_tasks_.get_refactored(iter->first, assigned_arg))) {
          LOG_WARN("fail to get refactored", K(iter->first));
        } else {
          assigned_arg.miss_counts_++;
          if (OB_FAIL(assigned_tasks_.set_refactored(iter->first, assigned_arg, 1))) {
            LOG_WARN("fail to set refactored", K(iter->first));
          } else if (assigned_arg.miss_counts_ >= MAX_MISS_COUNT) {
            if (OB_FAIL(need_release_array.push_back(iter->first))) {
              LOG_WARN("fail to push back", K(iter->first));
            }
          }
        }
      }
    }
  }

  ObDirectLoadResourceReleaseArg release_arg;
  release_arg.tenant_id_ = MTL_ID();
  if (need_release_array.count()) {
    LOG_INFO("need release assigned tasks", K(need_release_array));
    for (int64_t i = 0; i < need_release_array.count(); i++) {
      release_arg.task_key_ = need_release_array[i];
      release_resource(release_arg);
    }
  }

  ObDirectLoadResourceOpRes res;
  if (need_track_array.count()) {
    LOG_INFO("need track assigned tasks", K(need_track_array));
    for (int64_t i = 0; i < need_track_array.count(); i += 2) {
      if(OB_UNLIKELY(i + 1 >= need_track_array.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("need_track_array is error", KR(ret));
      } else if (OB_UNLIKELY(need_track_array[i] < 0 ||
                             need_track_array[i] >= check_res.count() ||
                             need_track_array[i + 1] < 0 ||
                             need_track_array[i + 1] >= check_res[i].assigned_array_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("need_track_array value overflow", KR(ret), K(need_track_array[i]), K(need_track_array[i + 1]));
      } else {
        apply_resource(check_res[need_track_array[i]].assigned_array_[need_track_array[i + 1]], res);
      }
    }
  }
}

int ObTableLoadResourceManager::refresh_and_check(bool first_check)
{
  int ret = OB_SUCCESS;
  ObDirectLoadResourceUpdateArg update_arg;
  common::ObArray<ObDirectLoadResourceOpRes> check_res;
  check_res.set_tenant_id(MTL_ID());
  if (OB_FAIL(gen_update_arg(update_arg))) {
    LOG_WARN("fail to gen_update_arg", KR(ret), K(MTL_ID()));
  } else if (OB_UNLIKELY(!update_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(update_arg));
  } else if (first_check) {
    if (OB_FAIL(gen_check_res(first_check, update_arg, check_res))) {
      LOG_WARN("fail to gen_check_res", KR(ret));
    } else {
      ObResourceCtx ctx;
      ObResourceAssigned assigned_arg;
      lib::ObMutexGuard guard(mutex_);
      for (int64_t i = 0; OB_SUCC(ret) && i < update_arg.addrs_.count(); i++) {
        if (OB_FAIL(resource_pool_.get_refactored(update_arg.addrs_[i], ctx))) {
          if (ret == OB_HASH_NOT_EXIST) {
            ret = OB_SUCCESS;
            if (OB_FAIL(resource_pool_.set_refactored(update_arg.addrs_[i], ctx))) {
              LOG_WARN("fail to set refactored", KR(ret), K(update_arg.addrs_[i]));
            }
          } else {
            LOG_WARN("fail to get refactored", K(update_arg.addrs_[i]));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < check_res.count(); i++) {
        for (int64_t j = 0; OB_SUCC(ret) && j < check_res[i].assigned_array_.count(); j++) {
          ObDirectLoadResourceApplyArg &arg = check_res[i].assigned_array_[j];
          if (OB_FAIL(assigned_tasks_.get_refactored(arg.task_key_, assigned_arg))) {
            if (ret == OB_HASH_NOT_EXIST) {
              int tmp_ret = OB_SUCCESS;
              ret = OB_SUCCESS;
              for(int64_t k = 0; OB_SUCC(ret) && k < arg.apply_array_.count(); k++) {
                if (OB_FAIL(resource_pool_.get_refactored(arg.apply_array_[k].addr_, ctx))) {
                  if (ret == OB_HASH_NOT_EXIST) {
                    tmp_ret = ret;
                    ret = OB_SUCCESS;
                    for (int64_t l = 0; OB_SUCC(ret) && l < k; l++) {
                      if (OB_FAIL(resource_pool_.get_refactored(arg.apply_array_[l].addr_, ctx))) {
                        LOG_WARN("fail to get refactored", K(arg.apply_array_[l].addr_));
                      } else {
                        ctx.thread_total_ -= arg.apply_array_[l].thread_count_;
                        ctx.memory_total_ -= arg.apply_array_[l].memory_size_;
                        if (OB_FAIL(resource_pool_.set_refactored(arg.apply_array_[l].addr_, ctx, 1))) {
                          LOG_WARN("fail to set refactored", K(arg.apply_array_[l].addr_));
                        }
                      }
                    }
                  } else {
                    LOG_WARN("fail to get refactored", K(arg.apply_array_[k].addr_));
                  }
                } else {
                  ctx.thread_total_ += arg.apply_array_[k].thread_count_;
                  ctx.memory_total_ += arg.apply_array_[k].memory_size_;
                  if (OB_FAIL(resource_pool_.set_refactored(arg.apply_array_[k].addr_, ctx, 1))) {
                    LOG_WARN("fail to set refactored", K(arg.apply_array_[k].addr_));
                  }
                }
              }
              if (OB_SUCC(tmp_ret)) {
                ObResourceAssigned resource_assigned(arg);
                if (OB_FAIL(assigned_tasks_.set_refactored(arg.task_key_, resource_assigned))) {
                  LOG_WARN("fail to set refactored", K(arg));
                }
              }
            } else {
              LOG_WARN("fail to get refactored", K(arg.task_key_));
            }
          }
          ret = OB_SUCCESS;
        }
        LOG_INFO("refresh_and_check first check", K(update_arg.tenant_id_), K(assigned_tasks_.size()));
      }
    }
  } else {
    if (OB_FAIL(update_resource(update_arg))) {
      LOG_WARN("fail to update_resource", KR(ret));
    }
    if (OB_FAIL(gen_check_res(first_check, update_arg, check_res))) {
      LOG_WARN("fail to gen_check_res", KR(ret));
    }
    check_assigned_task(check_res);
  }

  return ret;
}

int ObTableLoadResourceManager::init_resource()
{
  int ret = OB_SUCCESS;
  int64_t retry_time = 0;
  while (retry_time < MAX_INIT_RETRY_TIMES && resource_inited_ == false) {
    if (OB_FAIL(refresh_and_check(!resource_inited_))) {
      retry_time++;
      LOG_INFO("init_resource retry", KR(ret), K(MTL_ID()), K(retry_time));
    } else {
      resource_inited_ = true;
      if (OB_FAIL(refresh_and_check(!resource_inited_))) {
        LOG_WARN("fail to refresh_and_check", KR(ret));
      }
      LOG_INFO("ObTableLoadResourceManager::init_resource", KR(ret), K(MTL_ID()));
      break;
    }
  }

  return ret;
}

int ObTableLoadResourceManager::release_all_resource()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadResourceManager not init", KR(ret), KP(this));
  } else {
    ObDirectLoadResourceReleaseArg arg;
    arg.tenant_id_ = MTL_ID();
    for (ResourceAssignedMap::iterator iter = assigned_tasks_.begin(); iter != assigned_tasks_.end(); iter++) {
      arg.task_key_ = iter->first;
      if (OB_FAIL(release_resource(arg))) {
        break;
      }
    }
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase
