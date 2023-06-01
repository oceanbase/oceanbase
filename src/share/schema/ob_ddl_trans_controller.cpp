// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#define USING_LOG_PREFIX RS
#include "share/schema/ob_ddl_trans_controller.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_srv_rpc_proxy.h"


namespace oceanbase
{
namespace share
{
namespace schema
{

int ObDDLTransController::init(share::schema::ObMultiVersionSchemaService *schema_service)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    for (int i=0; OB_SUCC(ret) && i < DDL_TASK_COND_SLOT; i++) {
      if (OB_FAIL(cond_slot_[i].init(ObWaitEventIds::DEFAULT_COND_WAIT))) {
        LOG_WARN("init cond fail", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(schema_service)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema_service is null", KR(ret));
    } else if (OB_FAIL(tenants_.create(10))) {
      LOG_WARN("hashset create fail", KR(ret));
    } else if (OB_FAIL(tenant_for_ddl_trans_new_lock_.create(10))) {
      LOG_WARN("hashset create fail", KR(ret));
    } else if (OB_FAIL(ObThreadPool::start())) {
      LOG_WARN("thread start fail", KR(ret));
    } else {
      schema_service_ = schema_service;
      inited_ = true;
    }
  }
  return ret;
}

void ObDDLTransController::stop()
{
  ObThreadPool::stop();
  wait_cond_.signal();
}

void ObDDLTransController::wait()
{
  wait_cond_.signal();
  ObThreadPool::wait();
}

void ObDDLTransController::destroy()
{
  if (inited_) {
    inited_ = false;
    stop();
    wait();
    ObThreadPool::destroy();
    tasks_.destroy();
    tenants_.destroy();
    tenant_for_ddl_trans_new_lock_.destroy();
    schema_service_ = NULL;
  }
}

ObDDLTransController::~ObDDLTransController()
{
  destroy();
}

void ObDDLTransController::run1()
{
  lib::set_thread_name("DDLTransCtr");
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    ObArray<uint64_t> tenant_ids;
    {
      SpinWLockGuard guard(lock_);
      for (common::hash::ObHashSet<uint64_t>::iterator iter = tenants_.begin(); OB_SUCC(ret) && iter != tenants_.end(); iter++) {
        if (OB_FAIL(tenant_ids.push_back(iter->first))) {
          LOG_WARN("push_back fail", KR(ret), K(iter->first));
        }
      }
      if (OB_SUCC(ret) && tenant_ids.count() > 0) {
        tenants_.reuse();
      }
    }
    if (OB_SUCC(ret) && tenant_ids.count() > 0) {
      LOG_INFO("refresh_schema tenants", K(tenant_ids));
      if (OB_ISNULL(GCTX.root_service_)) {
      } else {
        // ignore ret continue
        for (int64_t i = 0; i < tenant_ids.count(); i++) {
          ObZone zone;
          ObArray<ObAddr> server_list;
          uint64_t tenant_id = tenant_ids.at(i);
          int64_t schema_version = OB_INVALID_VERSION;
          int64_t start_time = ObTimeUtility::current_time();
          ObCurTraceId::init(GCONF.self_addr_);

          if (OB_FAIL(GCTX.root_service_->get_ddl_service().get_unit_manager().get_tenant_unit_servers(tenant_id, zone, server_list))) {
            LOG_WARN("get alive server failed", KR(ret));
          } else if (OB_FAIL(GCTX.root_service_->get_ddl_service().publish_schema_and_get_schema_version(tenant_id, server_list, &schema_version))) {
            LOG_WARN("fail to publish_schema", KR(ret), K(tenant_id));
          } else if (OB_FAIL(broadcast_consensus_version(tenant_id, schema_version, server_list))) {
            LOG_WARN("fail to broadcast consensus version", KR(ret), K(tenant_id), K(schema_version));
          } else {
            int64_t end_time = ObTimeUtility::current_time();
            LOG_INFO("refresh_schema", KR(ret), K(tenant_id), K(end_time - start_time), K(schema_version));
          }
         }
      }
    }
    if (tenant_ids.empty()) {
      wait_cond_.timedwait(100 * 1000);
    }
  }
}

int ObDDLTransController::broadcast_consensus_version(const int64_t tenant_id,
                                                      const int64_t schema_version,
                                                      const ObArray<ObAddr> &server_list)
{
  int ret = OB_SUCCESS;
  obrpc::ObBroadcastConsensusVersionArg arg;
  rootserver::ObBroadcstConsensusVersionProxy proxy(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::broadcast_consensus_version);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", KR(ret));
  } else if (OB_INVALID_VERSION == schema_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema_version", KR(ret), K(schema_version));
  } else {
    arg.set_tenant_id(tenant_id);
    arg.set_consensus_version(schema_version);
    const int64_t rpc_timeout = GCONF.rpc_timeout;
    FOREACH_X(s, server_list, OB_SUCC(ret)) {
      if (OB_ISNULL(s)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("s is null", KR(ret));
      } else {
        // overwrite ret
        if (OB_FAIL(proxy.call(*s, rpc_timeout, arg))) {
          LOG_WARN("send broadcast consensus version rpc failed", KR(ret),
              K(rpc_timeout), K(schema_version), K(arg), "server", *s);
          ret = OB_SUCCESS;
        }
      }
    }
    int tmp_ret = OB_SUCCESS;
    ObArray<int> return_code_array;
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait result failed", KR(tmp_ret));
    }
  }
  LOG_INFO("broadcast consensus version finished", KR(ret), K(schema_version), K(arg), K(server_list));
  return ret;
}

int ObDDLTransController::create_task_and_assign_schema_version(const uint64_t tenant_id,
      const uint64_t schema_version_count,
      int64_t &task_id,
      ObIArray<int64_t> &schema_version_res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else if (tenant_id == OB_INVALID_TENANT_ID || schema_version_count == 0 || schema_version_res.count() != 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("register_task_and_assign_schema_version", KR(ret), K(tenant_id), K(schema_version_count), K(schema_version_res));
  } else {
    int64_t new_schema_version = 0;
    SpinWLockGuard guard(lock_);
    for (int i = 0; OB_SUCC(ret) && i < schema_version_count; i++) {
      if (OB_FAIL(schema_service_->gen_new_schema_version(tenant_id, new_schema_version))) {
        LOG_WARN("register_task_and_assign_schema_version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_version_res.push_back(new_schema_version))) {
        LOG_WARN("register_task_and_assign_schema_version", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tasks_.count() > 0 && schema_version_res.at(0) <= tasks_.at(tasks_.count()-1).task_id_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign schema_version", KR(ret), K(tasks_), K(schema_version_res));
    } else if (OB_FAIL(tasks_.push_back(TaskDesc{tenant_id, schema_version_res.at(0), false}))) {
      LOG_WARN("register_task_and_assign_schema_version", KR(ret));
    } else {
      task_id = schema_version_res.at(0);
    }
  }
  return ret;
}

int ObDDLTransController::check_task_ready(int64_t task_id, bool &ready)
{
  int ret = OB_SUCCESS;
  int idx = OB_INVALID_INDEX;
  SpinWLockGuard guard(lock_);
  for (int i = 0; i < tasks_.count(); i++) {
    if (tasks_.at(i).task_id_ == task_id) {
      idx = i;
      break;
    }
  }
  ready = false;
  if (OB_INVALID_INDEX == idx) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (0 == idx) {
    ready = true;
  } else {
    // gc end task
    for (int i = 0; i < 10; i++) {
      if (!tasks_.empty() && tasks_.at(0).task_id_ != task_id && tasks_.at(0).task_end_) {
        int tmp_ret = tasks_.remove(0);
        if (tmp_ret != OB_SUCCESS) {
          LOG_WARN("check_task_ready", KR(tmp_ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDDLTransController::wait_task_ready(int64_t task_id, int64_t wait_us)
{
  int ret = OB_SUCCESS;
  bool ready = false;
  uint64_t cond_idx = task_id % DDL_TASK_COND_SLOT;
  int64_t start_time = ObTimeUtility::current_time();
  while (OB_SUCC(ret) && ObTimeUtility::current_time() - start_time < wait_us) {
    if (OB_FAIL(check_task_ready(task_id, ready))) {
      LOG_WARN("wait_task_ready", KR(ret), K(task_id), K(ready));
    } else if (ready) {
      break;
    } else {
      ObThreadCondGuard guard(cond_slot_[cond_idx]);
      cond_slot_[cond_idx].wait(100);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!ready) {
    remove_task(task_id);
    ret = OB_TIMEOUT;
    LOG_WARN("wait_task_ready", KR(ret), K(task_id), K(tasks_), K(ready));
  }
  return ret;
}

int ObDDLTransController::remove_task(int64_t task_id)
{
  int ret = OB_SUCCESS;
  int idx = OB_INVALID_INDEX;
  SpinWLockGuard guard(lock_);
  for (int i = 0; i < tasks_.count(); i++) {
    if (tasks_.at(i).task_id_ == task_id) {
      tasks_.at(i).task_end_ = true;
      idx = i;
      uint64_t tenant_id = tasks_.at(i).tenant_id_;
      if (OB_FAIL(tasks_.remove(i))) {
        LOG_WARN("remove_task fail", KR(ret), K(task_id));
      } else if (OB_FAIL(tenants_.set_refactored(tenant_id, 1, 0, 1))) {
        LOG_WARN("set_refactored fail", KR(ret), K(tenant_id));
      } else {
        wait_cond_.signal();
      }
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_INVALID_INDEX == idx) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (idx < tasks_.count()) {
    // wake up next
    int64_t next_task_id = tasks_.at(idx).task_id_;
    uint64_t cond_idx = next_task_id % DDL_TASK_COND_SLOT;
    cond_slot_[cond_idx].broadcast();
  }
  return ret;
}

int ObDDLTransController::check_enable_ddl_trans_new_lock(int64_t tenant_id, bool &res)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else {
    SpinRLockGuard guard(lock_for_tenant_set_);
    ret = tenant_for_ddl_trans_new_lock_.exist_refactored(tenant_id);
    if (OB_HASH_EXIST == ret) {
      res = true;
      ret = OB_SUCCESS;
    } else if (OB_HASH_NOT_EXIST == ret) {
      res = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("check tenant in hashset fail", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObDDLTransController::set_enable_ddl_trans_new_lock(int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLTransController", KR(ret));
  } else {
    SpinWLockGuard guard(lock_for_tenant_set_);
    if (OB_FAIL(tenant_for_ddl_trans_new_lock_.set_refactored(tenant_id, 1, 0, 1))) {
      LOG_WARN("fail set enable_ddl_trans_new_lock", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

} // end schema
} // end share
} // end oceanbase
