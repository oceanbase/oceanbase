/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER

#include "ob_table_session_pool.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "share/table/ob_ttl_util.h" // for ObTTLUtil::TTL_THREAD_MAX_SCORE

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace table
{

int ObTableRelatedSysVars::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) { // double check
      if (OB_FAIL(update_sys_vars(false/*only_update_dynamic_vars*/))) {
        LOG_WARN("fail to init sys vars", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObTableRelatedSysVars::update_sys_vars(bool only_update_dynamic_vars)
{
  int ret = OB_SUCCESS;

  if (!is_inited_ && only_update_dynamic_vars) {
    // do nothing
  } else {
    int64_t tenant_id = MTL_ID();
    SMART_VAR(ObSQLSessionInfo, sess_info) {
      ObSchemaGetterGuard schema_guard;
      const ObTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("tenant schema is null", K(ret));
      } else if (OB_FAIL(ObTableApiSessUtil::init_sess_info(tenant_id,
                                                            tenant_schema->get_tenant_name_str(),
                                                            schema_guard,
                                                            sess_info))) {
        LOG_WARN("fail to init sess info", K(ret), K(tenant_id));
      } else {
        // static vars
        if (!only_update_dynamic_vars) {
          int64_t sess_mode_val = 0;
          if (OB_FAIL(sess_info.get_sys_variable(SYS_VAR_OB_KV_MODE, sess_mode_val))) {
            LOG_WARN("fail to get ob_kv_mode variable", K(ret));
          } else {
            static_vars_.set_kv_mode(static_cast<ObKvModeType>(sess_mode_val));
          }
        }

        // dynamic vars
        if (OB_SUCC(ret)) {
          int64_t binlog_row_image = -1;
          int64_t query_record_size_limit = sess_info.get_tenant_query_record_size_limit();
          bool enable_query_response_time_stats = sess_info.enable_query_response_time_stats();
          if (OB_FAIL(sess_info.get_sys_variable(SYS_VAR_BINLOG_ROW_IMAGE, binlog_row_image))) {
            LOG_WARN("fail to get binlog_row_image variable", K(ret));
          } else {
            dynamic_vars_.set_binlog_row_image(binlog_row_image);
            dynamic_vars_.set_query_record_size_limit(query_record_size_limit);
            dynamic_vars_.set_enable_query_response_time_stats(enable_query_response_time_stats);
            omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
            if (tenant_config.is_valid()) {
              int64_t batch_size = tenant_config->kv_group_commit_batch_size;
              dynamic_vars_.set_kv_group_commit_batch_size(batch_size);
              ObString rw_mode = tenant_config->kv_group_commit_rw_mode.get_value_string();
              if (batch_size > 1) {
                if (rw_mode.case_compare("all") == 0) { // 'ALL'
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::ALL);
                } else if (rw_mode.case_compare("read") == 0) {
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::READ);
                } else if (rw_mode.case_compare("write") == 0) {
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::WRITE);
                }
              }
            }
          }
        }
      }
    }
  }

  return ret;
}

/*
  init session pool manager when create tenant
  - we just obly init the metadata when mtl_init.
*/
int ObTableApiSessPoolMgr::mtl_init(ObTableApiSessPoolMgr *&mgr)
{
  return mgr->init();
}

/*
  start tableapi retired session task
  - 5 second interval
  - repeated
*/
int ObTableApiSessPoolMgr::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("table api session pool mgr isn't inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 elimination_task_,
                                 ELIMINATE_SESSION_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi retired session task", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 sys_var_update_task_,
                                 SYS_VAR_REFRESH_DELAY/* 5s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi update sys var task", K(ret));
  } else {
    elimination_task_.is_inited_ = true;
    sys_var_update_task_.is_inited_ = true;
  }

  return ret;
}

// stop tableapi retired session task
void ObTableApiSessPoolMgr::stop()
{
  if (OB_LIKELY(elimination_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
  }
  if (OB_LIKELY(sys_var_update_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
  }
}

// tableapi retired session task wait
void ObTableApiSessPoolMgr::wait()
{
  if (OB_LIKELY(elimination_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
  }
  if (OB_LIKELY(sys_var_update_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
  }
}

/*
  destroy session pool manager.
  - cancel timer task.
  - destroy session pool.
*/
void ObTableApiSessPoolMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // 1. cancel timer task
    if (elimination_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
          elimination_task_.is_inited_ = false;
        }
      }
    }
    if (sys_var_update_task_.is_inited_) {
      bool is_exist = true;
      if (OB_SUCC(TG_TASK_EXIST(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_, is_exist))) {
        if (is_exist) {
          TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
          TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), sys_var_update_task_);
          sys_var_update_task_.is_inited_ = false;
        }
      }
    }

    // 2. destroy session pool
    if (OB_NOT_NULL(pool_)) {
      pool_->destroy();
      pool_ = nullptr;
    }
    allocator_.reset(); // when mtl_destroy, all worker thread has beed existed, no need to lock allocator
    is_inited_ = false;
    LOG_INFO("ObTableApiSessPoolMgr destroy successfully");
  }
}

// init session pool manager.
int ObTableApiSessPoolMgr::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    elimination_task_.sess_pool_mgr_ = this;
    sys_var_update_task_.sess_pool_mgr_ = this;
    is_inited_ = true;
  }

  return ret;
}

/*
  get a session or create a new one if it doesn't exist
  - 1. the user should access the current tenant, so we check tenant id.
  - 2. ObTableApiSessGuard holds the reference count of session.
  - 3. pool_ have been created when login normally,
    But some inner operation did not login, such as ttl operation, so we create a new pool for ttl.
    In the upgrade scenario, the odp does not login again. so we init system vars.
*/
int ObTableApiSessPoolMgr::get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;

  if (credential.tenant_id_ != MTL_ID()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "access wrong tenant");
    LOG_WARN("access wrong tenant", K(ret), K(credential.tenant_id_), K(MTL_ID()));
  } else if (OB_UNLIKELY(OB_ISNULL(pool_)) && OB_FAIL(create_session_pool_safe())) {
    LOG_WARN("fail to create session pool", K(ret), K(credential));
  } else if (OB_ISNULL(pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session pool is null", K(ret), K(credential));
  } else if (OB_FAIL(pool_->get_sess_info(credential, guard))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
  } else if (!sys_vars_.is_inited_ && OB_FAIL(sys_vars_.init())) {
    LOG_WARN("fail to init sys vars", K(ret));
  }

  return ret;
}

/*
  create session pool safely.
  - lock for allocator concurrency.
*/
int ObTableApiSessPoolMgr::create_session_pool_safe()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(pool_)) {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (OB_ISNULL(pool_)) { // double check
      if (OB_FAIL(create_session_pool_unsafe())) {
        LOG_WARN("fail to create session pool", K(ret));
      }
    }
  }

  return ret;
}

int ObTableApiSessPoolMgr::create_session_pool_unsafe()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTableApiSessPool *tmp_pool = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessPool)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessPool", K(ret));
  } else if (FALSE_IT(tmp_pool = new (buf) ObTableApiSessPool())) {
  } else if (OB_FAIL(tmp_pool->init())) {
    LOG_WARN("fail to init sess pool", K(ret));
    allocator_.free(tmp_pool);
    tmp_pool = nullptr;
  } else {
    pool_ = tmp_pool;
  }

  return ret;
}

/*
  update session when login.
  - 1. because tableapi is not aware of changes to system variables,
    users need to log in again to get the latest system variables.
  - 2. we will create a new session node which has the latest system variables
    to replace the old session node.
  - 3. login is handled by sys tenant.
  - 4. login has concurrency, many thread will login together.
*/
int ObTableApiSessPoolMgr::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_ISNULL(pool_)) && OB_FAIL(create_session_pool_safe())) {
    LOG_WARN("fail to create session pool", K(ret), K(credential));
  } else if (OB_FAIL(pool_->update_sess(credential))) {
    LOG_WARN("fail to update sess pool", K(ret), K(credential));
  } else if (!sys_vars_.is_inited_ && OB_FAIL(sys_vars_.init())) {
    LOG_WARN("fail to init sys vars", K(ret));
  }

  return ret;
}

/*
  The background timer tasks to delete session node.
  - retire session node that have not been accessed for more than 3 minutes.
  - recycle session node in retired node list.
*/
void ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_retire_sess_task())) {
    LOG_WARN("fail to run retire sess task", K(ret));
  } else if (OB_FAIL(run_recycle_retired_sess_task())) {
    LOG_WARN("fail to run recycle retired sess task", K(ret));
  }
}

void ObTableApiSessPoolMgr::ObTableApiSessSysVarUpdateTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_update_sys_var_task())) {
    LOG_WARN("fail to run update sys var task", K(ret));
  }
}

/*
  retire session node that have not been accessed for more than 3 minutes.
  - move session node which have not been accessed for more than 3 minutes to retired node list.
*/
int ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::run_retire_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ is null", K(ret));
  } else if (OB_NOT_NULL(sess_pool_mgr_->pool_) && OB_FAIL(sess_pool_mgr_->pool_->retire_session_node())) {
    LOG_WARN("fail to retire session node", K(ret));
  }

  return ret;
}

/*
  evict retired session node from retired node list.
*/
int ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::run_recycle_retired_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ is null", K(ret));
  } else if (OB_NOT_NULL(sess_pool_mgr_->pool_) && OB_FAIL(sess_pool_mgr_->pool_->evict_retired_sess())) {
    LOG_WARN("fail to evict retired sess", K(ret));
  }

  return ret;
}

int ObTableApiSessPoolMgr::ObTableApiSessSysVarUpdateTask::run_update_sys_var_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ is null", K(ret));
  } else if (OB_FAIL(sess_pool_mgr_->update_sys_vars(true/*only_update_dynamic_vars*/))) {
    LOG_WARN("fail to update sys var", K(ret));
  }

  return ret;
}

/*
  init session pool
  - init key_node_map_ which is a hashmap, key is ObTableApiCredential.hash_val_, value is ObTableApiSessNode*
*/
int ObTableApiSessPool::init(int64_t hash_bucket/* = SESS_POOL_DEFAULT_BUCKET_NUM */)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_FAIL(key_node_map_.create(hash::cal_next_prime(hash_bucket),
                                     "HashBucApiSessP",
                                     "HasNodApiSess",
                                     MTL_ID()))) {
      LOG_WARN("fail to init sess pool", K(ret), K(hash_bucket), K(MTL_ID()));
    } else {
      const ObMemAttr attr(MTL_ID(), "TbSessPool");
      if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
        LOG_WARN("fail to init allocator", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

/*
  destroy session pool.
  - free all session.
*/
void ObTableApiSessPool::destroy()
{
  int ret = OB_SUCCESS;
  ObTableApiSessForeachOp op;

  // clear map
  if (OB_FAIL(key_node_map_.foreach_refactored(op))) {
    LOG_WARN("fail to foreach sess key node map", K(ret));
  } else {
    const ObTableApiSessForeachOp::SessKvArray &arr = op.get_key_value_array();
    const int64_t N = arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableApiSessForeachOp::ObTableApiSessKV &kv = arr.at(i);
      ObTableApiSessNode *del_node = nullptr;
      if (OB_FAIL(key_node_map_.erase_refactored(kv.key_, &del_node))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("fail to erase sess from sess hash map", K(ret), K(kv));
        }
      } else if (OB_NOT_NULL(del_node)) {
        del_node->destroy();
        allocator_.free(del_node);
        del_node = nullptr;
      }
    }
  }

  // clear retired_nodes_
  ObLockGuard<ObSpinLock> guard(retired_nodes_lock_); // lock retired_nodes_
  DLIST_FOREACH_REMOVESAFE_X(node, retired_nodes_, OB_SUCC(ret)) {
    if (OB_NOT_NULL(node)) {
      node->destroy();
      allocator_.free(node);
      node = nullptr;
    }
  }

  retired_nodes_.clear();
  key_node_map_.destroy();
  allocator_.reset();
  is_inited_ = false;
  LOG_INFO("ObTableApiSessPool destroy successfully", K(MTL_ID()));
}

/*
  loop all session node to retire.
  - nodes which have not been visited for more than 5 minutes will be retired.
  - move retired node to retired list.
  - why do I need to check whether the node is empty ？
    -- after a node is created, the session may be initialized in init_sess_info() for
    -- more than SESS_RETIRE_TIME (unit migration scenario).
    -- If the node is deleted during this time, it will be used after free.
*/
int ObTableApiSessPool::retire_session_node()
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::fast_current_time();
  ObTableApiSessForeachOp op;

  if (OB_FAIL(key_node_map_.foreach_refactored(op))) {
    LOG_WARN("fail to foreach sess key node map", K(ret));
  } else {
    const ObTableApiSessForeachOp::SessKvArray &arr = op.get_key_value_array();
    const int64_t N = arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableApiSessForeachOp::ObTableApiSessKV &kv = arr.at(i);
      if (cur_time - kv.node_->get_last_active_ts() >= SESS_RETIRE_TIME && !kv.node_->is_empty()) {
        ObTableApiSessNode *del_node = nullptr;
        if (OB_FAIL(key_node_map_.erase_refactored(kv.key_, &del_node))) {
          if (OB_HASH_NOT_EXIST != ret) {
            LOG_WARN("fail to erase sess from sess hash map", K(ret), K(kv.key_));
          }
        } else if (OB_FAIL(move_node_to_retired_list(del_node))) {
          LOG_WARN("fail to move session node to retired list", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableApiSessPool::move_node_to_retired_list(ObTableApiSessNode *node)
{
  int ret = OB_SUCCESS;

  ObLockGuard<ObSpinLock> guard(retired_nodes_lock_); // lock retired_nodes_
  if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("session node is null", K(ret));
  } else if (false == (retired_nodes_.add_last(node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add retired sess node to retired list", K(ret), K(*node));
  }

  return ret;
}

/*
  evit retired session.
  1. remove session val in free_list.
  2. remove session node from retired_nodes_ when node is empty.
  3. free node memory.
  4. delete 2000 session nodes per times
*/
int ObTableApiSessPool::evict_retired_sess()
{
  int ret = OB_SUCCESS;
  int64_t delete_count = 0;
  int64_t cur_time = ObTimeUtility::fast_current_time();
  ObLockGuard<ObSpinLock> guard(retired_nodes_lock_); // lock retired_nodes_

  DLIST_FOREACH_REMOVESAFE_X(node, retired_nodes_, delete_count < BACKCROUND_TASK_DELETE_SESS_NUM) {
    if (cur_time - node->get_last_active_ts() < SESS_UPDATE_TIME_INTERVAL) {
      // do nothing, this node maybe is from ObTableApiSessNodeReplaceOp, some threads maybe is using it.
      // we remove it next retire task.
    } else if (OB_FAIL(node->remove_unused_sess())) {
      LOG_WARN("fail to remove unused sess", K(ret), K(*node));
    } else {
      if (node->is_empty()) {
        ObTableApiSessNode *rm_node = retired_nodes_.remove(node);
        if (OB_NOT_NULL(rm_node)) {
          rm_node->~ObTableApiSessNode();
          allocator_.free(rm_node);
          rm_node = nullptr;
          delete_count++;
        }
      }
    }
  }

  if (delete_count != 0) {
    LOG_INFO("evict retired session node", K(delete_count), K(retired_nodes_.get_size()));
  }

  return ret;
}

int ObTableApiSessPool::get_sess_node(uint64_t key,
                                      ObTableApiSessNode *&node)
{
  ObTableApiSessNodeAtomicOp op;
  int ret = key_node_map_.read_atomic(key, op);

  switch (ret) {
    case OB_SUCCESS: {
        //get node and lock
        if (OB_FAIL(op.get_value(node))) {
          LOG_WARN("fail to lock and get sess node", K(ret), K(key));
        }
        break;
      }
    case OB_HASH_NOT_EXIST: {
        break;
      }
    default: {
        LOG_WARN("fail to get sess node from hash map", K(ret), K(key));
        break;
      }
  }

  return ret;
}

/*
  get session
  1. get session node
  2. create new one if not exist
  3. get session node value
    3.1 if there is no session node val in node list, extend it.

  struct pool {
    map: [key1:node1][key2:node:2]
  }

  struct node {
    list: node_val0 - node_val1 - node_val2 - ... - node_valn
  }
*/
int ObTableApiSessPool::get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *sess_node = nullptr;
  bool need_extend = false;

  if (OB_FAIL(get_sess_node(credential.hash_val_, sess_node))) { // first get
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get sess node", K(ret), K(credential));
    }
  }

  if (OB_FAIL(ret) && OB_HASH_NOT_EXIST != ret) {
    // do nothing
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret) && OB_FAIL(create_and_add_node_safe(credential))) { // not exist, create
    LOG_WARN("fail to create and add session node", K(ret), K(credential));
  } else if (OB_UNLIKELY(OB_ISNULL(sess_node)) && OB_FAIL(get_sess_node(credential.hash_val_, sess_node))) { // get again
    LOG_WARN("fail to get sess node", K(ret), K(credential));
  } else if (OB_UNLIKELY(OB_ISNULL(sess_node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(sess_node->get_sess_node_val(guard))) {
    LOG_WARN("fail to get sess node value", K(ret), K(*sess_node));
  }

  return ret;
}

int ObTableApiSessPool::create_node_safe(ObTableApiCredential &credential, ObTableApiSessNode *&node)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *tmp_node = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessNode", K(ret), K(sizeof(ObTableApiSessNode)));
  } else {
    tmp_node = new (buf) ObTableApiSessNode(credential);
    if (OB_FAIL(tmp_node->init())) {
      LOG_WARN("fail to init session node", K(ret));
    } else {
      node = tmp_node;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_node)) {
    tmp_node->~ObTableApiSessNode();
    allocator_.free(tmp_node);
    tmp_node = nullptr;
    node = nullptr;
  }

  return ret;
}

int ObTableApiSessPool::create_and_add_node_safe(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNode *node = nullptr;
  if (OB_FAIL(create_node_safe(credential, node))) {
    LOG_WARN("fail to create node", K(ret), K(credential));
  } else if (OB_FAIL(key_node_map_.set_refactored(credential.hash_val_, node))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to add sess node to hash map", K(ret), K(credential), K(*node));
    } else {
      ret = OB_SUCCESS; // replace error code
      // other thread has set_refactored, free current node
      node->~ObTableApiSessNode();
      allocator_.free(node);
      node = nullptr;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(node)) {
    node->~ObTableApiSessNode();
    allocator_.free(node);
    node = nullptr;
  }

  return ret;
}

/*
  1. only call in login
  2. move old to retired list when node exist, create new node otherwise.
  3. if the update interval is less than 5 seconds, ignore this update.
*/
int ObTableApiSessPool::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *node = nullptr;
  const uint64_t key = credential.hash_val_;
  int64_t cur_time = ObTimeUtility::fast_current_time();

  if (OB_FAIL(get_sess_node(key, node))) {
    if (OB_HASH_NOT_EXIST == ret) { // not exist, create
      if (OB_FAIL(create_and_add_node_safe(credential))) {
        LOG_WARN("fail to create and add node", K(ret), K(credential));
      } else {
        ATOMIC_STORE(&last_update_ts_, cur_time);
      }
    } else {
      LOG_WARN("fail to get session node", K(ret), K(key));
    }
  } else if (cur_time - last_update_ts_ < SESS_UPDATE_TIME_INTERVAL) {
    // if the update interval is less than 5 seconds, ignore this update.
  } else if (OB_FAIL(replace_sess_node_safe(credential))) { // exist, create and replace old node
    LOG_WARN("fail to replace session node", K(ret), K(credential));
  } else {
    ATOMIC_STORE(&last_update_ts_, cur_time);
  }

  return ret;
}

// create and replace old node in callback function
int ObTableApiSessPool::replace_sess_node_safe(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNodeReplaceOp replace_callback(*this, credential);
  if (OB_FAIL(key_node_map_.atomic_refactored(credential.hash_val_, replace_callback))) {
    LOG_WARN("fail to replace session", K(ret), K(credential));
  }

  return ret;
}

void ObTableApiSessNodeVal::destroy()
{
  sess_info_.~ObSQLSessionInfo();
  is_inited_ = false;
  owner_node_ = nullptr;
  tenant_id_ = OB_INVALID;
}

int ObTableApiSessNodeVal::init_sess_info()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K_(tenant_id));
    } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id_, tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant schema is null", K(ret));
    } else if (OB_FAIL(ObTableApiSessUtil::init_sess_info(tenant_id_,
                                                          tenant_schema->get_tenant_name_str(),
                                                          schema_guard,
                                                          sess_info_))) {
      LOG_WARN("fail to init sess info", K(ret), K_(tenant_id));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

/*
  push session back to queue
*/
int ObTableApiSessNodeVal::push_back_to_queue()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(owner_node_) && OB_FAIL(owner_node_->push_back_sess_to_queue(this))) {
    LOG_WARN("fail to push back session to queue", K(ret), K(owner_node_->sess_queue_.capacity()),
      K(owner_node_->sess_queue_.get_curr_total()), K(owner_node_->sess_ref_cnt_));
  }
  return ret;
}

int ObTableApiSessNode::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    MemoryContext tmp_mem_ctx = nullptr;
    ContextParam param;
    param.set_mem_attr(MTL_ID(), "TbSessNod", ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::ALLOC_THREAD_SAFE);
    if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(tmp_mem_ctx, param))) {
      LOG_WARN("fail to create mem context", K(ret));
    } else if (OB_ISNULL(tmp_mem_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null mem context ", K(ret));
    } else {
      mem_ctx_ = tmp_mem_ctx;
      ObSchemaGetterGuard schema_guard;
      const uint64_t tenant_id = credential_.tenant_id_;
      const uint64_t user_id = credential_.user_id_;
      const uint64_t database_id = credential_.database_id_;
      if (!GCTX.schema_service_->is_tenant_refreshed(tenant_id)) {
        ret = OB_SERVER_IS_INIT;
        LOG_WARN("tenant schema not refreshed yet", KR(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      } else {
        const ObSimpleTenantSchema *tenant_info = nullptr;
        const ObUserInfo *user_info = nullptr;
        const ObSimpleDatabaseSchema *db_info = nullptr;
        if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
          LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
        } else if (OB_ISNULL(tenant_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tenant info is null", K(ret));
        } else if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
          LOG_WARN("fail to get user info", K(ret), K(tenant_id), K(user_id));
        } else if (OB_ISNULL(user_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("user info is null", K(ret));
        } else if (OB_FAIL(schema_guard.get_database_schema(tenant_id, database_id, db_info))) {
          LOG_WARN("fail to get database info", K(ret), K(tenant_id), K(database_id));
        } else if (OB_ISNULL(db_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("database info is null", K(ret));
        } else if (OB_FAIL(ob_write_string(mem_ctx_->get_arena_allocator(), tenant_info->get_tenant_name(), tenant_name_))) {
          LOG_WARN("fail to deep copy tenant name", K(ret));
        } else if (OB_FAIL(ob_write_string(mem_ctx_->get_arena_allocator(), user_info->get_user_name(), user_name_))) {
          LOG_WARN("fail to deep copy user name", K(ret));
        } else if (OB_FAIL(ob_write_string(mem_ctx_->get_arena_allocator(), db_info->get_database_name(), db_name_))) {
          LOG_WARN("fail to deep copy database name", K(ret));
        } else {
          int64_t max_sess_num = 0; // async query processor need 2 session and TTL task need TTL_THREAD_MAX_SCORE at most
          ObMemAttr attr(tenant_id, "TbSessQueue");
          ObTenantBase *tenant_base = MTL_CTX();
          ObTenant *tenant = static_cast<ObTenant *>(tenant_base);
          if (OB_ISNULL(tenant_base)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get tenant is null", K(ret));
          } else if (FALSE_IT(max_sess_num = tenant->max_worker_cnt() * 2 + ObTTLUtil::TTL_THREAD_MAX_SCORE)) {
          } else if (OB_FAIL(sess_queue_.init(max_sess_num, &queue_allocator_, attr))) {
            LOG_WARN("fail to init queues", K(ret), K(max_sess_num));
          } else {
            last_active_ts_ = ObTimeUtility::fast_current_time();
            is_inited_ = true;
          }
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(mem_ctx_)) {
      DESTROY_CONTEXT(mem_ctx_);
      mem_ctx_ = nullptr;
    }

  }

  return ret;
}

void ObTableApiSessNode::destroy()
{
  int ret = OB_SUCCESS;
  ObTableApiSessNodeVal *sess = nullptr;

  while (OB_SUCC(sess_queue_.pop(sess))) {
    if (OB_NOT_NULL(sess)) {
      sess->destroy();
      if (OB_NOT_NULL(mem_ctx_)) {
        mem_ctx_->free(sess);
      }
    }
  }

  sess_queue_.destroy();
  queue_allocator_.reset();

  if (OB_NOT_NULL(mem_ctx_)) {
    DESTROY_CONTEXT(mem_ctx_);
    mem_ctx_ = nullptr;
  }
  sess_ref_cnt_ = 0;
}

int ObTableApiSessNode::remove_unused_sess()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("session node is not inited", K(ret));
  } else {
    ObTableApiSessNodeVal *sess = nullptr;
    while (OB_SUCC(sess_queue_.pop(sess))) {
      if (OB_NOT_NULL(sess)) {
        sess->destroy();
        if (OB_NOT_NULL(mem_ctx_)) {
          mem_ctx_->free(sess);
        }
      }
    }
  }

  if (ret == OB_ENTRY_NOT_EXIST) {
    ret = OB_SUCCESS;
  }

  return ret;
}

/*
  get session node val
  - add ref cnt first to avoid cleaning up by background tasks
  - pop session
  - dec ref cnt if failed
*/
int ObTableApiSessNode::get_sess_node_val(ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNodeVal *tmp_val = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("session node is not inited", K(ret));
  } else {
    ATOMIC_INC(&sess_ref_cnt_); // add ref cnt first
    if (OB_FAIL(sess_queue_.pop(tmp_val))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("fail to pop session from queue", K(ret), K(sess_queue_.get_total()));
      } else {
        ret = OB_SUCCESS;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(tmp_val)) { // has no session in sess_queue_, need extend
        if (OB_FAIL(extend_and_get_sess_val(guard))) {
          LOG_WARN("fail to extend and get sess val", K(ret));
        }
      } else {
        guard.sess_node_val_ = tmp_val;
      }
    } else {
      ATOMIC_DEC(&sess_ref_cnt_); // dec ref cnt if failed
    }
  }

  ATOMIC_STORE(&last_active_ts_, ObTimeUtility::fast_current_time()); // update last_active_ts_

  return ret;
}

/*
  extend a session node val and put it to guard
  - alloc new session node val.
  - add to use list.
  - put to guard.
*/
int ObTableApiSessNode::extend_and_get_sess_val(ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("session node is not inited", K(ret));
  } else if (OB_ISNULL(mem_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("memory context is null", K(ret));
  } else {
    ObTableApiSessNodeVal *val = nullptr;
    void *buf = nullptr;
    ObMemAttr attr(MTL_ID(), "TbSessNodVal", ObCtxIds::DEFAULT_CTX_ID);
    if (OB_ISNULL(buf = mem_ctx_->allocf(sizeof(ObTableApiSessNodeVal), attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for ObTableApiSessNodeVal", K(ret), K(sizeof(ObTableApiSessNodeVal)));
    } else {
      val = new (buf) ObTableApiSessNodeVal(this, credential_.tenant_id_);
      if (OB_FAIL(val->init_sess_info())) {
        LOG_WARN("fail to init sess info", K(ret), K(*val));
      } else {
        guard.sess_node_val_ = val;
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(val)) {
      val->~ObTableApiSessNodeVal();
      mem_ctx_->free(val);
      val = nullptr;
      buf = nullptr;
    }
  }

  return ret;
}

int ObTableApiSessNodeAtomicOp::get_value(ObTableApiSessNode *&node)
{
  int ret = OB_SUCCESS;

  node = nullptr;
  if (OB_ISNULL(sess_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess node is not init", K(ret));
  } else {
    node = sess_node_;
  }

  return ret;
}

/*
  replace session node operation
  1. create new node.
  2. replace them.
  3. move old node to retired list.
*/
int ObTableApiSessNodeReplaceOp::operator()(MapKV &entry)
{
  int ret = OB_SUCCESS;

  if (nullptr == entry.second) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session node", K(ret), K(entry.first));
  } else {
    // 1. create new session node
    ObTableApiSessNode *new_node = nullptr;
    if (OB_FAIL(pool_.create_node_safe(credential_, new_node))) {
      LOG_WARN("fail to create node", K(ret), K_(credential));
    } else {
      // 2. replace
      ObTableApiSessNode *old_node = entry.second;
      entry.second = new_node;
      // 3. move old node to retired list
      pool_.move_node_to_retired_list(old_node); // 添加到链表末尾，不会出错，故不判断返回值
    }
  }

  return ret;
}

int ObTableApiSessForeachOp::operator()(MapKV &entry)
{
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(key_value_array_.push_back(ObTableApiSessKV(entry.first, entry.second)))) {
    LOG_WARN("fail to push back key value", K(ret), K(entry.first));
  }

  return ret;
}

int ObTableApiSessUtil::init_sess_info(uint64_t tenant_id,
                                       const common::ObString &tenant_name,
                                       ObSchemaGetterGuard &schema_guard,
                                       sql::ObSQLSessionInfo &sess_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(sess_info.init(0, 0, nullptr))) {
    LOG_WARN("fail to init session into", K(ret));
  } else if (OB_FAIL(sess_info.init_tenant(tenant_name, tenant_id))) {
    LOG_WARN("fail to init session tenant", K(ret), K(tenant_id));
  } else if (OB_FAIL(sess_info.load_all_sys_vars(schema_guard))) {
    LOG_WARN("fail to load session system variable", K(ret));
  }

  return ret;
}
}  // namespace table
}  // namespace oceanbase
