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

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

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
  - 60 second interval
  - repeated
*/
int ObTableApiSessPoolMgr::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("table api session pool mgr isn't inited", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
                                 elimination_task_,
                                 ELIMINATE_SESSION_DELAY/* 60s */,
                                 true/* repeat */))) {
    LOG_WARN("failed to schedule tableapi retired session task", K(ret));
  } else {
    elimination_task_.is_inited_ = true;
  }

  return ret;
}

// stop tableapi retired session task
void ObTableApiSessPoolMgr::stop()
{
  if (OB_LIKELY(elimination_task_.is_inited_)) {
    TG_CANCEL_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
  }
}

// tableapi retired session task wait
void ObTableApiSessPoolMgr::wait()
{
  if (OB_LIKELY(elimination_task_.is_inited_)) {
    TG_WAIT_TASK(MTL(omt::ObSharedTimer*)->get_tg_id(), elimination_task_);
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

    // 2. destroy session pool
    if (OB_NOT_NULL(pool_)) {
      pool_->destroy();
      pool_ = nullptr;
    }
    allocator_.reset();
    is_inited_ = false;
  }
}

// init session pool manager.
int ObTableApiSessPoolMgr::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    elimination_task_.sess_pool_mgr_ = this;
    is_inited_ = true;
  }

  return ret;
}

/*
  get a session or create a new one if it doesn't exist
  - 1. the user should access the current tenant, so we check tenant id.
  - 2. ObTableApiSessGuard holds the reference count of session.
  - 3. pool_ have been created when login normally,
    but some inner operation did not login, such as ttl operation, so we create a new pool for ttl.
*/
int ObTableApiSessPoolMgr::get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;

  if (credential.tenant_id_ != MTL_ID()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("access wrong tenant", K(ret), K(credential.tenant_id_), K(MTL_ID()));
  } else if (OB_UNLIKELY(OB_ISNULL(pool_)) && OB_FAIL(create_session_pool_safe())) {
    LOG_WARN("fail to create session pool", K(ret), K(credential));
  } else if (OB_ISNULL(pool_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session pool is null", K(ret), K(credential));
  } else if (OB_FAIL(pool_->get_sess_info(credential, guard))) {
    LOG_WARN("fail to get session info", K(ret), K(credential));
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
      is_inited_ = true;
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
      }
    }
  }

  // clear retired_nodes_
  DLIST_FOREACH(node, retired_nodes_) {
    if (OB_NOT_NULL(node)) {
      node->destroy();
    }
  }

  retired_nodes_.clear();
  key_node_map_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

/*
  loop all session node to retire.
  - nodes which have not been visited for more than 5 minutes will be retired.
  - move retired node to retired list.
*/
int ObTableApiSessPool::retire_session_node()
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  ObTableApiSessForeachOp op;

  if (OB_FAIL(key_node_map_.foreach_refactored(op))) {
    LOG_WARN("fail to foreach sess key node map", K(ret));
  } else {
    const ObTableApiSessForeachOp::SessKvArray &arr = op.get_key_value_array();
    const int64_t N = arr.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      const ObTableApiSessForeachOp::ObTableApiSessKV &kv = arr.at(i);
      if (cur_time - kv.node_->get_last_active_ts() >= SESS_RETIRE_TIME) {
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

  ObLockGuard<ObSpinLock> guard(lock_);
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
*/
int ObTableApiSessPool::evict_retired_sess()
{
  int ret = OB_SUCCESS;

  DLIST_FOREACH(node, retired_nodes_) {
    if (OB_FAIL(node->remove_unused_sess())) {
      LOG_WARN("fail to remove unused sess", K(ret), K(*node));
    } else {
      ObLockGuard<ObSpinLock> guard(lock_);
      if (node->is_empty()) {
        ObTableApiSessNode *rm_node = retired_nodes_.remove(node);
        if (OB_NOT_NULL(rm_node)) {
          rm_node->~ObTableApiSessNode();
          allocator_.free(rm_node);
          rm_node = nullptr;
        }
      }
    }
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
  } else {
    ObTableApiSessNodeVal *sess_val = nullptr;
    if (OB_FAIL(sess_node->get_sess_node_val(sess_val))) {
      LOG_WARN("fail to get sess node value", K(ret), K(*sess_node));
    } else if (OB_ISNULL(sess_val)) {
      need_extend = true;
    } else {
      guard.sess_node_val_ = sess_val;
    }
  }

  if (need_extend) {
    if (OB_FAIL(sess_node->extend_and_get_sess_val(guard))) {
      LOG_WARN("fail to extend and get sess val", K(ret), K(*sess_node), K(credential));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(sess_node)) {
    int64_t cur_time = ObTimeUtility::current_time();
    ATOMIC_STORE(&sess_node->last_active_ts_, cur_time);
  }

  return ret;
}

int ObTableApiSessPool::create_node_safe(ObTableApiCredential &credential, ObTableApiSessNode *&node)
{
  int ret = OB_SUCCESS;
  ObLockGuard<ObSpinLock> guard(lock_);
  ObTableApiSessNode *tmp_node = nullptr;
  void *buf = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessNode)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessNode", K(ret), K(sizeof(ObTableApiSessNode)));
  } else {
    tmp_node = new (buf) ObTableApiSessNode(credential);
    tmp_node->last_active_ts_ = ObTimeUtility::current_time();
    node = tmp_node;
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
    }
    // this node has been set by other thread, free it
    ObLockGuard<ObSpinLock> guard(lock_);
    node->~ObTableApiSessNode();
    allocator_.free(node);
    node = nullptr;
  }

  return ret;
}

/*
  1. only call in login
  2. move old to retired list when node exist, create new node otherwise.
*/
int ObTableApiSessPool::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNode *node = nullptr;
  const uint64_t key = credential.hash_val_;
  if (OB_FAIL(get_sess_node(key, node))) {
    if (OB_HASH_NOT_EXIST == ret) { // not exist, create
      if (OB_FAIL(create_and_add_node_safe(credential))) {
        LOG_WARN("fail to create and add node", K(ret), K(credential));
      }
    } else {
      LOG_WARN("fail to get session node", K(ret), K(key));
    }
  } else if (OB_FAIL(replace_sess_node_safe(credential))) { // exist, create and replace old node
    LOG_WARN("fail to replace session node", K(ret), K(credential));
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
  sess_info_.destroy();
  allocator_.reset();
  is_inited_ = false;
  owner_node_ = nullptr;
}

int ObTableApiSessNodeVal::init_sess_info()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTenantSchema *tenant_schema = nullptr;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret), K(MTL_ID()));
    } else if (OB_FAIL(schema_guard.get_tenant_info(MTL_ID(), tenant_schema))) {
      LOG_WARN("fail to get tenant schema", K(ret), K(MTL_ID()));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("tenant schema is null", K(ret));
    } else if (OB_FAIL(ObTableApiSessUtil::init_sess_info(MTL_ID(),
                                                          tenant_schema->get_tenant_name_str(),
                                                          &allocator_,
                                                          schema_guard,
                                                          sess_info_))) {
      LOG_WARN("fail to init sess info", K(ret), K(MTL_ID()));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

/*
  move node val to free list from used list.
  - remove from used list.
  - add to free list.
*/
void ObTableApiSessNodeVal::give_back_to_free_list()
{
  if (OB_NOT_NULL(owner_node_)) {
    ObDList<ObTableApiSessNodeVal> &free_list = owner_node_->sess_lists_.free_list_;
    ObDList<ObTableApiSessNodeVal> &used_list = owner_node_->sess_lists_.used_list_;
    ObLockGuard<ObSpinLock> guard(owner_node_->sess_lists_.lock_);
    ObTableApiSessNodeVal *rm_sess = nullptr;
    if (OB_ISNULL(rm_sess = (used_list.remove(this)))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to remove sess from used list", K(*rm_sess));
    } else if (false == (free_list.add_last(rm_sess))) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to add sess val to free list", K(*rm_sess));
    }
  }
}

void ObTableApiSessNode::destroy()
{
  int ret = OB_SUCCESS;
  ObDList<ObTableApiSessNodeVal> &free_list = sess_lists_.free_list_;
  ObDList<ObTableApiSessNodeVal> &used_list = sess_lists_.used_list_;

  DLIST_FOREACH_REMOVESAFE(sess, free_list) {
    ObTableApiSessNodeVal *rm_sess = free_list.remove(sess);
    if (OB_NOT_NULL(rm_sess)) {
      rm_sess->destroy();
    }
  }
  DLIST_FOREACH_REMOVESAFE(sess, used_list) {
    ObTableApiSessNodeVal *rm_sess = used_list.remove(sess);
    if (OB_NOT_NULL(rm_sess)) {
      rm_sess->destroy();
    }
  }
  allocator_.reset();
}

int ObTableApiSessNode::remove_unused_sess()
{
  int ret = OB_SUCCESS;

  ObDList<ObTableApiSessNodeVal> &free_list = sess_lists_.free_list_;
  if (free_list.is_empty()) {
    // do nothing
  } else {
    ObLockGuard<ObSpinLock> guard(sess_lists_.lock_);
    DLIST_FOREACH_REMOVESAFE(sess, free_list) {
      ObTableApiSessNodeVal *rm_sess = free_list.remove(sess);
      if (OB_NOT_NULL(rm_sess)) {
        rm_sess->~ObTableApiSessNodeVal();
        allocator_.free(rm_sess);
        rm_sess = nullptr;
      }
    }
  }

  return ret;
}

/*
  get session node val
  - remove and get from free list.
  - add to used list.
*/
int ObTableApiSessNode::get_sess_node_val(ObTableApiSessNodeVal *&val)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNodeVal *tmp_val = nullptr;
  ObDList<ObTableApiSessNodeVal> &free_list = sess_lists_.free_list_;
  ObDList<ObTableApiSessNodeVal> &used_list = sess_lists_.used_list_;

  ObLockGuard<ObSpinLock> guard(sess_lists_.lock_);
  if (!free_list.is_empty()) {
    tmp_val = free_list.remove_first();
    // move to used list
    if (false == (used_list.add_last(tmp_val))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add sess val to used list", K(ret), K(*tmp_val));
    } else {
      val = tmp_val;
    }
  }

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

  ObLockGuard<ObSpinLock> alloc_guard(lock_); // avoid concurrent allocator_.alloc
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessNodeVal)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessNodeVal", K(ret), K(sizeof(ObTableApiSessNodeVal)));
  } else {
    ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(this);
    if (OB_FAIL(val->init_sess_info())) {
      LOG_WARN("fail to init sess info", K(ret), K(*val));
    } else {
      ObLockGuard<ObSpinLock> lock_guard(sess_lists_.lock_);
      if (false == (sess_lists_.used_list_.add_last(val))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to add sess val to list", K(ret), K(*val));
      } else {
        guard.sess_node_val_ = val;
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator_.free(buf);
    buf = nullptr;
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
                                       ObIAllocator *allocator,
                                       ObSchemaGetterGuard &schema_guard,
                                       sql::ObSQLSessionInfo &sess_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(sess_info.init(0, 0, allocator))) {
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
