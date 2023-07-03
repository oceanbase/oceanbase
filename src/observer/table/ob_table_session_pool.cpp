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
namespace oceanbase
{
namespace table
{

int ObTableApiSessPoolMgr::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ObMemAttr bucket_attr, node_attr;
    bucket_attr.label_ = "HashBucApiSessM";
    node_attr.label_ = "HasNodApiSessP";
    if (OB_FAIL(sess_pool_map_.create(hash::cal_next_prime(16),
                                      bucket_attr,
                                      node_attr))) {
      LOG_WARN("failed to init tableapi session pool", K(ret));
    } else {
      elimination_task_.sess_pool_mgr_ = this;
      timer_.set_run_wrapper(MTL_CTX());
      if (OB_FAIL(timer_.init())) {
        LOG_WARN("fail to init timer", K(ret));
      } else if (OB_FAIL(timer_.schedule(elimination_task_, ELIMINATE_SESSION_DELAY, true))) {
        LOG_WARN("fail to schedule session pool elimination task. ", K(ret));
      } else if (OB_FAIL(timer_.start())) {
        LOG_WARN("fail to start  session pool elimination task timer.", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}
void ObTableApiSessPoolMgr::stop()
{
  timer_.stop();
}
void ObTableApiSessPoolMgr::wait()
{
  timer_.wait();
}
void ObTableApiSessPoolMgr::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    timer_.destroy();
    for (SessPoolMap::iterator it = sess_pool_map_.begin();
         it != sess_pool_map_.end();
         it++) {
      if (OB_ISNULL(it->second)) {
        BACKTRACE_RET(ERROR, OB_ERR_UNEXPECTED, true, "session pool is null");
      } else {
        it->second->~ObTableApiSessPool();
        ob_free(it->second);
      }
    }
  }
}

int ObTableApiSessPoolMgr::get_sess_info(ObTableApiCredential &credential,
                                         ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTableApiSessPoolGuard &pool_guard = guard.get_sess_pool_guard();
  if (OB_FAIL(get_session_pool(credential.tenant_id_, pool_guard))) {
    LOG_WARN("fail to get session pool", K(ret));
  } else if (OB_FAIL(pool_guard.get_sess_pool()->get_sess_info(credential, guard))) {
    LOG_WARN("fail to get sess info", K(ret), K(credential));
  }

  return ret;
}

int ObTableApiSessPoolMgr::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;
  ObTableApiSessPoolGuard guard;
  const uint64_t tenant_id = credential.tenant_id_;
  if (OB_FAIL(get_session_pool(tenant_id, guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      if (OB_FAIL(extend_sess_pool(tenant_id, guard))) {
        LOG_WARN("fail to extend sess pool", K(ret), K(tenant_id));
      }
    } else {
      LOG_WARN("fait to get session pool", K(ret), K(tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(guard.get_sess_pool()->update_sess(credential))) {
    LOG_WARN("fail to update sess pool", K(ret), K(tenant_id), K(credential));
  }

  return ret;
}

int ObTableApiSessPoolMgr::extend_sess_pool(uint64_t tenant_id,
                                            ObTableApiSessPoolGuard &guard)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTableApiSessPool *tmp_pool = nullptr;

  if (OB_ISNULL(buf = ob_malloc(sizeof(ObTableApiSessPool), "ApiSessPool"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessPool", K(ret));
  } else if (FALSE_IT(tmp_pool = new (buf) ObTableApiSessPool(tenant_id))) {
  } else {
    if (OB_FAIL(tmp_pool->init())) {
      LOG_WARN("fail to init sess pool", K(ret), K(tenant_id));
    } else if (OB_FAIL(sess_pool_map_.set_refactored(tenant_id, tmp_pool))) {
      if (OB_HASH_EXIST != ret) {
        LOG_WARN("fail to add sess pool to hash map", K(ret), K(*tmp_pool));
      } else { // this pool has been set by other thread, free it
        tmp_pool->~ObTableApiSessPool();
        ob_free(tmp_pool);
        tmp_pool = nullptr;
        // get sess pool
        ObTableApiSessPoolMgrAtomic op;
        if (OB_FAIL(sess_pool_map_.read_atomic(tenant_id, op))) {
          LOG_WARN("fail to get sess pool", K(ret), K(tenant_id));
        } else {
          ObTableApiSessPool *pool = op.get_session_pool();
          pool->inc_ref_count();
          guard.set_sess_pool(pool);
        }
      }
    } else {
      tmp_pool->inc_ref_count();
      guard.set_sess_pool(tmp_pool);
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(tmp_pool)) {
      tmp_pool->~ObTableApiSessPool();
      ob_free(tmp_pool);
      tmp_pool = nullptr;
    }
  }

  return ret;
}

int ObTableApiSessPoolMgr::get_session_pool(uint64_t tenant_id, ObTableApiSessPoolGuard &guard)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ObTableApiSessPoolMgrAtomic op;
    ret = sess_pool_map_.read_atomic(tenant_id, op);
    if (OB_SUCC(ret)) { // exist
      ObTableApiSessPool *pool = op.get_session_pool();
      pool->inc_ref_count();
      guard.set_sess_pool(pool);
    } else if (OB_HASH_NOT_EXIST == ret) {
      // do nothing
    } else {
      LOG_WARN("fait to atomic get session pool", K(ret), K(tenant_id));
    }
  }

  return ret;
}

// 1. 淘汰长期未被使用的session
// 2. 回收淘汰的session
void ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::runTimerTask()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ not inited", K(ret));
  } else if (OB_FAIL(run_retire_sess_task())) {
    LOG_WARN("fail to run retire sess task", K(ret));
  } else if (OB_FAIL(run_recycle_retired_sess_task())) {
    LOG_WARN("fail to run recycle retired sess task", K(ret));
  }
}

int ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::run_retire_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ not inited", K(ret));
  } else {
    ObTableApiSessPoolForeachOp op;
    if (OB_FAIL(sess_pool_mgr_->sess_pool_map_.foreach_refactored(op))) {
      LOG_WARN("fail to foreach sess pool hash map", K(ret));
    } else {
      const ObTableApiSessPoolForeachOp::TelantIdArray &arr = op.get_telant_id_array();
      const int64_t N = arr.count();
      for (int64_t i = 0; i < N && OB_SUCC(ret); i++) {
        uint64_t tenant_id = arr.at(i);
        ObTableApiSessPoolGuard pool_guard;
        if (OB_FAIL(sess_pool_mgr_->get_session_pool(tenant_id, pool_guard))) {
          LOG_WARN("fail to get sess pool", K(ret), K(tenant_id));
        } else if (OB_FAIL(pool_guard.get_sess_pool()->move_sess_to_retired_list())) {
          LOG_WARN("fail to move retired session", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableApiSessPoolMgr::ObTableApiSessEliminationTask::run_recycle_retired_sess_task()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sess_pool_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sess_pool_mgr_ not inited", K(ret));
  } else {
    ObTableApiSessPoolForeachOp op;
    if (OB_FAIL(sess_pool_mgr_->sess_pool_map_.foreach_refactored(op))) {
      LOG_WARN("fail to foreach sess pool hash map", K(ret));
    } else {
      const ObTableApiSessPoolForeachOp::TelantIdArray &arr = op.get_telant_id_array();
      const int64_t N = arr.count();
      for (int64_t i = 0; i < N && OB_SUCC(ret); i++) {
        uint64_t tenant_id = arr.at(i);
        ObTableApiSessPoolGuard pool_guard;
        ObTableApiSessPool *pool = nullptr;
        if (OB_FAIL(sess_pool_mgr_->get_session_pool(tenant_id, pool_guard))) {
          LOG_WARN("fail to get sess pool", K(ret), K(tenant_id));
        } else if (FALSE_IT(pool = pool_guard.get_sess_pool())) {
        } else if (OB_FAIL(pool->evict_retired_sess())) {
          LOG_WARN("fail to evict retired sess", K(ret));
        } else if (pool->is_empty()) {
          // 1. 标记delete，由最后一个持有者释放内存
          // 2. 从hash表中摘出，避免新的请求获取到这个pool
          pool->set_deleted();
          ObTableApiSessPool *del_pool = nullptr;
          if (OB_FAIL(sess_pool_mgr_->sess_pool_map_.erase_refactored(tenant_id, &del_pool))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("fail to erase sess pool from sess pool hash map", K(ret), K(tenant_id));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableApiSessPoolMgrAtomic::operator() (MapKV &entry)
{
  int ret = common::OB_SUCCESS;

  if (OB_ISNULL(entry.second)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    sess_pool_ = entry.second;
  }

  return ret;
}

int ObTableApiSessPool::init(int64_t hash_bucket/* = SESS_POOL_DEFAULT_BUCKET_NUM */)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    if (OB_FAIL(key_node_map_.create(hash::cal_next_prime(hash_bucket),
                                     "HashBucApiSessP",
                                     "HasNodApiSess",
                                     tenant_id_))) {
      LOG_WARN("fail to init sess pool", K(ret), K(hash_bucket), K_(tenant_id));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObTableApiSessPool::destroy()
{
  if (is_inited_) {
    if (OB_SUCCESS != evict_all_session()) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to evict all seesion");
    }
    is_inited_ = false;
  }
}

// 将过期的node从hash map中摘掉，加入retired_nodes_链表中
int ObTableApiSessPool::move_sess_to_retired_list()
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
        if (OB_FAIL(move_sess_to_retired_list(kv.key_))) {
          LOG_WARN("fail to move retired session", K(ret), K(kv.key_));
        }
      }
    }
  }

  return ret;
}

int ObTableApiSessPool::move_sess_to_retired_list(uint64_t key)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *del_node = nullptr;

  if (OB_FAIL(key_node_map_.erase_refactored(key, &del_node))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to erase sess from sess hash map", K(ret), K(key));
    }
  } else {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (false == (retired_nodes_.add_last(del_node))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to add retired sess node to retired list", K(ret), K(*del_node));
    }
  }

  return ret;
}

int ObTableApiSessPool::move_sess_to_retired_list(ObTableApiSessNode *node)
{
  int ret = OB_SUCCESS;

  ObLockGuard<ObSpinLock> guard(lock_);
  if (false == (retired_nodes_.add_last(node))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to add retired sess node to retired list", K(ret), K(*node));
  }

  return ret;
}

// 清理retired_nodes_中没有引用的node
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

int ObTableApiSessPool::evict_all_session()
{
  int ret = OB_SUCCESS;
  ObTableApiSessForeachOp op;
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
      } else {
        ObLockGuard<ObSpinLock> guard(lock_);
        del_node->~ObTableApiSessNode();
        allocator_.free(del_node);
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

int ObTableApiSessPool::get_sess_info(ObTableApiCredential &credential, ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;
  ObTableApiSessNode *sess_node = nullptr;
  ObTableApiSessNodeVal *sess_val = nullptr;
  bool need_extend = false;

  if (OB_FAIL(get_sess_node(credential.user_id_, sess_node))) {
    LOG_WARN("fail to get sess node", K(ret), K(credential));
  }

  if (OB_HASH_NOT_EXIST == ret) {
    if (OB_FAIL(create_and_add_node(credential))) {
      LOG_WARN("fail to create and add session node to session pool", K(ret), K(credential));
    } else if (OB_FAIL(get_sess_node(credential.user_id_, sess_node))) { // get again
      LOG_WARN("fail to get sess node", K(ret), K(credential));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sess_node->get_sess_node_val(sess_val))) { // exist
      LOG_WARN("fail to get sess node value", K(ret), K(*sess_node));
    } else if (OB_ISNULL(sess_val)) {
      need_extend = true;
    } else {
      guard.sess_node_val_ = sess_val;
    }
  }

  if (need_extend) {
    if (OB_FAIL(sess_node->extend_sess_val(guard))) {
      LOG_WARN("fail to extend sess val", K(ret), K(*sess_node), K(credential));
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(sess_node)) {
    int64_t cur_time = ObTimeUtility::current_time();
    ATOMIC_STORE(&sess_node->last_active_ts_, cur_time);
  }

  return ret;
}

int ObTableApiSessPool::create_node(ObTableApiCredential &credential, ObTableApiSessNode *&node)
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

int ObTableApiSessPool::create_and_add_node(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNode *node = nullptr;
  if (OB_FAIL(create_node(credential, node))) {
    LOG_WARN("fail to create node", K(ret), K(credential));
  } else if (OB_FAIL(key_node_map_.set_refactored(credential.user_id_, node))) {
    if (OB_HASH_EXIST != ret) {
      LOG_WARN("fail to add sess node to hash map", K(ret), K(credential.user_id_), K(*node));
    } else { // this node has been set by other thread, free it
      ObLockGuard<ObSpinLock> guard(lock_);
      node->~ObTableApiSessNode();
      allocator_.free(node);
      node = nullptr;
    }
  }

  return ret;
}

// 1. login时调用
// 2. 不存在，创建; 存在，旧的移动到淘汰链表, 添加新的node
int ObTableApiSessPool::update_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNode *node = nullptr;
  const uint64_t key = credential.user_id_;
  if (OB_FAIL(get_sess_node(key, node))) {
    if (OB_HASH_NOT_EXIST == ret) { // not exist, create
      if (OB_FAIL(create_and_add_node(credential))) {
        LOG_WARN("fail to create and add node", K(ret), K(credential));
      }
    } else {
      LOG_WARN("fail to get session node", K(ret), K(key));
    }
  } else { // exist, 替换node，old node移动到淘汰链表等待淘汰
    if (OB_FAIL(replace_sess(credential))) {
      LOG_WARN("fail to replace session node", K(ret), K(credential));
    }
  }

  return ret;
}

int ObTableApiSessPool::replace_sess(ObTableApiCredential &credential)
{
  int ret = OB_SUCCESS;

  ObTableApiSessNodeReplaceOp replace_callback(*this, credential);
  if (OB_FAIL(key_node_map_.atomic_refactored(credential.user_id_, replace_callback))) {
    LOG_WARN("fail to replace session", K(ret), K(credential));
  }

  return ret;
}

int64_t ObTableApiSessPool::inc_ref_count()
{
  return ATOMIC_AAF((uint64_t *)&ref_count_, 1);
}

void ObTableApiSessPool::dec_ref_count()
{
  (void)ATOMIC_SAF((uint64_t *)&ref_count_, 1);
  if (is_deleted() && 0 == ref_count_) {
    this->~ObTableApiSessPool();
    ob_free(this);
  }
}

int ObTableApiSessNodeVal::init_sess_info()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
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
                                                          &allocator_,
                                                          schema_guard,
                                                          sess_info_))) {
      LOG_WARN("fail to init sess info", K(ret), K(tenant_id_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

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
      rm_sess->~ObTableApiSessNodeVal();
      allocator_.free(rm_sess);
    }
  }
  DLIST_FOREACH_REMOVESAFE(sess, used_list) {
    ObTableApiSessNodeVal *rm_sess = used_list.remove(sess);
    if (OB_NOT_NULL(rm_sess)) {
      rm_sess->~ObTableApiSessNodeVal();
      allocator_.free(rm_sess);
    }
  }
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

// 从free list中取出，添加到used list中
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

int ObTableApiSessNode::extend_sess_val(ObTableApiSessGuard &guard)
{
  int ret = OB_SUCCESS;

  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTableApiSessNodeVal)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem for ObTableApiSessNodeVal", K(ret), K(sizeof(ObTableApiSessNodeVal)));
  } else {
    ObTableApiSessNodeVal *val = new (buf) ObTableApiSessNodeVal(tenant_id_, this);
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

int ObTableApiSessNodeReplaceOp::operator()(MapKV &entry)
{
  int ret = OB_SUCCESS;

  if (nullptr == entry.second) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session node", K(ret), K(entry.first));
  } else {
    // 1. 创建新的session node
    ObTableApiSessNode *new_node = nullptr;
    if (OB_FAIL(pool_.create_node(credential_, new_node))) {
      LOG_WARN("fail to create node", K(ret), K_(credential));
    } else {
      // 2. 替换
      ObTableApiSessNode *old_node = entry.second;
      entry.second = new_node;
      // 3. old node移动到淘汰链表
      pool_.move_sess_to_retired_list(old_node); // 添加到链表末尾，不会出错，故不判断返回值
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

int ObTableApiSessPoolForeachOp::operator()(MapKV &entry)
{
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(telant_ids_.push_back(entry.first))) {
    LOG_WARN("fail to push back key", K(ret), K(entry.first));
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
