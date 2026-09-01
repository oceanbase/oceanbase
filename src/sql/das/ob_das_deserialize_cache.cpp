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

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_deserialize_cache.h"
#include "lib/utility/utility.h"
#include "share/ob_server_struct.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"


namespace oceanbase
{
namespace sql
{

ObDasDeserCacheSessionPool::ObDasDeserCacheSessionPool()
  : session_array_(),
    session_queue_(),
    mem_context_(nullptr),
    alloc_lock_(common::ObLatchIds::OB_AREAN_ALLOCATOR_LOCK),
    is_inited_(false)
{
  MEMSET(session_array_, 0, sizeof(session_array_));
}

ObDasDeserCacheSessionPool::~ObDasDeserCacheSessionPool()
{
  destroy();
}

int ObDasDeserCacheSessionPool::init(lib::MemoryContext &mem_context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("das deser cache session pool init twice", K(ret));
  } else if (OB_ISNULL(mem_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("das deser cache session pool memory context is null", K(ret));
  } else if (OB_FAIL(session_queue_.init(MAX_IDLE_SESSION_COUNT,
                                         reinterpret_cast<char *>(session_array_)))) {
    LOG_WARN("init das deser cache session pool failed", K(ret));
  } else {
    mem_context_ = &mem_context;
    is_inited_ = true;
  }
  return ret;
}

int ObDasDeserCacheSessionPool::alloc_session(uint64_t tenant_id,
                                              ObSQLSessionInfo *&session)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  session = nullptr;
  if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session manager is null", K(ret));
  } else if (OB_ISNULL(mem_context_) || OB_ISNULL(*mem_context_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das deser cache session pool memory context is null", K(ret));
  } else {
    {
      common::ObSpinLockGuard guard(alloc_lock_);
      buf = (*mem_context_)->allocf(
          sizeof(ObSQLSessionInfo),
          common::ObMemAttr(tenant_id, common::ObModIds::OB_SQL_SESSION));
    }
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate das deser cache session failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo *new_session = new (buf) ObSQLSessionInfo(tenant_id);
    uint32_t mock_sid = ObSQLSessionInfo::INVALID_SESSID;
    const uint64_t proxy_sid = 0;
    const bool in_mgr = false;
    if (OB_FAIL(GCTX.session_mgr_->create_sessid(mock_sid, in_mgr))) {
      LOG_WARN("create das deser cache session id failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(new_session->init(mock_sid, proxy_sid, nullptr, nullptr, 0, tenant_id))) {
      LOG_WARN("init das deser cache session failed", K(ret), K(mock_sid), K(tenant_id));
    } else {
      new_session->set_is_remote(true);
      session = new_session;
    }
    if (OB_FAIL(ret)) {
      new_session->~ObSQLSessionInfo();
      common::ObSpinLockGuard guard(alloc_lock_);
      (*mem_context_)->free(new_session);
    }
  }
  return ret;
}

int ObDasDeserCacheSessionPool::acquire(uint64_t tenant_id,
                                        ObSQLSessionInfo *&session,
                                        bool &reused)
{
  int ret = OB_SUCCESS;
  session = nullptr;
  reused = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("das deser cache session pool not init", K(ret));
  } else {
    int pop_ret = session_queue_.pop(session);
    if (OB_SUCCESS == pop_ret) {
      reused = true;
      LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL HIT",
                KP(session), "idle_count", session_queue_.get_curr_total());
    } else if (OB_ENTRY_NOT_EXIST == pop_ret) {
      if (OB_FAIL(alloc_session(tenant_id, session))) {
        LOG_WARN("allocate session for das deser cache pool failed", K(ret), K(tenant_id));
      } else {
        LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL MISS", KP(session), K(tenant_id));
      }
    } else {
      ret = pop_ret;
      LOG_WARN("pop das deser cache session failed", K(ret));
    }
  }
  return ret;
}

void ObDasDeserCacheSessionPool::destroy_session(ObSQLSessionInfo *session)
{
  if (OB_NOT_NULL(session)) {
    session->das_detach_borrowed_sys_vars();
    session->~ObSQLSessionInfo();
    if (OB_ISNULL(mem_context_) || OB_ISNULL(*mem_context_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "das deser cache session pool memory context is null");
    } else {
      common::ObSpinLockGuard guard(alloc_lock_);
      (*mem_context_)->free(session);
    }
  }
}

void ObDasDeserCacheSessionPool::release(ObSQLSessionInfo *session, bool can_return_to_pool)
{
  if (OB_NOT_NULL(session)) {
    int ret = OB_SUCCESS;
    if (!can_return_to_pool) {
      LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL DROP", KP(session), K(can_return_to_pool));
      destroy_session(session);
    } else if (!is_inited_) {
      LOG_WARN_RET(OB_NOT_INIT, "das deser cache session pool not init");
      LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL DROP", KP(session), K(can_return_to_pool));
      destroy_session(session);
    } else if (OB_SUCCESS != (ret = session_queue_.push(session))) {
      if (OB_SIZE_OVERFLOW != ret) {
        LOG_WARN("push das deser cache session failed", K(ret));
      }
      LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL DROP", KP(session), K(can_return_to_pool), K(ret));
      destroy_session(session);
    } else {
      LOG_TRACE("[DAS_DESER_CACHE] SESSION_POOL RETURN",
                KP(session), "idle_count", session_queue_.get_curr_total());
    }
  }
}

void ObDasDeserCacheSessionPool::destroy()
{
  if (is_inited_) {
    ObSQLSessionInfo *session = nullptr;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = session_queue_.pop(session))) {
      destroy_session(session);
      session = nullptr;
    }
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("drain das deser cache session pool failed", K(ret));
    }
    session_queue_.destroy();
    mem_context_ = nullptr;
    is_inited_ = false;
  }
}

ObDasSessionCacheVal::~ObDasSessionCacheVal()
{
  pool_.destroy();
}

int ObDasDeserializeCacheKey::deep_copy(common::ObIAllocator &allocator,
                                        const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObDasDeserializeCacheKey &key = static_cast<const ObDasDeserializeCacheKey&>(other);
  namespace_ = key.namespace_;
  type_ = key.type_;
  if (OB_FAIL(ob_write_string(allocator, key.key_, key_))) {
    LOG_WARN("deep copy das deserialize cache key failed", K(ret), K(key));
  }
  return ret;
}

uint64_t ObDasDeserializeCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&type_, sizeof(type_), hash_val);
  hash_val = key_.hash(hash_val);
  return hash_val;
}

bool ObDasDeserializeCacheKey::is_equal(const ObILibCacheKey &other) const
{
  const ObDasDeserializeCacheKey &key = static_cast<const ObDasDeserializeCacheKey&>(other);
  return type_ == key.type_
      && key_ == key.key_;
}

int ObDasDeserializeCacheNode::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                                   ObILibCacheKey *key,
                                                   ObILibCacheObject *&cache_obj)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj_)) {
    ret = OB_SQL_PC_NOT_EXIST;
    LOG_TRACE("[DAS_DESER_CACHE] node exists but cache obj not ready", K(ret), KPC(key));
  } else {
    cache_obj = cache_obj_;
    LOG_TRACE("[DAS_DESER_CACHE] get cache obj", KPC(key), KPC(cache_obj_));
  }
  return ret;
}

int ObDasDeserializeCacheNode::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                                   ObILibCacheKey *key,
                                                   ObILibCacheObject *cache_obj)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das deserialize cache obj is null", K(ret), KPC(key));
  } else if (OB_NOT_NULL(cache_obj_)) {
    // another thread won the add race; keep the existing obj as the canonical one
    ret = OB_SQL_PC_PLAN_DUPLICATE;
    LOG_TRACE("[DAS_DESER_CACHE] cache obj already exists", K(ret), KPC(key), KPC(cache_obj_));
  } else {
    cache_obj_ = cache_obj;
    LOG_TRACE("[DAS_DESER_CACHE] add cache obj", KPC(key), KPC(cache_obj));
  }
  return ret;
}

int ObDasDeserializeCacheObj::alloc_ctdef_expr_value(ObDasCtdefExprCacheVal *&value)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  value = nullptr;
  if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObDasCtdefExprCacheVal)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ctdef expr cache value failed", K(ret));
  } else {
    value = new (buf) ObDasCtdefExprCacheVal(get_allocator());
    value_ = value;
    type_ = ObDasDeserializeCacheKey::CacheType::CTDEF_EXPR;
  }
  return ret;
}

int ObDasDeserializeCacheObj::alloc_session_value(ObDasSessionCacheVal *&value)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  value = nullptr;
  if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObDasSessionCacheVal)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc session cache value failed", K(ret));
  } else {
    value = new (buf) ObDasSessionCacheVal();
    if (OB_FAIL(value->init(get_mem_context()))) {
      LOG_WARN("init session cache value failed", K(ret));
      value->~ObDasSessionCacheVal();
      get_allocator().free(value);
      value = nullptr;
    } else {
      value_ = value;
      type_ = ObDasDeserializeCacheKey::CacheType::SESSION;
    }
  }
  return ret;
}

int ObDasDeserializeCacheObj::build_session_template(uint64_t tenant_id, ObSQLSessionInfo *&templ)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObDasSessionCacheVal *value = get_session_value();
  templ = nullptr;
  if (OB_ISNULL(value)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session cache value not allocated", K(ret), "type", static_cast<int>(type_));
  } else if (OB_NOT_NULL(value->templ_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("session template already built", K(ret), KP(value->templ_));
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session manager is null", K(ret));
  } else if (OB_ISNULL(buf = get_allocator().alloc(sizeof(ObSQLSessionInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc session template failed", K(ret));
  } else {
    ObSQLSessionInfo *sess = new (buf) ObSQLSessionInfo(tenant_id);
    uint32_t mock_sid = ObSQLSessionInfo::INVALID_SESSID;
    const uint64_t proxy_sid = 0;
    const bool in_mgr = false;
    if (OB_FAIL(GCTX.session_mgr_->create_sessid(mock_sid, in_mgr))) {
      LOG_WARN("mock template session id failed", K(ret));
    } else if (OB_FAIL(sess->init(mock_sid, proxy_sid, nullptr, nullptr, 0, tenant_id))) {
      LOG_WARN("template session init failed", K(ret), K(mock_sid), K(tenant_id));
    } else {
      sess->set_is_remote(true);
      value->templ_ = sess;
      templ = sess;
    }
    if (OB_FAIL(ret)) {
      sess->~ObSQLSessionInfo();
    }
  }
  return ret;
}

void ObDasDeserializeCacheObj::reset_session_value()
{
  if (ObDasDeserializeCacheKey::CacheType::SESSION == type_ && OB_NOT_NULL(value_)) {
    ObDasSessionCacheVal *value = static_cast<ObDasSessionCacheVal*>(value_);
    value->pool_.destroy();
    if (OB_NOT_NULL(value->templ_)) {
      value->templ_->~ObSQLSessionInfo();
      value->templ_ = nullptr;
    }
    value->~ObDasSessionCacheVal();
    value_ = nullptr;
  }
}

ObDasDeserializeCacheObj::~ObDasDeserializeCacheObj()
{
  reset_session_value();
}

int ObDasDeserializeCacheObj::before_cache_evicted()
{
  int ret = OB_SUCCESS;
  reset_session_value();
  LOG_INFO("[DAS_DESER_CACHE] cache obj evicted",
           "type", static_cast<int>(type_), KP(value_), KPC(this));
  return ret;
}

} // namespace sql
} // namespace oceanbase
