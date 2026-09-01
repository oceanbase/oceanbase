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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_DESERIALIZE_CACHE_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_DESERIALIZE_CACHE_H_

#include "sql/plan_cache/ob_i_lib_cache_key.h"
#include "sql/plan_cache/ob_i_lib_cache_node.h"
#include "sql/plan_cache/ob_i_lib_cache_object.h"
#include "sql/plan_cache/ob_i_lib_cache_context.h"
#include "sql/plan_cache/ob_lib_cache_register.h"
#include "sql/das/ob_das_define.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/queue/ob_fixed_queue.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{
namespace sql
{

class ObSQLSessionInfo;

class ObDasDeserCacheSessionPool
{
public:
  static const int64_t MAX_IDLE_SESSION_COUNT = 1;

  ObDasDeserCacheSessionPool();
  ~ObDasDeserCacheSessionPool();

  int init(lib::MemoryContext &mem_context);
  int acquire(uint64_t tenant_id, ObSQLSessionInfo *&session, bool &reused);
  void release(ObSQLSessionInfo *session, bool can_return_to_pool);
  void destroy();

  int64_t idle_count() const { return session_queue_.get_curr_total(); }

private:
  int alloc_session(uint64_t tenant_id, ObSQLSessionInfo *&session);
  void destroy_session(ObSQLSessionInfo *session);

private:
  ObSQLSessionInfo *session_array_[MAX_IDLE_SESSION_COUNT];
  common::ObFixedQueue<ObSQLSessionInfo> session_queue_;
  lib::MemoryContext *mem_context_;
  common::ObSpinLock alloc_lock_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDasDeserCacheSessionPool);
};

struct ObDasSessionCacheVal
{
  ObDasSessionCacheVal()
      : templ_(nullptr),
        inv_len_(0),
        pool_()
  {  }

  ~ObDasSessionCacheVal();

  int init(lib::MemoryContext &mem_context) { return pool_.init(mem_context); }

  TO_STRING_KV(KP_(templ), K_(inv_len), "idle_session_count", pool_.idle_count());

  ObSQLSessionInfo *templ_;
  int64_t inv_len_;
  ObDasDeserCacheSessionPool pool_;
};

struct ObDasCtdefExprCacheVal
{
  explicit ObDasCtdefExprCacheVal(common::ObIAllocator &allocator)
    : rt_exprs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator)),
      ctdefs_(common::OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(allocator))
  {  }

  ~ObDasCtdefExprCacheVal() = default;

  TO_STRING_KV("rt_expr_cnt", rt_exprs_.count(), "ctdef_cnt", ctdefs_.count());

  common::ObArray<ObExpr> rt_exprs_;
  common::ObSEArray<const ObDASBaseCtDef*, 2> ctdefs_;
};

struct ObDasDeserializeCacheKey : public ObILibCacheKey
{
  enum class CacheType : int8_t
  {
    INVALID = -1,
    CTDEF_EXPR = 0, // CTDEF & EXPR
    SESSION = 1,
  };

  ObDasDeserializeCacheKey()
    : ObILibCacheKey(ObLibCacheNameSpace::NS_DAS_DESER),
      type_(CacheType::INVALID),
      key_()
  {  }

  virtual int deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other) override;
  virtual uint64_t hash() const override;
  virtual bool is_equal(const ObILibCacheKey &other) const override;

  TO_STRING_KV("type", static_cast<int>(type_),
               "key_len", key_.length(),
               K_(namespace));

  CacheType type_;
  common::ObString key_;
};

class ObDasDeserializeCacheNode : public ObILibCacheNode
{
public:
  ObDasDeserializeCacheNode(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      cache_obj_(nullptr)
  {  }

  virtual ~ObDasDeserializeCacheNode() {}

  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj) override;
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj) override;

private:
  ObILibCacheObject *cache_obj_;
};

class ObDasDeserializeCacheObj : public ObILibCacheObject
{
public:
  explicit ObDasDeserializeCacheObj(lib::MemoryContext &mem_context)
    : ObILibCacheObject(ObLibCacheNameSpace::NS_DAS_DESER, mem_context),
      type_(ObDasDeserializeCacheKey::CacheType::INVALID),
      value_(nullptr)
  {}

  virtual ~ObDasDeserializeCacheObj();

  // value_ interpretation depends on type_:
  //   CTDEF_EXPR: ObDasCtdefExprCacheVal*  (rt_exprs_ + wired ctdef tree)
  //   SESSION:    ObDasSessionCacheVal*     (detached read-only template session)
  OB_INLINE void *get_value() { return value_; }
  OB_INLINE void set_value(void *value) { value_ = value; }
  OB_INLINE ObDasDeserializeCacheKey::CacheType get_type() const { return type_; }
  OB_INLINE void set_type(ObDasDeserializeCacheKey::CacheType type) { type_ = type; }

  int alloc_ctdef_expr_value(ObDasCtdefExprCacheVal *&value);
  OB_INLINE ObDasCtdefExprCacheVal *get_ctdef_expr_value()
  {
    return ObDasDeserializeCacheKey::CacheType::CTDEF_EXPR == type_
        ? static_cast<ObDasCtdefExprCacheVal*>(value_) : nullptr;
  }

  int alloc_session_value(ObDasSessionCacheVal *&value);
  OB_INLINE ObDasSessionCacheVal *get_session_value()
  {
    return ObDasDeserializeCacheKey::CacheType::SESSION == type_
        ? static_cast<ObDasSessionCacheVal*>(value_) : nullptr;
  }

  int build_session_template(uint64_t tenant_id, ObSQLSessionInfo *&templ);

  virtual int before_cache_evicted() override;

private:
  void reset_session_value();

private:
  ObDasDeserializeCacheKey::CacheType type_;
  void *value_;
};

} // namespace sql
} // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_DESERIALIZE_CACHE_H_ */
