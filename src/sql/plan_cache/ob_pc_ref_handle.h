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

#ifndef OCEANBASE_PC_REF_HANDLE_H_
#define OCEANBASE_PC_REF_HANDLE_H_

#include "lib/atomic/ob_atomic.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace lib
{
  struct ObMemAttr;
}

namespace common
{
class ObIAllocator;
class ObString;
class ObArenaAllocator;
}
namespace sql
{
struct ObPreCalcExprFrameInfo;
class ObSqlExpression;
// pre-calculable expression list reference handler
class PreCalcExprHandler {
public:
  // allocator which allocate memory to this object
  common::ObIAllocator* pc_alloc_;
  // allocator for expressions
  common::ObArenaAllocator alloc_;
  // for old engine
  common::ObDList<ObPreCalcExprFrameInfo>* pre_calc_frames_;
  // for new engine
  common::ObDList<ObSqlExpression>* pre_calc_exprs_;
  volatile int64_t ref_cnt_;

  PreCalcExprHandler()
  : pc_alloc_(NULL),
    alloc_(),
    pre_calc_frames_(NULL),
    pre_calc_exprs_(NULL),
    ref_cnt_(1)
  {
  }
  ~PreCalcExprHandler()
  {
    alloc_.reset();
    pc_alloc_ = NULL;
    pre_calc_frames_ = NULL;
    pre_calc_exprs_ = NULL;
  }

  void init(uint64_t t_id, common::ObIAllocator* pc_alloc)
  {
    lib::ObMemAttr attr;
    attr.label_ = "PRE_CALC_EXPR";
    attr.tenant_id_ = t_id;
    attr.ctx_id_ = common::ObCtxIds::PLAN_CACHE_CTX_ID;
    alloc_.set_attr(attr);
    pc_alloc_ = pc_alloc;
  }
public:
  int64_t get_ref_count() const 
  {
    return ATOMIC_LOAD64(&ref_cnt_);
  }

  void inc_ref_cnt() 
  {
    ATOMIC_AAF(&ref_cnt_, 1);
  }

  int64_t dec_ref_cnt()
  {
    return ATOMIC_SAF(&ref_cnt_, 1);
  }
};

enum CacheRefHandleID
{
  PC_REF_PLAN_LOCAL_HANDLE = 0,
  PC_REF_PLAN_REMOTE_HANDLE,
  PC_REF_PLAN_DIST_HANDLE,
  PC_REF_PLAN_ARR_HANDLE,
  PC_REF_PLAN_STAT_HANDLE,
  PC_REF_PL_HANDLE,
  PC_REF_PL_STAT_HANDLE,
  PLAN_GEN_HANDLE,
  CLI_QUERY_HANDLE,
  OUTLINE_EXEC_HANDLE,
  PLAN_EXPLAIN_HANDLE,
  CHECK_EVOLUTION_PLAN_HANDLE,
  LOAD_BASELINE_HANDLE,
  PS_EXEC_HANDLE,
  GV_SQL_HANDLE,
  PL_ANON_HANDLE,
  PL_ROUTINE_HANDLE,
  PACKAGE_VAR_HANDLE,
  PACKAGE_TYPE_HANDLE,
  PACKAGE_SPEC_HANDLE,
  PACKAGE_BODY_HANDLE,
  PACKAGE_RESV_HANDLE,
  GET_PKG_HANDLE,
  INDEX_BUILDER_HANDLE,
  PCV_SET_HANDLE,
  PCV_RD_HANDLE,
  PCV_WR_HANDLE,
  PCV_GET_PLAN_KEY_HANDLE,
  PCV_GET_PL_KEY_HANDLE,
  PCV_EXPIRE_BY_USED_HANDLE,
  PCV_EXPIRE_BY_MEM_HANDLE,
  LC_REF_CACHE_NODE_HANDLE,
  LC_NODE_HANDLE,
  LC_NODE_RD_HANDLE,
  LC_NODE_WR_HANDLE,
  LC_REF_CACHE_OBJ_STAT_HANDLE,
  PLAN_BASELINE_HANDLE,
  TABLEAPI_NODE_HANDLE,
  SQL_PLAN_HANDLE,
  CALLSTMT_HANDLE,
  PC_DIAG_HANDLE,
  MAX_HANDLE
};

class ObCacheRefHandleMgr
{
friend class ObAllPlanCacheStat;
public:
  ObCacheRefHandleMgr()
    :tenant_id_(common::OB_INVALID_ID)
  {
    clear_ref_handles();
  }

  ~ObCacheRefHandleMgr()
  {
    clear_ref_handles();
  }

  void record_ref_op(const CacheRefHandleID ref_handle)
  {
    OB_ASSERT(ref_handle < MAX_HANDLE && ref_handle >= 0);
    if (GCONF._enable_plan_cache_mem_diagnosis) {
      // (void)ATOMIC_AAF(&CACHE_REF_HANDLES[ref_handle].cur_ref_cnt_, 1);
      ATOMIC_INC(&(CACHE_REF_HANDLES[ref_handle]));
    }
  }

  void record_deref_op(const CacheRefHandleID ref_handle)
  {
    OB_ASSERT(ref_handle < MAX_HANDLE && ref_handle >= 0);
    if (GCONF._enable_plan_cache_mem_diagnosis) {
      ATOMIC_DEC(&CACHE_REF_HANDLES[ref_handle]);
    }
  }

  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }

  int dump_handle_info(common::ObIAllocator &allocator,
                              common::ObString &dump_info);

  void clear_ref_handles()
  {
    for(uint64_t i = 0; i < MAX_HANDLE; i++) {
      ATOMIC_STORE(&CACHE_REF_HANDLES[i], 0);
    }
  }

  int64_t get_ref_cnt(const CacheRefHandleID ref_handle) const
  {
    return ATOMIC_LOAD(&CACHE_REF_HANDLES[ref_handle]);
  }
private:
  static const char* handle_name(const CacheRefHandleID handle);

private:
  volatile int64_t CACHE_REF_HANDLES[MAX_HANDLE];
  uint64_t tenant_id_;
};
} // end namespace sql
} // end namespace oceanbase
#endif // !OCEANBASE_PC_REF_HANDLE_H_
