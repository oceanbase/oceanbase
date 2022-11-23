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

#define USING_LOG_PREFIX SQL_PC
#include "ob_plan_cache.h"

#include "sql/ob_sql_context.h"
#include "sql/plan_cache/ob_ps_sql_utils.h"
#include "sql/plan_cache/ob_ps_plan_cache_callback.h"
#include "sql/plan_cache/ob_pcv_set.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

//添加plan到plan cache
// 1.判断plan cache内存是否达到上限；
// 2.判断plan大小是否超出限制
// 3.通过plan cache key获取pcv：
//     如果获取pcv成功：则将plan 加入pcv
//     如果获取pcv失败：则新生成pcv; 将plan 加入该pcv; 最后将该pcv 加入到key->pcv map中，
int ObPlanCache::add_ps_plan(const ObPsStmtId stmt_id,
                             ObPhysicalPlan *plan,
                             ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid physical plan", K(ret));
  } else if (is_reach_memory_limit()) {
    ret = OB_REACH_MEMORY_LIMIT;
    SQL_PC_LOG(DEBUG, "plan cache memory used reach the high water mark", K(mem_used_), K(get_mem_limit()), K(ret));
  } else if (plan->get_mem_size() > ObPlanCache::MAX_PLAN_SIZE) {
    ret = OB_SQL_PC_PLAN_SIZE_LIMIT;
    SQL_PC_LOG(DEBUG, "plan size too large, don't add to plan cache", "plan_size", plan->get_mem_size(), K(ret));
  } else {
    if (OB_FAIL(inner_add_ps_plan(stmt_id, plan, pc_ctx))) {
      if (!is_not_supported_err(ret) && OB_SQL_PC_PLAN_DUPLICATE != ret) {
        SQL_PC_LOG(WARN, "fail to add plan", K(ret));
      }
    }
  }

//  if (OB_SUCC(ret)) {
//    if (OB_FAIL(add_plan_stat(pc_ctx, plan))) {
//      LOG_WARN("Failed to add plan cache stat", K(ret));
//    }
//  }
  return ret;
}

// 通过plan cache key, 从stmt_id -> pcv map中获取pcv
int ObPlanCache::get_ps_pcv_set(const ObPsStmtId stmt_id,
                                ObPCVSet *&pcv_set,
                                ObPsPCVSetAtomicOp &op)
{
  int ret = OB_SUCCESS;
  //get pcv and inc ref count
  int hash_err = ps_pcvs_map_.read_atomic(stmt_id, op);
  switch (hash_err) {
    case OB_SUCCESS: {
      //get pcv and lock
      if (OB_FAIL(op.get_value(pcv_set))) {
        SQL_PC_LOG(WARN, "failed to lock pcv set", K(ret), K(stmt_id));
      }
      break;
    }
    case OB_HASH_NOT_EXIST: { //返回时 pcv_set = NULL; ret = OB_SUCCESS;
      SQL_PC_LOG(DEBUG, "entry does not exist.", K(stmt_id));
      break;
    }
    default: {
      SQL_PC_LOG(WARN, "failed to get pcv set", K(ret), K(stmt_id));
      ret = hash_err;
      break;
    }
  }
  return ret;
}

//only create pcv_set for stmt_id, not add plan
int ObPlanCache::get_or_create_pcv_set(const ObPsStmtId stmt_id, ObPlanCacheCtx &pc_ctx)
{
  int ret = OB_SUCCESS;
  ObPsPCVSetWlockAndRef w_ref_lock;
  ObPCVSet *pcv_set = NULL;
  if (OB_FAIL(get_ps_pcv_set(stmt_id, pcv_set, w_ref_lock/* write locked */))) {
    SQL_PC_LOG(WARN, "failed to get pcv_set form plan_cache by id", K(ret), K(stmt_id));
  } else if (NULL == pcv_set) {
    if (OB_FAIL(create_ps_pcv_set(pc_ctx, pcv_set))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "fail to create pcv_set and add plan", K(ret));
      }
    }
    //set key value
    if (OB_SUCC(ret)) {
      int hash_err = ps_pcvs_map_.set_refactored(stmt_id, pcv_set);
      if (OB_HASH_EXIST == hash_err) { //may be this pcv_set has been set by other thread。
        //TODO
        //pcv_set->remove_plan(exec_context, *plan);
        pcv_set->dec_ref_count();//will clean auto
      } else if (OB_SUCCESS == hash_err) {
        SQL_PC_LOG(INFO, "succeed to set pcv_set to sql_pcvs_map");
      } else {
        SQL_PC_LOG(WARN, "failed to add pcv_set to sql_pcvs_map", K(ret));
        //TODO
        //pcv_set->remove_plan(exec_context, *plan);
        pcv_set->dec_ref_count();
      }
    }

  } else {
    // release wlock whatever
    pcv_set->unlock();
    pcv_set->dec_ref_count();
    //如果add plan时check到table or view的version已过期，则删除该pcv_set
//    if (OB_OLD_SCHEMA_VERSION == ret) {
//      SQL_PC_LOG(INFO, "table or view in plan cache value is old", K(ret));
//      if (OB_FAIL(remove_ps_pcv_set(stmt_id))) {
//        SQL_PC_LOG(WARN, "fail to remove plan cache value", K(ret));
//      } else if (OB_FAIL(add_ps_pcv_set(stmt_id, pc_ctx))) {
//        SQL_PC_LOG(DEBUG, "fail to add plan", K(ret), K(plan));
//      }
//    }
  }
  return ret;
}

int ObPlanCache::create_ps_pcv_set(ObPlanCacheCtx &pc_ctx,
                                   ObPCVSet *&pcv_set)
{
  int ret = OB_SUCCESS;
  pcv_set = NULL;
  char *ptr = NULL;
  if (NULL == (ptr = (char *)inner_allocator_.alloc(sizeof(ObPCVSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pcv set", K(ret));
  } else {
    pcv_set = new (ptr)ObPCVSet();
    pcv_set->inc_ref_count();
    if (OB_FAIL(pcv_set->init(pc_ctx, this, 0))) {//hualong @@todo
      LOG_WARN("failed to init pcv set", K(ret));
    }
  }
  if (OB_FAIL(ret) && NULL!=pcv_set) {
    pcv_set->~ObPCVSet();
    inner_allocator_.free(pcv_set);
    pcv_set = NULL;
  }
  return ret;
}

int ObPlanCache::create_pcv_set_and_add_ps_plan(const ObPsStmtId stmt_id,
                                                ObPhysicalPlan *plan,
                                                ObPlanCacheCtx &pc_ctx,
                                                ObPCVSet *&pcv_set)
{
  int ret = OB_SUCCESS;
  pcv_set = NULL;
  char *ptr = NULL;
  UNUSED(stmt_id);
  if (OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_PC_LOG(WARN, "invalid argument", K(ret));
  } else if (NULL == (ptr = (char *)inner_allocator_.alloc(sizeof(ObPCVSet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for pcv set", K(ret));
  } else {
    pcv_set = new(ptr)ObPCVSet();
    pcv_set->inc_ref_count();//构造ObPCVSet后的引用计数
    if (OB_FAIL(pcv_set->init(pc_ctx, this, plan->get_merged_version()))) {
      LOG_WARN("fail to init pcv set", K(ret));
    }
  }

  //add plan
  if (OB_SUCC(ret)) {
    if (OB_FAIL(pcv_set->add_plan(plan, pc_ctx))) {
      if (!is_not_supported_err(ret)) {
        SQL_PC_LOG(WARN, "failed to add plan to plan cache value",  K(ret));
      }
    } else {
//      pcv_set->update_stmt_stat();
      //如果该value在set进plan cache时失败，则在释放value时会减去该plan在plan cache中的内存占用量。
      inc_mem_used(plan->get_mem_size());
    }
  }

  if (OB_FAIL(ret) && NULL != pcv_set) {
    pcv_set->~ObPCVSet();
    inner_allocator_.free(pcv_set);
    pcv_set = NULL;
  }
  return ret;
}

//淘汰指定plan cache key对应的pcv set
int ObPlanCache::remove_ps_pcv_set(const ObPsStmtId stmt_id)
{
  int ret = OB_SUCCESS;
  int hash_err = OB_SUCCESS;
  ObPCVSet *pcv_set = NULL;
  hash_err = ps_pcvs_map_.erase_refactored(stmt_id, &pcv_set);
  if (OB_SUCCESS == hash_err) {
    if (NULL != pcv_set) {
      // remove plan cache reference, even remove_plan_stat() failed
      SQL_PC_LOG(WARN, "xxx pcv_set ref_count", K(pcv_set->get_ref_count()));
      pcv_set->dec_ref_count();
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_PC_LOG(ERROR, "pcv_set should not be null", K(stmt_id));
    }
  } else if (OB_HASH_NOT_EXIST == hash_err) {
    // do nothing
    SQL_PC_LOG(INFO, "plan cache key is alreay be deleted", K(stmt_id));
  } else {
    ret = hash_err;
    SQL_PC_LOG(WARN, "failed to erase pcv_set from plan cache by key", K(stmt_id), K(hash_err));
  }

  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase

