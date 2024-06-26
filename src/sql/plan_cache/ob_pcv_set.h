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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PCV_SET_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PCV_SET_

#include "lib/list/ob_dlist.h"
#include "lib/string/ob_string.h"
#include "lib/stat/ob_latch_define.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "observer/omt/ob_th_worker.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObString;
}

namespace sql
{
class ObPhysicalPlan;
class ObPlanCache;
class ObPlanCacheValue;
class ObPlanCacheCtx;

class ObPCVSet : public ObILibCacheNode
{

struct PCColStruct
{
  common::ObSEArray<int64_t, 16> param_idxs_;
  bool is_last_;

  PCColStruct()
    : param_idxs_(),
      is_last_(false) {}

  void reset()
  {
    param_idxs_.reset();
    is_last_ = false;
  }
  TO_STRING_KV(K_(param_idxs),
               K_(is_last))
};

public:
  ObPCVSet(ObPlanCache *lib_cache, lib::MemoryContext &mem_context)
    : ObILibCacheNode(lib_cache, mem_context),
      is_inited_(false),
      pc_key_(),
      pc_alloc_(NULL),
      sql_(),
      normal_parse_const_cnt_(0),
      min_cluster_version_(0),
      plan_num_(0),
      need_check_gen_tbl_col_(false)
  {
  }
  virtual ~ObPCVSet()
  {
    destroy();
  };
  //init pcv set
  virtual int init(ObILibCacheCtx &ctx, const ObILibCacheObject *cache_obj);
  virtual int inner_get_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *&cache_obj);
  virtual int inner_add_cache_obj(ObILibCacheCtx &ctx,
                                  ObILibCacheKey *key,
                                  ObILibCacheObject *cache_obj);
  void destroy();
  common::ObIArray<common::ObString> &get_sql_id() { return sql_ids_; }
  int push_sql_id(common::ObString sql_id) { return sql_ids_.push_back(sql_id); }
  ObPlanCache *get_plan_cache() const { return lib_cache_; }
  common::ObIAllocator *get_pc_allocator() const { return pc_alloc_; }
  void set_plan_cache_key(ObPlanCacheKey &key) { pc_key_ = key; }
  // @shaoge
  // plan cache中,相同的pcv_set对象在map中可能同时存在三条记录，这三条记录的key为
  // old_stmt_id, new_stmt_id以及sql，下面接口返回的key是old_stmt_id
  ObPlanCacheKey &get_plan_cache_key() { return pc_key_; }
  const ObString &get_sql() { return sql_; }
  int deep_copy_sql(const common::ObString &sql);
  int check_contains_table(uint64_t db_id, common::ObString tab_name, bool &contains);
#ifdef OB_BUILD_SPM
  int get_evolving_evolution_task(EvolutionPlanList &evo_task_list);
#endif

  TO_STRING_KV(K_(is_inited));

private:
  static const int64_t MAX_PCV_SET_PLAN_NUM = 200;
  int create_pcv_and_add_plan(ObPlanCacheObject *cache_obj,
                              ObPlanCacheCtx &pc_ctx,
                              const common::ObIArray<PCVSchemaObj> &schema_array,
                              ObPlanCacheValue *&value);
  int64_t get_plan_num() const { return plan_num_; }
  int create_new_pcv(ObPlanCacheValue *&new_pcv);
  void free_pcv(ObPlanCacheValue *pcv);

  // 如果sql包含子查询，并且子查询的投影列被参数化，由于子查询有同名列的约束，plan cache在匹配的时候需要进行约束检查
  // 比如 select * (select 1, 2, 3 from dual), 参数化后select * (select ?, ?, ? from dual)，plan cache缓存了这一计划
  // 如果来了一条sql select * (select 1, 1, 3 from dual), 命中计划，但是子查询有通名列
  // plan cache应该拒绝命中计划，并让sql走硬解析，抛出同名列错误
  int set_raw_param_info_if_needed(ObPlanCacheObject *cache_obj);
  int check_raw_param_for_dup_col(ObPlanCacheCtx &pc_ctx, bool &contain_dup_col);
private:
  bool is_inited_;
  ObPlanCacheKey pc_key_; //used for manager key memory
  common::ObIAllocator *pc_alloc_;
  common::ObString sql_;  // 往plan cache中增加以sql为key的kv对时需要本成员
  common::ObDList<ObPlanCacheValue> pcv_list_;
  //正常parser时能够识别的常量的个数，用于校验faster parse识别的常量个数与正常parser识别个数是否一致。
  int64_t normal_parse_const_cnt_;
  int64_t min_cluster_version_;
  // 记录该pcv_set下面挂了多少plan，上限为MAX_PCV_SET_PLAN_NUM
  int64_t plan_num_;
  common::ObSEArray<common::ObString, 4> sql_ids_;
  bool need_check_gen_tbl_col_;
  common::ObFixedArray<PCColStruct, common::ObIAllocator> col_field_arr_;
};

}
}

#endif
