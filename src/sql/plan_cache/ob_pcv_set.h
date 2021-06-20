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
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/stat/ob_latch_define.h"
#include "sql/session/ob_basic_session_info.h"
#include "sql/plan_cache/ob_plan_cache_value.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "sql/plan_cache/ob_plan_cache_util.h"
#include "observer/omt/ob_th_worker.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
class ObString;
}  // namespace common

namespace sql {
class ObPhysicalPlan;
class ObPlanCache;
class ObPlanCacheValue;
class ObPlanCacheCtx;

class ObPCVSet {

  struct PCColStruct {
    common::ObSEArray<int64_t, 16> param_idxs_;
    bool is_last_;

    PCColStruct() : param_idxs_(), is_last_(false)
    {}

    void reset()
    {
      param_idxs_.reset();
      is_last_ = false;
    }
    TO_STRING_KV(K_(param_idxs), K_(is_last))
  };

public:
  ObPCVSet(ObPlanCache* plan_cache)
      : is_inited_(false),
        plan_cache_(plan_cache),
        pc_alloc_(NULL),
        rwlock_(),
        pc_key_(),
        sql_(),
        ref_count_(0),
        normal_parse_const_cnt_(0),
        min_merged_version_(0),
        min_cluster_version_(0),
        plan_num_(0),
        need_check_gen_tbl_col_(false)
  {}
  virtual ~ObPCVSet()
  {
    destroy();
  };
  // init pcv set
  int init(const ObPlanCacheCtx& pc_ctx, const ObCacheObject* cache_obj);
  void destroy();

  // get plan from pcv_set
  int get_plan(ObPlanCacheCtx& pc_ctx, ObCacheObject*& plan);
  // add plan to pcv_set
  int add_cache_obj(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx);

  int64_t inc_ref_count(const CacheRefHandleID ref_handle);
  int64_t dec_ref_count(const CacheRefHandleID ref_handle);
  int64_t get_ref_count() const
  {
    return ref_count_;
  }

  // SpinRWLock
  int lock(bool is_rdlock);
  inline int unlock()
  {
    return rwlock_.unlock();
  }

  common::ObIAllocator* get_pc_allocator() const
  {
    return pc_alloc_;
  }
  ObPlanCache* get_plan_cache() const
  {
    return plan_cache_;
  }
  void set_plan_cache_key(ObPlanCacheKey& key)
  {
    pc_key_ = key;
  }
  // @shaoge
  // In the plan cache, the same pcv_set object may have three records in the map at the same time,
  // and the key of these three records is old_stmt_id,
  // new_stmt_id and sql, the key returned by the following interface is old_stmt_id
  ObPlanCacheKey& get_plan_cache_key()
  {
    return pc_key_;
  }
  const ObString& get_sql()
  {
    return sql_;
  }
  int deep_copy_sql(const common::ObString& sql);

  int64_t get_min_merged_version() const
  {
    return min_merged_version_;
  }
  StmtStat* get_stmt_stat()
  {
    return &stmt_stat_;
  }
  int update_stmt_stat();

  TO_STRING_KV(K_(is_inited), K_(ref_count), K_(min_merged_version));

private:
  static const int64_t MAX_PCV_SET_PLAN_NUM = 200;
  int create_pcv_and_add_plan(ObCacheObject* cache_obj, ObPlanCacheCtx& pc_ctx,
      const common::ObIArray<PCVSchemaObj>& schema_array, ObPlanCacheValue*& value);
  int64_t get_mem_size();

  int64_t get_plan_num() const
  {
    return plan_num_;
  }

  int create_new_pcv(ObPlanCacheValue*& new_pcv);
  void free_pcv(ObPlanCacheValue* pcv);

  // If the sql contains a subquery, and the projection column of the subquery is parameterized,
  // because the subquery has a constraint on the column with the same name,
  // the plan cache needs to perform a constraint check when matching
  // For example, select * (select 1, 2, 3 from dual), select * (select ?, ?,? From dual) after parameterization,
  // the plan cache caches this plan
  // if a sql select * (select 1, 1, 3 from dual) comes,
  // it hits the plan, but the subquery has a common name column
  // the plan cache should refuse to hit the plan, and let SQL parse hard,
  // throwing an error with the same name column
  int set_raw_param_info_if_needed(ObCacheObject* cache_obj);
  int check_raw_param_for_dup_col(ObPlanCacheCtx& pc_ctx, bool& contain_dup_col);

private:
  bool is_inited_;
  ObPlanCache* plan_cache_;
  common::ObIAllocator* pc_alloc_;
  common::ObLatch rwlock_;
  ObPlanCacheKey pc_key_;  // used for manager key memory
  common::ObString sql_;
  int64_t ref_count_;
  common::ObDList<ObPlanCacheValue> pcv_list_;
  StmtStat stmt_stat_;  // stat for each parameterized sql
  // The number of constants that can be recognized during normal paster is used to verify
  // whether the number of constants recognized by faster parse is consistent with the number recognized by parser.
  int64_t normal_parse_const_cnt_;
  // Maintain a minimum merged version when adding plan,
  // which is used to check in the background when it is eliminated;
  int64_t min_merged_version_;
  int64_t min_cluster_version_;
  int64_t plan_num_;

  bool need_check_gen_tbl_col_;
  common::ObFixedArray<PCColStruct, common::ObIAllocator> col_field_arr_;
};

inline int ObPCVSet::lock(bool is_rdlock)
{
  int ret = OB_SUCCESS;
  const int64_t threshold = GCONF.large_query_threshold;
  const int64_t LOCK_PERIOD = 100;  // 100us
  // If the lock fails, keep retrying the lock until it exceeds the large query threshold
  if (OB_FAIL(is_rdlock ? rwlock_.rdlock(ObLatchIds::PCV_SET_LOCK, LOCK_PERIOD)
                        : rwlock_.wrlock(ObLatchIds::PCV_SET_LOCK, LOCK_PERIOD))) {
    ret = OB_SUCCESS;
    while (true) {
      const int64_t curr_time = ObTimeUtility::current_time();
      if (curr_time <= THIS_THWORKER.get_query_start_time() + threshold) {
        if (OB_FAIL(is_rdlock ? rwlock_.rdlock(ObLatchIds::PCV_SET_LOCK, LOCK_PERIOD)
                              : rwlock_.wrlock(ObLatchIds::PCV_SET_LOCK, LOCK_PERIOD))) {
          // ignore ret
          // do nothing
        } else {
          break;
        }
      } else {
        ret = OB_PC_LOCK_CONFLICT;
        break;
      }
    }  // while end
  }

  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif
