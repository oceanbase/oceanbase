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

#ifndef OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_MANAGER_
#define OCEANBASE_SQL_PLAN_CACHE_OB_PLAN_CACHE_MANAGER_

#include "lib/net/ob_addr.h"
#include "lib/hash/ob_hashmap.h"
#include "ob_plan_cache.h"
#include "sql/plan_cache/ob_ps_cache.h"
namespace test {
void test_plan_cache_manager();
}

namespace oceanbase {
namespace sql {
class ObPlanCache;

class ObPlanCacheManager {
public:
  typedef common::hash::ObHashMap<uint64_t, ObPlanCache*> PlanCacheMap;
  typedef common::hash::ObHashMap<uint64_t, ObPsCache*> PsPlanCacheMap;

  class ObPlanCacheEliminationTask : public common::ObTimerTask {
  public:
    ObPlanCacheEliminationTask() : plan_cache_manager_(NULL), run_task_counter_(0)
    {}
    // main routine
    void runTimerTask(void);

  private:
    void run_plan_cache_task();
    void run_ps_cache_task();
    void run_free_cache_obj_task();

  public:
    ObPlanCacheManager* plan_cache_manager_;
    int64_t run_task_counter_;
  };

  struct ObGetAllCacheKeyOp {
    explicit ObGetAllCacheKeyOp(common::ObIArray<uint64_t>* key_array) : key_array_(key_array)
    {}

    template <class T>
    int operator()(common::hash::HashMapPair<uint64_t, T*>& entry)
    {
      int ret = common::OB_SUCCESS;
      if (OB_ISNULL(key_array_)) {
        ret = common::OB_NOT_INIT;
        SQL_PC_LOG(WARN, "key_array not inited", K(ret));
      } else if (OB_FAIL(key_array_->push_back(entry.first))) {
        SQL_PC_LOG(WARN, "fail to push back key", K(ret));
      }
      return ret;
    }

    common::ObIArray<uint64_t>* key_array_;
  };

public:
  ObPlanCacheManager()
      : partition_location_cache_(NULL), tg_id_(-1), inited_(false), destroyed_(false), plan_cache_id_(0){};
  virtual ~ObPlanCacheManager()
  {
    destroy();
  }

  int init(share::ObIPartitionLocationCache* plc, common::ObAddr addr);
  void destroy();

  // get tenant plan cache, if not exists, create a new plan cache
  ObPlanCache* get_or_create_plan_cache(uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf);
  ObPsCache* get_or_create_ps_cache(const uint64_t tenant_id, const ObPCMemPctConf& pc_mem_conf);

  int revert_plan_cache(const uint64_t& tenant_id);
  int revert_ps_cache(const uint64_t& tenant_id);
  int flush_all_plan_cache();
  int flush_plan_cache(const uint64_t tenant_id);
  int flush_all_pl_cache();
  int flush_pl_cache(const uint64_t tenant_id);
  int flush_all_ps_cache();
  int flush_ps_cache(const uint64_t tenant_id);

  // load baseline from plan cache
  int load_baseline_from_all_pc();
  int load_baseline_from_pc(const uint64_t& tenant_id);
  int load_baseline_from_pc(const obrpc::ObLoadBaselineArg& arg);

  PlanCacheMap& get_plan_cache_map()
  {
    return pcm_;
  }
  PsPlanCacheMap& get_ps_cache_map()
  {
    return ps_pcm_;
  }
  // get tenant plan cache, if not exists, return NULL
  // only used when revert plan cache
  ObPlanCache* get_plan_cache(uint64_t tenant_id);
  ObPsCache* get_ps_cache(const uint64_t tenant_id);

private:
  enum PlanCacheGCStrategy { INVALID = -1, OFF = 0, REPORT = 1, AUTO = 2 };

  static int get_plan_cache_gc_strategy();
  friend void ::test::test_plan_cache_manager();
  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheManager);

private:
  share::ObIPartitionLocationCache* partition_location_cache_;
  common::ObAddr self_addr_;
  PlanCacheMap pcm_;
  PsPlanCacheMap ps_pcm_;
  int tg_id_;
  ObPlanCacheEliminationTask elimination_task_;
  bool inited_;
  bool destroyed_;
  volatile uint64_t plan_cache_id_;
};  // end of class ObPlanCaeManager

class ObPlanCacheManagerAtomic {
public:
  typedef common::hash::HashMapPair<uint64_t, ObPlanCache*> MapKV;

public:
  ObPlanCacheManagerAtomic() : plan_cache_(NULL){};
  virtual ~ObPlanCacheManagerAtomic(){};

  int operator()(MapKV& entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(entry.second)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else {
      plan_cache_ = entry.second;
      entry.second->inc_ref_count();
    }
    return ret;
  }

  ObPlanCache* get_plan_cache()
  {
    return plan_cache_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanCacheManagerAtomic);

private:
  ObPlanCache* plan_cache_;
};  // end of class ObPlanCacheManagerAtomic

class ObPsCacheManagerAtomic {
public:
  typedef common::hash::HashMapPair<uint64_t, ObPsCache*> MapKV;
  ObPsCacheManagerAtomic() : ps_cache_(NULL){};
  virtual ~ObPsCacheManagerAtomic()
  {}

  int operator()(MapKV& entry)
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(entry.second)) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_PC_LOG(WARN, "invalid argument", K(ret));
    } else {
      ps_cache_ = entry.second;
      entry.second->inc_ref_count();
    }
    return ret;
  }
  ObPsCache* get_ps_cache() const
  {
    return ps_cache_;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPsCacheManagerAtomic);
  ObPsCache* ps_cache_;
};

}  // end of namespace sql
}  // end of namespace oceanbase

#endif /* _OB_PLAN_CACHE_MANAGER_H_ */
