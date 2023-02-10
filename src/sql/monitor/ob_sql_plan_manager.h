// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of sql plan manager

#ifndef SRC_OBSERVER_SQL_PLAN_MGR_H_
#define SRC_OBSERVER_SQL_PLAN_MGR_H_
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "observer/mysql/ob_ra_queue.h"
#include "ob_plan_real_info_manager.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace sql
{
class ObSqlPlanMgr;
class ObSqlPlanEliminateTask : public common::ObTimerTask
{
public:
  ObSqlPlanEliminateTask();
  virtual ~ObSqlPlanEliminateTask();

  void runTimerTask();
  int init(const ObSqlPlanMgr *sql_plan_manager);
  int check_config_mem_limit(bool &is_change);
  int calc_evict_mem_level(int64_t &low, int64_t &high);
  int evict_memory_use(int64_t evict_low_level,
                       int64_t evict_high_level,
                       int64_t &evict_batch_count);
  int evict_queue_use(int64_t &evict_batch_count);

private:
  ObSqlPlanMgr *sql_plan_manager_;
  int64_t config_mem_limit_;
};

class ObSqlPlanMgr
{
public:
  static const int64_t SQL_PLAN_PAGE_SIZE = (1LL << 17); // 128K
  //进行一次release_old操作删除的记录数
  static const int32_t BATCH_RELEASE_COUNT = 5000;
  //初始化queue大小为100w
  static const int64_t MAX_QUEUE_SIZE = 1000000; //100w
  static const int64_t MINI_MODE_MAX_QUEUE_SIZE = 100000; // 10w
  //当sql_plan超过90w行记录时触发淘汰
  static constexpr const double HIGH_LEVEL_EVICT_SIZE_PERCENT = 0.9;
  //按行淘汰的低水位线
  static constexpr const double LOW_LEVEL_EVICT_SIZE_PERCENT = 0.8;
  //启动淘汰检查的时间间隔
  static const int64_t EVICT_INTERVAL = 1000000; //1s
  static const int64_t PLAN_TABLE_QUEUE_SIZE = 100000;

public:
  ObSqlPlanMgr();
  virtual ~ObSqlPlanMgr();
  int init(uint64_t tenant_id);
  void destroy();
  common::ObConcurrentFIFOAllocator *get_allocator();
  void* alloc(const int64_t size);
  void free(void *ptr);
  uint64_t get_tenant_id() const;
  bool is_valid() const;
  inline ObPlanItemMgr *get_plan_item_mgr() { return plan_item_mgr_; }
  inline ObPlanRealInfoMgr *get_plan_real_info_mgr() { return plan_real_info_mgr_; }
  inline ObIArray<ObPlanItemMgr*> &get_plan_table_mgrs() { return plan_table_mgrs_; }

  int get_mem_limit(int64_t &mem_limit);
  int init_plan_table_manager(ObPlanItemMgr* &plan_table_mgr);
  int destroy_plan_table_manager(ObPlanItemMgr* &plan_table_mgr);
  static int mtl_init(ObSqlPlanMgr* &sql_plan_mgr);
  static void mtl_destroy(ObSqlPlanMgr* &sql_plan_mgr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlPlanMgr);
friend class ObSqlPlanEliminateTask;
private:
  common::ObConcurrentFIFOAllocator allocator_;
  ObSqlPlanEliminateTask task_;
  ObSEArray<ObPlanItemMgr*, 8> plan_table_mgrs_;
  mutable ObSpinLock plan_table_mgr_lock_;
  ObPlanItemMgr *plan_item_mgr_;
  ObPlanRealInfoMgr *plan_real_info_mgr_;
  bool destroyed_;
  bool inited_;

  // tenant id of this manager
  uint64_t tenant_id_;
  int tg_id_;
};



} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_SQL_PLAN_MGR_H_ */
