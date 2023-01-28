// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of plan real info manager

#ifndef SRC_OBSERVER_PLAN_REAL_INFO_MGR_H_
#define SRC_OBSERVER_PLAN_REAL_INFO_MGR_H_
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "observer/mysql/ob_ra_queue.h"
#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace common
{
  class ObConcurrentFIFOAllocator;
}

namespace sql
{
class ObMonitorNode;
struct ObPlanRealInfo {
  ObPlanRealInfo();
  virtual ~ObPlanRealInfo();
  void reset();
  int64_t get_extra_size() const;

  int64_t plan_id_;
  char* sql_id_;
  int64_t sql_id_len_;
  uint64_t plan_hash_;
  int id_;
  int64_t real_cost_;
  int64_t real_cardinality_;
  int64_t cpu_cost_;
  int64_t io_cost_;

  TO_STRING_KV(
    K_(plan_id),
    K_(real_cost),
    K_(real_cardinality),
    K_(cpu_cost),
    K_(io_cost)
  );
};
struct ObPlanRealInfoRecord
{
  ObPlanRealInfoRecord();
  virtual ~ObPlanRealInfoRecord();
  virtual void destroy();
  TO_STRING_KV(
    K_(data)
  );
  ObPlanRealInfo data_;
  common::ObConcurrentFIFOAllocator *allocator_;
};

class ObPlanRealInfoMgr;
class ObPlanRealInfoEliminateTask : public common::ObTimerTask
{
public:
  ObPlanRealInfoEliminateTask();
  virtual ~ObPlanRealInfoEliminateTask();

  void runTimerTask();
  int init(const ObPlanRealInfoMgr *plan_real_info_manager);
  int check_config_mem_limit(bool &is_change);
  int calc_evict_mem_level(int64_t &low, int64_t &high);

private:
  ObPlanRealInfoMgr *plan_real_info_manager_;
  int64_t config_mem_limit_;
};

class ObPlanRealInfoMgr
{
public:
  static const int64_t SQL_PLAN_PAGE_SIZE = (1LL << 17); // 128K
  //进行一次release_old操作删除的记录数
  static const int32_t BATCH_RELEASE_COUNT = 5000;
  static const int32_t MAX_RELEASE_TIME = 5 * 1000; //5ms
  static const int64_t US_PER_HOUR = 3600000000;
  //初始化queue大小为1000w
  static const int64_t MAX_QUEUE_SIZE = 1000000; //100w
  static const int64_t MINI_MODE_MAX_QUEUE_SIZE = 100000; // 10w
  //当sql_plan超过900w行记录时触发淘汰
  static const int64_t HIGH_LEVEL_EVICT_SIZE = 900000; //90w
  static const int64_t MINI_MODE_HIGH_LEVEL_EVICT_SIZE = 90000; // 9w
  //按行淘汰的低水位线
  static const int64_t LOW_LEVEL_EVICT_SIZE = 800000; //80w
  static const int64_t MINI_MODE_LOW_LEVEL_EVICT_SIZE = 80000;  // 8w
  //启动淘汰检查的时间间隔
  static const int64_t EVICT_INTERVAL = 1000000; //1s
  static const int64_t PLAN_TABLE_QUEUE_SIZE = 1000;
  typedef common::ObRaQueue::Ref Ref;

public:
  ObPlanRealInfoMgr();
  virtual ~ObPlanRealInfoMgr();
  int init(uint64_t tenant_id,
           const int64_t queue_size);
  void destroy();
  int handle_plan_info(int64_t id,
                       const ObString& sql_id,
                       uint64_t plan_id,
                       uint64_t plan_hash,
                       const ObMonitorNode &plan_info);

  common::ObConcurrentFIFOAllocator *get_allocator();
  void* alloc(const int64_t size);
  void free(void *ptr);
  int get(const int64_t idx, void *&record, Ref* ref);
  int revert(Ref* ref);
  int release_old(int64_t limit = BATCH_RELEASE_COUNT);
  void clear_queue();

  uint64_t get_tenant_id() const;
  int64_t get_start_idx() const;
  int64_t get_end_idx() const;
  int64_t get_size_used();
  int64_t get_size();
  bool is_valid() const;

  static int get_mem_limit(uint64_t tenant_id, int64_t &mem_limit);
  static int mtl_init(ObPlanRealInfoMgr* &plan_real_info_mgr);
  static void mtl_destroy(ObPlanRealInfoMgr* &plan_real_info_mgr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanRealInfoMgr);

private:
  common::ObConcurrentFIFOAllocator allocator_;//alloc mem for string buf
  ObPlanRealInfoEliminateTask task_;
  common::ObRaQueue queue_;
  bool destroyed_;
  bool inited_;

  // tenant id of this manager
  uint64_t tenant_id_;
  int tg_id_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_PLAN_REAL_INFO_MGR_H_ */
