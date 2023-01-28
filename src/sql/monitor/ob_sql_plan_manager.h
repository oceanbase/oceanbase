// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines interface of sql plan manager

#ifndef SRC_OBSERVER_SQL_PLAN_MGR_H_
#define SRC_OBSERVER_SQL_PLAN_MGR_H_
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "observer/mysql/ob_ra_queue.h"
#include "lib/task/ob_timer.h"
namespace oceanbase
{
namespace sql
{

struct ObSqlPlanItem {
  ObSqlPlanItem();
  virtual ~ObSqlPlanItem();
  void reset();
  int64_t get_extra_size() const;

  int64_t plan_id_;
  char* sql_id_;
  int64_t sql_id_len_;
  int64_t db_id_;
  uint64_t plan_hash_;
  int64_t gmt_create_;
  char* operation_;
  int64_t operation_len_;
  char* options_;
  int64_t options_len_;
  char* object_node_;
  int64_t object_node_len_;
  int64_t object_id_;
  char* object_owner_;
  int64_t object_owner_len_;
  char* object_name_;
  int64_t object_name_len_;
  char* object_alias_;
  int64_t object_alias_len_;
  char* object_type_;
  int64_t object_type_len_;
  char* optimizer_;
  int64_t optimizer_len_;
  int id_;
  int parent_id_;
  int depth_;
  int position_;
  int search_columns_;
  bool is_last_child_;
  int64_t cost_;
  int64_t cardinality_;
  int64_t bytes_;
  int64_t rowset_;
  char* other_tag_;
  int64_t other_tag_len_;
  char* partition_start_;
  int64_t partition_start_len_;
  char* partition_stop_;
  int64_t partition_stop_len_;
  int64_t partition_id_;
  char* other_;
  int64_t other_len_;
  char* distribution_;
  int64_t distribution_len_;
  int64_t cpu_cost_;
  int64_t io_cost_;
  int64_t temp_space_;
  char* access_predicates_;
  int64_t access_predicates_len_;
  char* filter_predicates_;
  int64_t filter_predicates_len_;
  char* startup_predicates_;
  int64_t startup_predicates_len_;
  char* projection_;
  int64_t projection_len_;
  char* special_predicates_;
  int64_t special_predicates_len_;
  int64_t time_;
  char* qblock_name_;
  int64_t qblock_name_len_;
  char* remarks_;
  int64_t remarks_len_;
  char* other_xml_;
  int64_t other_xml_len_;

  TO_STRING_KV(
    K_(plan_id)
  );
};
struct ObSqlPlanItemRecord
{
  ObSqlPlanItemRecord();
  virtual ~ObSqlPlanItemRecord();
  virtual void destroy();
  TO_STRING_KV(
    K_(data)
  );
  ObSqlPlanItem data_;
  common::ObConcurrentFIFOAllocator *allocator_;
};

struct ObSqlPlanItemRecords {
  void reset() { records_.reuse(); }
  ObSEArray<ObSqlPlanItemRecord*, 4> records_;
};

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
  static const int32_t MAX_RELEASE_TIME = 5 * 1000; //5ms
  static const int64_t US_PER_HOUR = 3600000000;
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
  typedef common::ObRaQueue::Ref Ref;

public:
  ObSqlPlanMgr();
  virtual ~ObSqlPlanMgr();
  int init(uint64_t tenant_id,
           const int64_t queue_size);
  void destroy();
  int handle_plan_item(const ObSqlPlanItem &plan_item);
  int get_plan(int64_t plan_id,
               ObIArray<ObSqlPlanItem*> &plan);
  int get_plan(const ObString &sql_id,
               int64_t plan_id,
               ObIArray<ObSqlPlanItem*> &plan);
  int get_plan_by_hash(const ObString &sql_id,
                       uint64_t plan_hash,
                       ObIArray<ObSqlPlanItem*> &plan);

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
  int64_t get_capacity();
  int64_t get_next_plan_id();
  int64_t get_last_plan_id();
  bool is_valid() const;

  static int get_mem_limit(uint64_t tenant_id, int64_t &mem_limit);
  static int init_plan_manager(uint64_t tenant_id, ObSqlPlanMgr* &sql_plan_mgr);
  static int mtl_init(ObSqlPlanMgr* &sql_plan_mgr);
  static void mtl_destroy(ObSqlPlanMgr* &sql_plan_mgr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSqlPlanMgr);

private:
  common::ObConcurrentFIFOAllocator allocator_;//alloc mem for string buf
  ObSqlPlanEliminateTask task_;
  common::ObRaQueue queue_;
  int64_t plan_id_increment_;
  bool destroyed_;
  bool inited_;

  // tenant id of this manager
  uint64_t tenant_id_;
  int tg_id_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_SQL_PLAN_MGR_H_ */
