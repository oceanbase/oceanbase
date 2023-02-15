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

class ObPlanRealInfoMgr
{
public:
  typedef common::ObRaQueue::Ref Ref;

public:
  ObPlanRealInfoMgr(common::ObConcurrentFIFOAllocator *allocator);
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
  int64_t release_old(int64_t limit);
  void clear_queue();

  int64_t get_start_idx() const;
  int64_t get_end_idx() const;
  int64_t get_size_used();
  int64_t get_capacity();
  bool is_valid() const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanRealInfoMgr);

private:
  common::ObConcurrentFIFOAllocator *allocator_;
  common::ObRaQueue queue_;
  bool destroyed_;
  bool inited_;
};

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

class ObPlanItemMgr
{
public:
  typedef common::ObRaQueue::Ref Ref;

public:
  ObPlanItemMgr(common::ObConcurrentFIFOAllocator *allocator);
  virtual ~ObPlanItemMgr();
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
  int64_t release_old(int64_t limit);
  void clear_queue();

  int64_t get_start_idx() const;
  int64_t get_end_idx() const;
  int64_t get_size_used();
  int64_t get_capacity();
  int64_t get_next_plan_id();
  int64_t get_last_plan_id();
  bool is_valid() const;
  TO_STRING_KV(
    K_(plan_id_increment)
  );
private:
  DISALLOW_COPY_AND_ASSIGN(ObPlanItemMgr);

private:
  common::ObConcurrentFIFOAllocator *allocator_;
  common::ObRaQueue queue_;
  int64_t plan_id_increment_;
  bool destroyed_;
  bool inited_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_PLAN_REAL_INFO_MGR_H_ */
