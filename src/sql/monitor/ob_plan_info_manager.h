/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_OBSERVER_PLAN_INFO_MGR_H_
#define SRC_OBSERVER_PLAN_INFO_MGR_H_
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace sql
{

struct ObSqlPlanItem {
  ObSqlPlanItem();
  virtual ~ObSqlPlanItem();
  void reset();
  int64_t get_extra_size() const;

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
  int64_t real_cost_;
  int64_t cardinality_;
  int64_t real_cardinality_;
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
    K_(id)
  );
};

struct ObLogicalPlanHead
{
  ObLogicalPlanHead();
  virtual ~ObLogicalPlanHead();
  void reset();
  struct PlanItemPos
  {
    PlanItemPos();
    virtual ~PlanItemPos();
    void reset();
    int64_t offset_;
    int64_t length_;
  };
  int64_t count_;                 //operator count
  PlanItemPos *plan_item_pos_;    //operator data position in buffer
};

struct ObLogicalPlanRawData
{
  ObLogicalPlanRawData();
  virtual ~ObLogicalPlanRawData();
  void reset();
  bool is_valid() const;
  int compress_logical_plan(ObIAllocator &allocator, ObIArray<ObSqlPlanItem*> &plan_items);
  int uncompress_logical_plan(ObIAllocator &allocator, ObIArray<ObSqlPlanItem*> &plan_items);
  char *logical_plan_;        //serialize and compress data
  int64_t logical_plan_len_;  //compress data length
  //uncompress data length, used for uncompress function
  //if values is -1, logical plan not be compressed
  int64_t uncompress_len_;
};

} // end of namespace sql
} // end of namespace oceanbase



#endif /* SRC_OBSERVER_PLAN_INFO_MGR_H_ */
