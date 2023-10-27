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


#define USING_LOG_PREFIX SQL
#include "ob_plan_info_manager.h"
#include "lib/compress/ob_compressor.h"
#include "lib/compress/ob_compressor_pool.h"
#include "observer/ob_server.h"
namespace oceanbase
{
namespace sql
{
ObSqlPlanItem::ObSqlPlanItem()
{
  reset();
}

ObSqlPlanItem::~ObSqlPlanItem()
{
}

void ObSqlPlanItem::reset()
{
  operation_ = NULL;
  operation_len_ = 0;
  options_ = NULL;
  options_len_ = 0;
  object_node_ = NULL;
  object_node_len_ = 0;
  object_id_ = 0;
  object_owner_ = NULL;
  object_owner_len_ = 0;
  object_name_ = NULL;
  object_name_len_ = 0;
  object_alias_ = NULL;
  object_alias_len_ = 0;
  object_type_ = NULL;
  object_type_len_ = 0;
  optimizer_ = NULL;
  optimizer_len_ = 0;
  id_ = 0;
  parent_id_ = 0;
  depth_ = 0;
  position_ = 0;
  is_last_child_ = false;
  search_columns_ = 0;
  cost_ = 0;
  real_cost_ = 0;
  cardinality_ = 0;
  real_cardinality_ = 0;
  bytes_ = 0;
  rowset_ = 1;
  other_tag_ = NULL;
  other_tag_len_ = 0;
  partition_start_ = NULL;
  partition_start_len_ = 0;
  partition_stop_ = NULL;
  partition_stop_len_ = 0;
  partition_id_ = 0;
  other_ = NULL;
  other_len_ = 0;
  distribution_ = NULL;
  distribution_len_ = 0;
  cpu_cost_ = 0;
  io_cost_ = 0;
  temp_space_ = 0;
  access_predicates_ = NULL;
  access_predicates_len_ = 0;
  filter_predicates_ = NULL;
  filter_predicates_len_ = 0;
  startup_predicates_ = NULL;
  startup_predicates_len_ = 0;
  projection_ = NULL;
  projection_len_ = 0;
  special_predicates_ = NULL;
  special_predicates_len_ = 0;
  time_ = 0;
  qblock_name_ = NULL;
  qblock_name_len_ = 0;
  remarks_ = NULL;
  remarks_len_ = 0;
  other_xml_ = NULL;
  other_xml_len_ = 0;
}

int64_t ObSqlPlanItem::get_extra_size() const
{
  return operation_len_ +
        options_len_ +
        object_node_len_ +
        object_owner_len_ +
        object_name_len_ +
        object_alias_len_ +
        object_type_len_ +
        optimizer_len_ +
        other_tag_len_ +
        partition_start_len_ +
        partition_stop_len_ +
        other_len_ +
        distribution_len_ +
        access_predicates_len_ +
        filter_predicates_len_ +
        startup_predicates_len_ +
        projection_len_ +
        special_predicates_len_ +
        qblock_name_len_ +
        remarks_len_ +
        other_xml_len_;
}

ObLogicalPlanHead::ObLogicalPlanHead()
{
  reset();
}

ObLogicalPlanHead::~ObLogicalPlanHead()
{

}

void ObLogicalPlanHead::reset()
{
  count_ = 0;
  plan_item_pos_ = NULL;
}

ObLogicalPlanHead::PlanItemPos::PlanItemPos()
{
  reset();
}

ObLogicalPlanHead::PlanItemPos::~PlanItemPos()
{

}

void ObLogicalPlanHead::PlanItemPos::reset()
{
  offset_ = 0;
  length_ = 0;
}

ObLogicalPlanRawData::ObLogicalPlanRawData()
{
  reset();
}

ObLogicalPlanRawData::~ObLogicalPlanRawData()
{

}

void ObLogicalPlanRawData::reset()
{
  logical_plan_ = NULL;
  logical_plan_len_ = 0;
  uncompress_len_ = 0;
}

bool ObLogicalPlanRawData::is_valid() const
{
  return NULL != logical_plan_;
}

int ObLogicalPlanRawData::compress_logical_plan(ObIAllocator &allocator,
                                                ObIArray<ObSqlPlanItem*> &plan_items)
{
  int ret = OB_SUCCESS;
  //step 1: serialize logical plan
  char *buf = NULL;
  ObLogicalPlanHead *head = NULL;
  int64_t head_size = sizeof(ObLogicalPlanHead) +
    plan_items.count() * sizeof(ObLogicalPlanHead::PlanItemPos);
  int64_t total_size = 0;
  int64_t buf_pos = head_size;
  total_size += head_size;
  //calculate logical plan length
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_items.count(); ++i) {
    if (OB_ISNULL(plan_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else {
      total_size += sizeof(ObSqlPlanItem) + plan_items.at(i)->get_extra_size();
    }
  }
  //alloc memory
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (NULL == (buf = (char*)allocator.alloc(total_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("alloc mem failed", K(total_size), K(ret));
    }
  } else {
    //init operator count
    head = new(buf)ObLogicalPlanHead();
    head->count_ = plan_items.count();
    head->plan_item_pos_ = reinterpret_cast<ObLogicalPlanHead::PlanItemPos*>(
                              buf + sizeof(ObLogicalPlanHead));
  }
  //serialize each operator
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_items.count(); ++i) {
    ObSqlPlanItem* plan_item = plan_items.at(i);
    if (OB_ISNULL(plan_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null plan item", K(ret));
    } else {
      //init operator map info
      ObLogicalPlanHead::PlanItemPos *plan_item_pos = head->plan_item_pos_ + i;
      plan_item_pos->offset_ = buf_pos;
      plan_item_pos->length_ = sizeof(ObSqlPlanItem) + plan_item->get_extra_size();
      //init operator basic info
      ObSqlPlanItem *new_plan_item = new(buf+buf_pos)ObSqlPlanItem();
      *new_plan_item = *plan_item;
      buf_pos += sizeof(ObSqlPlanItem);
      #define DEEP_COPY_DATA(value)                                               \
      do {                                                                        \
        if (OB_FAIL(ret)) {                                                       \
        } else if (buf_pos + plan_item->value##len_ > total_size) {               \
          ret = OB_ERR_UNEXPECTED;                                                \
          LOG_WARN("unexpect record size", K(buf_pos), K(plan_item->value##len_), \
                                          K(total_size), K(ret));                 \
        } else if ((plan_item->value##len_ > 0) && (NULL != plan_item->value)) {  \
          MEMCPY(buf + buf_pos, plan_item->value, plan_item->value##len_);        \
          new_plan_item->value = reinterpret_cast<char*>(buf_pos);                \
          buf_pos += plan_item->value##len_;                                      \
        } else {                                                                  \
          new_plan_item->value = reinterpret_cast<char*>(buf_pos);                \
        }                                                                         \
      } while(0);
      //copy buffer data and convert ptr to offset
      DEEP_COPY_DATA(operation_);
      DEEP_COPY_DATA(options_);
      DEEP_COPY_DATA(object_node_);
      DEEP_COPY_DATA(object_owner_);
      DEEP_COPY_DATA(object_name_);
      DEEP_COPY_DATA(object_alias_);
      DEEP_COPY_DATA(object_type_);
      DEEP_COPY_DATA(optimizer_);
      DEEP_COPY_DATA(other_tag_);
      DEEP_COPY_DATA(partition_start_);
      DEEP_COPY_DATA(partition_stop_);
      DEEP_COPY_DATA(other_);
      DEEP_COPY_DATA(distribution_);
      DEEP_COPY_DATA(access_predicates_);
      DEEP_COPY_DATA(filter_predicates_);
      DEEP_COPY_DATA(startup_predicates_);
      DEEP_COPY_DATA(projection_);
      DEEP_COPY_DATA(special_predicates_);
      DEEP_COPY_DATA(qblock_name_);
      DEEP_COPY_DATA(remarks_);
      DEEP_COPY_DATA(other_xml_);
    }
  }
  //step 2: compress data
  common::ObCompressorType compressor_type = LZ4_COMPRESSOR;
  common::ObCompressor *compressor = NULL;
  char *compress_buf = NULL;
  int64_t compress_size = total_size * 2;
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                             compressor))) {
    LOG_WARN("fail to get compressor", K(compressor_type), K(ret));
  } else if (OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null compressor", K(ret));
  } else if (NULL == (compress_buf = (char*)allocator.alloc(compress_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("alloc mem failed", K(compress_size), K(ret));
    }
  } else if (OB_FAIL(compressor->compress(buf,
                                          total_size,
                                          compress_buf,
                                          compress_size,
                                          compress_size))) {
    LOG_WARN("failed to compress data", K(ret));
  } else if (compress_size >= total_size) {
    //will not use compress buffer
    logical_plan_ = buf;
    logical_plan_len_ = total_size;
    uncompress_len_ = -1;
    allocator.free(compress_buf);
    compress_buf = NULL;
  } else {
    //use compress buffer
    logical_plan_ = compress_buf;
    logical_plan_len_ = compress_size;
    uncompress_len_ = total_size;
    allocator.free(buf);
    buf = NULL;
  }
  return ret;
}

int ObLogicalPlanRawData::uncompress_logical_plan(ObIAllocator &allocator,
                                                  ObIArray<ObSqlPlanItem*> &plan_items)
{
  int ret = OB_SUCCESS;
  //step 1: uncompress data
  common::ObCompressorType compressor_type = LZ4_COMPRESSOR;
  common::ObCompressor *compressor = NULL;
  char *uncompress_buf = NULL;
  int64_t uncompress_size = uncompress_len_;
  if (NULL == logical_plan_ || logical_plan_len_ <= 0) {
    //do nothing
  } else if (uncompress_len_ < 0) {
    //do not need decompress
    uncompress_size = logical_plan_len_;
    if (NULL == (uncompress_buf = (char*)allocator.alloc(uncompress_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        LOG_WARN("alloc mem failed", K(uncompress_size), K(ret));
      }
    } else {
      MEMCPY(uncompress_buf, logical_plan_, uncompress_size);
    }
  } else if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compressor_type,
                                                                             compressor))) {
    LOG_WARN("fail to get compressor", K(compressor_type), K(ret));
  } else if (OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null compressor", K(ret));
  } else if (NULL == (uncompress_buf = (char*)allocator.alloc(uncompress_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      LOG_WARN("alloc mem failed", K(uncompress_size), K(ret));
    }
  } else if (OB_FAIL(compressor->decompress(logical_plan_,
                                            logical_plan_len_,
                                            uncompress_buf,
                                            uncompress_size,
                                            uncompress_size))) {
    LOG_WARN("failed to compress data", K(ret));
  }
  if (OB_FAIL(ret) || NULL == uncompress_buf) {
    //do nothing
  } else {
    //step 2: deserialize logical plan
    //get logical plan head info
    ObLogicalPlanHead *head = NULL;
    head = reinterpret_cast<ObLogicalPlanHead*>(uncompress_buf);
    head->plan_item_pos_ = reinterpret_cast<ObLogicalPlanHead::PlanItemPos*>(
                              uncompress_buf + sizeof(ObLogicalPlanHead));
    //deserialize each operator info
    for (int64_t i = 0; OB_SUCC(ret) && i < head->count_; ++i) {
      //get operator map info
      ObLogicalPlanHead::PlanItemPos *plan_item_pos = head->plan_item_pos_ + i;
      if (plan_item_pos->offset_ < 0 ||
          plan_item_pos->offset_ + plan_item_pos->length_ > uncompress_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("broken compressed data", K(ret));
      } else {
        ObSqlPlanItem *plan_item = reinterpret_cast<ObSqlPlanItem*>(
                                    uncompress_buf+plan_item_pos->offset_);
        #define CONVERT_OFFSET_TO_PTR(value)                                                  \
        do {                                                                                  \
          int64_t offset = reinterpret_cast<int64_t>(plan_item->value);                       \
          if (OB_FAIL(ret)) {                                                                 \
          } else if (offset + plan_item->value##len_ > uncompress_size) {                     \
            ret = OB_ERR_UNEXPECTED;                                                          \
            LOG_WARN("unexpect record size", K(offset), K(plan_item->value##len_),            \
                                            K(uncompress_size), K(ret));                      \
          } else {                                                                            \
            plan_item->value = uncompress_buf + offset;                                       \
          }                                                                                   \
        } while(0);
        //convert offset to ptr
        CONVERT_OFFSET_TO_PTR(operation_);
        CONVERT_OFFSET_TO_PTR(options_);
        CONVERT_OFFSET_TO_PTR(object_node_);
        CONVERT_OFFSET_TO_PTR(object_owner_);
        CONVERT_OFFSET_TO_PTR(object_name_);
        CONVERT_OFFSET_TO_PTR(object_alias_);
        CONVERT_OFFSET_TO_PTR(object_type_);
        CONVERT_OFFSET_TO_PTR(optimizer_);
        CONVERT_OFFSET_TO_PTR(other_tag_);
        CONVERT_OFFSET_TO_PTR(partition_start_);
        CONVERT_OFFSET_TO_PTR(partition_stop_);
        CONVERT_OFFSET_TO_PTR(other_);
        CONVERT_OFFSET_TO_PTR(distribution_);
        CONVERT_OFFSET_TO_PTR(access_predicates_);
        CONVERT_OFFSET_TO_PTR(filter_predicates_);
        CONVERT_OFFSET_TO_PTR(startup_predicates_);
        CONVERT_OFFSET_TO_PTR(projection_);
        CONVERT_OFFSET_TO_PTR(special_predicates_);
        CONVERT_OFFSET_TO_PTR(qblock_name_);
        CONVERT_OFFSET_TO_PTR(remarks_);
        CONVERT_OFFSET_TO_PTR(other_xml_);
        if (OB_SUCC(ret) &&
            OB_FAIL(plan_items.push_back(plan_item))) {
          LOG_WARN("failed to push back plan item", K(ret));
        }
      }
    }
  }
  return ret;
}

} // end of namespace sql
} // end of namespace oceanbase
