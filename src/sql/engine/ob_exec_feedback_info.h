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

#ifndef _OB_EXEC_FEEDBACK_INFO_H
#define _OB_EXEC_FEEDBACK_INFO_H

#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_array_serialization.h"
namespace oceanbase
{
namespace sql
{


struct ObExecFeedbackNode final
{
  OB_UNIS_VERSION(1);
public:
  ObExecFeedbackNode(int64_t op_id) : op_id_(op_id), output_row_count_(0),
      op_open_time_(INT64_MAX), op_close_time_(0), op_first_row_time_(INT64_MAX),
      op_last_row_time_(0), db_time_(0),  block_time_(0), worker_count_(0) {}
  ObExecFeedbackNode() : op_id_(OB_INVALID_ID), output_row_count_(0),
      op_open_time_(INT64_MAX), op_close_time_(0), op_first_row_time_(INT64_MAX),
      op_last_row_time_(0), db_time_(0),  block_time_(0), worker_count_(0) {}
  ~ObExecFeedbackNode() {}
  TO_STRING_KV(K_(op_id), K_(output_row_count), K_(op_open_time),
               K_(op_close_time), K_(op_first_row_time), K_(op_last_row_time),
               K_(db_time), K_(block_time));
public:
  int64_t op_id_;
  int64_t output_row_count_;
  int64_t op_open_time_;
  int64_t op_close_time_;
  int64_t op_first_row_time_;
  int64_t op_last_row_time_;
  int64_t db_time_;    // rdtsc cpu cycles spend on this op, include cpu instructions & io
  int64_t block_time_; // rdtsc cpu cycles wait for network, io etc
  int64_t worker_count_;
};

class ObExecFeedbackInfo final
{
  OB_UNIS_VERSION(1);
public:
  ObExecFeedbackInfo() : nodes_(), total_db_time_(0), is_valid_(true) {}
  ~ObExecFeedbackInfo() { reset(); }
  int merge_feedback_info(const ObExecFeedbackInfo &info);
  int add_feedback_node(ObExecFeedbackNode &node) { return nodes_.push_back(node); };
  common::ObIArray<ObExecFeedbackNode> &get_feedback_nodes() { return nodes_; };
  const common::ObIArray<ObExecFeedbackNode> &get_feedback_nodes() const { return nodes_; };
  bool is_valid() const { return is_valid_ && nodes_.count() > 0; }
  int assign(const ObExecFeedbackInfo &other);
  int64_t get_total_db_time() const { return total_db_time_; }
  int64_t &get_total_db_time() { return total_db_time_; }
  void reset() { nodes_.reset(); }
  TO_STRING_KV(K_(nodes), K_(total_db_time), K_(is_valid));
private:
  common::ObSArray<ObExecFeedbackNode> nodes_;
  int64_t total_db_time_;
  bool is_valid_;
};



}
}

#endif
