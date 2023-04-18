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
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/ob_exec_feedback_info.h"


using namespace oceanbase::common;
using namespace oceanbase::sql;

OB_SERIALIZE_MEMBER(ObExecFeedbackNode,
                    op_id_,
                    output_row_count_,
                    op_open_time_,
                    op_close_time_,
                    op_first_row_time_,
                    op_last_row_time_,
                    db_time_,
                    block_time_,
                    worker_count_);

OB_SERIALIZE_MEMBER(ObExecFeedbackInfo,
                    nodes_,
                    total_db_time_);

int ObExecFeedbackInfo::merge_feedback_info(const ObExecFeedbackInfo &feedback_info)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || !feedback_info.is_valid()) {
    LOG_TRACE("feedback info is not valid", K(ret), K(is_valid_), K(feedback_info.is_valid_));
  } else if (nodes_.count() < feedback_info.get_feedback_nodes().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of feedback info is unexpected", K(nodes_.count()), K(feedback_info.get_feedback_nodes().count()));
  } else {
    const common::ObIArray<ObExecFeedbackNode> &fb_nodes = feedback_info.get_feedback_nodes();
    int left = 0, right = 0;
    while (left < nodes_.count() && right < fb_nodes.count() && OB_SUCC(ret)) {
      if (nodes_.at(left).op_id_ == fb_nodes.at(right).op_id_) {
        nodes_.at(left).op_open_time_ =
            min(fb_nodes.at(right).op_open_time_, nodes_.at(left).op_open_time_);
        nodes_.at(left).op_first_row_time_ =
            min(fb_nodes.at(right).op_first_row_time_, nodes_.at(left).op_first_row_time_);
        nodes_.at(left).block_time_ =
            max(fb_nodes.at(right).block_time_, nodes_.at(left).block_time_);
        nodes_.at(left).op_last_row_time_ =
            max(fb_nodes.at(right).op_last_row_time_, nodes_.at(left).op_last_row_time_);
        nodes_.at(left).op_close_time_ =
            max(fb_nodes.at(right).op_close_time_, nodes_.at(left).op_close_time_);
        nodes_.at(left).db_time_ = max(fb_nodes.at(right).db_time_, nodes_.at(left).db_time_);
        nodes_.at(left).output_row_count_ += fb_nodes.at(right).output_row_count_;
        nodes_.at(left).worker_count_ += fb_nodes.at(right).worker_count_;
        left++;
        right++;
        continue;
      } else if (nodes_.at(left).op_id_ < fb_nodes.at(right).op_id_) {
        left++;
        continue;
      } else if (nodes_.at(left).op_id_ > fb_nodes.at(right).op_id_) {
        is_valid_ = false;
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected id node", K(ret));
        break;
      }
    }
  }
  if (OB_FAIL(ret)) {
    is_valid_ = false;
    LOG_WARN("mark the feedback info is invalid", K(ret));
  } else {
    total_db_time_ += feedback_info.get_total_db_time();
  }
  return ret;
}

int ObExecFeedbackInfo::assign(const ObExecFeedbackInfo &other)
{
  int ret = OB_SUCCESS;
  total_db_time_ = other.get_total_db_time();
  is_valid_ = other.is_valid();
  OZ(nodes_.assign(other.get_feedback_nodes()));
  return ret;
}
