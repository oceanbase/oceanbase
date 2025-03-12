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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_range_generator.h"
#include "lib/timezone/ob_time_convert.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/geo/ob_geo_utils.h"
#include "lib/rc/ob_rc.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/engine/expr/ob_geo_expr_utils.h"
#include "sql/rewrite/ob_query_range.h"
#include "src/share/object/ob_obj_cast_util.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "sql/engine/expr/ob_expr_json_utils.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

#define GET_RANGE_NODE_DOMAIN_TYPE(n) (static_cast<ObDomainOpType>((n)->domain_extra_.domain_releation_type_))

int ObTmpRange::copy(ObTmpRange &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    include_start_ = other.include_start_;
    include_end_ = other.include_end_;
    always_true_ = other.always_true_;
    always_false_ = other.always_false_;
    min_offset_ = other.min_offset_;
    max_offset_ = other.max_offset_;
    column_cnt_ = other.column_cnt_;
    is_phy_rowid_ = other.is_phy_rowid_;
    for (int64_t i = 0; i < column_cnt_; ++i) {
      start_[i] = other.start_[i];
      end_[i] = other.end_[i];
    }
  }
  return ret;
}

DEF_TO_STRING(ObTmpRange)
{
  int64_t pos = 0;
  J_OBJ_START();
  // print parameters
  J_KV(K(always_true_),
       K(always_false_),
       K(min_offset_),
       K(max_offset_),
       K(column_cnt_),
       K(include_start_),
       K(include_end_),
       K(is_phy_rowid_));
  // print range
  J_COMMA();
  BUF_PRINTF("range:");
  J_ARRAY_START();
  for (int64_t i = 0; i < column_cnt_; ++i) {
    BUF_PRINTO(start_[i]);
    if (i == column_cnt_ - 1) {
      BUF_PRINTF("; ");
    } else {
      J_COMMA();
    }
  }
  for (int64_t i = 0; i < column_cnt_; ++i) {
    BUF_PRINTO(end_[i]);
    if (i < column_cnt_ - 1) {
      J_COMMA();
    }
  }
  J_ARRAY_END();
  J_OBJ_END();
  return pos;
}

int ObTmpRange::init_tmp_range(int64_t column_count)
{
  int ret = OB_SUCCESS;
  void *start_ptr = NULL;
  void *end_ptr = NULL;
  if (OB_UNLIKELY(column_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column count", K(column_count));
  } else if (OB_ISNULL(start_ptr = allocator_.alloc(sizeof(ObObj) * column_count)) ||
             OB_ISNULL(end_ptr = allocator_.alloc(sizeof(ObObj) * column_count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memeory failed", K(start_ptr), K(end_ptr));
  } else {
    start_ = new(start_ptr) ObObj[column_count];
    end_ = new(end_ptr) ObObj[column_count];
    column_cnt_ = column_count;
  }
  return ret;
}

void ObTmpRange::set_always_true()
{
  always_true_ = true;
  include_start_ = false;
  include_end_ = false;
  is_phy_rowid_ = false;
  min_offset_ = 0;
  max_offset_ = 0;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    start_[i].set_min_value();
    end_[i].set_max_value();
  }
}

void ObTmpRange::set_always_false()
{
  always_false_ = true;
  include_start_ = false;
  include_end_ = false;
  is_phy_rowid_ = false;
  min_offset_ = 0;
  max_offset_ = 0;
  for (int64_t i = 0; i < column_cnt_; ++i) {
    start_[i].set_max_value();
    end_[i].set_min_value();
  }
}

int ObTmpRange::intersect(ObTmpRange &other, bool &not_consistent)
{
  int ret = OB_SUCCESS;
  not_consistent = false;
  if (OB_ISNULL(other.start_) || OB_ISNULL(other.end_) ||
      OB_ISNULL(start_) || OB_ISNULL(end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(other.start_), K(other.end_), K(start_), K(end_));
  } else if (other.always_false_) {
    set_always_false();
  } else if (other.always_true_ || always_false_) {
    // do nothing
  } else if (always_true_) {
    if (OB_FAIL(copy(other))) {
      LOG_WARN("failed to copy tmp range");
    }
  } else if (min_offset_ <= other.min_offset_) {
    if (max_offset_ < other.min_offset_ - 1) {
      not_consistent = true;
    } else {
      // int64_t idx = other.min_offset_;
      bool merge_start = false;
      bool merge_end = false;
      max_offset_ = std::max(max_offset_, other.max_offset_);
      for (int64_t i = other.min_offset_; i < column_cnt_; ++i) {
        int cmp = start_[i].compare(other.start_[i]);
        if (cmp > 0) {
          break;
        } else if (cmp < 0) {
          merge_start = true;
          for (; i < column_cnt_; ++i) {
            start_[i] = other.start_[i];
          }
          include_start_ = other.include_start_;
        } else if (i == column_cnt_ - 1) {
          if (include_start_ && !other.include_start_) {
            merge_start = true;
          }
          include_start_ = include_start_ && other.include_start_;
        }
      }
      for (int64_t i = other.min_offset_; i < column_cnt_; ++i) {
        int cmp = end_[i].compare(other.end_[i]);
        if (cmp > 0) {
          merge_end = true;
          for (; i < column_cnt_; ++i) {
            end_[i] = other.end_[i];
          }
          include_end_ = other.include_end_;
        } else if (cmp < 0) {
          break;
        } else if (i == column_cnt_ - 1) {
          if (include_end_ && !other.include_end_) {
            merge_end = true;
          }
          include_end_ = include_end_ && other.include_end_;
        }
      }
      if (merge_start != merge_end) {
        // only merge part of start key and end key, need to check if get always false range
        bool always_false = false;
        bool all_equal = true;
        for (int64_t i = min_offset_; !always_false && i < column_cnt_; ++i) {
          int cmp = start_[i].compare(end_[i]);
          if (cmp > 0) {
            always_false = true;
          } else if (cmp < 0) {
            break;
          } else if (i == column_cnt_ - 1) {
            always_false = !(include_start_ && include_end_);
          }
        }
        if (always_false) {
          set_always_false();
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected offset", KPC(this), K(other));
  }
  return ret;
}

int ObTmpRange::formalize()
{
  int ret = OB_SUCCESS;
  if (always_true_ || always_false_) {
    // do nothing
  } else {
    bool always_true = true;
    for (int64_t i = min_offset_; OB_SUCC(ret) && i < column_cnt_; ++i) {
      if (start_[i].is_min_value() && end_[i].is_max_value()) {
        // do nothing
      } else if (OB_UNLIKELY(!start_[i].can_compare(end_[i]))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start obj can not compare with end obj", K(start_[i]), K(end_[i]));
      } else {
        always_true = false;
        int cmp = start_[i].compare(end_[i]);
        if (cmp < 0) {
          // find first start[i] < end[i], ignore following value
          // e.g. (c1,c2) between (1,5) and (3,1) => (1, 5, max; 3, 1, min)
          break;
        } else if (cmp > 0) {
          always_false_ = true;
          break;
        } else if (i == column_cnt_ - 1 && !(include_start_ && include_end_)) {
          always_false_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (always_true) {
        always_true_ = always_true;
      } else if (always_false_) {
        set_always_false();
      }
    }
  }
  return ret;
}

int ObTmpRange::refine_final_range()
{
  int ret = OB_SUCCESS;
  bool skip_start = false;
  bool skip_end = false;
  if (OB_ISNULL(start_) || OB_ISNULL(end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(start_), K(end_));
  } else if (min_offset_ != 0) {
    always_true_ = true;
    include_start_ = false;
    include_end_ = false;
    for (int64_t i = 0; i < column_cnt_; ++i) {
      start_[i].set_min_value();
      end_[i].set_max_value();
    }
  } else {
    // when find a min/max in start/end key, following start/end key is meanless.
    //  e.g. `(1, min, 3;` equal to `(1, min, min)`
    bool find_min_start = false;
    bool find_max_end = false;
    for (int64_t i = 0; i < column_cnt_; ++i) {
      if (find_min_start) {
        start_[i].set_min_value();
        include_start_ = false;
      } else if (start_[i].is_min_value()) {
        find_min_start = true;
        include_start_ = false;
      } else if (OB_UNLIKELY(start_[i].is_nop_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected nop value");
      }
      if (find_max_end) {
        end_[i].set_max_value();
        include_end_ = false;
      } else if (end_[i].is_max_value()) {
        find_max_end = true;
        include_end_ = false;
      } else if (OB_UNLIKELY(end_[i].is_nop_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected nop value");
      }
    }
  }
  return ret;
}

int ObRangeGenerator::generate_tmp_range(ObTmpRange *&tmp_range, const int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  tmp_range = nullptr;
  if (OB_ISNULL(tmp_range = static_cast<ObTmpRange *>(allocator_.alloc(sizeof(ObTmpRange))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    new(tmp_range) ObTmpRange(allocator_);
    if (OB_FAIL(tmp_range->init_tmp_range(column_cnt))) {
      LOG_WARN("failed to init tmp range");
    }
  }
  return ret;
}

int ObRangeGenerator::generate_one_range(ObTmpRange &tmp_range)
{
  int ret = OB_SUCCESS;
  ObNewRange *range = nullptr;
  if (OB_FAIL(create_new_range(range, tmp_range.column_cnt_))) {
    LOG_WARN("failed to create new range");
  } else if (OB_FAIL(tmp_range.refine_final_range())) {
    LOG_WARN("failed to refine final range");
  } else {
    ObObj *starts = range->start_key_.get_obj_ptr();
    ObObj *ends = range->end_key_.get_obj_ptr();
    for (int i = 0; OB_SUCC(ret) && i < tmp_range.column_cnt_; ++i) {
      new(starts + i) ObObj();
      new(ends + i) ObObj();
      if (OB_FAIL(ob_write_obj(allocator_, *(tmp_range.start_ + i), *(starts + i)))) {
        LOG_WARN("deep copy start obj failed", K(i));
      } else if (OB_FAIL(ob_write_obj(allocator_, *(tmp_range.end_ + i), *(ends + i)))) {
        LOG_WARN("deep copy end obj failed", K(i));
      }
    }
    if (OB_SUCC(ret)) {
      range->is_physical_rowid_range_ = tmp_range.is_phy_rowid_;
      if (OB_UNLIKELY(tmp_range.is_phy_rowid_ && tmp_range.column_cnt_ > 1)) {
        // rowid for table location, only has one column
        if (starts[1].is_min_value()) {
          range->border_flag_.set_inclusive_start();
        } else {
          range->border_flag_.unset_inclusive_start();
        }
        if (ends[1].is_max_value()) {
          range->border_flag_.set_inclusive_end();
        } else {
          range->border_flag_.unset_inclusive_end();
        }
        range->start_key_.assign(starts, 1);
        range->end_key_.assign(ends, 1);
      } else {
        if (tmp_range.include_start_) {
          range->border_flag_.set_inclusive_start();
        } else {
          range->border_flag_.unset_inclusive_start();
        }
        if (tmp_range.include_end_) {
          range->border_flag_.set_inclusive_end();
        } else {
          range->border_flag_.unset_inclusive_end();
        }
      }
      if (tmp_range.always_false_) {
        always_false_range_ = range;
      } else if (tmp_range.always_true_) {
        always_true_range_ = range;
        all_single_value_ranges_ = false;
      } else if (OB_FAIL(ranges_.push_back(range))) {
        LOG_WARN("failed to push back range");
      } else if (!(range->border_flag_.inclusive_start() &&
                   range->border_flag_.inclusive_end() &&
                   range->start_key_ == range->end_key_)) {
        all_single_value_ranges_ = false;
      }
    }
    LOG_TRACE("succeed to generate one range", KPC(range), K(tmp_range));
  }
  return ret;
}

int ObRangeGenerator::generate_ranges()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_ctx = exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(pre_range_graph_) || OB_ISNULL(phy_ctx) ||
      OB_ISNULL(pre_range_graph_->get_range_head())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range graph", KPC(pre_range_graph_), K(phy_ctx));
  } else if (pre_range_graph_->has_exec_param() && !phy_ctx->is_exec_param_readable()) {
    // pre range graph has exec param and not exec stage, generate (min; max)
    if (OB_FAIL(generate_contain_exec_param_range())) {
      LOG_WARN("faield to generate contain exec param range");
    }
  } else if (OB_LIKELY(pre_range_graph_->is_precise_get())) {
    if (OB_FAIL(generate_precise_get_range(*pre_range_graph_->get_range_head()))) {
      LOG_WARN("failed to genenrate precise get range");
    }
  } else if (pre_range_graph_->is_standard_range()) {
    if (OB_FAIL(generate_standard_ranges(pre_range_graph_->get_range_head()))) {
      LOG_WARN("failed to genenrate precise get range");
    }
  } else if (OB_FAIL(generate_complex_ranges(pre_range_graph_->get_range_head()))) {
    LOG_WARN("failed to generate final ranges");
  }
  return ret;
}

OB_INLINE int ObRangeGenerator::generate_precise_get_range(const ObRangeNode &node)
{
  int ret = OB_SUCCESS;
  ObNewRange *range = nullptr;
  if (OB_NOT_NULL(node.and_next_) || OB_NOT_NULL(node.or_next_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected precise get range", K(node));
  } else if (OB_FAIL(create_new_range(range, node.column_cnt_))) {
    LOG_WARN("failed to create new range");
  } else {
    bool always_false = false;
    ObObj *starts = range->start_key_.get_obj_ptr();
    ObObj *ends = range->end_key_.get_obj_ptr();
    for (int64_t i = 0; i < node.column_cnt_; ++i) {
      starts[i].set_min_value();
      ends[i].set_max_value();
    }
    for (int i = 0; OB_SUCC(ret) && !always_false && i < node.column_cnt_; ++i) {
      int64_t start_idx = node.start_keys_[i];
      int64_t end_idx = node.end_keys_[i];
      const ObRangeColumnMeta *meta = pre_range_graph_->get_column_meta(i);
      int64_t cmp = 0;
      bool is_valid = true;
      if (OB_ISNULL(meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column meta");
      } else if (OB_UNLIKELY(start_idx != end_idx ||
                             !is_const_expr_or_null(start_idx) ||
                             !is_const_expr_or_null(end_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("start idx should equal to end idx", K(start_idx), K(end_idx));
      } else if (start_idx == OB_RANGE_NULL_VALUE) {
        starts[i].set_null();
        ends[i].set_null();
      } else if (OB_FAIL(get_result_value(start_idx, starts[i], is_valid, exec_ctx_))) {
        LOG_WARN("failed to get result vlaue", K(start_idx));
      } else if (!is_valid) {
        always_false = true;
      } else if (OB_LIKELY(ObSQLUtils::is_same_type_for_compare(starts[i].get_meta(),
                                                                meta->column_type_.get_obj_meta()) &&
                           !starts[i].is_decimal_int())) {
        starts[i].set_collation_type(meta->column_type_.get_collation_type());
      } else if (OB_LIKELY(!starts[i].is_overflow_integer(meta->column_type_.get_type()))) {
        //fast cast with integer value
        starts[i].set_meta_type(meta->column_type_);
      } else if (!node.is_phy_rowid_ && OB_FAIL(try_cast_value(*meta, starts[i], cmp, CO_EQ))) {
        LOG_WARN("failed to cast value", K(meta), K(starts[i]));
      } else if (cmp != 0) {
        always_false = true;
      } else {
        starts[i].set_collation_type(meta->column_type_.get_collation_type());
      }
      if (OB_SUCC(ret) && !always_false) {
        ends[i] = starts[i];
      }
    }

    if (OB_SUCC(ret)) {
      range->is_physical_rowid_range_ = node.is_phy_rowid_;
      if (OB_LIKELY(!always_false)) {
        range->border_flag_.set_inclusive_start();
        range->border_flag_.set_inclusive_end();
      } else {
        range->border_flag_.unset_inclusive_start();
        range->border_flag_.unset_inclusive_end();
        for (int i = 0; i < node.column_cnt_; ++i) {
          starts[i].set_max_value();
          ends[i].set_min_value();
        }
        all_single_value_ranges_ = false;
      }
      if (OB_FAIL(ranges_.push_back(range))) {
        LOG_WARN("failed to push back range");
      }
    }
  }
  return ret;
}

/**
 * standard range doesn't have any or_next node, we can merge each range node directly
*/
int ObRangeGenerator::generate_standard_ranges(const ObRangeNode *node)
{
  int ret = OB_SUCCESS;
  ObTmpRange *init_range = nullptr;
  if (OB_FAIL(generate_tmp_range(init_range, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else {
    init_range->set_always_true();
    if (OB_FAIL(formalize_standard_range(node, *init_range))) {
      LOG_WARN("failed to formalize range");
    } else if (OB_FAIL(merge_and_remove_ranges())) {
      LOG_WARN("faield to merge and remove ranges");
    }
  }
  return ret;
}

int ObRangeGenerator::formalize_standard_range(const ObRangeNode *node, ObTmpRange &range)
{
  int ret = OB_SUCCESS;
  ObTmpRange *new_range = nullptr;
  bool not_consistent = false;
  if (OB_UNLIKELY(node->contain_in_) || OB_NOT_NULL(node->or_next_) ||
      OB_UNLIKELY(node->is_not_in_node_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range node in standard range", KPC(node));
  } else if (OB_FAIL(final_range_node(node, new_range, false))) {
    LOG_WARN("failed to final range node");
  } else if (OB_FAIL(range.intersect(*new_range, not_consistent))) {
    LOG_WARN("failed to do tmp range intersect");
  } else if (not_consistent || node->and_next_ == nullptr) {
    if (is_generate_ss_range_) {
      range.column_cnt_ -= pre_range_graph_->get_skip_scan_offset();
      range.min_offset_ -= pre_range_graph_->get_skip_scan_offset();
      range.max_offset_ -= pre_range_graph_->get_skip_scan_offset();
      range.start_ += pre_range_graph_->get_skip_scan_offset();
      range.end_ += pre_range_graph_->get_skip_scan_offset();
    }
    if (OB_FAIL(generate_one_range(range))) {
      LOG_WARN("faield to generate one range", K(range));
    }
  } else if (OB_FAIL(SMART_CALL(formalize_standard_range(node->and_next_, range)))) {
    LOG_WARN("failed to formalize range");
  }
  return ret;
}

/**
 * complex range contain or_next node, offset of range node may not consistent along and_next.
 * For example, (c1 > 1 or c2 > 1) and c1 = 2 will generate a range graph like this.
 *     c1 -- c1
 *     |   /
 *     c2 -
 * We final each range node firstly and merge them laterly.
*/
int ObRangeGenerator::generate_complex_ranges(const ObRangeNode *node)
{
  int ret = OB_SUCCESS;
  bool need_merge = true;
  tmp_range_lists_ = static_cast<TmpRangeList*>(
          allocator_.alloc(sizeof(TmpRangeList) * pre_range_graph_->get_column_cnt()));
  if (OB_ISNULL(tmp_range_lists_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for tmp range list");
  } else if (OB_FAIL(all_tmp_ranges_.prepare_allocate(pre_range_graph_->get_node_count()))) {
    LOG_WARN("failed to init fixed array size");
  } else if (OB_FAIL(all_tmp_node_caches_.prepare_allocate(pre_range_graph_->get_node_count()))) {
    LOG_WARN("failed to init fixed array size");
  } else if (nullptr == always_false_tmp_range_ &&
             OB_FAIL(generate_tmp_range(always_false_tmp_range_, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else {
    always_false_tmp_range_->set_always_false();
    for (int64_t i = 0; i < pre_range_graph_->get_node_count(); ++i) {
      all_tmp_ranges_.at(i) = nullptr;
    }
    for (int64_t i = 0; i < pre_range_graph_->get_column_cnt(); ++i) {
      new(tmp_range_lists_ + i)TmpRangeList();
    }
    for (int64_t i = 0; i < pre_range_graph_->get_node_count(); ++i) {
      all_tmp_node_caches_.at(i) = nullptr;
    }
    if (OB_FAIL(formalize_complex_range(node))) {
      LOG_WARN("failed to formalize range");
    } else if (OB_FAIL(check_need_merge_range_nodes(node, need_merge))) {
      LOG_WARN("failed to check need merge range nodes");
    } else if (!need_merge) {
      // do nothing
    } else if (OB_FAIL(merge_and_remove_ranges())) {
      LOG_WARN("faield to merge and remove ranges");
    }
  }
  return ret;
}



int ObRangeGenerator::formalize_complex_range(const ObRangeNode *node)
{
  int ret = OB_SUCCESS;
  int64_t pre_node_offset = -1;
  bool add_last = false;
  for (const ObRangeNode *cur_node = node; OB_SUCC(ret) && cur_node != nullptr; cur_node = cur_node->or_next_) {
    if (pre_node_offset != -1 && add_last) {
      tmp_range_lists_[pre_node_offset].remove_last();
    }
    add_last = false;
    // min_offset = -1 means current node is a const value node. After final node will become always true of false.
    pre_node_offset = cur_node->min_offset_ == -1 ? 0 : cur_node->min_offset_;
    if (!cur_node->contain_in_ &&
        !cur_node->is_not_in_node_ &&
        !cur_node->is_domain_node_) {
      ObTmpRange *new_range = nullptr;
      if (OB_FAIL(final_range_node(cur_node, new_range, true))) {
        LOG_WARN("failed to final range node");
      } else if (new_range->always_false_) {
        if (OB_FAIL(generate_one_range(*new_range))) {
          LOG_WARN("failed to generate one range", KPC(new_range));
        }
      } else {
        if (new_range->always_true_) {
          // do nothing
        } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to add last to dlist", KPC(new_range));
        } else {
          add_last = true;
        }
        if (OB_FAIL(ret)) {
        } else if (cur_node->and_next_ == nullptr) {
          if (OB_FAIL(generate_one_complex_range())) {
            LOG_WARN("faield to generate one range");
          }
        } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
          LOG_WARN("failed to formalize range");
        }
      }
    } else if (cur_node->contain_in_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_node->in_param_count_; ++i) {
        ObTmpRange *new_range = nullptr;
        if (pre_node_offset != -1 && add_last) {
          tmp_range_lists_[pre_node_offset].remove_last();
        }
        add_last = false;
        if (OB_FAIL(final_in_range_node(cur_node, i, new_range))) {
          LOG_WARN("failed to final in range node");
        } else if (new_range->always_false_) {
          if (OB_FAIL(generate_one_range(*new_range))) {
            LOG_WARN("failed to generate one range", KPC(new_range));
          }
        } else {
          if (new_range->always_true_) {
            // do nothing
          } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add last to dlist", KPC(new_range));
          } else {
            add_last = true;
          }
          if (OB_FAIL(ret)) {
          } else if (cur_node->and_next_ == nullptr) {
            if (OB_FAIL(generate_one_complex_range())) {
              LOG_WARN("faield to generate one range");
            }
          } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
            LOG_WARN("failed to formalize range");
          }
        }
      }
    } else if (cur_node->is_not_in_node_) {
      bool always_false = false;
      ObTmpInParam *tmp_in_param = nullptr;
      if (OB_FAIL(generate_tmp_not_in_param(*cur_node, tmp_in_param))) {
        LOG_WARN("failed to generate tmp not in param", K(ret));
      } else if (OB_ISNULL(tmp_in_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(tmp_in_param));
      } else if (tmp_in_param->always_false_) {
        if (OB_FAIL(generate_one_range(*always_false_tmp_range_))) {
          LOG_WARN("failed to generate one range", K(ret));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_in_param->in_param_.count() + 1; ++i) {
          ObTmpRange *new_range = nullptr;
          if (pre_node_offset != -1 && add_last) {
            tmp_range_lists_[pre_node_offset].remove_last();
          }
          add_last = false;
          if (OB_FAIL(final_not_in_range_node(*cur_node, i, tmp_in_param, new_range))) {
            LOG_WARN("failed to final in range node");
          } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add last to dlist", KPC(new_range));
          } else if (OB_FALSE_IT(add_last = true)) {
          } else if (cur_node->and_next_ == nullptr) {
            if (OB_FAIL(generate_one_complex_range())) {
              LOG_WARN("faield to generate one range");
            }
          } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
            LOG_WARN("failed to formalize range");
          }
        }
      }
    } else if (cur_node->is_domain_node_ &&
               is_geo_type(GET_RANGE_NODE_DOMAIN_TYPE(cur_node))) {
      ObTmpGeoParam *tmp_geo_param = nullptr;
      if (OB_FAIL(generate_tmp_geo_param(*cur_node, tmp_geo_param))) {
        LOG_WARN("failed to generate tmp geo param", K(ret));
      } else if (OB_ISNULL(tmp_geo_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (tmp_geo_param->always_true_) {
        if (cur_node->and_next_ == nullptr) {
          if (OB_FAIL(generate_one_complex_range())) {
            LOG_WARN("faield to generate one range");
          }
        } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
          LOG_WARN("failed to formalize range");
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_geo_param->start_keys_.count(); ++i) {
          ObTmpRange *new_range = nullptr;
          if (pre_node_offset != -1 && add_last) {
            tmp_range_lists_[pre_node_offset].remove_last();
          }
          add_last = false;
          if (OB_FAIL(final_geo_range_node(*cur_node,
                                           tmp_geo_param->start_keys_.at(i),
                                           tmp_geo_param->end_keys_.at(i),
                                           new_range))) {
            LOG_WARN("failed to final in range node");
          } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add last to dlist", KPC(new_range));
          } else if (OB_FALSE_IT(add_last = true)) {
          } else if (cur_node->and_next_ == nullptr) {
            if (OB_FAIL(generate_one_complex_range())) {
              LOG_WARN("faield to generate one range");
            }
          } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
            LOG_WARN("failed to formalize range");
          }
        }
      }
    } else if (cur_node->is_domain_node_ &&
               ObDomainOpType::T_JSON_MEMBER_OF == GET_RANGE_NODE_DOMAIN_TYPE(cur_node)) {
      ObTmpRange *new_range = nullptr;
      if (OB_FAIL(final_json_member_of_range_node(cur_node, new_range, true))) {
        LOG_WARN("failed to final range node");
      } else if (new_range->always_false_) {
        if (OB_FAIL(generate_one_range(*new_range))) {
          LOG_WARN("failed to generate one range", KPC(new_range));
        }
      } else {
        if (new_range->always_true_) {
          // do nothing
        } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to add last to dlist", KPC(new_range));
        } else {
          add_last = true;
        }
        if (OB_FAIL(ret)) {
        } else if (cur_node->and_next_ == nullptr) {
          if (OB_FAIL(generate_one_complex_range())) {
            LOG_WARN("faield to generate one range");
          }
        } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
          LOG_WARN("failed to formalize range");
        }
      }
    } else if (cur_node->is_domain_node_ &&
               (ObDomainOpType::T_JSON_CONTAINS == GET_RANGE_NODE_DOMAIN_TYPE(cur_node) ||
                ObDomainOpType::T_JSON_OVERLAPS == GET_RANGE_NODE_DOMAIN_TYPE(cur_node))) {
      ObTmpInParam *tmp_in_param = nullptr;
      if (OB_FAIL(generate_tmp_json_array_param(*cur_node, tmp_in_param))) {
        LOG_WARN("failed to generate tmp not in param", K(ret));
      } else if (OB_ISNULL(tmp_in_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(tmp_in_param));
      } else if (tmp_in_param->always_false_) {
        if (OB_FAIL(generate_one_range(*always_false_tmp_range_))) {
          LOG_WARN("failed to generate one range", K(ret));
        }
      } else if (tmp_in_param->always_true_) {
        if (cur_node->and_next_ == nullptr) {
          if (OB_FAIL(generate_one_complex_range())) {
            LOG_WARN("faield to generate one range");
          }
        } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
          LOG_WARN("failed to formalize range");
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_in_param->in_param_.count(); ++i) {
          ObTmpRange *new_range = nullptr;
          if (pre_node_offset != -1 && add_last) {
            tmp_range_lists_[pre_node_offset].remove_last();
          }
          add_last = false;
          if (OB_FAIL(final_domain_range_node(*cur_node, i, tmp_in_param, new_range))) {
            LOG_WARN("failed to final in range node");
          } else if (OB_UNLIKELY(!tmp_range_lists_[pre_node_offset].add_last(new_range))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add last to dlist", KPC(new_range));
          } else if (OB_FALSE_IT(add_last = true)) {
          } else if (cur_node->and_next_ == nullptr) {
            if (OB_FAIL(generate_one_complex_range())) {
              LOG_WARN("faield to generate one range");
            }
          } else if (OB_FAIL(SMART_CALL(formalize_complex_range(cur_node->and_next_)))) {
            LOG_WARN("failed to formalize range");
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && pre_node_offset != -1 && add_last) {
    tmp_range_lists_[pre_node_offset].remove_last();
  }
  return ret;
}

int ObRangeGenerator::generate_one_complex_range()
{
  int ret = OB_SUCCESS;
  ObTmpRange *range = nullptr;
  bool and_next = true;
  if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else {
    range->set_always_true();
  }
  for (int64_t i = 0; OB_SUCC(ret) && and_next && i < pre_range_graph_->get_column_cnt(); ++i) {
    DLIST_FOREACH_X(cur_range, tmp_range_lists_[i], (OB_SUCC(ret) && and_next)) {
      bool not_consistent = false;
      if (OB_ISNULL(cur_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null range node");
      } else if (OB_FAIL(range->intersect(*cur_range, not_consistent))) {
        LOG_WARN("failed to do tmp range intersect");
      } else if (not_consistent) {
        and_next = false;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(generate_one_range(*range))) {
      LOG_WARN("faield to generate one range", K(range));
    }
  }
  return ret;
}

int ObRangeGenerator::final_range_node(const ObRangeNode *node, ObTmpRange *&range, bool need_cache) {
  int ret = OB_SUCCESS;
  range = need_cache ? all_tmp_ranges_.at(node->node_id_) : nullptr;
  if (range != nullptr) {
    // already generated
  } else if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else {
    bool always_false = false;
    bool check_next = true;
    int64_t truncated_key_idx_start = -1;
    int64_t truncated_key_idx_end = -1;
    for (int64_t i = 0; OB_SUCC(ret) && !always_false && i < pre_range_graph_->get_column_cnt(); ++i) {
      int64_t start = node->start_keys_[i];
      int64_t end = node->end_keys_[i];
      bool is_valid = true;
      if (start == OB_RANGE_EMPTY_VALUE && end == OB_RANGE_EMPTY_VALUE) {
        range->min_offset_++;
        range->start_[i].set_nop_value();
        range->end_[i].set_nop_value();
      } else if (OB_UNLIKELY(start == OB_RANGE_EMPTY_VALUE || end == OB_RANGE_EMPTY_VALUE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", KPC(node));
      } else {
        if (start == OB_RANGE_MIN_VALUE) {
          range->start_[i].set_min_value();
        } else if (start == OB_RANGE_MAX_VALUE) {
          range->start_[i].set_max_value();
        } else if (start == OB_RANGE_NULL_VALUE) {
          range->start_[i].set_null();
        } else if (OB_FAIL(get_result_value(start, range->start_[i], is_valid, exec_ctx_))) {
          LOG_WARN("failed to get result vlaue", K(start));
        } else if (!is_valid) {
          const ObRangeMap::ExprFinalInfo& expr_info = range_map_.expr_final_infos_.at(start);
          if (expr_info.is_not_first_col_in_row_ && -1 == truncated_key_idx_start) {
            truncated_key_idx_start = i;
          } else {
            always_false = true;
          }
        }
        if (OB_FAIL(ret) || always_false) {
        } else if (end == OB_RANGE_MIN_VALUE) {
          range->end_[i].set_min_value();
        } else if (end == OB_RANGE_MAX_VALUE) {
          range->end_[i].set_max_value();
        } else if (end == OB_RANGE_NULL_VALUE) {
          range->end_[i].set_null();
        } else if (OB_FAIL(get_result_value(end, range->end_[i], is_valid, exec_ctx_))) {
          LOG_WARN("failed to get result vlaue", K(end));
        } else if (!is_valid) {
          const ObRangeMap::ExprFinalInfo& expr_info = range_map_.expr_final_infos_.at(end);
          if (expr_info.is_not_first_col_in_row_ && -1 == truncated_key_idx_end) {
            truncated_key_idx_end = i;
          } else {
            always_false = true;
          }
        }
      }
    }
    if (OB_SUCC(ret) && truncated_key_idx_start != -1) {
      for (int64_t i = truncated_key_idx_start; i < pre_range_graph_->get_column_cnt(); ++i) {
        range->start_[i].set_min_value();
      }
    }
    if (OB_SUCC(ret) && truncated_key_idx_end != -1) {
      for (int64_t i = truncated_key_idx_end; i < pre_range_graph_->get_column_cnt(); ++i) {
        range->end_[i].set_max_value();
      }
    }
    if (OB_SUCC(ret)) {
      if (always_false) {
        range->set_always_false();
      } else {
        range->include_start_ = node->include_start_;
        range->include_end_ = node->include_end_;
        range->is_phy_rowid_ = node->is_phy_rowid_;
        range->min_offset_ = node->min_offset_ > 0 ? node->min_offset_ : 0;
        range->max_offset_ = node->max_offset_ > 0 ? node->max_offset_ : 0;
        if (!node->is_phy_rowid_ && OB_FAIL(cast_value_type(*range))) {
          LOG_WARN("cast value type failed", K(ret));
        } else if (need_cache) {
          all_tmp_ranges_.at(node->node_id_) = range;
        }
      }
    }
  }
  return ret;
}

int ObRangeGenerator::final_in_range_node(const ObRangeNode *node,
                                          const int64_t in_idx,
                                          ObTmpRange *&range) {
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(node->node_id_ < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected node", K(node->node_id_));
  } else if (OB_FALSE_IT(range = all_tmp_ranges_.at(node->node_id_))) {
  } else if (range == nullptr && OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else {
    range->always_false_ = false;
    bool always_false = false;
    for (int64_t i = 0; OB_SUCC(ret) && !always_false && i < pre_range_graph_->get_column_cnt(); ++i) {
      int64_t start = node->start_keys_[i];
      int64_t end = node->end_keys_[i];
      bool is_valid = true;
      if (start == OB_RANGE_EMPTY_VALUE && end == OB_RANGE_EMPTY_VALUE) {
        range->min_offset_++;
        range->start_[i].set_nop_value();
        range->end_[i].set_nop_value();
      } else if (OB_UNLIKELY(start == OB_RANGE_EMPTY_VALUE || end == OB_RANGE_EMPTY_VALUE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected range node", KPC(node));
      } else {
        if (start == OB_RANGE_MIN_VALUE) {
          range->start_[i].set_min_value();
        } else if (start == OB_RANGE_MAX_VALUE) {
          range->start_[i].set_max_value();
        } else if (start == OB_RANGE_NULL_VALUE) {
          range->start_[i].set_null();
        } else if (start >= 0) {
          if (OB_FAIL(get_result_value(start, range->start_[i], is_valid, exec_ctx_))) {
            LOG_WARN("failed to get result vlaue", K(start));
          } else if (!is_valid) {
            always_false = true;
          }
        } else {
          int64_t param_idx = -start - 1;
          InParam *in_param = nullptr;
          if (OB_UNLIKELY(param_idx < 0 || param_idx >= range_map_.in_params_.count()) ||
              OB_ISNULL(in_param = range_map_.in_params_.at(param_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected in param", K(param_idx), K(range_map_.in_params_.count()),
                        K(in_param));
          } else if (OB_FAIL(get_result_value(in_param->at(in_idx), range->start_[i], is_valid, exec_ctx_))) {
            LOG_WARN("failed to get result vlaue", K(start));
          } else if (!is_valid) {
            always_false = true;
          }
        }

        if (OB_FAIL(ret) || always_false) {
        } else if (end == OB_RANGE_MIN_VALUE) {
          range->end_[i].set_min_value();
        } else if (end == OB_RANGE_MAX_VALUE) {
          range->end_[i].set_max_value();
        } else if (end == OB_RANGE_NULL_VALUE) {
          range->end_[i].set_null();
        } else if (end >= 0) {
          if (OB_FAIL(get_result_value(end, range->end_[i], is_valid, exec_ctx_))) {
            LOG_WARN("failed to get result vlaue", K(end));
          } else if (!is_valid) {
            always_false = true;
          }
        } else if (OB_LIKELY(end == start)) {
          range->end_[i] = range->start_[i];
        } else {
          int64_t param_idx = -end - 1;
          InParam *in_param = nullptr;
          if (OB_UNLIKELY(param_idx < 0 || param_idx >= range_map_.in_params_.count()) ||
              OB_ISNULL(in_param = range_map_.in_params_.at(param_idx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected in param", K(param_idx), K(range_map_.in_params_.count()),
                        K(in_param));
          } else if (OB_FAIL(get_result_value(in_param->at(in_idx), range->end_[i], is_valid, exec_ctx_))) {
            LOG_WARN("failed to get result vlaue", K(start));
          } else if (!is_valid) {
            always_false = true;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (always_false) {
        range->set_always_false();
      } else {
        range->include_start_ = node->include_start_;
        range->include_end_ = node->include_end_;
        range->is_phy_rowid_ = node->is_phy_rowid_;
        range->min_offset_ = node->min_offset_ > 0 ? node->min_offset_ : 0;
        range->max_offset_ = node->max_offset_ > 0 ? node->max_offset_ : 0;
        if (!node->is_phy_rowid_ && OB_FAIL(cast_value_type(*range))) {
          LOG_WARN("cast value type failed", K(ret));
        } else {
          all_tmp_ranges_.at(node->node_id_) = range;
        }
      }
    }
  }
  return ret;
}

int ObRangeGenerator::get_result_value(const int64_t val_idx,
                                       ObObj &value,
                                       bool &is_valid,
                                       ObExecContext &exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_ctx = NULL;
  is_valid = true;
  if (OB_ISNULL(phy_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_UNLIKELY(val_idx < 0 || val_idx >= range_map_.expr_final_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param idx", K(val_idx));
  } else {
    const ObRangeMap::ExprFinalInfo& expr_info = range_map_.expr_final_infos_.at(val_idx);
    if (expr_info.is_param_) {
      int64_t idx = expr_info.param_idx_;
      if (OB_UNLIKELY(idx < 0 || idx >= phy_ctx->get_param_store().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param idx", K(idx));
      } else {
        value = phy_ctx->get_param_store().at(idx);
        if (OB_UNLIKELY(value.is_nop_value())) {
          ret = OB_ERR_UNEXPECTED;
        } else if (value.is_lob_storage()) {
          if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(value, value, NULL, &allocator_, true))) {
            LOG_WARN("fail to convert to inrow lob", K(value));
          }
        }
      }
    } else if (expr_info.is_const_) {
      ObObj *const_obj = expr_info.const_val_;
      if (OB_ISNULL(const_obj)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null const obj");
      } else if (OB_FALSE_IT(value = *const_obj)) {
      } else if (OB_UNLIKELY(value.is_nop_value())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (value.is_lob_storage()) {
        if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(value, value, NULL, &allocator_, true))) {
          LOG_WARN("fail to convert to inrow lob", K(value));
        }
      }
    } else if (expr_info.is_expr_) {
      ObTempExpr *temp_expr = expr_info.temp_expr_;
      ObNewRow tmp_row;
      ObObj result;
      if (OB_ISNULL(temp_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null temp expr", K(expr_info));
      } else if (OB_FAIL(temp_expr->eval(exec_ctx, tmp_row, result))) {
        LOG_WARN("failed to eval temp expr");
      } else if (OB_UNLIKELY(result.is_nop_value())) {
        ret = OB_ERR_UNEXPECTED;
      } else if (result.is_lob_storage()) {
        if (OB_FAIL(ObTextStringIter::convert_outrow_lob_to_inrow_templob(result, value, NULL, &allocator_, true, true))) {
          LOG_WARN("fail to convert to inrow lob", K(ret), K(result));
        }
      } else if (OB_FAIL(ob_write_obj(allocator_, result, value))) {
        LOG_WARN("failed to write obj", K(result));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected temp expr", K(expr_info));
    }
    if (OB_SUCC(ret) && value.is_null() && !expr_info.null_safe_) {
      is_valid = false;
    }
    if (OB_SUCC(ret) && expr_info.rowid_idx_ > 0) {
      if (value.is_urowid()) {
        uint64_t pk_cnt;
        ObArray<ObObj> pk_vals;
        const ObURowIDData &urowid_data = value.get_urowid();
        bool is_physical_rowid = expr_info.rowid_idx_ == PHYSICAL_ROWID_IDX;
        if (OB_UNLIKELY((urowid_data.is_physical_rowid() && !is_physical_rowid) ||
                        (!urowid_data.is_physical_rowid() && is_physical_rowid))) {
          ret = OB_INVALID_ROWID;
          LOG_WARN("get inconsistent rowid", K(urowid_data), K(expr_info.rowid_idx_));
        } else if (urowid_data.is_physical_rowid()) {
          // will convert in table scan.
        } else if (OB_FAIL(urowid_data.get_pk_vals(pk_vals))) {
          LOG_WARN("failed to get pk values", K(ret));
        } else if (OB_UNLIKELY((pk_cnt = urowid_data.get_real_pk_count(pk_vals)) < expr_info.rowid_idx_)) {
          ret = OB_INVALID_ROWID;
          LOG_WARN("get inconsistent rowid", K(pk_cnt), K(expr_info.rowid_idx_));
        } else {
          value = pk_vals.at(expr_info.rowid_idx_ - 1);
        }
      }
    }
  }
  return ret;
}

int ObRangeGenerator::cast_value_type(ObTmpRange &range)
{
  int ret = OB_SUCCESS;
  int64_t start_cmp = 0;
  int64_t end_cmp = 0;
  for (int64_t i = range.min_offset_; OB_SUCC(ret) && i < range.column_cnt_; ++i) {
    const ObRangeColumnMeta *meta = pre_range_graph_->get_column_meta(i);
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta");
    } else if (OB_FAIL(try_cast_value(*meta, range.start_[i], start_cmp, CO_GE))) {
      LOG_WARN("failed to cast value", K(meta), K(range.start_[i]));
    } else {
      range.start_[i].set_collation_type(meta->column_type_.get_collation_type());
      if (start_cmp < 0) {
        // after cast, precise becomes bigger
        // * if is last value. `(` -> `[`
	      //   	e.g. (1, 2, 2.9; max, max, max) -> [1, 2, 3; max, max, max)
	      // * if is not last value. `[x, val, x` -> `(x, val, min`
	      //    e.g. [1, 1.9, 3; max, max, max) -> (1, 2, min; max, max, max)
        if (i == range.column_cnt_ - 1) {
          range.include_start_ = true;
        } else {
          for (i = i + 1; i < range.column_cnt_; ++i) {
            range.start_[i].set_min_value();
          }
          range.include_start_ = false;
        }
      } else if (start_cmp > 0) {
        // after cast, the result becomes smaller
        // * if is last value. `[` -> `(`
        // 	  e.g. [1, 2, 3.1; max, max, max) -> （1, 2, 3; max, max, max)
        // * if is not last value. `[x, val, x` -> `(x, val, max`
        //    e.g. [1, 2.1, 3; max, max, max) -> (1, 2, max; max, max, max)
        if (i == range.column_cnt_ - 1) {
          range.include_start_ = false;
        } else {
          for (i = i + 1; i < range.column_cnt_; ++i) {
            range.start_[i].set_max_value();
          }
          range.include_start_ = false;
        }
      }
    }
  }
  for (int64_t i = range.min_offset_; OB_SUCC(ret) && i < range.column_cnt_; ++i) {
    const ObRangeColumnMeta *meta = pre_range_graph_->get_column_meta(i);
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta");
    } else if (OB_FAIL(try_cast_value(*meta, range.end_[i], end_cmp, CO_LE))) {
      LOG_WARN("failed to cast value", K(meta), K(range.end_[i]));
    } else {
      range.end_[i].set_collation_type(meta->column_type_.get_collation_type());
      if (end_cmp < 0) {
        // after cast, the result becomes bigger
        // * if is last value. `]` -> `)`
		    //    e.g. (min, min, min; 1, 2, 2.9] -> (min, min, min; 1, 2, 3)
	      // * if is not last value. `x, val, x]` -> `x, val, min)`
	      //    e.g. (min, min, min; 1, 1.9, 3] -> (min, min, min; 1, 2, min)
        if (i == range.column_cnt_ - 1) {
          range.include_end_ = false;
        } else {
          for (i = i + 1; i < range.column_cnt_; ++i) {
            range.end_[i].set_min_value();
          }
          range.include_end_ = false;
        }
      } else if (end_cmp > 0) {
        // after cast, the result becomes smaller
        // * if is last value. `)` -> `]`
		    //    e.g. (min, min, min; 1, 2, 3.1) -> (min, min, min; 1, 2, 3]
	      // * if is not last value. `x, val, x]` -> `x, val, max)`
	      //    e.g. (min, min, min; 1, 2.1, 3] -> (min, min, min; 1, 2, max)
        if (i == range.column_cnt_ - 1) {
          range.include_end_ = true;
        } else {
          for (i = i + 1; i < range.column_cnt_; ++i) {
            range.end_[i].set_max_value();
          }
          range.include_end_ = false;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(range.formalize())) {
      LOG_WARN("failed to formalize tmp range");
    }
  }
  return ret;
}

int ObRangeGenerator::try_cast_value(const ObRangeColumnMeta &meta,
                                     ObObj &value,
                                     int64_t &cmp,
                                     common::ObCmpOp cmp_op)
{
  int ret = OB_SUCCESS;
  if (!value.is_min_value() && !value.is_max_value() && !value.is_unknown() &&
      (!ObSQLUtils::is_same_type_for_compare(value.get_meta(), meta.column_type_.get_obj_meta()) ||
       value.is_decimal_int() ||
       (meta.column_type_.get_obj_meta().get_type_class() == ObDoubleTC &&
        meta.column_type_.get_accuracy().get_scale() != value.get_meta().get_scale()))) {
    const ObObj *dest_val = NULL;
    ObCollationType collation_type = meta.column_type_.get_collation_type();
    ObCastCtx cast_ctx(&allocator_, &dtc_params_, cur_datetime_, CM_WARN_ON_FAIL, collation_type);
    cast_ctx.exec_ctx_ = &exec_ctx_;
    ObExpectType expect_type;
    if (meta.column_type_.is_enum_set_with_subschema()) {
      const ObEnumSetMeta *enum_set_meta = NULL;
      if (OB_FAIL(ObRawExprUtils::extract_enum_set_meta(
            meta.column_type_, exec_ctx_.get_my_session(), enum_set_meta))) {
        LOG_WARN("fail to extrac enum set meta", K(ret));
      } else {
        expect_type.set_type_infos(enum_set_meta->get_str_values());
      }
    }
    expect_type.set_type(meta.column_type_.get_type());
    expect_type.set_collation_type(collation_type);
    ObAccuracy res_acc;
    ObObj tmp_dest_obj;
    if (lib::is_mysql_mode() &&
        value.get_meta().get_type_class() == ObDoubleTC &&
        meta.column_type_.get_obj_meta().get_type_class() == ObDoubleTC &&
        meta.column_type_.get_accuracy().get_scale() != SCALE_UNKNOWN_YET) {
      /*
        The code here is primarily designed to fix the issue with double(-1,-1) -> double(20, 10), for example,
        in the following SQL, the c2 column is an unsigned double(20,10), and the generated range
        is (1.175494351e-38,MIN; 1.175494351e-38,MAX). However, at the storage layer,
        this range is transformed to (0.,MIN; 0., MAX), which could result in the erroneous scanning of 0.

        select * from t1 where c2 = 1.175494351e-38;
      */
      if (OB_FAIL(cast_double_to_fixed_double(meta, value, tmp_dest_obj))) {
        LOG_WARN("failed to cast double to fixed double", K(ret));
      } else {
        dest_val = &tmp_dest_obj;
      }
    } else {
      if (meta.column_type_.is_decimal_int()) {
        res_acc = meta.column_type_.get_accuracy();
        ObScale in_scale = value.get_scale();
        int32_t in_bytes = value.get_int_bytes();
        ObScale out_scale = res_acc.get_scale();
        int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(res_acc.get_precision());
        if (ObDatumCast::need_scale_decimalint(in_scale, in_bytes, out_scale, out_bytes)) {
          // simply get range, using eq const mode
          cast_ctx.cast_mode_ |= ObRelationalExprOperator::get_const_cast_mode(cmp_op, true);
        }
        cast_ctx.res_accuracy_ = &res_acc;
      }
      cast_ctx.gen_query_range_ = true;
      if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, value, tmp_dest_obj, dest_val))) {
        LOG_WARN("failed to cast object to expect_type", K(ret), K(value), K(expect_type));
      }
    }

    // to check if EXPR CAST losses number precise
    ObObjType cmp_type = ObMaxType;
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(dest_val)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null value");
      } else if (ob_is_double_tc(expect_type.get_type()) || ob_is_enumset_tc(expect_type.get_type())) {
        const_cast<ObObj *>(dest_val)->set_scale(meta.column_type_.get_accuracy().get_scale());
        const_cast<ObObj *>(dest_val)->set_subschema_id(meta.column_type_.get_subschema_id());
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type,
                                                                value.get_type(),
                                                                dest_val->get_type()))) {
        LOG_WARN("failed to get relational cmp type",
            K(cmp_type), K(value.get_type()), K(dest_val->get_type()));
      } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(cmp, value, *dest_val,
                                                                    cast_ctx, cmp_type,
                                                                    collation_type))) {
        LOG_WARN("failed to compare obj null safe", K(value), KPC(dest_val));
      } else {
        value = *dest_val;
      }
    }
  }
  return ret;
}

int ObRangeGenerator::generate_contain_exec_param_range()
{
  int ret = OB_SUCCESS;
  ObNewRange *range = nullptr;
  int64_t column_cnt = pre_range_graph_->get_column_count();
  if (OB_FAIL(create_new_range(range, column_cnt))) {
    LOG_WARN("failed to create new range");
  } else {
    ObObj *starts = range->start_key_.get_obj_ptr();
    ObObj *ends = range->end_key_.get_obj_ptr();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      starts[i].set_min_value();
      ends[i].set_max_value();
    }
    range->is_physical_rowid_range_ = false;
    range->border_flag_.unset_inclusive_start();
    range->border_flag_.unset_inclusive_end();
    all_single_value_ranges_ = false;
    if (OB_FAIL(ranges_.push_back(range))) {
      LOG_WARN("failed to push back range");
    }
  }
  return ret;
}

// sort range according to start key
struct RangeCmp
{
  inline bool operator()(const ObNewRange *left, const ObNewRange *right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      bret = (left->compare_with_startkey2(*right) < 0);
    }
    return bret;
  }
};

int ObRangeGenerator::merge_and_remove_ranges()
{
  int ret = OB_SUCCESS;
  if (always_true_range_ != nullptr) {
    ranges_.reset();
    if (OB_FAIL(ranges_.push_back(always_true_range_))) {
      LOG_WARN("failed to push back always true range");
    }
  } else if (0 == ranges_.count()) {
    if (OB_ISNULL(always_false_range_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null always false range");
    } else if (OB_FAIL(ranges_.push_back(always_false_range_))) {
      LOG_WARN("failed to push back always false range");
    } else {
      all_single_value_ranges_ = false;
    }
  } else if (1 == ranges_.count()) {
    // do nothing
  } else if (pre_range_graph_->is_equal_range()) {
    hash::ObHashSet<ObQueryRange::ObRangeWrapper, hash::NoPthreadDefendMode> range_set;
    ObSEArray<ObNewRange *, 16> out_ranges;
    if (OB_FAIL(range_set.create(pre_range_graph_->get_range_size() == 0 ?
                RANGE_BUCKET_SIZE : pre_range_graph_->get_range_size()))) {
      LOG_WARN("create range set bucket failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges_.count(); ++i) {
      ObQueryRange::ObRangeWrapper range_wrapper;
      range_wrapper.range_ = ranges_.at(i);
      if (OB_HASH_EXIST == (ret = range_set.set_refactored(range_wrapper, 0))) {
        ret = OB_SUCCESS;
      } else if (OB_UNLIKELY(OB_SUCCESS != ret)) {
        LOG_WARN("failed to set range", K(ret));
      } else if (OB_FAIL(out_ranges.push_back(ranges_.at(i)))) {
        LOG_WARN("failed to push back range");
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ranges_.assign(out_ranges))) {
        LOG_WARN("failed to assign ranges");
      } else if (OB_FAIL(range_set.destroy())) {
        LOG_WARN("failed to destroy hash set");
      }
    }
  } else {
    lib::ob_sort(&ranges_.at(0), &ranges_.at(0) + ranges_.count(), RangeCmp());
    ObSEArray<ObNewRange *, 16> out_ranges;
    ObNewRange *l_range = ranges_.at(0);
    if (OB_ISNULL(l_range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null range");
    }
    for (int64_t i = 1; OB_SUCC(ret) && i < ranges_.count(); ++i) {
      ObNewRange *r_range = ranges_.at(i);
      if (OB_ISNULL(r_range)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null range");
      } else {
        int cmp = l_range->compare_endkey_with_startkey(*r_range);
        if (cmp < 0) {
          if (OB_FAIL(out_ranges.push_back(l_range))) {
            LOG_WARN("failed to push back range");
          } else {
            l_range = r_range;
          }
        } else if (cmp == 0) {
          l_range->end_key_.assign(r_range->end_key_.get_obj_ptr(), r_range->end_key_.get_obj_cnt());
          if (r_range->border_flag_.inclusive_end()) {
            l_range->border_flag_.set_inclusive_end();
          } else {
            l_range->border_flag_.unset_inclusive_end();
          }
        } else {
          int cmp2 = l_range->compare_with_endkey2(*r_range);
          if (cmp2 < 0) {
            l_range->end_key_.assign(r_range->end_key_.get_obj_ptr(), r_range->end_key_.get_obj_cnt());
            if (r_range->border_flag_.inclusive_end()) {
              l_range->border_flag_.set_inclusive_end();
            } else {
              l_range->border_flag_.unset_inclusive_end();
            }
          } else if (cmp2 == 0) {
            // do nothing
          } else {
            // do nothing
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(out_ranges.push_back(l_range))) {
        LOG_WARN("failed to push back range");
      } else if (OB_FAIL(ranges_.assign(out_ranges))) {
        LOG_WARN("failed to assign ranges");
      }
    }
  }
  return ret;
}

int ObRangeGenerator::generate_ss_ranges()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *phy_ctx = exec_ctx_.get_physical_plan_ctx();
  if (OB_ISNULL(pre_range_graph_) || OB_ISNULL(phy_ctx) ||
      OB_ISNULL(pre_range_graph_->get_range_head())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null range graph", KPC(pre_range_graph_), K(phy_ctx));
  } else if (OB_UNLIKELY(pre_range_graph_->get_skip_scan_offset() < 0 ||
                         pre_range_graph_->get_skip_scan_offset() >= pre_range_graph_->get_column_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected skip scan range", K(pre_range_graph_->get_skip_scan_offset()),
                                               K(pre_range_graph_->get_column_cnt()));
  } else {
    const ObRangeNode *ss_head = nullptr;
    const ObRangeNode *cur_node = pre_range_graph_->get_range_head();
    while (cur_node != nullptr && cur_node->min_offset_ < pre_range_graph_->get_skip_scan_offset()) {
      cur_node = cur_node->and_next_;
    }
    if (cur_node != nullptr && cur_node->min_offset_ == pre_range_graph_->get_skip_scan_offset()) {
      ss_head = cur_node;
      if (pre_range_graph_->has_exec_param() && !phy_ctx->is_exec_param_readable()) {
        // pre range graph has exec param and not exec stage, generate (min; max)
        if (OB_FAIL(generate_contain_exec_param_range())) {
          LOG_WARN("faield to generate contain exec param range");
        }
      } else {
        is_generate_ss_range_ = true;
        if (OB_FAIL(generate_standard_ranges(ss_head))) {
          LOG_WARN("failed to genenrate precise get range");
        }
      }
    }
  }
  return ret;
}

int ObRangeGenerator::create_new_range(ObNewRange *&range, int64_t column_cnt)
{
  int ret = OB_SUCCESS;
  range = nullptr;
  ObObj *starts = nullptr;
  ObObj *ends = nullptr;
  size_t rowkey_size = sizeof(ObObj) * column_cnt * 2;
  size_t range_size = sizeof(ObNewRange) + rowkey_size;
  void *range_buffer = nullptr;
  if (OB_ISNULL(range_buffer = allocator_.alloc(range_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory for range failed", K(range_size));
  } else {
    range = new(range_buffer) ObNewRange();
    starts = reinterpret_cast<ObObj*>(static_cast<char*>(range_buffer) + sizeof(ObNewRange));
    ends = starts + column_cnt;
    range->table_id_ = pre_range_graph_->get_table_id();
    range->start_key_.assign(starts, column_cnt);
    range->end_key_.assign(ends, column_cnt);
  }
  return ret;
}

struct InParamObjCmp
{
  inline bool operator()(const ObObj *left, const ObObj *right)
  {
    bool bret = false;
    if (left != nullptr && right != nullptr) {
      bret = (left->compare(*right) < 0);
    }
    return bret;
  }
};

int ObRangeGenerator::generate_tmp_not_in_param(const ObRangeNode &node,
                                                ObTmpInParam *&tmp_in_param)
{
  int ret = OB_SUCCESS;
  InParam *in_param = nullptr;
  ObObj* objs_ptr = nullptr;
  const ObRangeColumnMeta *meta = nullptr;
  if (OB_UNLIKELY(!node.is_not_in_node_ ||
                  node.node_id_ < 0 ||
                  node.node_id_ >= all_tmp_node_caches_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not in node", K(node.node_id_), K(all_tmp_node_caches_.count()));
  } else if (OB_NOT_NULL(tmp_in_param = static_cast<ObTmpInParam*>(all_tmp_node_caches_.at(node.node_id_)))) {
    // do nothing
  } else if (OB_UNLIKELY(node.min_offset_ < 0 ||
                         node.min_offset_ >= pre_range_graph_->get_column_cnt() ||
                         node.start_keys_[node.min_offset_] >= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range", K(node));
  } else if (OB_ISNULL(meta = pre_range_graph_->get_column_meta(node.min_offset_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexecpted null", K(meta));
  } else if (OB_ISNULL(in_param = range_map_.in_params_.at(-node.start_keys_[node.min_offset_] - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected in param", K(node.node_id_));
  } else if (OB_ISNULL(tmp_in_param = (ObTmpInParam*)allocator_.alloc(sizeof(ObTmpInParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memeory failed", K(tmp_in_param));
  } else if (OB_ISNULL(objs_ptr = (ObObj*)allocator_.alloc(sizeof(ObObj) * in_param->count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memeory failed", K(objs_ptr));
  } else if (OB_FALSE_IT(tmp_in_param = new(tmp_in_param) ObTmpInParam(allocator_))) {
  } else if (OB_FAIL(tmp_in_param->in_param_.init(in_param->count()))) {
     LOG_WARN("failed to init fixed array size", K(ret));
  } else {
    objs_ptr = new(objs_ptr) ObObj[in_param->count()];
    bool always_false = false;
    for (int64_t i = 0; OB_SUCC(ret) && !always_false && i < in_param->count(); ++i) {
      bool is_valid = false;
      bool need_add = true;
      int64_t cmp = 0;
      if (OB_FAIL(get_result_value(in_param->at(i), objs_ptr[i], is_valid, exec_ctx_))) {
        LOG_WARN("failed to get result vlaue", K(i));
      } else if (!is_valid) {
        always_false = true;
        need_add = false;
      } else if (OB_LIKELY(ObSQLUtils::is_same_type_for_compare(objs_ptr[i].get_meta(),
                                                                meta->column_type_.get_obj_meta()) &&
                           !objs_ptr[i].is_decimal_int())) {
        objs_ptr[i].set_collation_type(meta->column_type_.get_collation_type());
      } else if (!node.is_phy_rowid_ && OB_FAIL(try_cast_value(*meta, objs_ptr[i], cmp, CO_EQ))) {
        LOG_WARN("failed to cast value", K(meta), K(objs_ptr[i]));
      } else if (cmp != 0) {
        need_add = false;
      } else {
        objs_ptr[i].set_collation_type(meta->column_type_.get_collation_type());
      }
      if (OB_SUCC(ret) && need_add) {
        if(OB_FAIL(tmp_in_param->in_param_.push_back(&objs_ptr[i]))) {
          LOG_WARN("failed to push back in param", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FALSE_IT(all_tmp_node_caches_.at(node.node_id_) = tmp_in_param)) {
    } else if (always_false) {
      tmp_in_param->always_false_ = always_false;
    } else if (tmp_in_param->in_param_.empty()) {
      // do nothing
    } else {
      lib::ob_sort(&tmp_in_param->in_param_.at(0),
                   &tmp_in_param->in_param_.at(0) + tmp_in_param->in_param_.count(),
                   InParamObjCmp());
    }
  }
  return ret;
}

int ObRangeGenerator::final_not_in_range_node(const ObRangeNode &node,
                                              const int64_t not_in_idx,
                                              ObTmpInParam *tmp_in_param,
                                              ObTmpRange *&range)
{
  int ret = OB_SUCCESS;
  const ObRangeColumnMeta *meta = nullptr;
  range = all_tmp_ranges_.at(node.node_id_);
  if (range == nullptr) {
    if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
      LOG_WARN("failed to generate tmp range");
    }
  }
  if (OB_SUCC(ret)) {
    int64_t start_padding = OB_RANGE_MIN_VALUE;
    int64_t end_padding = OB_RANGE_MAX_VALUE;
    range->always_false_ = false;
    if (OB_ISNULL(range)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_ISNULL(tmp_in_param)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(tmp_in_param));
    } else {
      for (int64_t i = 0; i < node.min_offset_; ++i) {
        range->start_[i].set_nop_value();
        range->end_[i].set_nop_value();
      }
      range->min_offset_ = node.min_offset_;
      range->max_offset_ = node.max_offset_;
      range->include_start_ = false;
      range->include_end_ = false;
      range->is_phy_rowid_ = node.is_phy_rowid_;
    }

    if (OB_FAIL(ret)) {
    } else if (tmp_in_param->in_param_.empty()) {
      if (lib::is_mysql_mode()) {
        range->start_[range->min_offset_].set_null();
        start_padding = OB_RANGE_MAX_VALUE;
      } else {
        range->start_[range->min_offset_].set_min_value();
        start_padding = OB_RANGE_MIN_VALUE;
      }
      if (lib::is_oracle_mode()) {
        range->end_[range->min_offset_].set_null();
        end_padding = OB_RANGE_MIN_VALUE;
      } else {
        range->end_[range->min_offset_].set_max_value();
        end_padding = OB_RANGE_MAX_VALUE;
      }
    } else if (0 == not_in_idx) {
      if (lib::is_mysql_mode()) {
        range->start_[range->min_offset_].set_null();
        start_padding = OB_RANGE_MAX_VALUE;
      } else {
        range->start_[range->min_offset_].set_min_value();
        start_padding = OB_RANGE_MIN_VALUE;
      }
      if (OB_FAIL(ob_write_obj(allocator_, *tmp_in_param->in_param_.at(not_in_idx),
                                      range->end_[range->min_offset_]))) {
        LOG_WARN("failed to write obj", K(ret));
      } else {
        end_padding = OB_RANGE_MIN_VALUE;
      }
    } else if (tmp_in_param->in_param_.count() <= not_in_idx) {
      if (OB_FAIL(ob_write_obj(allocator_, *tmp_in_param->in_param_.at(not_in_idx - 1),
                               range->start_[range->min_offset_]))) {
        LOG_WARN("failed to write obj", K(ret));
      } else {
        if (lib::is_oracle_mode()) {
          range->end_[range->min_offset_].set_null();
          end_padding = OB_RANGE_MIN_VALUE;
        } else {
          range->end_[range->min_offset_].set_max_value();
          end_padding = OB_RANGE_MAX_VALUE;
        }
        start_padding = OB_RANGE_MAX_VALUE;
      }
    } else {
      if (OB_FAIL(ob_write_obj(allocator_, *tmp_in_param->in_param_.at(not_in_idx - 1),
                               range->start_[range->min_offset_]))) {
        LOG_WARN("failed to write obj", K(ret));
      } else if (OB_FAIL(ob_write_obj(allocator_, *tmp_in_param->in_param_.at(not_in_idx),
                                      range->end_[range->min_offset_]))) {
        LOG_WARN("failed to write obj", K(ret));
      } else {
        start_padding = OB_RANGE_MAX_VALUE;
        end_padding = OB_RANGE_MIN_VALUE;
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = range->min_offset_ + 1; i < pre_range_graph_->get_column_cnt(); ++i) {
        if (start_padding == OB_RANGE_MIN_VALUE) {
          range->start_[i].set_min_value();
        } else if (start_padding == OB_RANGE_MAX_VALUE) {
          range->start_[i].set_max_value();
        }
        if (end_padding == OB_RANGE_MIN_VALUE) {
          range->end_[i].set_min_value();
        } else if (start_padding == OB_RANGE_MAX_VALUE) {
          range->end_[i].set_max_value();
        }
      }
      if (!node.is_phy_rowid_ && OB_FAIL(cast_value_type(*range))) {
        LOG_WARN("cast value type failed", K(ret));
      } else {
        all_tmp_ranges_.at(node.node_id_) = range;
      }
    }
  }
  return ret;
}

int ObRangeGenerator::generate_tmp_geo_param(const ObRangeNode &node,
                                             ObTmpGeoParam *&tmp_geo_param)
{
  int ret = OB_SUCCESS;
  uint32_t input_srid;
  ObObj objs_ptr[2];
  ObString wkb_str;
  double distance = NAN;
  bool is_valid = false;
  ObArenaAllocator tmp_allocator(ObModIds::OB_LOB_ACCESS_BUFFER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObDomainOpType op_type = GET_RANGE_NODE_DOMAIN_TYPE(&node);
  if (OB_UNLIKELY(!node.is_domain_node_ ||
                  node.node_id_ < 0 ||
                  node.node_id_ >= all_tmp_node_caches_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not in node", K(node.node_id_), K(all_tmp_node_caches_.count()));
  } else if (OB_NOT_NULL(tmp_geo_param = static_cast<ObTmpGeoParam*>(all_tmp_node_caches_.at(node.node_id_)))) {
    // do nothing
  } else if (OB_UNLIKELY(node.min_offset_ < 0 ||
                         node.min_offset_ >= pre_range_graph_->get_column_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range", K(node));
  } else if (OB_ISNULL(tmp_geo_param = (ObTmpGeoParam*)allocator_.alloc(sizeof(ObTmpGeoParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memeory failed", K(tmp_geo_param));
  } else if (OB_FALSE_IT(tmp_geo_param = new(tmp_geo_param) ObTmpGeoParam(allocator_))) {
  } else if (OB_FAIL(get_result_value(node.start_keys_[node.min_offset_], objs_ptr[0], is_valid, exec_ctx_))) {
    LOG_WARN("failed to get result value", K(node.start_keys_[node.min_offset_]));
  } else if (!is_valid) {
    tmp_geo_param->always_true_ = true;
  } else if ((op_type == ObDomainOpType::T_GEO_DWITHIN ||
              op_type == ObDomainOpType::T_GEO_RELATE) &&
             OB_FAIL(get_result_value(node.end_keys_[node.min_offset_], objs_ptr[1], is_valid, exec_ctx_))) {
    LOG_WARN("failed to get result value", K(ret));
  } else if (!is_valid) {
    tmp_geo_param->always_true_ = true;
  } else if (op_type == ObDomainOpType::T_GEO_RELATE &&
             OB_FAIL(get_spatial_relationship_by_mask(objs_ptr[1], op_type))) {
    LOG_WARN("failed to get spatial relationship by mask", K(ret));
  } else if (!is_geo_type(op_type)) {
    tmp_geo_param->always_true_ = true;
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator, objs_ptr[0], wkb_str))) {
    LOG_WARN("fail to get real string data", K(ret), K(objs_ptr[0]));
  } else if (OB_FAIL(ObGeoTypeUtil::get_srid_from_wkb(wkb_str, input_srid))) {
    LOG_WARN("failed to get srid", K(ret), K(wkb_str));
  } else if (OB_FAIL(ObSqlGeoUtils::check_srid(node.domain_extra_.srid_, input_srid))) {
    ret = OB_ERR_WRONG_SRID_FOR_COLUMN;
    LOG_USER_ERROR(OB_ERR_WRONG_SRID_FOR_COLUMN, static_cast<uint64_t>(input_srid),
      static_cast<uint64_t>(node.domain_extra_.srid_));
  } else{
    if (op_type == ObDomainOpType::T_GEO_DWITHIN) {
      distance = objs_ptr[1].get_double();
    }
    switch (op_type) {
      case ObDomainOpType::T_GEO_INTERSECTS:
      case ObDomainOpType::T_GEO_COVERS:
      case ObDomainOpType::T_GEO_DWITHIN:
        if (OB_FAIL(get_intersects_tmp_geo_param(input_srid, wkb_str, op_type, distance, tmp_geo_param))) {
          LOG_WARN("failed to get keypart from intersects_keypart", K(ret), K(op_type));
        }
        break;
      case ObDomainOpType::T_GEO_COVEREDBY:
        if (OB_FAIL(get_coveredby_tmp_geo_param(input_srid, wkb_str, op_type, tmp_geo_param))) {
          LOG_WARN("failed to get keypart from intersects_keypart", K(ret), K(op_type));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Not support op_type", K(ret), K(op_type));
        break;
    }
    if (OB_SUCC(ret)) {
      all_tmp_node_caches_.at(node.node_id_) = tmp_geo_param;
    }
  }
  return ret;
}

int ObRangeGenerator::get_intersects_tmp_geo_param(uint32_t input_srid,
                                                   const common::ObString &wkb_str,
                                                   const common::ObDomainOpType op_type,
                                                   const double &distance,
                                                   ObTmpGeoParam *geo_param)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc(lib::ObLabel("GisIndex"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObS2Cellids cells;
  ObS2Cellids cells_with_ancestors;
  ObSpatialMBR mbr_filter(op_type);
  ObGeoType geo_type = ObGeoType::GEOMETRY;
  const ObSrsItem *srs_item = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsBoundsItem *srs_bound = NULL;
  ObS2Adapter *s2object = NULL;
  ObString buffer_geo;

  if ((input_srid != 0) && OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("get tenant srs guard failed", K(input_srid), K(ret));
  } else if ((input_srid != 0) && OB_FAIL(srs_guard.get_srs_item(input_srid, srs_item))) {
    LOG_WARN("get tenant srs failed", K(input_srid), K(ret));
  } else if (((input_srid == 0) || !(srs_item->is_geographical_srs())) &&
             OB_FAIL(OTSRS_MGR->get_srs_bounds(input_srid, srs_item, srs_bound))) {
    LOG_WARN("failed to get srs item", K(ret));
  } else if (op_type == ObDomainOpType::T_GEO_DWITHIN) {
    if (std::isnan(distance)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid distance para", K(ret));
    } else if (input_srid != 0 && srs_item->is_geographical_srs()) {
      double sphere_radius = (srs_item->semi_major_axis() * 2 + srs_item->semi_minor_axis()) /  3;
      const double SPHERIOD_ERR_FRACTION = 0.005;
      double radius = ((1.0 + SPHERIOD_ERR_FRACTION) * distance) / sphere_radius;
      s2object = OB_NEWx(ObS2Adapter, (&tmp_alloc), (&tmp_alloc), true, radius);
      if (OB_ISNULL(s2object)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc s2 object", K(ret));
      }
    } else {
      if (OB_FAIL(ObGeoTypeUtil::get_buffered_geo(&tmp_alloc, wkb_str, distance, srs_item, buffer_geo))) {
        LOG_WARN("failed to get buffer geo", K(ret));
        if (ret == OB_INVALID_ARGUMENT) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, N_ST_BUFFER);
        } else if (ret == OB_ERR_GIS_INVALID_DATA) {
          LOG_USER_ERROR(OB_ERR_GIS_INVALID_DATA, N_ST_BUFFER);
        }
      }
    }
  }

  if (s2object == NULL && OB_SUCC(ret)) {
    s2object = OB_NEWx(ObS2Adapter, (&tmp_alloc), (&tmp_alloc), (input_srid != 0 ? srs_item->is_geographical_srs() : false), true);
    if (OB_ISNULL(s2object)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc s2 object", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "S2Adapter"));
    // build s2 object from wkb
    if (OB_FAIL(ObGeoTypeUtil::get_type_from_wkb((buffer_geo.empty() ? wkb_str : buffer_geo), geo_type))) {
      LOG_WARN("fail to get geo type by wkb", K(ret));
    } else if (OB_FAIL(s2object->init((buffer_geo.empty() ? wkb_str : buffer_geo), srs_bound))) {
      LOG_WARN("Init s2object failed", K(ret));
    } else if (OB_FAIL(s2object->get_cellids_and_unrepeated_ancestors(cells, cells_with_ancestors))) {
      LOG_WARN("Get cellids from s2object failed", K(ret));
    } else if (OB_FAIL(s2object->get_mbr(mbr_filter))) {
      LOG_WARN("Get mbr from s2object failed", K(ret));
    } else if (OB_FAIL(mbr_filters_.push_back(mbr_filter))) {
      LOG_WARN("Push back to mbr_filters array failed", K(ret));
    } else if (mbr_filter.is_empty()) {
      if (cells.size() == 0) {
        LOG_INFO("it's might be empty geometry collection", K(wkb_str));
        geo_param->always_true_ = true;
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid geometry", K(ret), K(wkb_str));
      }
    } else {
      int64_t range_count = cells_with_ancestors.size();
      range_count += cells.size();
      if (OB_FAIL(geo_param->start_keys_.init(range_count))) {
        LOG_WARN("failed to init start keys", K(ret));
      } else if (OB_FAIL(geo_param->end_keys_.init(range_count))) {
        LOG_WARN("failed to init end keys", K(ret));
      }
      // build keypart from cells_with_ancestors
      for (uint64_t i = 0; OB_SUCC(ret) && i < cells_with_ancestors.size(); i++) {
        if (OB_FAIL(geo_param->start_keys_.push_back(cells_with_ancestors[i]))) {
          LOG_WARN("failed to push back into array", K(ret));
        } else if (OB_FAIL(geo_param->end_keys_.push_back(cells_with_ancestors[i]))) {
          LOG_WARN("failed to push back into array", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (((geo_type != ObGeoType::POINT && geo_type != ObGeoType::POINTZ) || !std::isnan(distance))) {
        // build keypart to index child_of_cellid
        for (uint64_t i = 0; OB_SUCC(ret) && i < cells.size(); i++) {
          uint64_t cellid = cells.at(i);
          uint64_t start_id = 0;
          uint64_t end_id = 0;
          ObS2Adapter::get_child_of_cellid(cellid, start_id, end_id);
          if (OB_FAIL(geo_param->start_keys_.push_back(start_id))) {
            LOG_WARN("failed to push back into array", K(ret));
          } else if (OB_FAIL(geo_param->end_keys_.push_back(end_id))) {
            LOG_WARN("failed to push back into array", K(ret));
          }
        }
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < cells.size(); i++) {
          if (OB_FAIL(geo_param->start_keys_.push_back(cells[i]))) {
            LOG_WARN("failed to push back into array", K(ret));
          } else if (OB_FAIL(geo_param->end_keys_.push_back(cells[i]))) {
            LOG_WARN("failed to push back into array", K(ret));
          }
        }
      }
    }
  }

  if (OB_NOT_NULL(s2object)) {
    s2object->~ObS2Adapter();
  }

  return ret;
}

int ObRangeGenerator::get_coveredby_tmp_geo_param(uint32_t input_srid,
                                                  const common::ObString &wkb_str,
                                                  const common::ObDomainOpType op_type,
                                                  ObTmpGeoParam *geo_param)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_alloc(lib::ObLabel("GisIndex"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObS2Cellids cells;
  ObSpatialMBR mbr_filter(op_type);
  const ObSrsItem *srs_item = NULL;
  omt::ObSrsCacheGuard srs_guard;
  const ObSrsBoundsItem *srs_bound = NULL;
  ObS2Adapter *s2object = NULL;
  ObString buffer_geo;

  if ((input_srid != 0) && OB_FAIL(OTSRS_MGR->get_tenant_srs_guard(srs_guard))) {
    LOG_WARN("get tenant srs guard failed", K(input_srid), K(ret));
  } else if ((input_srid != 0) && OB_FAIL(srs_guard.get_srs_item(input_srid, srs_item))) {
    LOG_WARN("get tenant srs failed", K(input_srid), K(ret));
  } else if (((input_srid == 0) || !(srs_item->is_geographical_srs())) &&
             OB_FAIL(OTSRS_MGR->get_srs_bounds(input_srid, srs_item, srs_bound))) {
    LOG_WARN("failed to get srs item", K(ret));
  }
  if (s2object == NULL && OB_SUCC(ret)) {
    s2object = OB_NEWx(ObS2Adapter, (&tmp_alloc), (&tmp_alloc), (input_srid != 0 ? srs_item->is_geographical_srs() : false));
    if (OB_ISNULL(s2object)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc s2 object", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(MTL_ID(), "S2Adapter"));
    // build s2 object from wkb
    if (OB_FAIL(s2object->init((buffer_geo.empty() ? wkb_str : buffer_geo), srs_bound))) {
      LOG_WARN("Init s2object failed", K(ret));
    } else if (OB_FAIL(s2object->get_inner_cover_cellids(cells))) {
      LOG_WARN("Get cellids from s2object failed", K(ret));
    } else if (OB_FAIL(s2object->get_mbr(mbr_filter))) {
      LOG_WARN("Get mbr from s2object failed", K(ret));
    } else if (OB_FAIL(mbr_filters_.push_back(mbr_filter))) {
      LOG_WARN("Push back to mbr_filters array failed", K(ret));
    } else if (mbr_filter.is_empty()) {
      if (cells.size() == 0) {
        LOG_INFO("it's might be empty geometry collection", K(wkb_str));
        geo_param->always_true_ = true;
      } else {
        ret = OB_ERR_GIS_INVALID_DATA;
        LOG_WARN("invalid geometry", K(ret), K(wkb_str));
      }
    } else {
      hash::ObHashSet<uint64_t> cellid_set;
      if (OB_FAIL(cellid_set.create(128, "CoveredByKeyPart", "HashNode", MTL_ID()))) {
        LOG_WARN("failed to create cellid set", K(ret));
      } else if (!cellid_set.created()) {
        ret = OB_NOT_INIT;
        LOG_WARN("fail to init cellid set", K(ret));
      }
      for (uint64_t i = 0; OB_SUCC(ret) && i < cells.size(); i++) {
        int hash_ret = cellid_set.exist_refactored(cells[i]);
        ObS2Cellids ancestors;
        if (OB_HASH_NOT_EXIST == hash_ret) {
          if (OB_FAIL(cellid_set.set_refactored(cells[i]))) {
            LOG_WARN("failed to add cellid into set", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(s2object->get_ancestors(cells[i], ancestors))) {
            LOG_WARN("Get ancestors of cell failed", K(ret));
          }
          // if cur cellid is exists in set, then it's ancestors also exist in set
          int hash_ret = OB_HASH_NOT_EXIST;
          for (uint64_t i = 0; OB_SUCC(ret) && i < ancestors.size(); i++) {
            hash_ret = cellid_set.exist_refactored(ancestors[i]);
            if (hash_ret == OB_HASH_NOT_EXIST) {
              if (OB_FAIL(cellid_set.set_refactored(ancestors[i]))) {
                LOG_WARN("failed to add cellid into set", K(ret));
              }
            } else if (OB_HASH_EXIST != hash_ret) {
              ret = hash_ret;
              LOG_WARN("fail to check if key exist", K(ret), K(ancestors[i]), K(i));
            }
          }
        } else if (OB_HASH_EXIST != hash_ret) {
          ret = hash_ret;
          LOG_WARN("fail to check if key exist", K(ret), K(cells[i]), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(geo_param->start_keys_.init(cellid_set.size()))) {
          LOG_WARN("failed to init start keys", K(ret));
        } else if (OB_FAIL(geo_param->end_keys_.init(cellid_set.size()))) {
          LOG_WARN("failed to init end keys", K(ret));
        } else {
          for (hash::ObHashSet<uint64_t>::const_iterator itr = cellid_set.begin(); OB_SUCC(ret) && itr != cellid_set.end();
               ++itr) {
            if (OB_FAIL(geo_param->start_keys_.push_back(itr->first))) {
              LOG_WARN("failed to push back array", K(ret));
            } else if (OB_FAIL(geo_param->end_keys_.push_back(itr->first))) {
              LOG_WARN("failed to push back array", K(ret));
            }
          }
        }
      }
      // clear hashset
      if (cellid_set.created()) {
        int tmp_ret = cellid_set.destroy();
        if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
          LOG_WARN("failed to destory param set", K(ret));
        }
      }
    }
  }
  if (OB_NOT_NULL(s2object)) {
    s2object->~ObS2Adapter();
  }
  return ret;
}

int ObRangeGenerator::final_geo_range_node(const ObRangeNode &node,
                                           const uint64_t start,
                                           const uint64_t end,
                                           ObTmpRange *&range)
{
  int ret = OB_SUCCESS;
  range = all_tmp_ranges_.at(node.node_id_);
  if (range == nullptr) {
    if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
      LOG_WARN("failed to generate tmp range");
    }
  }
  if (OB_SUCC(ret)) {
    ObObj start_val;
    ObObj end_val;
    start_val.set_uint64(start);
    end_val.set_uint64(end);
    if (OB_FAIL(fill_domain_range_node(node, start_val, end_val, range))) {
      LOG_WARN("failed to fill domain equal range node", K(ret));
    }
  }
  return ret;
}

int ObRangeGenerator::check_need_merge_range_nodes(const ObRangeNode *node,
                                                   bool &need_merge)
{
  int ret = OB_SUCCESS;
  need_merge = true;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (node->is_domain_node_ && OB_ISNULL(node->or_next_)) {
    need_merge = false;
  }
  return ret;
}
int ObRangeGenerator::generate_fast_nlj_range(const ObPreRangeGraph &pre_range_graph,
                                              const ParamStore &param_store,
                                              ObIAllocator &allocator,
                                              void *range_buffer)
{
  int ret = OB_SUCCESS;
  bool always_false = false;
  const ObRangeNode *node = pre_range_graph.get_range_head();
  const ObRangeMap &range_map = pre_range_graph.get_range_map();
  ObNewRange *range = static_cast<ObNewRange*>(range_buffer);
  ObObj *starts = reinterpret_cast<ObObj*>(static_cast<char*>(range_buffer) + sizeof(ObNewRange));
  ObObj *ends = starts + node->column_cnt_;
  for (int i = 0; OB_SUCC(ret) && !always_false && i < node->column_cnt_; ++i) {
    int64_t start_idx = node->start_keys_[i];
    int64_t end_idx = node->end_keys_[i];
    if (start_idx == OB_RANGE_MIN_VALUE &&
        end_idx == OB_RANGE_MAX_VALUE) {
      starts[i].set_min_value();
      ends[i].set_max_value();
    } else if (start_idx == OB_RANGE_NULL_VALUE) {
      starts[i].set_null();
      ends[i].set_null();
    } else {
      const ObRangeMap::ExprFinalInfo& expr_info = range_map.expr_final_infos_.at(start_idx);
      const ObObj* src_obj = nullptr;
      if (expr_info.is_param_) {
        src_obj = &param_store.at(expr_info.param_idx_);
      } else if (expr_info.is_const_) {
        src_obj = expr_info.const_val_;
      }
      if (src_obj->is_null() && !expr_info.null_safe_) {
        always_false = true;
      } else if (OB_FAIL(ob_write_obj(allocator, *src_obj, *(starts + i)))) {
        LOG_WARN("failed to write obj", K(ret));
      } else {
        *(ends + i) = *(starts + i);
      }
    }
  }
  if (OB_SUCC(ret)) {
    range->is_physical_rowid_range_ = node->is_phy_rowid_;
    if (OB_LIKELY(!always_false)) {
      if (node->include_start_) {
        range->border_flag_.set_inclusive_start();
      } else {
        range->border_flag_.unset_inclusive_start();
      }
      if (node->include_end_) {
        range->border_flag_.set_inclusive_end();
      } else {
        range->border_flag_.unset_inclusive_end();
      }
      range->start_key_.assign(starts, node->column_cnt_);
      range->end_key_.assign(ends, node->column_cnt_);
    } else {
      range->set_false_range();
    }
  }
  return ret;
}

int ObRangeGenerator::check_can_final_fast_nlj_range(const ObPreRangeGraph &pre_range_graph,
                                                     const ParamStore &param_store,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  const ObRangeNode *node = pre_range_graph.get_range_head();
  const ObRangeMap &range_map = pre_range_graph.get_range_map();
  is_valid = true;
  for (int i = 0; OB_SUCC(ret) && i < node->column_cnt_; ++i) {
    int64_t start_idx = node->start_keys_[i];
    int64_t end_idx = node->end_keys_[i];
    const ObRangeColumnMeta *meta = pre_range_graph.get_column_meta(i);
    int64_t cmp = 0;
    const ObObj* src_obj = nullptr;
    if (OB_ISNULL(meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column meta");
    } else if (start_idx == OB_RANGE_MIN_VALUE &&
               end_idx == OB_RANGE_MAX_VALUE) {
      // do nothing
    } else if (OB_UNLIKELY(start_idx != end_idx ||
                           !is_const_expr_or_null(start_idx) ||
                           !is_const_expr_or_null(end_idx) ||
                           start_idx < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("start idx should equal to end idx", K(start_idx), K(end_idx));
    } else if (start_idx == OB_RANGE_NULL_VALUE) {
      // do nothing
    } else if (range_map.expr_final_infos_.at(start_idx).is_param_) {
      src_obj = &param_store.at(range_map.expr_final_infos_.at(start_idx).param_idx_);
    } else if (range_map.expr_final_infos_.at(start_idx).is_const_) {
      src_obj = range_map.expr_final_infos_.at(start_idx).const_val_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected range node in fast nlj range", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (src_obj == nullptr) {
      // do nothing
    } else if (OB_LIKELY(ObSQLUtils::is_same_type_for_compare(src_obj->get_meta(),
                                                              meta->column_type_.get_obj_meta()) &&
                         !src_obj->is_decimal_int())) {
      // do nothing
    } else {
      is_valid = false;
    }
  }
  return ret;
}

int ObRangeGenerator::cast_double_to_fixed_double(const ObRangeColumnMeta &meta,
                                                  const ObObj& in_value,
                                                  ObObj &out_value)
{
  int ret = OB_SUCCESS;
  double value = in_value.get_double();
  if (ObUDoubleType == meta.column_type_.get_type() && value < 0.) {
    out_value.set_double(meta.column_type_.get_type(), 0.);
  } else if (OB_FAIL(refine_real_range(meta.column_type_.get_accuracy(), value))) {
    LOG_WARN("failed to real range check", K(ret));
  } else {
    out_value.set_double(meta.column_type_.get_type(), value);
  }
  return ret;
}

int ObRangeGenerator::refine_real_range(const ObAccuracy &accuracy, double &value)
{
  int ret = OB_SUCCESS;
  const ObPrecision precision = accuracy.get_precision();
  const ObScale scale = accuracy.get_scale();
  if (OB_LIKELY(precision > 0) &&
      OB_LIKELY(scale >= 0) &&
      OB_LIKELY(precision >= scale)) {
    double integer_part = static_cast<double>(pow(10.0, static_cast<double>(precision - scale)));
    double decimal_part = static_cast<double>(pow(10.0, static_cast<double>(scale)));
    double max_value = static_cast<double>(integer_part - 1 / decimal_part);
    double min_value = static_cast<double>(-max_value);
    if (value < min_value) {
      value = min_value;
    } else if (value > max_value) {
      value = max_value;
    } else {
      value = static_cast<double>(rint((value -
                                  floor(static_cast<double>(value)))* decimal_part) /
                                  decimal_part + floor(static_cast<double>(value)));
    }
  }
  return ret;
}

int ObRangeGenerator::get_spatial_relationship_by_mask(const ObObj& extra, ObDomainOpType& op_type)
{
  int ret = OB_SUCCESS;
  if (!ob_is_string_type(extra.get_type())) {
    op_type = ObDomainOpType::T_DOMAIN_OP_END;
  } else {
    ObString mask_str(extra.get_string());
    common::ObArenaAllocator temp_allocator(lib::ObLabel("GisIndex"), OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObString upper_str;
    void* ptr = NULL;
    char *cmp_str = NULL;
    if (OB_FAIL(ob_simple_low_to_up(temp_allocator, mask_str, upper_str))) {
      LOG_WARN("failed to get upper string", K(ret));
    }  else if (NULL == (ptr = temp_allocator.alloc(upper_str.length() + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(upper_str.length()));
    } else {
      cmp_str = static_cast<char*>(ptr);
      cmp_str[upper_str.length()] = '\0';
      MEMCPY(cmp_str, upper_str.ptr(), upper_str.length());
      if (nullptr != strstr(cmp_str, "ANYINTERACT")) {
        op_type = ObDomainOpType::T_GEO_INTERSECTS;
      } else {
        // other spatial relationsh is not supported yet, no need to continue
        op_type = ObDomainOpType::T_DOMAIN_OP_END;
      }
    }
  }
  return ret;
}

bool ObRangeGenerator::is_geo_type(const ObDomainOpType& op_type)
{
  bool bret = false;
  if (op_type == ObDomainOpType::T_GEO_COVERS ||
      op_type == ObDomainOpType::T_GEO_INTERSECTS ||
      op_type == ObDomainOpType::T_GEO_DWITHIN ||
      op_type == ObDomainOpType::T_GEO_COVEREDBY ||
      op_type == ObDomainOpType::T_GEO_RELATE) {
    bret = true;
  }
  return bret;
}

int ObRangeGenerator::final_json_member_of_range_node(const ObRangeNode *node,
                                                      ObTmpRange *&range,
                                                      bool need_cache)
{
  int ret = OB_SUCCESS;
  ObObj obj;
  const ObRangeColumnMeta *meta = NULL;
  ObIJsonBase* j_base = nullptr;
  ObObj cast_obj;
  range = need_cache ? all_tmp_ranges_.at(node->node_id_) : nullptr;
  bool is_valid = false;
  int64_t cmp = 0;
  if (range != nullptr) {
    // already generated
  } else if (OB_UNLIKELY(node->min_offset_ < 0 ||
                         node->min_offset_ >= pre_range_graph_->get_column_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range", K(node));
  } else if (OB_ISNULL(meta = pre_range_graph_->get_column_meta(node->min_offset_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column meta", K(ret));
  } else if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
    LOG_WARN("failed to generate tmp range");
  } else if (OB_FAIL(get_result_value(node->start_keys_[node->min_offset_], obj, is_valid, exec_ctx_))) {
    LOG_WARN("failed to get result value", K(node->start_keys_[node->min_offset_]));
  } else if (!is_valid) {
    range->set_always_false();
  } else {
    cast_obj = obj;
    if (ob_is_json(obj.get_type()) ||
        ob_is_datetime_tc(meta->column_type_.get_type()) ||
        ob_is_date_tc(meta->column_type_.get_type()) ||
        ob_is_time_tc(meta->column_type_.get_type())) {
      if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(obj,
                                                                  &exec_ctx_,
                                                                  false,
                                                                  &allocator_,
                                                                  j_base))) {
        LOG_WARN("failed cast to json scalar.", K(ret), K(cast_obj));
      } else if (OB_FAIL(ObJsonUtil::cast_json_scalar_to_sql_obj(&allocator_,
                                                                 &exec_ctx_,
                                                                 j_base,
                                                                 meta->column_type_,
                                                                 cast_obj))) {
        LOG_WARN("failed cast to sql scalar.", K(ret), K(cast_obj));
      }
    }

    if (OB_FAIL(ret)) {
      ret = OB_SUCCESS;
      range->set_always_true();
    } else if (OB_FAIL(try_cast_value(*meta, cast_obj, cmp, CO_EQ))) {
      LOG_WARN("failed to cast value", K(ret));
    } else if (cmp != 0) {
      range->set_always_true();
    } else if (OB_FAIL(fill_domain_range_node(*node, cast_obj, cast_obj, range))) {
      LOG_WARN("failed to fill domain range node", K(ret));
    }
  }
  return ret;
}

int ObRangeGenerator::fill_domain_range_node(const ObRangeNode &node,
                                             const ObObj& start_val,
                                             const ObObj& end_val,
                                             ObTmpRange *range)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(range)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    range->always_false_ = false;
    range->always_true_ = false;
    range->min_offset_ = node.min_offset_;
    range->max_offset_ = node.max_offset_;
    range->include_start_ = node.include_start_;
    range->include_end_ = node.include_end_;
    range->is_phy_rowid_ = node.is_phy_rowid_;

    for (int64_t i = 0; OB_SUCC(ret) && i < pre_range_graph_->get_column_cnt(); ++i) {
      if (i == node.min_offset_) {
        range->start_[node.min_offset_] = start_val;
        range->end_[node.min_offset_] = end_val;
      } else {
        int64_t start = node.start_keys_[i];
        int64_t end = node.end_keys_[i];
        if (start == OB_RANGE_MIN_VALUE) {
          range->start_[i].set_min_value();
        } else if (start == OB_RANGE_MAX_VALUE) {
          range->start_[i].set_max_value();
        } else if (start == OB_RANGE_NULL_VALUE) {
          range->start_[i].set_null();
        } else if (start == OB_RANGE_EMPTY_VALUE) {
          range->start_[i].set_nop_value();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected range node", K(ret), K(node));
        }
        if (OB_FAIL(ret)) {
        } else if (end == OB_RANGE_MIN_VALUE) {
          range->end_[i].set_min_value();
        } else if (end == OB_RANGE_MAX_VALUE) {
          range->end_[i].set_max_value();
        } else if (end == OB_RANGE_NULL_VALUE) {
          range->end_[i].set_null();
        } else if (end == OB_RANGE_EMPTY_VALUE) {
          range->end_[i].set_nop_value();
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected range node", K(ret), K(node));
        }
      }
    }
  }
  return ret;
}

int ObRangeGenerator::generate_tmp_json_array_param(const ObRangeNode &node,
                                                    ObTmpInParam *&tmp_in_param)
{
  int ret = OB_SUCCESS;
  InParam *in_param = nullptr;
  ObObj* objs_ptr = nullptr;
  const ObRangeColumnMeta *meta = nullptr;
  ObObj const_param;
  bool is_valid = false;
  ObIJsonBase* j_base = nullptr;
  if (OB_UNLIKELY(!node.is_domain_node_ ||
                  node.node_id_ < 0 ||
                  node.node_id_ >= all_tmp_node_caches_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not in node", K(node.node_id_), K(all_tmp_node_caches_.count()));
  } else if (OB_NOT_NULL(tmp_in_param = static_cast<ObTmpInParam*>(all_tmp_node_caches_.at(node.node_id_)))) {
    // do nothing
  } else if (OB_UNLIKELY(node.min_offset_ < 0 ||
                         node.min_offset_ >= pre_range_graph_->get_column_cnt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected range", K(node));
  } else if (OB_ISNULL(meta = pre_range_graph_->get_column_meta(node.min_offset_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexecpted null", K(meta));
  } else if (OB_ISNULL(tmp_in_param = (ObTmpInParam*)allocator_.alloc(sizeof(ObTmpInParam)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memeory failed", K(tmp_in_param));
  } else if (OB_FALSE_IT(tmp_in_param = new(tmp_in_param) ObTmpInParam(allocator_))) {
  } else if (OB_FAIL(get_result_value(node.start_keys_[node.min_offset_], const_param, is_valid, exec_ctx_))) {
    LOG_WARN("failed to get result value", K(node.start_keys_[node.min_offset_]));
  } else if (!is_valid) {
    tmp_in_param->always_false_ = true;
  } else if (OB_FAIL(ObJsonExprHelper::refine_range_json_value_const(const_param, &exec_ctx_, false, &allocator_, j_base))) {
    LOG_WARN("fail to get json val", K(ret), K(const_param));
  } else if (OB_ISNULL(j_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get json base", K(ret));
  } else if (j_base->is_json_scalar(j_base->json_type())) {
    ObObj cast_obj = const_param;
    int64_t cmp = 0;
    if (OB_FAIL(ObJsonUtil::cast_json_scalar_to_sql_obj(&allocator_,
                                                        &exec_ctx_,
                                                        j_base,
                                                        meta->column_type_,
                                                        cast_obj))) {
      ret = OB_SUCCESS;
      tmp_in_param->always_true_ = true;
    } else if (OB_FAIL(try_cast_value(*meta, cast_obj, cmp, CO_EQ))) {
      LOG_WARN("failed to cast value", K(ret));
    } else if (cmp != 0) {
      tmp_in_param->always_true_ = true;
    } else if (OB_ISNULL(objs_ptr = (ObObj*)allocator_.alloc(sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memeory failed", K(objs_ptr));
    } else if (OB_FAIL(tmp_in_param->in_param_.init(1))) {
      LOG_WARN("failed to init fixed array size", K(ret));
    } else if (OB_FAIL(tmp_in_param->in_param_.push_back(objs_ptr))) {
      LOG_WARN("failed to push back array.", K(ret));
    } else {
      *objs_ptr = cast_obj;
    }
  } else if (j_base->json_type() == common::ObJsonNodeType::J_ARRAY) {
    int64_t size = j_base->element_count();
    int64_t cmp = 0;
    if (size == 0) {
      tmp_in_param->always_true_ = true;
    } else if (OB_ISNULL(objs_ptr = (ObObj*)allocator_.alloc(sizeof(ObObj) * size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memeory failed", K(objs_ptr));
    } else if (OB_FAIL(tmp_in_param->in_param_.init(size))) {
      LOG_WARN("failed to init fixed array size", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !tmp_in_param->always_true_ && i < size; ++i) {
        ObIJsonBase* tmp_j_base = nullptr;
        if (OB_FAIL(j_base->get_array_element(i, tmp_j_base))) {
          LOG_WARN("fail to get json array element", K(i), K(ret));
        } else if (OB_ISNULL(tmp_j_base)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get json array element result is null", K(i), K(ret));
        } else if (OB_FAIL(ObJsonUtil::cast_json_scalar_to_sql_obj(&allocator_,
                                                                   &exec_ctx_,
                                                                   tmp_j_base,
                                                                   meta->column_type_,
                                                                   objs_ptr[i]))) {
          ret = OB_SUCCESS;
          tmp_in_param->always_true_ = true;
        } else if (OB_FAIL(try_cast_value(*meta, objs_ptr[i], cmp, CO_EQ))) {
          LOG_WARN("failed to cast value", K(ret));
        } else if (cmp != 0) {
          tmp_in_param->always_true_ = true;
        } else {
          tmp_in_param->in_param_.push_back(&objs_ptr[i]);
        }
      }
    }
  } else {
    tmp_in_param->always_true_ = true;
  }
  return ret;
}

int ObRangeGenerator::final_domain_range_node(const ObRangeNode &node,
                                              const int64_t in_idx,
                                              ObTmpInParam *in_param,
                                              ObTmpRange *&range)
{
  int ret = OB_SUCCESS;
  range = all_tmp_ranges_.at(node.node_id_);
  if (range == nullptr) {
    if (OB_FAIL(generate_tmp_range(range, pre_range_graph_->get_column_cnt()))) {
      LOG_WARN("failed to generate tmp range");
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(in_param) ||
        OB_UNLIKELY(in_idx >= in_param->in_param_.count()) ||
        OB_ISNULL(in_param->in_param_.at(in_idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected in idx", K(ret), K(in_idx), KPC(in_param));
    } else if (OB_FAIL(fill_domain_range_node(node,
                                              *in_param->in_param_.at(in_idx),
                                              *in_param->in_param_.at(in_idx),
                                              range))) {
      LOG_WARN("failed to fill domain equal range node", K(ret));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
