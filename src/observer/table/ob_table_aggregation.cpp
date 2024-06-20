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

#define USING_LOG_PREFIX SERVER
#include "ob_table_aggregation.h"
#include "common/row/ob_row.h"
#include "common/object/ob_obj_compare.h"
#include "sql/engine/expr/ob_expr_add.h" // for ObExprAdd::is_add_out_of_range

using namespace oceanbase::common;
using namespace oceanbase::table;

int ObTableAggCalculator::init() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(results_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate aggregate result", K(ret), K_(size));
  } else if (OB_FAIL(counts_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for counts", K(ret), K_(size));
  } else if (OB_FAIL(sums_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sums", K(ret), K_(size));
  } else if (OB_FAIL(deep_copy_buffers_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for deep_copy_buffers_", K(ret), K_(size));
  } else {
    for (int64_t i = 0; i < size_; i++) {
      results_.at(i).set_null();
    }
  }
  return ret;
}

// 在min/max字符串类型时，需要不断的替换最大值/最小值，导致中间内存耗费很多
// 因此使用中间临时内存，临时内存足够大时，可以复用，不够大时，先释放后重新申请
int ObTableAggCalculator::deep_copy_value(int64_t idx, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  int64_t src_deep_copy_size = src.get_deep_copy_size();
  int64_t dst_deep_copy_size = dst.get_deep_copy_size();
  TempBuffer tmp_buf = deep_copy_buffers_.at(idx);
  int64_t pos = 0;

  if (tmp_buf.size_ >= src_deep_copy_size && OB_NOT_NULL(tmp_buf.buf_)) { // enough
    if (OB_FAIL(dst.deep_copy(src, tmp_buf.buf_, src_deep_copy_size, pos))) {
      LOG_WARN("fail deep copy src obj", K(ret), K(src_deep_copy_size));
    }
  } else { // not enough, free old and alloc new
    // free
    if (OB_NOT_NULL(tmp_buf.buf_)) {
      allocator_.free(tmp_buf.buf_);
    }
    // alloc
    tmp_buf.buf_ = static_cast<char*>(allocator_.alloc(src_deep_copy_size));
    if (OB_ISNULL(tmp_buf.buf_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(src_deep_copy_size));
    } else if (OB_FAIL(dst.deep_copy(src, tmp_buf.buf_, src_deep_copy_size, pos))) {
      LOG_WARN("fail deep copy src obj", K(ret), K(src_deep_copy_size));
    } else {
      tmp_buf.size_ = src_deep_copy_size;
    }
  }

  return ret;
}

int ObTableAggCalculator::assign_value(int64_t idx, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(src.need_deep_copy())) {
    if (OB_FAIL(deep_copy_value(idx, src, dst))) {
      LOG_WARN("fail to deep copy value", K(ret), K(src), K(dst));
    }
  } else {
    dst = src;
  }

  return ret;
}

int ObTableAggCalculator::aggregate_max(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_value = row.get_cell(projs_->at(idx));
  ObObj &max_val = results_.at(idx);
  if (!max_val.is_null() && !cur_value.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(max_val.compare(cur_value, cmp_ret))) {
      LOG_WARN("fail to compare", K(ret), K(max_val), K(cur_value));
    } else if (cmp_ret == ObObjCmpFuncs::CR_LT) {
      if (OB_FAIL(assign_value(idx, cur_value, max_val))) {
        LOG_WARN("fail to assign value", K(ret), K(max_val), K(cur_value));
      }
    }
  } else if (!cur_value.is_null() && OB_FAIL(assign_value(idx, cur_value, max_val))) {
    LOG_WARN("fail to assign value", K(ret), K(max_val), K(cur_value));
  }
  return ret;
}

int ObTableAggCalculator::aggregate_min(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_value = row.get_cell(projs_->at(idx));
  ObObj &min_val = results_.at(idx);
  if (!min_val.is_null() && !cur_value.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(min_val.compare(cur_value, cmp_ret))) {
      LOG_WARN("fail to compare", K(ret), K(min_val), K(cur_value));
    } else if (cmp_ret == ObObjCmpFuncs::CR_GT) {
      if (OB_FAIL(assign_value(idx, cur_value, min_val))) {
        LOG_WARN("fail to assign value", K(ret), K(min_val), K(cur_value));
      }
    }
  } else if (!cur_value.is_null() && OB_FAIL(assign_value(idx, cur_value, min_val))) {
    LOG_WARN("fail to assign value", K(ret), K(min_val), K(cur_value));
  }
  return ret;
}

void ObTableAggCalculator::aggregate_count(uint64_t idx, const ObNewRow &row, const ObString &key_word)
{
  const ObObj &cur_value = row.get_cell(projs_->at(idx));
  ObObj &count_val = results_.at(idx);
  if (key_word.empty() || key_word == "*") {
    if (!count_val .is_null()) {
      count_val.set_int(count_val.get_int() + 1);
    } else {
      count_val.set_int(1);
    }
  } else {
    if (!cur_value.is_null()) {
      if (!count_val.is_null()) {
        count_val.set_int(count_val.get_int() + 1);
      } else {
        count_val.set_int(1);
      }
    }
  }
}

int ObTableAggCalculator::aggregate_sum(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_value = row.get_cell(projs_->at(idx));
  ObObj &sum_val = results_.at(idx);
  const ObObjType &sum_type = cur_value.get_type();
  if (!cur_value.is_null()) {
    switch(sum_type) {
      //signed int
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        if (sum_val.is_null()) {
          sum_val.set_int(cur_value.get_int());
        } else {
          int64_t res = 0;
          if (sql::ObExprAdd::is_add_out_of_range(sum_val.get_int(), cur_value.get_int(), res)) {
            ret = OB_DATA_OUT_OF_RANGE;
            if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
            LOG_WARN("data out of range", K(ret), K(sum_val), K(cur_value));
          } else {
            sum_val.set_int(res);
          }
        }
        break;
      }
      //unsigned int
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        if (sum_val.is_null()) {
          sum_val.set_uint64(cur_value.get_uint64());
        } else {
          uint64_t res = 0;
          if (sql::ObExprAdd::is_add_out_of_range(sum_val.get_uint64(), cur_value.get_uint64(), res)) {
            ret = OB_DATA_OUT_OF_RANGE;
            if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
            LOG_WARN("data out of range", K(ret), K(sum_val), K(cur_value));
          } else {
            sum_val.set_uint64(res);
          }
        }
        break;
      }
      //float and ufloat
      case ObFloatType:
      case ObUFloatType: {
        if (sum_val.is_null()) {
          sum_val.set_double(cur_value.get_float());
        } else {
          double res = sum_val.get_double() + (double)cur_value.get_float();
          if (INFINITY == res) {
            ret = OB_DATA_OUT_OF_RANGE;
            if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
            LOG_WARN("data out of range", K(ret), K(sum_val), K(cur_value));
          } else {
            sum_val.set_double(res);
          }
        }
        break;
      }
      //double and udouble
      case ObDoubleType:
      case ObUDoubleType: {
        if (sum_val.is_null()) {
          sum_val.set_double(cur_value.get_double());
        } else {
          double res = sum_val.get_double() + cur_value.get_double();
          if (INFINITY == res) {
            ret = OB_DATA_OUT_OF_RANGE;
            if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
            LOG_WARN("data out of range", K(ret), K(sum_val), K(cur_value));
          } else {
            sum_val.set_double(res);
          }
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        char err_msg[128];
        const char *type_str = ob_obj_type_str(sum_type);
        (void)sprintf(err_msg, "%s type for sum aggregation", type_str);
        LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
        LOG_WARN("this data type does not support aggregate sum operation", K(ret), K(sum_type));
      }
    }
  }
  return ret;
}

int ObTableAggCalculator::aggregate_avg(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &cur_value = row.get_cell(projs_->at(idx));
  const ObObjType &avg_type = cur_value.get_type();
  int64_t &count = counts_[idx];
  double &sum = sums_[idx];
  if (!cur_value.is_null()) {
    count++;
    switch(avg_type) {
      //signed int
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        double res = sum + (double)cur_value.get_int();
        if (INFINITY == res) {
          ret = OB_DATA_OUT_OF_RANGE;
          if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
          LOG_WARN("data out of range", K(ret), K(sum), K(cur_value));
        } else {
          sum = res;
        }
        break;
      }
      //unsigned int
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        double res = sum + (double)cur_value.get_uint64();
        if (INFINITY == res) {
          ret = OB_DATA_OUT_OF_RANGE;
          if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
          LOG_WARN("data out of range", K(ret), K(sum), K(cur_value));
        } else {
          sum = res;
        }
        break;
      }
      //float and ufloat
      case ObFloatType:
      case ObUFloatType: {
        double res = sum + (double)cur_value.get_float();
        if (INFINITY == res) {
          ret = OB_DATA_OUT_OF_RANGE;
          if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
          LOG_WARN("data out of range", K(ret), K(sum), K(cur_value));
        } else {
          sum = res;
        }
        break;
      }
      //double and udouble
      case ObDoubleType:
      case ObUDoubleType: {
        double res = sum + cur_value.get_double();
        if (INFINITY == res) {
          ret = OB_DATA_OUT_OF_RANGE;
          if (idx < agg_columns_.count()) {
              int64_t row_num = 0;
              LOG_USER_ERROR(OB_DATA_OUT_OF_RANGE, agg_columns_.at(idx).length(), agg_columns_.at(idx).ptr(), row_num);
            }
          LOG_WARN("data out of range", K(ret), K(sum), K(cur_value));
        } else {
          sum = res;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        char err_msg[128];
        const char *type_str = ob_obj_type_str(avg_type);
        (void)sprintf(err_msg, "%s type for avg aggregation", type_str);
        LOG_USER_ERROR(OB_NOT_SUPPORTED, err_msg);
        LOG_WARN("this data type does not support aggregate avg operation", K(ret), K(avg_type));
      }
    }
  }
  return ret;
}

int ObTableAggCalculator::aggregate(const ObNewRow &row) {
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < size_; i++) {
    if (projs_->at(i) == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the column for aggregation is not exist", K(ret), K(i), K_(projs));
    } else {
      ObTableAggregationType agg_type = query_aggs_.at(i).get_type();
      switch (agg_type) {
        case ObTableAggregationType::MAX: {
          if (OB_FAIL(aggregate_max(i, row))) {
            LOG_WARN("fail to aggregate max", K(ret), K(i), K(row));
          }
          break;
        }
        case ObTableAggregationType::MIN: {
          if (OB_FAIL(aggregate_min(i, row))) {
            LOG_WARN("fail to aggregate min", K(ret), K(i), K(row));
          }
          break;
        }
        case ObTableAggregationType::COUNT: {
          const ObString &key_word = query_aggs_.at(i).get_column();
          aggregate_count(i, row, key_word);
          break;
        }
        case ObTableAggregationType::SUM: {
          if (OB_FAIL(aggregate_sum(i, row))) {
            LOG_WARN("fail to aggregate sum", K(ret), K(i), K(row));
          }
          break;
        }
        case ObTableAggregationType::AVG: {
          if (OB_FAIL(aggregate_avg(i, row))) {
            LOG_WARN("fail to aggregate avg", K(ret), K(i), K(row));
          }
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid aggregation type");
          LOG_WARN("unexpected agg type", K(ret), K(agg_type));
          break;
        }
      }
    }
  }

  return ret;
}

void ObTableAggCalculator::final_aggregate()
{
  for (int64_t i = 0; i < size_; i++) {
    const ObTableAggregationType &type = query_aggs_.at(i).get_type();
    if (type == ObTableAggregationType::AVG) {
      const int64_t &count = counts_[i];
      ObObj &avg = results_.at(i);
      if (count != 0) {
        avg.set_double(sums_[i] / count);
      }
    } else if (type == ObTableAggregationType::COUNT) {
      ObObj &count = results_.at(i);
      if (count.is_null()) {   // if count result is null, should be zero
        count.set_int(0);
      }
    }
  }
}