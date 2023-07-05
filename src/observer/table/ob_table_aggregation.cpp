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

using namespace oceanbase::common;
using namespace oceanbase::table;
        
int ObTableAggCalculator::init() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(results_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate aggregate result", K(ret), K_(size));
  } else if (OB_FAIL(counts_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for counts", K(ret), K_(counts));
  } else if (OB_FAIL(sums_.prepare_allocate(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for sums", K(ret), K_(sums));
  }
  return ret;
}

int ObTableAggCalculator::deep_copy_value(int64_t idx, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;
  int64_t src_deep_copy_size = src.get_deep_copy_size();
  int64_t dst_deep_copy_size = dst.get_deep_copy_size();
  char *buf = deep_copy_buffers_.at(idx);
  int64_t pos = 0;

  if (dst_deep_copy_size >= src_deep_copy_size) { // enough
    if (OB_FAIL(dst.deep_copy(src, buf, src_deep_copy_size, pos))) {
      LOG_WARN("fail deep copy src obj", K(ret), K(src_deep_copy_size));
    }
  } else { // not enough, free old and alloc new
    // free
    allocator_.free(buf);
    // alloc
    buf = static_cast<char*>(allocator_.alloc(src_deep_copy_size));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(src_deep_copy_size));
    } else if (OB_FAIL(dst.deep_copy(src, buf, src_deep_copy_size, pos))) {
      LOG_WARN("fail deep copy src obj", K(ret), K(src_deep_copy_size));
    }
  }

  return ret;
}

int ObTableAggCalculator::assign_value(int64_t idx, const ObObj &src, ObObj &dst)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(src.need_deep_copy()) && OB_FAIL(deep_copy_value(idx, src, dst))) {
    LOG_WARN("fail to deep copy value", K(ret), K(src), K(dst));
  } else {
    dst = src;
  }

  return ret;
}

int ObTableAggCalculator::aggregate_max(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &value = row.get_cell(projs_->at(idx));
  ObObj &agg_result = results_.at(idx);
  if (!agg_result.is_null() && !value.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(agg_result.compare(value, cmp_ret))) {
      LOG_WARN("fail to compare", K(ret), K(agg_result), K(value));
    } else if (cmp_ret == ObObjCmpFuncs::CR_LT) {
      if (OB_FAIL(assign_value(idx, value, agg_result))) {
        LOG_WARN("fail to assign value", K(ret), K(agg_result), K(value)); 
      }
    }
  } else if (!value.is_null() && OB_FAIL(ob_write_obj(allocator_, value, agg_result))) {
    LOG_WARN("fail to assign value", K(ret), K(agg_result), K(value));
  }
  return ret;
}

int ObTableAggCalculator::aggregate_min(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &value = row.get_cell(projs_->at(idx));
  ObObj &agg_result = results_.at(idx);
  if (!agg_result.is_null() && !value.is_null()) {
    int cmp_ret = 0;
    if (OB_FAIL(agg_result.compare(value, cmp_ret))) {
      LOG_WARN("fail to compare", K(ret), K(agg_result), K(value));
    } else if (cmp_ret == ObObjCmpFuncs::CR_GT) {
      if (OB_FAIL(assign_value(idx, value, agg_result))) {
        LOG_WARN("fail to assign value", K(ret), K(agg_result), K(value)); 
      }
    }
  } else if (!value.is_null() && OB_FAIL(ob_write_obj(allocator_, value, agg_result))) {
    LOG_WARN("fail to assign value", K(ret), K(agg_result), K(value));
  }
  return ret;
}

void ObTableAggCalculator::aggregate_count(uint64_t idx, const ObNewRow &row, const ObString &key_word)
{
  const ObObj &value = row.get_cell(projs_->at(idx));
  ObObj &agg_result = results_.at(idx);
  if (key_word.empty() || key_word == "*") {
    if (!agg_result.is_null()) {
      agg_result.set_int(agg_result.get_int() + 1); 
    } else {
      agg_result.set_int(1); 
    }
  } else {
    if (!value.is_null()) {
      if (!agg_result.is_null()) {
        agg_result.set_int(agg_result.get_int() + 1); 
      } else {
        agg_result.set_int(1); 
      }
    }
  }
}

int ObTableAggCalculator::aggregate_sum(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &value = row.get_cell(projs_->at(idx));
  ObObj &agg_result = results_.at(idx);
  const ObObjType &sum_type = value.get_type();
  if (!value.is_null()) {
    if (!agg_result.is_null()) {
      switch(sum_type) {
        //signed int
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          agg_result.set_int(agg_result.get_int() + value.get_int());
          break;
        }
        //unsigned int
        case ObUTinyIntType:
        case ObUSmallIntType:
        case ObUMediumIntType:
        case ObUInt32Type:
        case ObUInt64Type: {
          agg_result.set_uint64(agg_result.get_uint64() + value.get_uint64());
          break;
        }
        //float and ufloat
        case ObFloatType:
        case ObUFloatType: {
          agg_result.set_double(agg_result.get_double() + value.get_float());
          break;
        }
        //double and udouble
        case ObDoubleType:
        case ObUDoubleType: {
          agg_result.set_double(agg_result.get_double() + value.get_double());
          break;
        }
        default: {
          LOG_WARN("this data type does not support aggregate sum operation", K(ret), K(sum_type));  
        }
      }
    } else {
      switch(sum_type) {
        //signed int
        case ObTinyIntType:
        case ObSmallIntType:
        case ObMediumIntType:
        case ObInt32Type:
        case ObIntType: {
          agg_result.set_int(value.get_int());
          break;
        }
        //unsigned int
        case ObUTinyIntType:
        case ObUSmallIntType:
        case ObUMediumIntType:
        case ObUInt32Type:
        case ObUInt64Type: {
          agg_result.set_uint64(value.get_uint64());
          break;
        }
        //float and ufloat
        case ObFloatType:
        case ObUFloatType: {
          agg_result.set_double(value.get_float());
          break;
        }
        //double and udouble
        case ObDoubleType:
        case ObUDoubleType: {
          agg_result.set_double(value.get_double());
          break;
        }
        default: {
          LOG_WARN("this data type does not support aggregate sum operation", K(ret), K(sum_type));  
        }
      }
    }
  }
  return ret;
}

int ObTableAggCalculator::aggregate_avg(uint64_t idx, const ObNewRow &row)
{
  int ret = OB_SUCCESS;
  const ObObj &value = row.get_cell(projs_->at(idx));
  const ObObjType &avg_type = value.get_type();
  int64_t &count = counts_[idx];
  double &count_double = sums_[idx];
  if (!value.is_null()) {
    count++;
    switch(avg_type) {
      //signed int
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        count_double += value.get_int();
        break;
      }
      //unsigned int
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        count_double += value.get_uint64();
        break;
      }
      //float and ufloat
      case ObFloatType:
      case ObUFloatType: {
        count_double += value.get_float();
        break;
      }
      //double and udouble
      case ObDoubleType:
      case ObUDoubleType: {
        count_double += value.get_double();
        break;
      }
      default: {
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
          ret = OB_ERR_UNEXPECTED;
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
    if (query_aggs_.at(i).get_type() == ObTableAggregationType::AVG) {
      const int64_t &count = counts_[i];
      ObObj &agg_result = results_.at(i);
      if (count != 0) {
        agg_result.set_double(sums_[i] / count);
      }
    } else if (query_aggs_.at(i).get_type() == ObTableAggregationType::COUNT) {
      if (results_.at(i).is_null()) {   // if count result is null, should be zero
        results_.at(i).set_int(0);
      }
    }
  }
}