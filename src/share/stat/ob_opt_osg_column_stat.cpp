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

#define USING_LOG_PREFIX SQL_OPT

#include "share/stat/ob_opt_osg_column_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_stat_item.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "sql/engine/expr/ob_expr_sys_op_opnsize.h"
namespace oceanbase {
namespace common {
using namespace sql;

int ObMinMaxValEval::get_obj(ObObj &obj) const
{
  int ret = OB_SUCCESS;
  if (datum_ == NULL) {
    obj.set_null();
  } else if (datum_->to_obj(obj, meta_)) {
    LOG_WARN("failed to to obj");
  }
  return ret;
}

int ObMinMaxValEval::deep_copy(const ObMinMaxValEval &other, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  datum_ = OB_NEWx(ObDatum, (&alloc));
  if (OB_ISNULL(datum_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory");
  } else if (OB_FAIL(datum_->deep_copy(*other.datum_, alloc))) {
    LOG_WARN("failed to deep copy datum");
  } else {
    meta_ = other.meta_;
    cmp_func_ = other.cmp_func_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObOptOSGColumnStat, *col_stat_);

void ObOptOSGColumnStat::reset()
{
  if (col_stat_ != NULL) {
    col_stat_->reset();
    col_stat_ = NULL;
  }
  min_val_.reset();
  max_val_.reset();
  inner_min_allocator_.reset();
  inner_max_allocator_.reset();
}

ObOptOSGColumnStat* ObOptOSGColumnStat::create_new_osg_col_stat(common::ObIAllocator &allocator)
{
  ObOptOSGColumnStat *new_osg_col_stat = OB_NEWx(ObOptOSGColumnStat, (&allocator), allocator);
  ObOptColumnStat *new_col_stat = ObOptColumnStat::malloc_new_column_stat(allocator);
  if (OB_NOT_NULL(new_osg_col_stat) && OB_NOT_NULL(new_col_stat)) {
    new_osg_col_stat->col_stat_ = new_col_stat;
  } else {
    if (new_osg_col_stat != NULL) {
      new_osg_col_stat->~ObOptOSGColumnStat();
      allocator.free(new_osg_col_stat);
      new_osg_col_stat = NULL;
    }
    if (new_col_stat != NULL) {
      new_col_stat->~ObOptColumnStat();
      allocator.free(new_col_stat);
      new_col_stat = NULL;
    }
  }
  return new_osg_col_stat;
}

int ObOptOSGColumnStat::deep_copy(const ObOptOSGColumnStat &other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(col_stat_->deep_copy(*other.col_stat_))) {
    LOG_WARN("failed to deep copy col stat");
  } else if (other.min_val_.is_valid() && OB_FAIL(min_val_.deep_copy(other.min_val_, allocator_))) {
    LOG_WARN("failed to deep copy min val");
  } else if (other.max_val_.is_valid() && OB_FAIL(max_val_.deep_copy(other.max_val_, allocator_))) {
    LOG_WARN("failed to deep copy max val");
  }
  return ret;
}

int ObOptOSGColumnStat::set_min_max_datum_to_obj()
{
  int ret = OB_SUCCESS;
  ObObj *min_obj = NULL;
  ObObj *max_obj = NULL;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_ISNULL(min_obj = OB_NEWx(ObObj, (&allocator_))) ||
             OB_ISNULL(max_obj = OB_NEWx(ObObj, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory");
  } else if (OB_FAIL(min_val_.get_obj(*min_obj))) {
    LOG_WARN("failed to get min obj");
  } else if (OB_FAIL(max_val_.get_obj(*max_obj))) {
    LOG_WARN("failed to get max obj");
  } else if (OB_FAIL(ObDbmsStatsUtils::shadow_truncate_string_for_opt_stats(*min_obj))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::shadow_truncate_string_for_opt_stats(*max_obj))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else {
    const ObObj &min_val = col_stat_->get_min_value();
    const ObObj &max_val = col_stat_->get_max_value();
    LOG_TRACE("set min/max val", KPC(min_obj), KPC(max_obj), K(min_val), K(max_val));
    if (min_val.is_null() || (!min_obj->is_null() && *min_obj < min_val)) {
      col_stat_->set_min_value(*min_obj);
    }
    if (max_val.is_null() || (!max_obj->is_null() && *max_obj > max_val)) {
      col_stat_->set_max_value(*max_obj);
    }
  }
  return ret;
}

int ObOptOSGColumnStat::merge_column_stat(const ObOptOSGColumnStat &other)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_) || OB_ISNULL(other.col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(col_stat_));
  } else if (OB_UNLIKELY(col_stat_->get_table_id() != other.col_stat_->get_table_id() ||
                         col_stat_->get_partition_id() != other.col_stat_->get_partition_id() ||
                         col_stat_->get_column_id() != other.col_stat_->get_column_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the key not match", K(col_stat_->get_table_id()),
                                  K(col_stat_->get_partition_id()),
                                  K(col_stat_->get_column_id()));
  } else if (other.min_val_.is_valid() &&
             OB_FAIL(inner_merge_min(*other.min_val_.datum_,
                                     other.min_val_.meta_,
                                     other.min_val_.cmp_func_))) {
    LOG_WARN("failed to inner merge min val");
  } else if (other.max_val_.is_valid() &&
             OB_FAIL(inner_merge_max(*other.max_val_.datum_,
                                     other.max_val_.meta_,
                                     other.max_val_.cmp_func_))) {
    LOG_WARN("failed to inner merge min val");
  } else {
    col_stat_->add_col_len(other.col_stat_->get_total_col_len());
    col_stat_->add_num_null(other.col_stat_->get_num_null());
    col_stat_->add_num_not_null(other.col_stat_->get_num_not_null());
    if (col_stat_->get_llc_bitmap_size() == other.col_stat_->get_llc_bitmap_size()) {
      ObGlobalNdvEval::update_llc(col_stat_->get_llc_bitmap(), other.col_stat_->get_llc_bitmap());
    }
  }
  return ret;
}

int ObOptOSGColumnStat::update_column_stat_info(const ObDatum *datum,
                                                const ObObjMeta &meta,
                                                const ObDatumCmpFuncType cmp_func)
{
  int ret = OB_SUCCESS;
  int64_t col_len = 0;
  if (OB_ISNULL(datum) || OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(calc_col_len(*datum, meta, col_len))) {
    LOG_WARN("failed to calc col len", K(datum), K(meta));
  } else if (OB_FALSE_IT(col_stat_->add_col_len(col_len))) {
    // do nothing
  } else if (datum->is_null()) {
    col_stat_->add_num_null(1);
  } else {
    col_stat_->add_num_not_null(1);
    uint64_t hash_value = 0;
    ObObj tmp_obj;
    if (OB_FAIL(datum->to_obj(tmp_obj, meta))) {
      LOG_WARN("failed to to obj");
    } else if (tmp_obj.is_string_type()) {
      hash_value = tmp_obj.varchar_hash(tmp_obj.get_collation_type(), hash_value);
    } else if (OB_FAIL(tmp_obj.hash(hash_value, hash_value))) {
      LOG_WARN("fail to do hash", K(ret), K(tmp_obj));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(col_stat_->get_llc_bitmap() == NULL || col_stat_->get_llc_bitmap_size() == 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid llc_bitmap", K(ret));
    } else if (OB_FAIL(ObAggregateProcessor::llc_add_value(hash_value,
                                                            col_stat_->get_llc_bitmap(),
                                                            col_stat_->get_llc_bitmap_size()))) {
      LOG_WARN("fail to calc llc", K(ret));
    } else if (OB_FAIL(inner_merge_min(*datum, meta, cmp_func))) {
      LOG_WARN("failed to inner merge min val");
    } else if (OB_FAIL(inner_merge_max(*datum, meta, cmp_func))) {
      LOG_WARN("failed to inner merge max val");
    }
  }
  if (OB_SUCC(ret)) {
    col_stat_->calc_avg_len();
  }
  return ret;
}

int ObOptOSGColumnStat::calc_col_len(const ObDatum &datum, const ObObjMeta &meta, int64_t &col_len)
{
  int ret = OB_SUCCESS;
  if (!is_lob_storage(meta.get_type()) || datum.is_null()) {
    col_len = sizeof(datum) + (datum.is_null() ? 0 : datum.len_);
  } else {
    ObLobLocatorV2 locator(datum.get_string(), meta.has_lob_header());
    int64_t lob_data_byte_len = 0;
    if (OB_FAIL(locator.get_lob_data_byte_len(lob_data_byte_len))) {
      LOG_WARN("get lob data byte length failed", K(ret), K(locator));
    } else {
      col_len = sizeof(datum) + static_cast<int64_t>(lob_data_byte_len);
    }
  }
  return ret;
}

int ObOptOSGColumnStat::inner_merge_min(const ObDatum &datum, const ObObjMeta &meta, const ObDatumCmpFuncType cmp_func)
{
  int ret = OB_SUCCESS;
  if (meta.is_enum_or_set()) {
    //disable online gather enum/set max/min value. TODO,jiangxiu.wt
  } else if (min_val_.datum_ == NULL) {
    min_val_.datum_ = OB_NEWx(ObDatum, (&allocator_));
    if (OB_ISNULL(min_val_.datum_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory");
    } else if (OB_FAIL(min_val_.datum_->deep_copy(datum, inner_min_allocator_))) {
      LOG_WARN("failed to deep copy datum");
    } else {
      min_val_.meta_ = meta;
      min_val_.cmp_func_ = cmp_func;
    }
    LOG_TRACE("succeed to merge min datum", K(*min_val_.datum_), K(datum), K(meta));
  } else if (min_val_.datum_->is_null()) {
    inner_min_allocator_.reuse();
    if (OB_FAIL(min_val_.datum_->deep_copy(datum, inner_min_allocator_))) {
      LOG_WARN("failed to deep copy datum");
    }
    LOG_TRACE("succeed to merge min datum", K(*min_val_.datum_), K(datum), K(meta));
  } else {
    int cmp_ret = 0;
    if (OB_FAIL(min_val_.cmp_func_(*min_val_.datum_, datum, cmp_ret))) {
      LOG_WARN("failed to perform compare");
    } else if (cmp_ret > 0) {
      inner_min_allocator_.reuse();
      if (OB_FAIL(min_val_.datum_->deep_copy(datum, inner_min_allocator_))) {
        LOG_WARN("failed to deep copy datum");
      }
    }
    LOG_TRACE("succeed to merge min datum", K(cmp_ret), K(*min_val_.datum_), K(datum), K(meta));
  }
  return ret;
}

int ObOptOSGColumnStat::inner_merge_max(const ObDatum &datum, const ObObjMeta &meta, const ObDatumCmpFuncType cmp_func)
{
  int ret = OB_SUCCESS;
  if (meta.is_enum_or_set()) {
    //disable online gather enum/set max/min value. TODO,jiangxiu.wt
  } else if (max_val_.datum_ == NULL) {
    max_val_.datum_ = OB_NEWx(ObDatum, (&allocator_));
    if (OB_ISNULL(max_val_.datum_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory");
    } else if (OB_FAIL(max_val_.datum_->deep_copy(datum, inner_max_allocator_))) {
      LOG_WARN("failed to deep copy datum");
    } else {
      max_val_.meta_ = meta;
      max_val_.cmp_func_ = cmp_func;
    }
  } else if (max_val_.datum_->is_null()) {
    inner_max_allocator_.reuse();
    if (OB_FAIL(max_val_.datum_->deep_copy(datum, inner_max_allocator_))) {
      LOG_WARN("failed to deep copy datum");
    }
  } else {
    int cmp_ret = 0;
    if (OB_FAIL(max_val_.cmp_func_(*max_val_.datum_, datum, cmp_ret))) {
      LOG_WARN("failed to perform compare");
    } else if (cmp_ret < 0) {
      inner_max_allocator_.reuse();
      if (OB_FAIL(max_val_.datum_->deep_copy(datum, inner_max_allocator_))) {
        LOG_WARN("failed to deep copy datum");
      }
    }
    LOG_TRACE("succeed to merge max datum", K(cmp_ret), K(*max_val_.datum_), K(datum), K(meta));
  }
  return ret;
}

}
}