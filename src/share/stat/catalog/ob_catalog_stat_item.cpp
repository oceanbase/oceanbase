/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT
#include "share/stat/catalog/ob_catalog_stat_item.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

int ObCatalogStatRowCount::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " COUNT(*)"))) {
    LOG_WARN("failed to print buf row count expr", K(ret));
  }
  return ret;
}

int ObCatalogStatRowCount::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  if (OB_ISNULL(tab_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat entry is not properly given", K(ret));
  } else if (OB_FAIL(cast_int(obj, row_count))) {
    LOG_WARN("failed to get int value", K(ret));
  } else {
    tab_stat_->set_row_count(row_count);
  }
  return ret;
}

int ObCatalogStatAvgRowLen::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // dummy estimator
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " 1"))) {
    LOG_WARN("failed to print buf", K(ret));
  }
  return ret;
}

int ObCatalogStatAvgRowLen::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t avg_row_size = 0;
  UNUSED(obj);
  if (OB_ISNULL(tab_stat_) || OB_ISNULL(col_stats_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat and column stats are not set", K(ret), K(tab_stat_), K(col_stats_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_stats_->count(); ++i) {
    if (OB_ISNULL(col_stats_->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column stat is null", K(ret), K(i));
    } else {
      avg_row_size += col_stats_->at(i)->get_avg_length();
    }
  }
  if (OB_SUCC(ret)) {
    tab_stat_->set_avg_row_len(avg_row_size);
  }
  return ret;
}

int ObCatalogStatColItem::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_) || OB_ISNULL(get_fmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, get_fmt(),
                                col_param_->column_name_.length(),
                                col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print column expr", K(ret));
  }
  return ret;
}

int ObCatalogStatMaxValue::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                lib::is_oracle_mode() ? " MAX(\"%.*s\")" : " MAX(`%.*s`)",
                                col_param_->column_name_.length(),
                                col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print max(col) expr", K(ret));
  }
  return ret;
}

int ObCatalogStatMaxValue::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(obj, allocator))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else {
    col_stat_->set_max_value(obj);
  }
  return ret;
}

int ObCatalogStatMinValue::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                lib::is_oracle_mode() ? " MIN(\"%.*s\")" : " MIN(`%.*s`)",
                                col_param_->column_name_.length(),
                                col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print min(col) expr", K(ret));
  }
  return ret;
}

int ObCatalogStatMinValue::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(obj, allocator))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else {
    col_stat_->set_min_value(obj);
  }
  return ret;
}

int ObCatalogStatNumNull::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t num_not_null = 0;
  if (OB_ISNULL(col_stat_) || OB_ISNULL(tab_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column stat is not given", K(ret), K(col_stat_), K(tab_stat_));
  } else if (OB_FAIL(cast_int(obj, num_not_null))) {
    LOG_WARN("failed to get int value", K(ret));
  } else {
    col_stat_->set_num_not_null(num_not_null);
    col_stat_->set_num_null(tab_stat_->get_row_count() - num_not_null);
  }
  return ret;
}

int ObCatalogStatNumDistinct::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t num_distinct = 0;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(cast_int(obj, num_distinct))) {
    LOG_WARN("failed to get num distinct", K(ret));
  } else {
    col_stat_->set_num_distinct(num_distinct);
  }
  return ret;
}

int ObCatalogStatAvgLen::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, get_fmt(),
                                col_param_->column_name_.length(),
                                col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print avg len expr", K(ret));
  }
  return ret;
}

int ObCatalogStatAvgLen::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t avg_len = 0;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column stat is not given", K(ret));
  } else if (OB_FAIL(cast_int(obj, avg_len))) {
    LOG_WARN("failed to get average length", K(ret));
  } else {
    col_stat_->set_avg_length(avg_len);
  }
  return ret;
}

int ObCatalogPartitionValue::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!calc_partition_value_str_.empty()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
                                calc_partition_value_str_.length(),
                                calc_partition_value_str_.ptr()))) {
      LOG_WARN("failed to print partition value expr", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("calc_partition_value_str_ is empty", K(ret));
  }
  return ret;
}

int ObCatalogPartitionValue::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tab_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat is not set", K(ret), K(tab_stat_));
  } else {
    ObObj dest_obj;
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
    if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, obj, dest_obj))) {
      LOG_WARN("cast to varchar value failed", K(ret), K(obj));
    } else if (!dest_obj.is_null()) {
      ObString tmp_str;
      if (OB_FAIL(ob_write_string(allocator, dest_obj.get_string(), tmp_str))) {
        LOG_WARN("failed to write string", K(ret));
      } else {
        tab_stat_->set_partition_value(tmp_str);
      }
    }
  }
  return ret;
}

int ObCatalogStatLlcBitmap::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObString llc_bitmap_buf;
  if (OB_ISNULL(col_stat_) || OB_UNLIKELY(!obj.is_varchar() && !obj.is_varbinary())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(col_stat_), K(obj));
  } else if (OB_FAIL(obj.get_string(llc_bitmap_buf))) {
    LOG_WARN("failed to get varchar", K(ret));
  } else if (llc_bitmap_buf.length() > 0) {
    // Catalog table: allocate bitmap buffer and copy
    int64_t bitmap_size = llc_bitmap_buf.length();
    char *bitmap_buf = nullptr;
    if (OB_ISNULL(bitmap_buf = static_cast<char *>(allocator.alloc(bitmap_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for bitmap", K(ret), K(bitmap_size));
    } else {
      MEMCPY(bitmap_buf, llc_bitmap_buf.ptr(), bitmap_size);
      col_stat_->set_llc_bitmap(bitmap_buf, bitmap_size);
    }
  } else {
    col_stat_->set_llc_bitmap(nullptr, 0);
  }
  return ret;
}

// =================== ObCatalogStatTopKHist ===================

bool ObCatalogStatTopKHist::is_needed() const
{
  return NULL != col_param_ &&
         col_param_->need_basic_stat() &&
         col_param_->bucket_num_ > 1;
}

int ObCatalogStatTopKHist::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret), K(col_param_));
  } else {
    int64_t bkt_num = col_param_->bucket_num_;
    if (bkt_num < MIN_BUCKET_SIZE) {
      bkt_num = MIN_BUCKET_SIZE;
    } else if (bkt_num > MAX_BUCKET_SIZE) {
      ret = OB_ERR_INVALID_SIZE_SPECIFIED;
      LOG_WARN("get invalid argument, expected value in the range[1, 2048]",
               K(ret), K(bkt_num));
    }
    double err_rate = 1.0 / get_window_size(bkt_num);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  lib::is_oracle_mode()
                                    ? " TOP_K_FRE_HIST(%lf, \"%.*s\", %ld, %ld)"
                                    : " TOP_K_FRE_HIST(%lf, `%.*s`, %ld, %ld)",
                                  err_rate,
                                  col_param_->column_name_.length(),
                                  col_param_->column_name_.ptr(),
                                  col_param_->bucket_num_,
                                  max_disuse_cnt_))) {
        LOG_WARN("failed to print buf topk hist expr", K(ret));
      }
    }
  }
  return ret;
}

int ObCatalogStatTopKHist::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // TODO: histogram not yet supported for catalog tables
  UNUSED(obj);
  UNUSED(allocator);
  return ret;
}

// =================== ObCatalogStatHybridHist ===================

bool ObCatalogStatHybridHist::is_needed() const
{
  return col_param_ != NULL && col_param_->need_basic_stat();
}

int ObCatalogStatHybridHist::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret), K(col_param_));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     lib::is_oracle_mode()
                                       ? " HYBRID_HIST(\"%.*s\", %ld)"
                                       : " HYBRID_HIST(`%.*s`, %ld)",
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr(),
                                     col_param_->bucket_num_))) {
    LOG_WARN("failed to print buf", K(ret));
  }
  return ret;
}

int ObCatalogStatHybridHist::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  // TODO: histogram not yet supported for catalog tables
  UNUSED(obj);
  UNUSED(allocator);
  return ret;
}

} // namespace common
} // namespace oceanbase
