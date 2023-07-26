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
#include "share/stat/ob_stat_item.h"
#include "lib/utility/ob_print_utils.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_hybrid_hist_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"
namespace oceanbase
{
using namespace sql;
namespace common
{

int ObStatRowCount::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf( buf, buf_len, pos, " COUNT(1)"))) {
    LOG_WARN("failed to print buf row count expr", K(ret));
  }
  return ret;
}

int ObStatRowCount::decode(ObObj &obj)
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

int ObStatAvgRowLen::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  // dummy estimator
  if (OB_FAIL(databuff_printf(buf, buf_len, pos, " 1"))) {
    LOG_WARN("failed to print buf", K(ret));
  }
  return ret;
}

int ObStatAvgRowLen::decode(ObObj &obj)
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
      avg_row_size += col_stats_->at(i)->get_avg_len();
    }
  }
  if (OB_SUCC(ret)) {
    tab_stat_->set_avg_row_size(avg_row_size);
  }
  return ret;
}

int ObStatColItem::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_) || OB_ISNULL(get_fmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, get_fmt(),
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print max(col) expr", K(ret));
  }
  return ret;
}

int ObStatMaxValue::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
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

int ObStatMaxValue::decode(ObObj &obj)
{
  // print cstring and hex string here
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::shadow_truncate_string_for_opt_stats(obj))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else {
    col_stat_->set_max_value(obj);
  }
  return ret;
}

int ObStatMinValue::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     lib::is_oracle_mode() ? " MIN(\"%.*s\")" : " MIN(`%.*s`)",
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr()))) {
    LOG_WARN("failed to print max(col) expr", K(ret));
  }
  return ret;
}

int ObStatMinValue::decode(ObObj &obj)
{
  // print cstring and hex string here
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::shadow_truncate_string_for_opt_stats(obj))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else {
    col_stat_->set_min_value(obj);
  }
  return ret;
}

int ObStatNumNull::decode(ObObj &obj)
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

int ObStatNumDistinct::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t num_distinct = 0;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(col_stat_));
  } else if (OB_FAIL(cast_int(obj, num_distinct))) {
    LOG_WARN("failed to get num distinct", K(ret));
  } else {
    col_stat_->set_num_distinct(num_distinct);
  }
  return ret;
}

int ObStatAvgLen::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t avg_len = 0;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column stat is not given", K(ret));
  } else if (OB_FAIL(cast_int(obj, avg_len))) {
    LOG_WARN("failed to get average length", K(ret));
  } else {
    col_stat_->set_avg_len(avg_len);
  }
  return ret;
}

int ObStatLlcBitmap::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObString llc_bitmap_buf;
  if (OB_ISNULL(col_stat_) ||
      OB_ISNULL(col_stat_->get_llc_bitmap()) ||
      OB_UNLIKELY(!obj.is_varchar())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(col_stat_), K(obj));
  } else if (OB_FAIL(obj.get_varchar(llc_bitmap_buf))) {
    LOG_WARN("failed to get varchar", K(ret));
  } else if (OB_UNLIKELY(llc_bitmap_buf.length() > col_stat_->get_llc_bitmap_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(llc_bitmap_buf.length()),
                                     K(col_stat_->get_llc_bitmap_size()));
  } else {
    MEMCPY(col_stat_->get_llc_bitmap(), llc_bitmap_buf.ptr(), llc_bitmap_buf.length());
    col_stat_->set_llc_bitmap_size(llc_bitmap_buf.length());
  }
  return ret;
}

bool ObStatTopKHist::is_needed() const
{
  return NULL != col_param_ &&
         col_param_->need_basic_stat() &&
         col_param_->bucket_num_ > 1;
}

int ObStatTopKHist::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_BUCKET_SIZE = 256;
  const int64_t MAX_BUCKET_SIZE = 2048;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret), K(col_param_));
  } else {
    int64_t bkt_num = col_param_->bucket_num_;
    if (bkt_num < MIN_BUCKET_SIZE) {
      bkt_num = MIN_BUCKET_SIZE;
    } else if (bkt_num > MAX_BUCKET_SIZE) {
      ret = OB_ERR_INVALID_SIZE_SPECIFIED;
      LOG_WARN("get invalid argument, expected value in the range[1, 2048]", K(ret), K(bkt_num));
    }
    double err_rate = 1.0 / (1000 * (bkt_num / MIN_BUCKET_SIZE));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                  lib::is_oracle_mode() ? " TOP_K_FRE_HIST(%lf, \"%.*s\", %ld)" :
                                  " TOP_K_FRE_HIST(%lf, `%.*s`, %ld)",
                                  err_rate,
                                  col_param_->column_name_.length(),
                                  col_param_->column_name_.ptr(),
                                  col_param_->bucket_num_))) {
        LOG_WARN("failed to print buf topk hist expr", K(ret));
      }
    }
  }
  return ret;
}


struct ObBucketCompare
{
  ObBucketCompare()
  {
    ret_ = OB_SUCCESS;
  }

  bool operator()(const ObHistBucket &l,
                  const ObHistBucket &r)
  {
    bool bret = false;
    int &ret = ret_;
    if (OB_FAIL(ret)) {
      // already fail
    } else {
      bret = l.endpoint_value_.compare(r.endpoint_value_) < 0;
    }
    return bret;
  }

  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBucketCompare);
};

class CopyableBucketComparer
{
public:
  CopyableBucketComparer(ObBucketCompare &compare) : compare_(compare) {}
  bool operator()(const ObHistBucket &l, const ObHistBucket &r)
  {
    return compare_(l, r);
  }
  ObBucketCompare &compare_;
};

int ObStatTopKHist::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTopKFrequencyHistograms topk_hist;
  int64_t bucket_num = -1;
  if (OB_ISNULL(col_param_) || OB_ISNULL(tab_stat_) || OB_ISNULL(col_stat_) ||
      OB_UNLIKELY((bucket_num = col_param_->bucket_num_) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret), K(bucket_num), K(col_param_));
  } else if (OB_FAIL(topk_hist.read_result(obj))) {
    LOG_WARN("failed to read result from obj", K(ret));
  } else if (OB_FAIL(build_histogram_from_topk_items(allocator,
                                                     topk_hist.get_buckets(),
                                                     col_param_->bucket_num_,
                                                     tab_stat_->get_row_count(),
                                                     col_stat_->get_num_not_null(),
                                                     col_stat_->get_num_distinct(),
                                                     col_stat_->get_histogram()))) {
    LOG_WARN("failed to build topk histogram", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObStatTopKHist::build_histogram_from_topk_items(ObIAllocator &allocator,
                                                    const ObIArray<ObTopkItem> &buckets,
                                                    int64_t max_bucket_num,
                                                    int64_t total_row_count,
                                                    int64_t not_null_count,
                                                    int64_t num_distinct,
                                                    ObHistogram &histogram)
{
  int ret = OB_SUCCESS;
  ObArray<ObHistBucket> tmp;
  histogram.reset();
  int64_t num_cnt = std::min(buckets.count(), max_bucket_num);
  for (int64_t i = 0; OB_SUCC(ret) && i < num_cnt; ++i) {
    ObHistBucket bkt(buckets.at(i).col_obj_,
                     buckets.at(i).fre_times_,
                     buckets.at(i).fre_times_);
    if (OB_FAIL(tmp.push_back(bkt))) {
      LOG_WARN("failed to push back topk buckets", K(ret));
    }
  }
  if (OB_SUCC(ret) && tmp.count() > 0) {
    ObBucketCompare cmp;
    std::sort(&tmp.at(0),
              &tmp.at(0) + tmp.count(),
              CopyableBucketComparer(cmp));
    if (OB_FAIL(cmp.ret_)) {
      LOG_WARN("failed to sort histogram buckets", K(ret));
    }
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < tmp.count(); ++i) {
    tmp.at(i).endpoint_num_ += tmp.at(i - 1).endpoint_num_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(try_build_topk_histogram(allocator,
                                         tmp,
                                         max_bucket_num,
                                         total_row_count,
                                         not_null_count,
                                         num_distinct,
                                         histogram))) {
      LOG_WARN("failed to build topk histogram", K(ret));
    } else {
      LOG_TRACE("Succeed to build topk histogram", K(histogram), K(histogram.get_bucket_size()));
    }
  }
  return ret;
}

/**
 * @brief ObStatTopKHist::try_build_topk_histogram
 * @param bkts, an approximated topk histogram collected by a full table scan
 * @param max_bucket_num, the maximum bucket number specified by the user
 * @param total_row_count, the total row count estimated by a full table scan
 * @param num_distinct, the approximate number of distinct estimated by a full table scan
 * @param histogram, the result histogram built from bkts
 * @return
 */
int ObStatTopKHist::try_build_topk_histogram(ObIAllocator &allocator,
                                             const ObIArray<ObHistBucket> &bkts,
                                             const int64_t max_bucket_num,
                                             const int64_t total_row_count,
                                             const int64_t not_null_count,
                                             const int64_t num_distinct,
                                             ObHistogram &histogram)
{
  int ret = OB_SUCCESS;
  int64_t num = std::min(bkts.count(), max_bucket_num);
  LOG_TRACE("topk histogram info", K(bkts), K(max_bucket_num), K(total_row_count), K(not_null_count), K(num_distinct));
  if (OB_UNLIKELY(max_bucket_num <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket size", K(ret), K(max_bucket_num));
  } else if (not_null_count == 0) {
    // all vals are null, there is no need to build a histogram
    histogram.set_type(ObHistType::INVALID_TYPE);
    histogram.set_sample_size(0);
    histogram.set_bucket_cnt(0);
    histogram.set_density(0);
  } else if (num > 0 && bkts.at(num - 1).endpoint_num_ == not_null_count) {
    histogram.set_type(ObHistType::FREQUENCY);
    histogram.set_sample_size(not_null_count);
    histogram.set_bucket_cnt(bkts.count());
    histogram.calc_density(ObHistType::FREQUENCY,
                           not_null_count,
                           not_null_count,
                           num_distinct,
                           bkts.count());
    if (OB_FAIL(histogram.prepare_allocate_buckets(allocator, bkts.count()))) {
      LOG_WARN("failed to prepare allocate buckets", K(ret));
    } else if (OB_FAIL(histogram.assign_buckets(bkts))) {
      LOG_WARN("failed to assign buckets", K(ret));
    } else {/*do nothing*/}
  } else if (num > 0 && bkts.at(num - 1).endpoint_num_ >=
             (not_null_count * (1 - 1.0 / max_bucket_num))) {
    histogram.set_type(ObHistType::TOP_FREQUENCY);
    histogram.set_sample_size(not_null_count);
    histogram.calc_density(ObHistType::TOP_FREQUENCY,
                           not_null_count,
                           bkts.at(num - 1).endpoint_num_,
                           num_distinct,
                           num);
    if (OB_FAIL(histogram.prepare_allocate_buckets(allocator, num))) {
      LOG_WARN("failed to prepare allocate buckets", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < num; ++i) {
        ret = histogram.add_bucket(bkts.at(i));
      }
    }
  } else {
    histogram.set_type(ObHistType::HYBIRD);
    if (!bkts.empty() &&
        bkts.at(bkts.count() -1).endpoint_num_ >= not_null_count) {
      // if the topk histogram contains all records of the table
      // then we can build hybrid histogram directly from the topk result.
      histogram.set_sample_size(not_null_count);
      histogram.set_bucket_cnt(bkts.count());
      if (OB_FAIL(histogram.prepare_allocate_buckets(allocator, bkts.count()))) {
        LOG_WARN("failed to prepare allocate buckets", K(ret));
      } else if (OB_FAIL(histogram.assign_buckets(bkts))) {
        LOG_WARN("failed to assign buckets", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObPartitionId::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (!calc_partition_id_str_.empty()) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%.*s",
                                calc_partition_id_str_.length(),
                                calc_partition_id_str_.ptr()))) {
      LOG_WARN("failed to print max(col) expr", K(ret));
    } else {/*do nothing*/}
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%ld", partition_id_))) {
    LOG_WARN("failed to print buf", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObPartitionId::decode(ObObj &obj)
{
  int ret = OB_SUCCESS;
  int64_t partition_id = -1;
  if (OB_ISNULL(tab_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table stat and column stats are not set", K(ret), K(tab_stat_));
  } else if (OB_FAIL(cast_int(obj, partition_id))) {
    LOG_WARN("failed to get num distinct", K(ret));
  } else {
    tab_stat_->set_partition_id(partition_id);
  }
  return ret;
}

int ObStatItem::cast_int(const ObObj &obj, int64_t &ret_value)
{
  int ret = OB_SUCCESS;
  ObObj dest_obj;
  ObArenaAllocator calc_buffer(ObModIds::OB_BUFFER);
  ObCastCtx cast_ctx(&calc_buffer, NULL, CM_NONE, ObCharset::get_system_collation());
  if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, obj, dest_obj))) {
    LOG_WARN("cast to int value failed", K(ret), K(obj));
  } else if (dest_obj.is_null()) {
    /*do nothing*/
  } else if (OB_FAIL(dest_obj.get_int(ret_value))) {
    LOG_WARN("get int value failed", K(ret), K(dest_obj), K(obj));
  }
  return ret;
}

void ObGlobalTableStat::add(int64_t rc, int64_t rs, int64_t ds, int64_t mac, int64_t mic)
{
  // skip empty partition
  if (rc > 0) {
    row_count_ += rc;
    row_size_ += rs;
    data_size_ += ds;
    macro_block_count_ += mac;
    micro_block_count_ += mic;
    part_cnt_ ++;
  }
}

int64_t ObGlobalTableStat::get_row_count() const
{
  return row_count_;
}

int64_t ObGlobalTableStat::get_avg_row_size() const
{
  int64_t ret = row_count_ > 0 ? row_size_ / part_cnt_ : 0;
  return ret;
}

int64_t ObGlobalTableStat::get_avg_data_size() const
{
  int64_t ret = part_cnt_ > 0 ? data_size_ / part_cnt_ : 0;
  return ret;
}

int64_t ObGlobalTableStat::get_macro_block_count() const
{
  return macro_block_count_;
}

int64_t ObGlobalTableStat::get_micro_block_count() const
{
  return micro_block_count_;
}

void ObGlobalNdvEval::add(int64_t ndv, const char *llc_bitmap)
{
  if (llc_bitmap != NULL) {
    update_llc(global_llc_bitmap_, llc_bitmap, part_cnt_ == 0);
    ++ part_cnt_;
  }
  if (ndv > global_ndv_) {
    global_ndv_ = ndv;
  }
}

void ObGlobalNdvEval::update_llc(char *dst_llc_bitmap, const char *src_llc_bitmap, bool force_update)
{
  if (dst_llc_bitmap != NULL && src_llc_bitmap != NULL) {
    for (int64_t k = 0; k < ObOptColumnStat::NUM_LLC_BUCKET; ++k) {
      if (force_update ||
          static_cast<uint8_t>(src_llc_bitmap[k]) > static_cast<uint8_t>(dst_llc_bitmap[k])) {
        dst_llc_bitmap[k] = src_llc_bitmap[k];
      }
    }
  }
}

int64_t ObGlobalNdvEval::get() const
{
  int64_t num_distinct = 0;
  if (part_cnt_ <= 1) {
    num_distinct = global_ndv_;
  } else {
    num_distinct = get_ndv_from_llc(global_llc_bitmap_);
  }
  return num_distinct;
}

//splict the get function in to two, so get function should be used outside the NdvEval.
int64_t ObGlobalNdvEval::get_ndv_from_llc(const char *llc_bitmap)
{
  int64_t num_distinct = 0;
  if (OB_ISNULL(llc_bitmap)) {
    // ret is useless here, we just need to raise a warn to avoid core.
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "get unexpected null pointer");
  } else {
    double sum_of_pmax = 0;
    double alpha = select_alpha_value(ObOptColumnStat::NUM_LLC_BUCKET);
    int64_t empty_bucket_num = 0;
    for (int64_t i = 0; i < ObOptColumnStat::NUM_LLC_BUCKET; ++i) {
      sum_of_pmax += (1 / pow(2, (llc_bitmap[i])));
      if (llc_bitmap[i] == 0) {
        ++empty_bucket_num;
      }
    }
    double estimate_ndv = (alpha * ObOptColumnStat::NUM_LLC_BUCKET
                          * ObOptColumnStat::NUM_LLC_BUCKET)  / sum_of_pmax;
    num_distinct = static_cast<int64_t>(estimate_ndv);
    // check if estimate result too tiny or large.
    if (estimate_ndv <= 5 * ObOptColumnStat::NUM_LLC_BUCKET / 2) {
      if (0 != empty_bucket_num) {
        // use linear count
        num_distinct = static_cast<int64_t>(ObOptColumnStat::NUM_LLC_BUCKET
                                            * log(ObOptColumnStat::NUM_LLC_BUCKET / double(empty_bucket_num)));
      }
    }
    if (estimate_ndv > (static_cast<double>(ObOptColumnStat::LARGE_NDV_NUMBER) / 30)) {
      num_distinct = static_cast<int64_t>((0-pow(2, 32)) * log(1 - estimate_ndv / ObOptColumnStat::LARGE_NDV_NUMBER));
    }
  }

  return num_distinct;
}

double ObGlobalNdvEval::select_alpha_value(const int64_t num_bucket)
{
  double ret = 0.0;
  switch (num_bucket) {
  case 16:
    ret = 0.673;
    break;
  case 32:
    ret = 0.697;
    break;
  case 64:
    ret = 0.709;
    break;
  default:
    ret = 0.7213 / (1 + 1.079 / double(num_bucket));
    break;
  }
  return ret;
}

void ObGlobalNdvEval::get_llc_bitmap(char *llc_bitmap, const int64_t llc_bitmap_size) const
{
  if (llc_bitmap != NULL && llc_bitmap_size >= NUM_LLC_BUCKET) {
    MEMCPY(llc_bitmap, global_llc_bitmap_, NUM_LLC_BUCKET);
  }
}

void ObGlobalMaxEval::add(const ObObj &obj)
{
  if (global_max_.is_null()) {
    global_max_ = obj;
  } else if (obj > global_max_) {
    global_max_ = obj;
  }
}

void ObGlobalMinEval::add(const ObObj &obj)
{
  if (global_min_.is_null()) {
    global_min_ = obj;
  } else if (global_min_ > obj) {
    global_min_ = obj;
  }
}

int ObStatHybridHist::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column param is null", K(ret), K(col_param_));
  } else if (is_null_item_) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NULL"))) {
      LOG_WARN("failed to print buf", K(ret));
    } else {/*do nothing*/}
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     lib::is_oracle_mode() ? " HYBRID_HIST(\"%.*s\", %ld)" :
                                     " HYBRID_HIST(`%.*s`, %ld)",
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr(),
                                     col_param_->bucket_num_))) {
    LOG_WARN("failed to print buf", K(ret));
  }
  return ret;
}

int ObStatHybridHist::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObHybridHistograms hybrid_hist;
  ObArray<ObHistBucket> tmp;
  int64_t bucket_num = -1;
  if (OB_ISNULL(col_param_) || OB_ISNULL(col_stat_) ||
      OB_UNLIKELY((bucket_num = col_param_->bucket_num_) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret), K(bucket_num), K(col_param_), K(col_stat_));
  } else if (obj.is_null()) {
    col_stat_->get_histogram().reset();
  } else if (OB_FAIL(hybrid_hist.read_result(obj))) {
    LOG_WARN("failed to read result from obj", K(ret));
  } else {
    col_stat_->get_histogram().get_buckets().reset();
    col_stat_->get_histogram().set_bucket_cnt(hybrid_hist.get_buckets().count());
    if (hybrid_hist.get_buckets().empty()) {
      //do nothing, maybe the sample data is all null
    } else if (OB_FAIL(col_stat_->get_histogram().prepare_allocate_buckets(allocator, hybrid_hist.get_buckets().count()))) {
      LOG_WARN("failed to prepare allocate buckets", K(ret));
    } else if (OB_FAIL(col_stat_->get_histogram().assign_buckets(hybrid_hist.get_buckets()))) {
      LOG_WARN("failed to assign buckets", K(ret));
    } else {
      col_stat_->get_histogram().set_type(ObHistType::HYBIRD);
      col_stat_->get_histogram().set_sample_size(hybrid_hist.get_total_count());
      col_stat_->get_histogram().set_pop_frequency(hybrid_hist.get_pop_freq());
      col_stat_->get_histogram().set_pop_count(hybrid_hist.get_pop_count());
      LOG_TRACE("succeed to build hybrid hist", K(hybrid_hist), K(col_stat_->get_histogram()));
    }
  }
  return ret;
}

} // end of common
} // end of oceanbase
