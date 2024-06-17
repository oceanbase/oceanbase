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

#ifndef OB_STAT_ITEM_H
#define OB_STAT_ITEM_H
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_opt_column_stat.h"
namespace oceanbase {

namespace common {

class ObOptTableStat;
class ObTopkItem;

/**
 * @brief The ObStatItem class
 * Different type of statistics items during gather stats
 * Describe how to collect the stat item and how to decode the result
 */
class ObStatItem
{
public:
  ObStatItem() {}
  virtual ~ObStatItem() {}
  virtual bool is_needed() const { return true; }
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
  {
    UNUSED(buf);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_IMPLEMENT;
  }
  virtual int decode(ObObj &obj)
  {
    UNUSED(obj);
    return OB_NOT_IMPLEMENT;
  }
  virtual int decode(ObObj &obj, ObIAllocator &allocator)
  {
    UNUSED(allocator);
    return decode(obj);
  }

  TO_STRING_KV(K(is_needed()));

  int cast_int(const ObObj &obj, int64_t &ret_value);
};

/**
 * @brief The ObStatTabItem class
 *  Table-Level Stat item
 */
class ObStatTabItem : public ObStatItem
{
public:
  ObStatTabItem() : tab_stat_(NULL) {}
  ObStatTabItem(ObOptTableStat *stat) :
    tab_stat_(stat)
  {}

protected:
  ObOptTableStat *tab_stat_;
};

class ObStatRowCount : public ObStatTabItem
{
public:
  ObStatRowCount() {}
  ObStatRowCount(ObOptTableStat *stat) :
    ObStatTabItem(stat)
  {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj) override;
};

class ObStatAvgRowLen : public ObStatTabItem
{
public:
  ObStatAvgRowLen() : col_stats_(NULL) {}
  ObStatAvgRowLen(ObOptTableStat *stat,
                  ObIArray<ObOptColumnStat*> &col_stats) :
    ObStatTabItem(stat),
    col_stats_(&col_stats)
  {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj) override;

protected:
  ObIArray<ObOptColumnStat*> *col_stats_;
};

/**
 * @brief The ObStatColItem class
 *  Column-Level Stat Item
 */
class ObStatColItem : public ObStatItem
{
public:
  ObStatColItem() : col_param_(NULL), col_stat_(NULL) {}
  ObStatColItem(const ObColumnStatParam *param,
                ObOptColumnStat *stat) :
    col_param_(param), col_stat_(stat)
  {}
  virtual bool is_needed() const { return col_param_ != NULL && col_param_->need_basic_stat(); }
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual const char *get_fmt() const { return NULL; }
protected:
  const ObColumnStatParam *col_param_;
  ObOptColumnStat *col_stat_;
};

class ObStatMaxValue : public ObStatColItem
{
public:
  ObStatMaxValue() {}
  ObStatMaxValue(const ObColumnStatParam *param,
                ObOptColumnStat *stat) :
    ObStatColItem(param, stat)
  {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

class ObStatMinValue : public ObStatColItem
{
public:
  ObStatMinValue() {}
  ObStatMinValue(const ObColumnStatParam *param,
                 ObOptColumnStat *stat) :
    ObStatColItem(param, stat)
  {}

  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

class ObStatNumNull : public ObStatColItem
{
public:
  ObStatNumNull() : tab_stat_(NULL) {}
  ObStatNumNull(const ObColumnStatParam *param,
                ObOptTableStat *tab_stat,
                ObOptColumnStat *stat) :
    ObStatColItem(param, stat),
    tab_stat_(tab_stat)
  {}

  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " COUNT(\"%.*s\")" : " COUNT(`%.*s`)";
  }
  virtual int decode(ObObj &obj) override;
protected:
  ObOptTableStat *tab_stat_;
};

class ObStatNumDistinct : public ObStatColItem
{
public:
  ObStatNumDistinct() : need_approx_ndv_(true) {}
  ObStatNumDistinct(const ObColumnStatParam *param,
                    ObOptColumnStat *stat,
                    bool need_approx_ndv = true) :
    ObStatColItem(param, stat), need_approx_ndv_(need_approx_ndv)
  {}

  const char *get_fmt() const
  {
    if (need_approx_ndv_) {
      return lib::is_oracle_mode() ? " APPROX_COUNT_DISTINCT(\"%.*s\")"
                                     : " APPROX_COUNT_DISTINCT(`%.*s`)";
    } else {
      return lib::is_oracle_mode() ? " COUNT(DISTINCT \"%.*s\")"
                                     : " COUNT(DISTINCT `%.*s`)";
    }
  }
  virtual int decode(ObObj &obj) override;
private:
  bool need_approx_ndv_;
};

class ObStatAvgLen : public ObStatColItem
{
public:
  ObStatAvgLen() {}
  ObStatAvgLen(const ObColumnStatParam *param,
               ObOptColumnStat *stat) :
    ObStatColItem(param, stat)
  {}
  virtual bool is_needed() const { return col_param_ != NULL && col_param_->need_avg_len(); }
  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " SUM_OPNSIZE(\"%.*s\")/decode(COUNT(*),0,1,COUNT(*))" : " SUM_OPNSIZE(`%.*s`)/(case when COUNT(*) = 0 then 1 else COUNT(*) end)";
  }
  virtual int decode(ObObj &obj) override;
};

class ObStatLlcBitmap : public ObStatColItem
{
public:
  ObStatLlcBitmap() {}
  ObStatLlcBitmap(const ObColumnStatParam *param,
                  ObOptColumnStat *stat) :
    ObStatColItem(param, stat)
  {}

  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " APPROX_COUNT_DISTINCT_SYNOPSIS(\"%.*s\")"
                                   : " APPROX_COUNT_DISTINCT_SYNOPSIS(`%.*s`)";
  }
  virtual int decode(ObObj &obj) override;
};

class ObStatTopKHist : public ObStatColItem
{
public:
  ObStatTopKHist() : ObStatColItem(), tab_stat_(NULL), max_disuse_cnt_(0) {}
  ObStatTopKHist(const ObColumnStatParam *param,
                 ObOptTableStat *tab_stat,
                 ObOptColumnStat *stat,
                 int64_t max_disuse_cnt) :
    ObStatColItem(param, stat),
    tab_stat_(tab_stat),
    max_disuse_cnt_(max_disuse_cnt)
  {}

  static int build_histogram_from_topk_items(ObIAllocator &allocator,
                                             const ObIArray<ObTopkItem> &buckets,
                                             int64_t max_bucket_num,
                                             int64_t total_row_count,
                                             int64_t not_null_count,
                                             int64_t num_distinct,
                                             ObHistogram &histogram);

  static int try_build_topk_histogram(ObIAllocator &allocator,
                                      const ObIArray<ObHistBucket> &bkts,
                                      const int64_t max_bucket_num,
                                      const int64_t total_row_count,
                                      const int64_t not_null_count,
                                      const int64_t num_distinct,
                                      ObHistogram &histogram);

  // Let N: the total number of rows
  //     B: the bucket number of the histogram
  //
  // const double thresold = 1.0 / col_param.bucket_num_;
  // const double err_rate = 0.001;
  // const bucket_size = 256;
  virtual bool is_needed() const override;
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
protected:
  ObOptTableStat *tab_stat_;
  int64_t max_disuse_cnt_;
};

class ObPartitionId : public ObStatTabItem
{
  public:
  ObPartitionId() : calc_partition_id_str_(), partition_id_(common::OB_INVALID_ID) {}
  ObPartitionId(ObOptTableStat *stat,
                ObString &calc_part_id_str,
                int64_t partition_id) :
    ObStatTabItem(stat),
    calc_partition_id_str_(calc_part_id_str),
    partition_id_(partition_id)
  {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj) override;

  ObString calc_partition_id_str_;
  int64_t partition_id_;
};

class ObStatHybridHist : public ObStatColItem
{
public:
  ObStatHybridHist() {}
  ObStatHybridHist(const ObColumnStatParam *param,
                   ObOptColumnStat *stat) :
    ObStatColItem(param, stat)
  {}

  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

class ObGlobalTableStat
{
public:
  ObGlobalTableStat()
    : row_count_(0), row_size_(0), data_size_(0),
      macro_block_count_(0), micro_block_count_(0), part_cnt_(0), last_analyzed_(0),
      cg_macro_cnt_arr_(), cg_micro_cnt_arr_(), stat_locked_(false),
      sstable_row_cnt_(0), memtable_row_cnt_(0)
  {}

  void add(int64_t rc, int64_t rs, int64_t ds, int64_t mac, int64_t mic);
  int add(int64_t rc, int64_t rs, int64_t ds, int64_t mac, int64_t mic,
          ObIArray<int64_t> &cg_macro_arr, ObIArray<int64_t> &cg_micro_arr,
          int64_t scnt, int64_t mcnt);

  int64_t get_row_count() const;
  int64_t get_avg_row_size() const;
  int64_t get_avg_data_size() const;
  int64_t get_macro_block_count() const;
  int64_t get_micro_block_count() const;
  int64_t get_last_analyzed() const { return last_analyzed_; }
  const ObIArray<int64_t> &get_cg_macro_arr() const { return cg_macro_cnt_arr_; }
  const ObIArray<int64_t> &get_cg_micro_arr() const { return cg_micro_cnt_arr_; }
  void set_last_analyzed(int64_t last_analyzed) { last_analyzed_ = last_analyzed; }
  void set_stat_locked(bool locked) { stat_locked_ = locked; }
  bool get_stat_locked() const { return stat_locked_; }
  int64_t get_sstable_row_cnt() const { return sstable_row_cnt_; }
  int64_t get_memtable_row_cnt() const { return memtable_row_cnt_; }


  TO_STRING_KV(K(row_count_),
               K(row_size_),
               K(data_size_),
               K(macro_block_count_),
               K(micro_block_count_),
               K(part_cnt_),
               K(last_analyzed_),
               K(cg_macro_cnt_arr_),
               K(cg_micro_cnt_arr_),
               K(stat_locked_),
               K(sstable_row_cnt_),
               K(memtable_row_cnt_));

private:
  int64_t row_count_;
  int64_t row_size_;
  int64_t data_size_;
  int64_t macro_block_count_;
  int64_t micro_block_count_;
  int64_t part_cnt_;
  int64_t last_analyzed_;
  ObArray<int64_t> cg_macro_cnt_arr_;
  ObArray<int64_t> cg_micro_cnt_arr_;
  bool stat_locked_;
  int64_t sstable_row_cnt_;
  int64_t memtable_row_cnt_;
};

class ObGlobalNullEval
{
public:
  ObGlobalNullEval() : global_num_null_(0) {}

  void add(int64_t num_null)
  { global_num_null_ += num_null; }

  int64_t get() const
  { return global_num_null_; }
private:
  int64_t global_num_null_;
};

class ObGlobalNdvEval
{
  const int64_t NUM_LLC_BUCKET =  ObOptColumnStat::NUM_LLC_BUCKET;
public:
  ObGlobalNdvEval() : global_ndv_(0), part_cnt_(0) {
    MEMSET(global_llc_bitmap_, 0, ObOptColumnStat::NUM_LLC_BUCKET); }

  void add(int64_t ndv, const char *llc_bitmap);

  int64_t get() const;

  void get_llc_bitmap(char *llc_bitmap, const int64_t llc_bitmap_size) const;

  static double select_alpha_value(const int64_t num_bucket);
  static int64_t get_ndv_from_llc(const char *llc_bitmap);
  static void update_llc(char *dst_llc_bitmap, const char *src_llc_bitmap, bool force_update = false);

private:
  int64_t global_ndv_;
  int64_t part_cnt_;
  char global_llc_bitmap_[ObOptColumnStat::NUM_LLC_BUCKET];
};

class ObGlobalMaxEval
{
public:
  ObGlobalMaxEval() : global_max_() {
    global_max_.set_null();
  }

  void add(const ObObj &obj);

  bool is_valid() const { return !global_max_.is_null(); }

  const ObObj& get() const { return global_max_; }
private:
  ObObj global_max_;
};

class ObGlobalMinEval
{
public:
  ObGlobalMinEval() : global_min_() {
    global_min_.set_null();
  }

  void add(const ObObj &obj);

  bool is_valid() const { return !global_min_.is_null(); }

  const ObObj& get() const { return global_min_; }
private:
  ObObj global_min_;
};

class ObGlobalAvglenEval
{
public:
  ObGlobalAvglenEval() : global_avglen_(0), part_cnt_(0) {}

  void add(int64_t avg_len)
  { global_avglen_ += avg_len; ++part_cnt_; }

  int64_t get() const
  { return part_cnt_ > 0 ? global_avglen_ / part_cnt_ : 0; }

private:
  int64_t global_avglen_;
  int64_t part_cnt_;
};

class ObGlobalNotNullEval
{
public:
  ObGlobalNotNullEval() : global_num_not_null_(0) {}

  void add(int64_t num_not_null)
  { global_num_not_null_ += num_not_null; }

  int64_t get() const
  { return global_num_not_null_; }
private:
  int64_t global_num_not_null_;
};

struct ObGlobalColumnStat
{
  ObGlobalColumnStat() :
    min_val_(),
    max_val_(),
    null_val_(0),
    avglen_val_(0),
    ndv_val_(0),
    cg_macro_blk_cnt_(0),
    cg_micro_blk_cnt_(0),
    cg_skip_rate_(1.0)
  {
    min_val_.set_min_value();
    max_val_.set_max_value();
  }
  void add_cg_blk_cnt(int64_t cg_macro_blk_cnt, int64_t cg_micro_blk_cnt)
  {
    cg_macro_blk_cnt_ += cg_macro_blk_cnt;
    cg_micro_blk_cnt_ += cg_micro_blk_cnt;
  }
  TO_STRING_KV(K(min_val_),
               K(max_val_),
               K(null_val_),
               K(avglen_val_),
               K(ndv_val_),
               K(cg_macro_blk_cnt_),
               K(cg_micro_blk_cnt_),
               K(cg_skip_rate_));
  ObObj min_val_;
  ObObj max_val_;
  int64_t null_val_;
  int64_t avglen_val_;
  int64_t ndv_val_;
  int64_t cg_macro_blk_cnt_;
  int64_t cg_micro_blk_cnt_;
  double cg_skip_rate_;
};

template <class T>
static T *copy_stat_item(ObIAllocator &allocator, const T &src)
{
  T *ret = NULL;
  void *ptr = allocator.alloc(sizeof(T));
  if (NULL != ptr) {
    ret = new (ptr) T() ;
    *ret = src;
  }
  return ret;
}

}
}
#endif // OB_STAT_ITEM_H
