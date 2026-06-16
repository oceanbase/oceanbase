/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_STAT_CATALOG_OB_CATALOG_STAT_ITEM_H
#define OCEANBASE_SHARE_STAT_CATALOG_OB_CATALOG_STAT_ITEM_H

#include "share/stat/ob_stat_item.h"
#include "share/stat/catalog/ob_opt_catalog_table_stat.h"
#include "share/stat/catalog/ob_opt_catalog_column_stat.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"

namespace oceanbase
{
namespace common
{

/**
 * @brief ObCatalogStatItem
 * Base class for catalog table statistics items
 * Inherits from ObStatItem to reuse gen_expr/decode framework
 * Uses ObOptCatalogTableStat and ObOptCatalogColumnStat instead of internal table types
 */
class ObCatalogStatItem : public ObStatItem
{
public:
  ObCatalogStatItem() {}
  virtual ~ObCatalogStatItem() {}
};

/**
 * @brief ObCatalogStatTabItem
 * Table-Level Stat item for catalog tables
 */
class ObCatalogStatTabItem : public ObCatalogStatItem
{
public:
  ObCatalogStatTabItem() : tab_stat_(NULL) {}
  ObCatalogStatTabItem(share::ObOptCatalogTableStat *stat) : tab_stat_(stat) {}

protected:
  share::ObOptCatalogTableStat *tab_stat_;
};

/**
 * @brief ObCatalogStatRowCount
 * Row count statistic for catalog tables
 */
class ObCatalogStatRowCount : public ObCatalogStatTabItem
{
public:
  ObCatalogStatRowCount() {}
  ObCatalogStatRowCount(share::ObOptCatalogTableStat *stat) : ObCatalogStatTabItem(stat) {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj) override;
};

/**
 * @brief ObCatalogStatAvgRowLen
 * Average row length statistic for catalog tables
 */
class ObCatalogStatAvgRowLen : public ObCatalogStatTabItem
{
public:
  ObCatalogStatAvgRowLen() : col_stats_(NULL) {}
  ObCatalogStatAvgRowLen(share::ObOptCatalogTableStat *stat,
                         ObIArray<share::ObOptCatalogColumnStat *> &col_stats)
      : ObCatalogStatTabItem(stat), col_stats_(&col_stats) {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj) override;

protected:
  ObIArray<share::ObOptCatalogColumnStat *> *col_stats_;
};

/**
 * @brief ObCatalogStatColItem
 * Column-Level Stat Item for catalog tables
 */
class ObCatalogStatColItem : public ObCatalogStatItem
{
public:
  ObCatalogStatColItem() : col_param_(NULL), col_stat_(NULL) {}
  ObCatalogStatColItem(const ObCatalogColumnStatParam *param,
                       share::ObOptCatalogColumnStat *stat)
      : col_param_(param), col_stat_(stat) {}
  virtual bool is_needed() const { return col_param_ != NULL && col_param_->need_basic_stat(); }
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual const char *get_fmt() const { return NULL; }

protected:
  const ObCatalogColumnStatParam *col_param_;
  share::ObOptCatalogColumnStat *col_stat_;
};

/**
 * @brief ObCatalogStatMaxValue
 * Maximum value statistic for catalog tables
 */
class ObCatalogStatMaxValue : public ObCatalogStatColItem
{
public:
  ObCatalogStatMaxValue() {}
  ObCatalogStatMaxValue(const ObCatalogColumnStatParam *param,
                        share::ObOptCatalogColumnStat *stat)
      : ObCatalogStatColItem(param, stat) {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

/**
 * @brief ObCatalogStatMinValue
 * Minimum value statistic for catalog tables
 */
class ObCatalogStatMinValue : public ObCatalogStatColItem
{
public:
  ObCatalogStatMinValue() {}
  ObCatalogStatMinValue(const ObCatalogColumnStatParam *param,
                        share::ObOptCatalogColumnStat *stat)
      : ObCatalogStatColItem(param, stat) {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

/**
 * @brief ObCatalogStatNumNull
 * NULL count statistic for catalog tables
 * Aligned with internal table's ObStatNumNull
 */
class ObCatalogStatNumNull : public ObCatalogStatColItem
{
public:
  ObCatalogStatNumNull() : tab_stat_(NULL) {}
  // Constructor aligned with internal table implementation
  // Parameters: (col_param, tab_stat, col_stat)
  ObCatalogStatNumNull(const ObCatalogColumnStatParam *col_param,
                       share::ObOptCatalogTableStat *tab_stat,
                       share::ObOptCatalogColumnStat *col_stat)
      : ObCatalogStatColItem(col_param, col_stat), tab_stat_(tab_stat) {}

  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " COUNT(\"%.*s\")" : " COUNT(`%.*s`)";
  }
  virtual int decode(ObObj &obj) override;

protected:
  share::ObOptCatalogTableStat *tab_stat_;
};

/**
 * @brief ObCatalogStatNumDistinct
 * Distinct count statistic for catalog tables
 */
class ObCatalogStatNumDistinct : public ObCatalogStatColItem
{
public:
  ObCatalogStatNumDistinct() : need_approx_ndv_(true) {}
  ObCatalogStatNumDistinct(const ObCatalogColumnStatParam *param,
                           share::ObOptCatalogColumnStat *stat,
                           bool need_approx_ndv = true)
      : ObCatalogStatColItem(param, stat), need_approx_ndv_(need_approx_ndv) {}

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

/**
 * @brief ObCatalogStatAvgLen
 * Average length statistic for catalog tables
 */
class ObCatalogStatAvgLen : public ObCatalogStatColItem
{
public:
  ObCatalogStatAvgLen() {}
  ObCatalogStatAvgLen(const ObCatalogColumnStatParam *param,
                      share::ObOptCatalogColumnStat *stat)
      : ObCatalogStatColItem(param, stat) {}
  virtual bool is_needed() const { return col_param_ != NULL && col_param_->need_avg_len(); }
  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " SUM_OPNSIZE(\"%.*s\")/decode(COUNT(*),0,1,COUNT(*))"
                                  : " SUM_OPNSIZE(`%.*s`)/(case when COUNT(*) = 0 then 1 else COUNT(*) end)";
  }
  virtual int decode(ObObj &obj) override;
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
};

/**
 * @brief ObCatalogStatLlcBitmap
 * LLC bitmap statistic for catalog tables
 */
class ObCatalogStatLlcBitmap : public ObCatalogStatColItem
{
public:
  ObCatalogStatLlcBitmap() {}
  ObCatalogStatLlcBitmap(const ObCatalogColumnStatParam *param,
                         share::ObOptCatalogColumnStat *stat)
      : ObCatalogStatColItem(param, stat) {}

  const char *get_fmt() const
  {
    return lib::is_oracle_mode() ? " APPROX_COUNT_DISTINCT_SYNOPSIS(\"%.*s\")"
                                  : " APPROX_COUNT_DISTINCT_SYNOPSIS(`%.*s`)";
  }
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

/**
 * @brief ObCatalogPartitionValue
 * Partition value statistic for catalog tables
 */
class ObCatalogPartitionValue : public ObCatalogStatTabItem
{
public:
  ObCatalogPartitionValue() : calc_partition_value_str_() {}
  ObCatalogPartitionValue(share::ObOptCatalogTableStat *stat,
                          ObString &calc_part_value_str) :
    ObCatalogStatTabItem(stat),
    calc_partition_value_str_(calc_part_value_str)
  {}
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;

  // Sql expression for partition value, e.g. "CONCAT(partition_col1, '/', partition_col2)"
  ObString calc_partition_value_str_;
};

/**
 * @brief ObCatalogStatTopKHist
 * TopK frequency histogram stat item for catalog tables.
 * Mirrors internal table ObStatTopKHist; histogram path not wired yet.
 */
class ObCatalogStatTopKHist : public ObCatalogStatColItem
{
  static const int64_t MIN_BUCKET_SIZE = 256;
  static const int64_t MAX_BUCKET_SIZE = 2048;
public:
  ObCatalogStatTopKHist()
      : ObCatalogStatColItem(), tab_stat_(NULL), max_disuse_cnt_(0)
  {
  }
  ObCatalogStatTopKHist(const ObCatalogColumnStatParam *param,
                        share::ObOptCatalogTableStat *tab_stat,
                        share::ObOptCatalogColumnStat *stat,
                        int64_t max_disuse_cnt)
      : ObCatalogStatColItem(param, stat),
        tab_stat_(tab_stat),
        max_disuse_cnt_(max_disuse_cnt)
  {
  }

  virtual bool is_needed() const override;
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;

  static int64_t get_window_size(int64_t bucket_num)
  {
    return 1000 * (bucket_num < MIN_BUCKET_SIZE ? 1 : bucket_num / MIN_BUCKET_SIZE);
  }

protected:
  share::ObOptCatalogTableStat *tab_stat_;
  int64_t max_disuse_cnt_;
};

/**
 * @brief ObCatalogStatHybridHist
 * Hybrid histogram stat item for catalog tables.
 * Mirrors internal table ObStatHybridHist; histogram path not wired yet.
 */
class ObCatalogStatHybridHist : public ObCatalogStatColItem
{
public:
  ObCatalogStatHybridHist() {}
  ObCatalogStatHybridHist(const ObCatalogColumnStatParam *param,
                          share::ObOptCatalogColumnStat *stat)
      : ObCatalogStatColItem(param, stat)
  {
  }

  virtual bool is_needed() const override;
  virtual int gen_expr(char *buf, const int64_t buf_len, int64_t &pos) override;
  virtual int decode(ObObj &obj, ObIAllocator &allocator) override;
};

// Catalog table specific copy function (外表专用，不复用内表的 copy_stat_item)
template <class T>
static T *copy_catalog_stat_item(ObIAllocator &allocator, const T &src)
{
  T *ret = NULL;
  void *ptr = allocator.alloc(sizeof(T));
  if (NULL != ptr) {
    ret = new (ptr) T();
    *ret = src;
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_CATALOG_OB_CATALOG_STAT_ITEM_H
