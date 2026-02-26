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

#ifndef OB_LAKE_TABLE_STAT_H
#define OB_LAKE_TABLE_STAT_H
#include "share/stat/ob_stat_define.h"
#include "share/stat/ob_opt_column_stat.h"


namespace oceanbase {
namespace sql {
class ObIcebergFileDesc;
class ObColumnRefRawExpr;
}
namespace common {

struct ObLakeTableStat
{
public:
  ObLakeTableStat()
  : total_row_count_(0), pruned_row_count_(0),
    data_size_(0), part_cnt_(0), file_cnt_(0), last_analyzed_(0)
  {}
  TO_STRING_KV(K(total_row_count_),
               K(pruned_row_count_),
               K(data_size_),
               K(part_cnt_),
               K(file_cnt_),
               K(last_analyzed_));

  int64_t total_row_count_;
  int64_t pruned_row_count_;
  int64_t data_size_;
  int64_t part_cnt_;
  int64_t file_cnt_;
  int64_t last_analyzed_;
};

struct ObLakeColumnStat
{
  ObLakeColumnStat() :
    min_val_(),
    max_val_(),
    null_count_(0),
    size_(0),
    record_count_(0),
    last_analyzed_(0),
    num_distinct_(0)
  {
    min_val_.set_min_value();
    max_val_.set_max_value();
  }
  void reset();
  int merge(ObLakeColumnStat &other);
  TO_STRING_KV(K(min_val_),
               K(max_val_),
               K(null_count_),
               K(size_),
               K(record_count_),
               K(last_analyzed_),
               K(num_distinct_));
  ObObj min_val_;
  ObObj max_val_;
  int64_t null_count_;
  int64_t size_;
  int64_t record_count_;
  int64_t last_analyzed_;
  int64_t num_distinct_;
};

class ObLakeTableStatUtils
{
public:
  static int construct_stat_from_iceberg(const uint64_t tenant_id,
                                         const ObIArray<uint64_t> &column_ids,
                                         const ObIArray<sql::ObColumnRefRawExpr*> &column_exprs,
                                         ObIArray<sql::ObIcebergFileDesc*> &file_descs,
                                         ObLakeTableStat &table_stat,
                                         ObIArray<ObLakeColumnStat*> &column_stats,
                                         ObIAllocator *allocator);
  static int scale_column_stats(const int64_t row_cnt,
                                const double scale_ratio,
                                ObIArray<ObLakeColumnStat*> &column_stats);
  static int merge_iceberg_column_stats(const ObIArray<ObLakeColumnStat*> &common_column_stats,
                                        ObIArray<ObLakeColumnStat*> &file_column_stats);
  static int scale_table_stat(const double scale_ratio,
                              ObLakeTableStat &table_stat);
};

}
}
#endif // OB_STAT_ITEM_H
