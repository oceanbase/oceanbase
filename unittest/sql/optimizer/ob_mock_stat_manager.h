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

#ifndef _OB_MOCK_STAT_MANAGER_H
#define _OB_MOCK_STAT_MANAGER_H 1
#include "sql/optimizer/ob_opt_default_stat.h"
#include "lib/timezone/ob_time_convert.h"
#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_column_schema.h"
namespace test
{
class MockTableStatService : public oceanbase::common::ObTableStatDataService
{
public:
  static const int64_t DEFAULT_ROW_COUNT = 100;
  static const int64_t DEFAULT_ROW_SIZE = 100;
  static const int64_t DEFAULT_DATA_SIZE = DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE;
  static const int64_t DEFAULT_MACROC_BLOCKS = DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE / DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t DEFAULT_MICRO_BLOCKS = DEFAULT_DATA_SIZE / DEFAULT_MICRO_BLOCK_SIZE;
  MockTableStatService() {
    reset_table_stat();
  }
  virtual ~MockTableStatService() {}
  //int get_table_stat(const ObPartitionKey &key, ObTableStat &tstat)
  //{
  //  UNUSED(key);
  //  tstat.reset();
  //  tstat= table_stat_;
  //  return OB_SUCCESS;
  //}


  void reset_table_stat()
  {
    table_stat_.set_row_count(DEFAULT_ROW_COUNT);
    table_stat_.set_data_size(DEFAULT_DATA_SIZE);
    table_stat_.set_macro_blocks_num(DEFAULT_MACROC_BLOCKS);
    table_stat_.set_micro_blocks_num(DEFAULT_MICRO_BLOCKS);
    table_stat_.set_average_row_size(DEFAULT_ROW_SIZE);
    table_stat_.set_data_version(1);
  }

  //void set_table_stat(ObTableStat &table_stat)
 // { table_stat_ = table_stat; }

  void set_table_row_count(int64_t row_count)
  { table_stat_.set_row_count(row_count); }

//private:
  //ObTableStat table_stat_;
};

class MockColumnStatService : public oceanbase::common::ObColumnStatDataService
{
public:
  MockColumnStatService() {}
  virtual ~MockColumnStatService() {}
  int get_column_stat(const ObColumnStat::Key &key, const bool force_new, ObColumnStatValueHandle &handle)
  {
    UNUSED(key);
    UNUSED(force_new);
    UNUSED(handle);
    return OB_SUCCESS;
  }

  int get_column_stat(const ObColumnStat::Key &key, const bool force_new, ObColumnStat &cstat, ObIAllocator &alloc)
  {
    UNUSED(key);
    UNUSED(force_new);
    UNUSED(cstat);
    UNUSED(alloc);
    return OB_SUCCESS;
  }

  int get_batch_stat(const uint64_t table_id,
                     const ObIArray<uint64_t> &partition_id,
                     const ObIArray<uint64_t> &column_ids,
                     ObIArray<ObColumnStatValueHandle> &handles)
  {
    UNUSED(table_id);
    UNUSED(partition_id);
    UNUSED(column_ids);
    UNUSED(handles);
    return OB_SUCCESS;
  }

  int get_batch_stat(const oceanbase::share::schema::ObMergeSchema &table_schema,
                     const uint64_t partition_id,
                     ObIArray<ObColumnStat *> &stats,
                     ObIAllocator &allocator)
  {
    UNUSED(table_schema);
    UNUSED(partition_id);
    UNUSED(stats);
    UNUSED(allocator);

    return OB_SUCCESS;
  }

  int update_column_stats(const ObIArray<ObColumnStat *> &column_stats)
  {
    UNUSED(column_stats);
    return OB_SUCCESS;
  }
  int update_column_stat(const ObColumnStat &cstat)
  {
    UNUSED(cstat);
    return OB_SUCCESS;
  }

  // int erase_column_stat(const ObPartitionKey &pkey, const int64_t column_id)
  // {
  //   UNUSEDx(pkey, column_id);
  //   return OB_SUCCESS;
  // }

};

class MockStatManager : public oceanbase::common::ObStatManager
{
  static const uint64_t DEFAULT_TABLE1_ID = 1099511677777;
  static const uint64_t DEFAULT_TABLE2_ID = 1099511677778;
  static const int64_t TIME_MAX_VALUE = TIME_MAX_VAL;
  static const int64_t DATE_MAX_VALUE = 2932896;
  static const int64_t DATE_MIN_VALUE = -354258;
public:
  struct ColumnStatInfo
  {
    ObObj min_;
    ObObj max_;
    int64_t distinct_num_;
    int64_t null_num_;
    void set_column_info(ObObj min, ObObj max, int64_t distinct_num, int64_t null_num)
    {
      min_ = min;
      max_ = max;
      distinct_num_ = distinct_num;
      null_num_ = null_num;
    }
  };
  MockStatManager() : schema_checker_(NULL)
  {
    ObObj min;
    min.set_int(0);
    ObObj max;
    max.set_int(20);
    int_info_.set_column_info(min, max, 10, 10);
    ObObj str_min;
    ObObj str_max;
    str_min.set_varchar("aa");
    str_min.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    str_max.set_varchar("az");
    str_max.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    str_info_.set_column_info(str_min, str_max, 80, 10);
    table1_row_count_ = 100;
    table2_row_count_ = 100;
    default_stat_ = false;
  }
  virtual ~MockStatManager() {}

  void set_schema_checker(oceanbase::sql::ObSchemaChecker *schema_checker)
  { schema_checker_ = schema_checker; }

  void set_default_stat(bool is_default)
  {
    default_stat_ = is_default;
  }

  // int get_table_stat(const ObPartitionKey &pkey, ObTableStat &tstat)
  // {
  //   int ret = OB_SUCCESS;
  //   if (default_stat_) {
  //     tstat.set_row_count(0);
  //     tstat.set_data_version(1);
  //   } else if (DEFAULT_TABLE2_ID == pkey.get_table_id()) { // for opt_sel
  //     tstat.set_row_count(table2_row_count_);
  //     tstat.set_data_size(DEFAULT_ROW_SIZE * tstat.get_row_count());
  //     tstat.set_data_version(1);
  //   } else {
  //     tstat.set_row_count(table1_row_count_); //table1 or default
  //     tstat.set_data_size(DEFAULT_ROW_SIZE * tstat.get_row_count());
  //     tstat.set_data_version(1);
  //   }
  //   return ret;
  // }

  int get_column_stat(uint64_t table_id,
                      uint64_t partition_id,
                      uint64_t column_id,
                      ObColumnStatValueHandle &handle)
  {
    int ret = oceanbase::common::OB_SUCCESS;
    UNUSED(partition_id);
    UNUSED(handle);
    const oceanbase::share::schema::ObColumnSchemaV2 *col_schema = NULL;
    ObObj min;
    ObObj max;
    ObColumnStat *column_stat = (ObColumnStat *)allocator_.alloc(sizeof(ObColumnStat));
    char *llc_bitmap = (char *)allocator_.alloc(oceanbase::common::ObOptColumnStat::NUM_LLC_BUCKET);
    MEMSET(llc_bitmap, 1, 96);
    MEMSET(llc_bitmap + 96, 0, oceanbase::common::ObOptColumnStat::NUM_LLC_BUCKET - 96);
    if (NULL == column_stat) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to allocate column stat", K(ret));
    } else if (OB_FAIL(column_stats_.push_back(column_stat))) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "failed to allocate column stat", K(ret));
    } else {
      column_stat = new (column_stat) ObColumnStat();
      column_stat->set_table_id(table_id);
      column_stat->set_partition_id(partition_id);
      column_stat->set_column_id(column_id);
      column_stat->set_num_distinct(int_info_.distinct_num_);
      column_stat->set_num_null(int_info_.null_num_);
      column_stat->set_llc_bitmap(llc_bitmap, oceanbase::common::ObOptColumnStat::NUM_LLC_BUCKET);
      column_stat->set_version(1);
    }
    if (OB_SUCC(ret) && NULL != schema_checker_) {
      if (OB_FAIL(schema_checker_->get_column_schema(
                  table_id, column_id, col_schema, true))) {
        COMMON_LOG(WARN, "Failed to get column schema", K(ret));
      } else if (NULL == col_schema) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "Col schema is NULL", K(ret));
      } else if (default_stat_) {
        column_stat->set_num_distinct(0);
        column_stat->set_num_null(0);
        min.set_min_value();
        max.set_max_value();
        MEMSET(llc_bitmap, 0, oceanbase::common::ObOptColumnStat::NUM_LLC_BUCKET);
      } else if (oceanbase::common::ObStringTC == col_schema->get_meta_type().get_type_class()) {
        column_stat->set_num_distinct(str_info_.distinct_num_);
        column_stat->set_num_null(str_info_.null_num_);
        min = str_info_.min_; // todo(yeti): special use, need deep copy
        max = str_info_.max_;
      } else if (col_schema->get_meta_type().get_type() >= ObDateTimeType
                 && col_schema->get_meta_type().get_type() <= ObYearType) {
        switch (col_schema->get_meta_type().get_type()) {
        case ObDateTimeType: {
            min.set_datetime(DATETIME_MIN_VAL);
            max.set_datetime(DATETIME_MAX_VAL);
            break;
          }
        case ObTimestampType: {
            min.set_timestamp(DATETIME_MIN_VAL);
            max.set_timestamp(DATETIME_MAX_VAL);
            break;
          }
        case ObDateType: {
            min.set_date(DATE_MIN_VALUE);
            ObObj xx ;
            xx.set_varchar("9999-12-31");
            max.set_date(DATE_MAX_VALUE);
            break;
          }
        case ObTimeType: {
            min.set_time(ObTimeConverter::ZERO_TIME);
            max.set_time(TIME_MAX_VALUE);
            break;
          }
        case ObYearType: {
            uint8_t ymin = 0;
            ObTimeConverter::int_to_year(1970, ymin);
            min.set_year(ymin);
            uint8_t ymax = 0;
            ObTimeConverter::int_to_year(2155, ymax);
            max.set_year(ymax);
            break;
          }
        default: break;
        }
      } else {//分更多种类型,default
        min = int_info_.min_;
        max = int_info_.max_;
      }
    }
    column_stat->set_min_value(min);
    column_stat->set_max_value(max);
    handle.cache_value_ = column_stat;
    return ret;
  }

  int get_batch_stat(const uint64_t table_id,
                     const ObIArray<uint64_t> &partition_ids,
                     const ObIArray<uint64_t> &column_ids,
                     ObIArray<ObColumnStatValueHandle> &handles) override
  {
    int ret = OB_SUCCESS;
    ObColumnStatValueHandle handle;
    column_stats_.reuse();
    allocator_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < column_ids.count(); ++j) {
        if (OB_FAIL(get_column_stat(table_id, partition_ids.at(i), column_ids.at(j), handle))) {
          COMMON_LOG(WARN, "failed to get single column stat", K(ret));
        } else if (OB_FAIL(handles.push_back(handle))) {
          COMMON_LOG(WARN, "failed to push back column stat", K(ret));
        }
      }
    }
    return ret;
  }

  int get_batch_stat(const oceanbase::share::schema::ObTableSchema &table_schema,
                     const uint64_t partition_id,
                     ObIArray<ObColumnStat *> &stats,
                     ObIAllocator &allocator)
  {
    UNUSED(table_schema);
    UNUSED(partition_id);
    UNUSED(stats);
    UNUSED(allocator);
    return OB_SUCCESS;
  }

  void set_column_stat(int64_t distinct, ObObj min, ObObj max, int64_t null_num)
  {
    if (ObStringTC == min.get_type_class()) {
      str_info_.set_column_info(min, max, distinct, null_num);
    } else {
      int_info_.set_column_info(min, max, distinct, null_num);
    }
  }

  void set_table_stat(int64_t table1_row_count, int64_t table2_row_count)
  {
    table1_row_count_ = table1_row_count;
    table2_row_count_ = table2_row_count;
  }

private:
  oceanbase::sql::ObSchemaChecker *schema_checker_;
  ObArenaAllocator allocator_;
  ObSEArray<ObColumnStat*, 8> column_stats_;
  int64_t table1_row_count_;
  int64_t table2_row_count_;
  ColumnStatInfo int_info_;
  ColumnStatInfo str_info_;
  bool default_stat_;
};

} // end namespace test

#endif /* _OB_MOCK_STAT_MANAGER_H */

