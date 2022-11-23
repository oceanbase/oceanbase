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

#ifndef _OB_OPT_MOCK_STAT_MANAGER_H
#define _OB_OPT_MOCK_STAT_MANAGER_H
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_stat_service.h"
#include "share/stat/ob_opt_table_stat.h"

#include "sql/resolver/ob_schema_checker.h"

#include "sql/optimizer/ob_opt_default_stat.h"
#include "lib/timezone/ob_time_convert.h"

namespace test
{
// class MockOptTableStatService : public oceanbase::common::ObOptTableStatService
// {
// public:
//   static const int64_t DEFAULT_ROW_COUNT = 100;
//   static const int64_t DEFAULT_SSTABLE_ROW_COUNT = 50;
//   static const int64_t DEFAULT_MEMTABLE_ROW_COUNT = 50;
//   static const int64_t DEFAULT_ROW_SIZE = 100;
//   static const int64_t DEFAULT_DATA_SIZE = DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE;
//   static const int64_t DEFAULT_MACROC_BLOCKS = DEFAULT_ROW_COUNT * DEFAULT_ROW_SIZE / DEFAULT_MACRO_BLOCK_SIZE;
//   static const int64_t DEFAULT_MICRO_BLOCKS = DEFAULT_DATA_SIZE / DEFAULT_MICRO_BLOCK_SIZE;
//   MockOptTableStatService() {
//     reset_table_stat();
//   }
//   virtual ~MockOptTableStatService() {}
// 
//   void reset_table_stat()
//   {
//     stat_.set_data_size(DEFAULT_DATA_SIZE);
//     stat_.set_macro_block_num(DEFAULT_MACROC_BLOCKS);
//     stat_.set_memtable_avg_row_size(DEFAULT_ROW_SIZE);
//     stat_.set_memtable_row_count(DEFAULT_MEMTABLE_ROW_COUNT);
//     stat_.set_micro_block_num(DEFAULT_MICRO_BLOCKS);
//     stat_.set_sstable_avg_row_size(DEFAULT_ROW_SIZE);
//     stat_.set_sstable_row_count(DEFAULT_SSTABLE_ROW_COUNT);
//   }
// 
//   void set_table_stat(ObOptTableStat &table_stat)
//   { stat_ = table_stat; }
// 
//   void set_table_row_count(int64_t row_count)
//   {
//     stat_.set_memtable_row_count(row_count);
//     stat_.set_sstable_row_count(0);
//   }
// private:
//   ObOptTableStat stat_;
// };
// 
// class MockOptColumnStatService : public oceanbase::common::ObOptColumnStatService
// {
// public:
//   MockOptColumnStatService() {}
//   virtual ~MockOptColumnStatService() {}
//   int get_column_stat(const ObColumnStat::Key &key, const bool force_new, ObColumnStatValueHandle &handle)
//   {
//     UNUSED(key);
//     UNUSED(force_new);
//     UNUSED(handle);
//     return OB_SUCCESS;
//   }
// 
//   int get_column_stat(const ObColumnStat::Key &key, const bool force_new, ObColumnStat &cstat, ObIAllocator &alloc)
//   {
//     UNUSED(key);
//     UNUSED(force_new);
//     UNUSED(cstat);
//     UNUSED(alloc);
//     return OB_SUCCESS;
//   }
// 
//   int get_batch_stat(const oceanbase::share::schema::ObTableSchema &table_schema,
//                      const uint64_t partition_id,
//                      ObIArray<ObColumnStat *> &stats,
//                      ObIAllocator &allocator)
//   {
//     UNUSED(table_schema);
//     UNUSED(partition_id);
//     UNUSED(stats);
//     UNUSED(allocator);
// 
//     return OB_SUCCESS;
//   }
// 
//   int update_column_stats(const ObIArray<ObColumnStat *> &column_stats)
//   {
//     UNUSED(column_stats);
//     return OB_SUCCESS;
//   }
//   int update_column_stat(const ObColumnStat &cstat)
//   {
//     UNUSED(cstat);
//     return OB_SUCCESS;
//   }
// 
//   int refresh_column_stat(const int64_t data_version)
//   {
//     UNUSED(data_version);
//     return OB_SUCCESS;
//   }
// 
// };
// 
// class MockOptStatManager : public oceanbase::common::ObOptStatManager
// {
//   static const uint64_t DEFAULT_TABLE1_ID = 1099511677777;
//   static const uint64_t DEFAULT_TABLE2_ID = 1099511677778;
//   static const int64_t TIME_MAX_VALUE = (3020399 * 1000000LL);
//   static const int64_t DATE_MAX_VALUE = 2932896;
//   static const int64_t DATE_MIN_VALUE = -354258;;
// public:
//   struct ColumnStatInfo
//   {
//     ObObj min_;
//     ObObj max_;
//     int64_t distinct_num_;
//     int64_t null_num_;
//     void set_column_info(ObObj min, ObObj max, int64_t distinct_num, int64_t null_num)
//     {
//       min_ = min;
//       max_ = max;
//       distinct_num_ = distinct_num;
//       null_num_ = null_num;
//     }
//   };
//   MockOptStatManager() : schema_checker_(NULL)
//   {
//     column_stat_.set_num_distinct(10);
//     ObObj min;
//     min.set_int(0);
//     ObObj max;
//     max.set_int(20);
//     int_info_.set_column_info(min, max, 10, 10);
//     ObObj str_min;
//     ObObj str_max;
//     str_min.set_varchar("aa");
//     str_max.set_varchar("az");
//     str_info_.set_column_info(str_min, str_max, 80, 10);
//     table1_row_count_ = 100;
//     table2_row_count_ = 100;
//     default_stat_ = false;
//   }
//   ~MockOptStatManager() {}
// 
//   void set_schema_checker(oceanbase::sql::ObSchemaChecker *schema_checker)
//   { schema_checker_ = schema_checker; }
// 
//   void set_default_stat(bool is_default)
//   {
//     default_stat_ = is_default;
//   }
// 
//   int get_table_stat(const ObPartitionKey &pkey, ObOptTableStat &tstat)
//   {
//     int ret = OB_SUCCESS;
//     if (default_stat_) {
//       tstat.set_memtable_row_count(0);
//       tstat.set_sstable_row_count(0);
//     } else if (DEFAULT_TABLE2_ID == pkey.get_table_id()) { // for opt_sel
//       tstat.set_memtable_row_count(table2_row_count_);
//       tstat.set_sstable_row_count(0);
//       tstat.set_data_size(DEFAULT_ROW_SIZE * tstat.get_row_count());
//     } else {
//       tstat.set_memtable_row_count(table1_row_count_);
//       tstat.set_sstable_row_count(0);
//       tstat.set_data_size(DEFAULT_ROW_SIZE * tstat.get_row_count());
//     }
//     return ret;
//   }
// 
//   virtual int get_column_stat(const uint64_t table_id,
//                               const uint64_t partition_id,
//                               const uint64_t column_id,
//                               ObOptColumnStatHandle &handle)
//   {
//     int ret = oceanbase::common::OB_SUCCESS;
//     UNUSED(partition_id);
//     UNUSED(handle);
//     const oceanbase::share::schema::ObColumnSchemaV2 *col_schema = NULL;
//     ObObj min;
//     ObObj max;
//     column_stat_.set_num_distinct(int_info_.distinct_num_);
//     column_stat_.set_num_null(int_info_.null_num_);
//     if (NULL != schema_checker_) {
//       if (OB_FAIL(schema_checker_->get_column_schema(
//                   table_id, column_id, col_schema, true))) {
//         COMMON_LOG(WARN, "Failed to get column schema", K(ret));
//       } else if (NULL == col_schema) {
//         ret = OB_ERR_UNEXPECTED;
//         COMMON_LOG(WARN, "Col schema is NULL", K(ret));
//       } else if (oceanbase::common::ObStringTC == col_schema->get_meta_type().get_type_class()) {
//         column_stat_.set_num_distinct(str_info_.distinct_num_);
//         column_stat_.set_num_null(str_info_.null_num_);
//         min = str_info_.min_; // todo(yeti): special use, need deep copy
//         max = str_info_.max_;
//       } else if (col_schema->get_meta_type().get_type() >= ObDateTimeType
//                  && col_schema->get_meta_type().get_type() <= ObYearType) {
//         switch (col_schema->get_meta_type().get_type()) {
//         case ObDateTimeType: {
//             min.set_datetime(DATETIME_MIN_VAL);
//             max.set_datetime(DATETIME_MAX_VAL);
//             break;
//           }
//         case ObTimestampType: {
//             min.set_timestamp(DATETIME_MIN_VAL);
//             max.set_timestamp(DATETIME_MAX_VAL);
//             break;
//           }
//         case ObDateType: {
//             min.set_date(DATE_MIN_VALUE);
//             ObObj xx ;
//             xx.set_varchar("9999-12-31");
//             max.set_date(DATE_MAX_VALUE);
//             break;
//           }
//         case ObTimeType: {
//             min.set_time(ObTimeConverter::ZERO_TIME);
//             max.set_time(TIME_MAX_VALUE);
//             break;
//           }
//         case ObYearType: {
//             uint8_t ymin = 0;
//             ObTimeConverter::int_to_year(1970, ymin);
//             min.set_year(ymin);
//             uint8_t ymax = 0;
//             ObTimeConverter::int_to_year(2155, ymax);
//             max.set_year(ymax);
//             break;
//           }
//         default: break;
//         }
//       } else {//分更多种类型,default
//         min = int_info_.min_;
//         max = int_info_.max_;
//       }
//     }
//     column_stat_.set_min_value(min);
//     column_stat_.set_max_value(max);
//     column_stat_.set_histogram(hist_);
//     handle.stat_ = &column_stat_;
//     return ret;
//   }
// 
//   virtual int get_column_stat(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle)
//   {
//     UNUSED(key);
//     UNUSED(handle);
//     return OB_SUCCESS;
//   }
// 
//   int get_batch_stat(const oceanbase::share::schema::ObTableSchema &table_schema,
//                      const uint64_t partition_id,
//                      ObIArray<ObOptColumnStat *> &stats,
//                      ObIAllocator &allocator)
//   {
//     UNUSED(table_schema);
//     UNUSED(partition_id);
//     UNUSED(stats);
//     UNUSED(allocator);
//     return OB_SUCCESS;
//   }
// 
//   void set_column_stat(int64_t distinct, ObObj min, ObObj max, int64_t null_num)
//   {
//     if (ObStringTC == min.get_type_class()) {
//       str_info_.set_column_info(min, max, distinct, null_num);
//     } else {
//       int_info_.set_column_info(min, max, distinct, null_num);
//     }
//   }
// 
//   void set_table_stat(int64_t table1_row_count, int64_t table2_row_count)
//   {
//     table1_row_count_ = table1_row_count;
//     table2_row_count_ = table2_row_count;
//   }
// 
//   void set_histogram(ObHistogram *hist)
//   {
//     hist_ = hist;
//   }
// 
// private:
//   oceanbase::sql::ObSchemaChecker *schema_checker_;
//   ObOptColumnStat column_stat_;
//   ObHistogram *hist_;
//   int64_t table1_row_count_;
//   int64_t table2_row_count_;
//   ColumnStatInfo int_info_;
//   ColumnStatInfo str_info_;
//   bool default_stat_;
// };

class MockOptStatService : public oceanbase::common::ObOptStatService
{
public:
  static MockOptStatService &get_instance() 
  {
    static MockOptStatService instance_;
    return instance_;
  }

  MockOptStatService() {
    reset_table_stat();
  }
  virtual ~MockOptStatService() {}
  virtual int init(oceanbase::common::ObMySQLProxy *proxy, ObServerConfig *config)
  {
    UNUSED(proxy);
    UNUSED(config);
    return OB_SUCCESS;
  }

  virtual int get_table_stat(const ObOptTableStat::Key &key, ObOptTableStat &tstat) 
  {
    UNUSED(key);
    // UNUSED now;
    tstat = tstat_;
    return OB_SUCCESS;
  }
  virtual int get_column_stat(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle)
  {
    UNUSED(key);
    handle.stat_ = &cstat_;
    return OB_SUCCESS;
  }
  virtual int load_table_stat_and_put_cache(const ObOptTableStat::Key &key, ObOptTableStatHandle &handle)
  {
    UNUSED(key);
    UNUSED(handle);
    return OB_SUCCESS;
  }

  ObOptColumnStat &get_column_stat() { return cstat_; }
  ObHistogram &get_histogram() { return hist_; }
  void use_histogram() { cstat_.get_histogram() = hist_; }
  void no_use_histogram() { cstat_.get_histogram().set_type(ObHistType::INVALID_TYPE); }

  void reset_table_stat()
  {
    tstat_.set_row_count(1.0);
    tstat_.set_data_size(
          oceanbase::common::ObOptStatManager::get_default_data_size());
    tstat_.set_avg_row_size(
          oceanbase::common::ObOptStatManager::get_default_avg_row_size());
    tstat_.set_last_analyzed(1);
    cstat_.set_last_analyzed(1);
  }
private:
  ObOptTableStat tstat_;
  ObOptColumnStat cstat_;
  ObHistogram hist_;
};

class MockOptStatManager : public oceanbase::common::ObOptStatManager
{
 public:
  static MockOptStatManager &get_instance() 
  {
    static MockOptStatManager instance_;
    return instance_;
  }

  virtual int init(ObOptStatService *stat_service,
                   ObMySQLProxy *proxy,
                   ObServerConfig *config)
  {
    inited_ = true;
    UNUSED(stat_service);
    UNUSED(proxy);
    UNUSED(config);
    mock_stat_service_ = stat_service;
    if (NULL != mock_stat_service_) {
      mock_stat_service_->init(NULL, NULL);
    }
    return OB_SUCCESS;
  }

  // virtual int get_column_stat(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle)
  // {
  //   return mock_stat_service_->get_column_stat(key, handle);
  // }

  // virtual int get_table_stat(const ObOptTableStat::Key &key, ObOptTableStat &tstat)
  // {
  //   int ret = OB_SUCCESS;
  //   return mock_stat_service_->get_table_stat(key, tstat);
  //   return ret;
  // }

  // virtual int get_column_stat(const ObOptColumnStat::Key &key, ObOptColumnStatHandle &handle);
  virtual int update_column_stat(const oceanbase::common::ObIArray<ObOptColumnStat *> &column_stats) 
  {
    UNUSED(column_stats);
    return OB_SUCCESS;
  }
  virtual int delete_column_stat(const oceanbase::common::ObIArray<ObOptColumnStat *> &column_stats) 
  {
    UNUSED(column_stats);
    return OB_SUCCESS;
  }
  virtual int refresh_column_stat(const ObOptColumnStat::Key &key) 
  {
    UNUSED(key);
    return OB_SUCCESS;
  }
  virtual int refresh_table_stat(const ObOptTableStat::Key &key) 
  {
    UNUSED(key);
    return OB_SUCCESS;
  }

  // virtual int get_table_stat(const ObOptTableStat::Key &key, ObOptTableStat &tstat);
  virtual int add_refresh_stat_task(const oceanbase::obrpc::ObUpdateStatCacheArg &analyze_arg) 
  {
    UNUSED(analyze_arg);
    return OB_SUCCESS;
  }
  
  ObOptStatService *mock_stat_service_;
};

} // end namespace test

#endif /* _OB_MOCK_OPT_STAT_MANAGER_H */

