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

#ifndef OCEANBASE_LIBOBLOG_FORMATTER_H__
#define OCEANBASE_LIBOBLOG_FORMATTER_H__

#include "lib/allocator/ob_allocator.h"             // ObIAllocator
#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "storage/ob_i_store.h"                     // ObRowDml

#include "ob_log_binlog_record.h"                   // ILogRecord,  ObLogBR

#include "ob_log_part_trans_task.h"                 // ColValueList, PartTransTask, IStmtTask, DmlStmtTask
#include "ob_log_schema_cache_info.h"               // TableSchemaInfo
#include "ob_log_hbase_mode.h"                      // ObLogHbaseUtil
#include "ob_log_schema_getter.h"                   // DBSchemaInfo
#include "ob_log_work_mode.h"                       // WorkingMode

using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace common
{
class ObString;
}

namespace liboblog
{
/////////////////////////////////////////////////////////////////////////////////////////
// IObLogFormatter

class IObLogFormatter
{
public:
  enum
  {
    MAX_FORMATTER_NUM = 64,
    PAGE_SIZE = common::OB_MALLOC_NORMAL_BLOCK_SIZE,
    GET_SCHEMA_TIMEOUT = 1 * 1000 * 1000,
  };

public:
  virtual ~IObLogFormatter() {}

public:
  virtual int start() = 0;
  virtual void stop() = 0;
  virtual void mark_stop_flag() = 0;
  virtual int push(IStmtTask *task, volatile bool &stop_flag) = 0;
  virtual int get_task_count(int64_t &br_count, int64_t &log_entry_task_count) = 0;
};



/////////////////////////////////////////////////////////////////////////////////////////
// ObLogFormatter

class IObLogMetaManager;
class IObLogSchemaGetter;
class IObLogStorager;
class IObLogErrHandler;
class ObObj2strHelper;
class IObLogBRPool;
class ObLogSchemaGuard;

typedef common::ObMQThread<IObLogFormatter::MAX_FORMATTER_NUM, IObLogFormatter> FormatterThread;

class ObLogFormatter : public IObLogFormatter, public FormatterThread
{
public:
  ObLogFormatter();
  virtual ~ObLogFormatter();

public:
  int start();
  void stop();
  void mark_stop_flag() { FormatterThread::mark_stop_flag(); }
  int push(IStmtTask *task, volatile bool &stop_flag);
  int get_task_count(int64_t &br_count,
      int64_t &log_entry_task_count);
  int handle(void *data, const int64_t thread_index, volatile bool &stop_flag);

public:
  int init(const int64_t thread_num,
      const int64_t queue_size,
      const WorkingMode working_mode,
      ObObj2strHelper *obj2str_helper,
      IObLogBRPool *br_pool,
      IObLogMetaManager *meta_manager,
      IObLogSchemaGetter *schema_getter,
      IObLogStorager *storager,
      IObLogErrHandler *err_handler,
      const bool skip_dirty_data,
      const bool enable_hbase_mode,
      ObLogHbaseUtil &hbase_util,
      const bool skip_hbase_mode_put_column_count_not_consistency,
      const bool enable_output_hidden_primary_key);
  void destroy();

private:
  struct RowValue
  {
    int64_t column_num_;
    bool contain_old_column_;
    BinLogBuf *new_column_array_;
    BinLogBuf *old_column_array_;

    common::ObString *new_columns_[common::OB_MAX_COLUMN_NUMBER];
    common::ObString *old_columns_[common::OB_MAX_COLUMN_NUMBER];
    common::ObString *orig_default_value_[common::OB_MAX_COLUMN_NUMBER];

    bool is_rowkey_[common::OB_MAX_COLUMN_NUMBER];
    bool is_changed_[common::OB_MAX_COLUMN_NUMBER];

    void reset();
    int init(const int64_t column_num, const bool contain_old_column);
  };

private:
  typedef share::schema::ObSimpleTableSchemaV2 TableSchemaType;
  static const int64_t DATA_OP_TIMEOUT = 1 * 1000 * 1000;
  static const int64_t PRINT_LOG_INTERVAL = 10 * 1000 * 1000;

  void handle_non_full_columns_(DmlStmtTask &dml_stmt_task,
      const TableSchemaType &table_schema);
  int init_row_value_array_(const int64_t row_value_num);
  void destroy_row_value_array_();
  int set_meta_info_(ObLogSchemaGuard &schema_guard,
      const TableSchemaType *&simple_table_schema,
      const DBSchemaInfo &db_schema_info,
      ObLogBR *br,
      volatile bool &stop_flag);
  int build_row_value_(RowValue *rv,
      DmlStmtTask *stmt_task,
      const TableSchemaType *simple_table_schema,
      int64_t &new_column_cnt);
  int fill_normal_cols_(RowValue *rv,
      ColValueList &cv_list,
      const TableSchemaType *simple_table_schema,
      const TableSchemaInfo &tb_schema_info,
      const bool is_new_value);
  int fill_rowkey_cols_(RowValue *rv,
      ColValueList &rowkey_cols,
      const TableSchemaType *simple_table_schema,
      const TableSchemaInfo &tb_schema_info);
  int build_binlog_record_(ObLogBR *br,
      RowValue *rv,
      const int64_t new_column_cnt,
      const storage::ObRowDml &dml_type,
      const TableSchemaType *simple_table_schema);
  // HBase mode put
  // 1. hbase table
  // 2. update type
  // 3. new value all columns, old value empty
  int is_hbase_mode_put_(const uint64_t table_id,
      const storage::ObRowDml &dml_type,
      const int64_t column_number,
      const int64_t new_column_cnt,
      const bool contain_old_column,
      bool &is_hbase_mode_put);
  int set_src_category_(ILogRecord *br,
      RowValue *rv,
      const storage::ObRowDml &dml_type,
      const bool is_hbase_mode_put);
  int format_dml_delete_(ILogRecord *binlog_record, const RowValue *row_value);
  int format_dml_insert_(ILogRecord *binlog_record, const RowValue *row_value);
  int format_dml_update_(ILogRecord *binlog_record, const RowValue *row_value);
  int fill_orig_default_value_(RowValue *rv,
      const TableSchemaType *simple_table_schema,
      const TableSchemaInfo &tb_schema_info,
      common::ObIAllocator &allocator);

  int get_schema_(IObLogSchemaGetter *schema_getter,
      const int64_t version,
      const uint64_t table_id,
      volatile bool &stop_flag,
      ObLogSchemaGuard &schema_guard,
      const TableSchemaType *&table_schema,
      DBSchemaInfo &db_schema_info);
  int finish_format_(PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      volatile bool &stop_flag);
  int init_binlog_record_for_dml_stmt_task_(DmlStmtTask *stmt_task,
      ObLogBR *&br,
      bool &is_ignore);
  int handle_memory_data_sync_work_mode_(PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      volatile bool &stop_flag);
  int handle_storage_data_sync_work_mode_(PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      volatile bool &stop_flag);
  int dispatch_to_storager_(ObLogEntryTask &log_entry_task,
      volatile bool &stop_flag);

private:
  bool                      inited_;
  WorkingMode               working_mode_;
  ObObj2strHelper           *obj2str_helper_;
  IObLogBRPool              *br_pool_;
  IObLogErrHandler          *err_handler_;
  IObLogMetaManager         *meta_manager_;
  IObLogSchemaGetter        *schema_getter_;
  IObLogStorager            *storager_;
  RowValue                  *row_value_array_;
  common::ObArenaAllocator  allocator_;
  // Used to ensure that statement tasks are evenly distributed to Formatter threads
  uint64_t                   round_value_;
  bool                       skip_dirty_data_;
  bool                       enable_hbase_mode_;
  ObLogHbaseUtil             *hbase_util_;
  bool                       skip_hbase_mode_put_column_count_not_consistency_;
  bool                       enable_output_hidden_primary_key_;
  int64_t                    log_entry_task_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFormatter);
};

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBLOG_FORMATTER_H__ */
