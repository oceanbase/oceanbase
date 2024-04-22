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
 *
 * Formatter
 */

#ifndef OCEANBASE_LIBOBCDC_FORMATTER_H__
#define OCEANBASE_LIBOBCDC_FORMATTER_H__

#ifndef OB_USE_DRCMSG
#include "ob_cdc_msg_convert.h"
#else
#include <drcmsg/binlogBuf.h>                       // binlogBuf
#endif

#include "lib/allocator/ob_allocator.h"             // ObIAllocator
#include "lib/thread/ob_multi_fixed_queue_thread.h" // ObMQThread
#include "storage/blocksstable/ob_datum_row.h"      // ObDmlRowFlag

#include "ob_log_binlog_record.h"                   // IBinlogRecord,  ObLogBR

#include "ob_log_part_trans_task.h"                 // ColValueList, PartTransTask, IStmtTask, DmlStmtTask
#include "ob_log_schema_cache_info.h"               // TableSchemaInfo
#include "ob_log_hbase_mode.h"                      // ObLogHbaseUtil
#include "ob_log_schema_getter.h"                   // DBSchemaInfo
#include "ob_log_work_mode.h"                       // WorkingMode

namespace oceanbase
{
namespace common
{
class ObString;
}

namespace datadict
{
class ObDictTableMeta;
}

namespace libobcdc
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
  virtual int push_single_task(IStmtTask *task, volatile bool &stop_flag) = 0;
  virtual int get_task_count(int64_t &br_count, int64_t &log_entry_task_count, int64_t &stmt_in_lob_merger_count) = 0;
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
class ObDictTenantInfoGuard;

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
  int push_single_task(IStmtTask *task, volatile bool &stop_flag);
  int get_task_count(
      int64_t &br_count,
      int64_t &log_entry_task_count,
      int64_t &stmt_in_lob_merger_count);
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
    binlogBuf *new_column_array_;
    binlogBuf *old_column_array_;

    common::ObString *new_columns_[common::OB_MAX_COLUMN_NUMBER];
    common::ObString *old_columns_[common::OB_MAX_COLUMN_NUMBER];
    common::ObString *orig_default_value_[common::OB_MAX_COLUMN_NUMBER];

    bool is_rowkey_[common::OB_MAX_COLUMN_NUMBER];
    bool is_changed_[common::OB_MAX_COLUMN_NUMBER];
    bool is_null_lob_columns_[common::OB_MAX_COLUMN_NUMBER];  // lob column value not recorded in log
    bool is_diff_[common::OB_MAX_COLUMN_NUMBER]; // is lob_diff column
    bool is_old_col_nop_[common::OB_MAX_COLUMN_NUMBER];       // old column that marked nop in log(most likely happened in minimal mode)

    // invoke before handle format stmt task
    // incase of usage of column_num but row doesn't contain valid column and column_num is not set
    void reset_column_num() { column_num_ = 0; };
    void reset();
    int init(const int64_t column_num, const bool contain_old_column);

    TO_STRING_KV(
        K_(column_num),
        K_(contain_old_column));
  };

private:
  typedef share::schema::ObSimpleTableSchemaV2 TableSchemaType;
  static const int64_t DATA_OP_TIMEOUT = 1 * 1000 * 1000;
  static const int64_t PRINT_LOG_INTERVAL = 10 * 1000 * 1000;

  int handle_dml_stmt_(
      DmlStmtTask &dml_stmt_task,
      RowValue *row_value,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);

  int handle_dml_stmt_with_online_schema_(
      DmlStmtTask &dml_stmt_task,
      RowValue &row_value,
      ObLogBR &br,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);

  int handle_dml_stmt_with_dict_schema_(
      DmlStmtTask &dml_stmt_task,
      RowValue &row_value,
      ObLogBR &br,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);

  template<class TABLE_SCHEMA>
  int check_table_need_ignore_(
      const TABLE_SCHEMA *table_schema,
      DmlStmtTask &dml_stmt_task,
      bool &need_ignore);

  template<class TABLE_SCHEMA>
  int format_row_(
      const TABLE_SCHEMA &table_schema,
      RowValue &row_value,
      DmlStmtTask &dml_stmt_task,
      ObLogBR &br,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);

  template<class TABLE_SCHEMA>
  void handle_non_full_columns_(
      DmlStmtTask &dml_stmt_task,
      const TABLE_SCHEMA &table_schema);
  int init_row_value_array_(const int64_t row_value_num);
  void destroy_row_value_array_();
  int set_meta_info_with_online_schema_(
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      const TableSchemaType *simple_table_schema,
      const DBSchemaInfo &db_schema_info,
      ObLogSchemaGuard &schema_guard,
      ObLogBR *br,
      volatile bool &stop_flag);

  int set_meta_info_with_data_dict_(
      const uint64_t tenant_id,
      const int64_t global_schema_version,
      datadict::ObDictTableMeta *simple_table_schema,
      const DBSchemaInfo &db_schema_info,
      ObLogBR *br,
      volatile bool &stop_flag);
  // 1. For LOB column(data is out of row storage), the actual data is consist of LobAuxMeta data.
  // 2. The callback process for table row(which contain LOB column that data is outrow) is performed,
  //    when all the data processing of the associated LobAuxMeta table is completed.
  //    The purpose is avoid deadlocks in Formatter due to concurrent processing and non-blocking.
  //
  // @param [out] cur_stmt_need_callback  true means need callback processing again.
  template<class TABLE_SCHEMA>
  int build_row_value_(
      const uint64_t tenant_id,
      RowValue *rv,
      DmlStmtTask *stmt_task,
      const TABLE_SCHEMA *simple_table_schema,
      int64_t &new_column_cnt,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);
  int handle_lob_ctx_cols_(
      const uint64_t tenant_id,
      const uint64_t aux_lob_meta_tid,
      DmlStmtTask &dml_stmt_task,
      ObLobDataOutRowCtxList *new_lob_ctx_cols,
      bool &cur_stmt_need_callback,
      volatile bool &stop_flag);

  int group_udt_column_values_(
      const ColumnSchemaInfo &column_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool is_new_value,
      DmlStmtTask &stmt_task,
      ObLobDataOutRowCtxList &ob_ctx_cols,
      ColValue &cv);

  template<class TABLE_SCHEMA>
  int fill_normal_cols_(
      DmlStmtTask &stmt_task,
      RowValue *rv,
      ColValueList &cv_list,
      ObLobDataOutRowCtxList &ob_ctx_cols,
      const TABLE_SCHEMA *simple_table_schema,
      const TableSchemaInfo &tb_schema_info,
      const ObTimeZoneInfoWrap *tz_info_wrap,
      const bool is_new_value);
  template<class TABLE_SCHEMA>
  int fill_rowkey_cols_(
      RowValue *rv,
      ColValueList &rowkey_cols,
      const TABLE_SCHEMA *simple_table_schema,
      const TableSchemaInfo &tb_schema_info);
  template<class TABLE_SCHEMA>
  int build_binlog_record_(
      ObLogBR *br,
      RowValue *rv,
      const int64_t new_column_cnt,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const TABLE_SCHEMA *simple_table_schema);
  // HBase mode put
  // 1. hbase table
  // 2. update type
  // 3. new value all columns, old value empty
  int is_hbase_mode_put_(
      const uint64_t table_id,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const int64_t column_number,
      const int64_t new_column_cnt,
      const bool contain_old_column,
      bool &is_hbase_mode_put);
  int set_src_category_(IBinlogRecord *br,
      RowValue *rv,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const bool is_hbase_mode_put);
  int format_dml_delete_(IBinlogRecord *binlog_record, const RowValue *row_value);
  int format_dml_insert_(IBinlogRecord *binlog_record, const RowValue *row_value);
  int format_dml_update_(IBinlogRecord *binlog_record, const RowValue *row_value);
  int format_dml_put_(IBinlogRecord *binlog_record, const RowValue *row_value);
  template<class TABLE_SCHEMA>
  int fill_orig_default_value_(
      RowValue *rv,
      const TABLE_SCHEMA *simple_table_schema,
      const TableSchemaInfo &tb_schema_info,
      common::ObIAllocator &allocator);

  int get_schema_with_online_schema_(
      const int64_t version,
      const uint64_t tenant_id,
      const uint64_t table_id,
      volatile bool &stop_flag,
      ObLogSchemaGuard &schema_guard,
      const TableSchemaType *&table_schema,
      DBSchemaInfo &db_schema_info);

  int get_schema_with_data_dict_(
      const int64_t version,
      const uint64_t tenant_id,
      const uint64_t table_id,
      volatile bool &stop_flag,
      ObDictTenantInfoGuard &dict_tenant_info_guard,
      datadict::ObDictTableMeta *&table_schema,
      DBSchemaInfo &db_schema_info);

  int get_tenant_compat_mode_with_data_dict_(
      const uint64_t tenant_id,
      ObDictTenantInfoGuard &dict_tenant_info_guard,
      lib::Worker::CompatMode &compat_mode,
      volatile bool &stop_flag);

  int finish_format_(PartTransTask &part_trans_task,
      ObLogEntryTask &redo_log_entry_task,
      volatile bool &stop_flag);
  int init_binlog_record_for_dml_stmt_task_(DmlStmtTask *stmt_task,
      ObLogBR *&br,
      bool &is_ignore);

  int init_dml_unique_id_(DmlStmtTask &stmt_task,
      ObLogEntryTask &log_entry_task,
      PartTransTask &part_trans_task,
      common::ObString &dml_unique_id);

  int parse_aux_lob_meta_table_(
      DmlStmtTask &stmt_task);
  // Parse LobMeta table insert row
  int parse_aux_lob_meta_table_insert_(
      ObLogEntryTask &log_entry_task,
      DmlStmtTask &stmt_task,
      ColValueList &new_cols);
  // Parse LobMeta table delete row
  int parse_aux_lob_meta_table_delete_(
      ObLogEntryTask &log_entry_task,
      DmlStmtTask &stmt_task,
      ColValueList &old_cols);

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
  int64_t                    stmt_in_lob_merger_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogFormatter);
};

} // namespace libobcdc
} // namespace oceanbase
#endif /* OCEANBASE_LIBOBCDC_FORMATTER_H__ */
