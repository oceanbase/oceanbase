/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_br_filter.h"
#include "ob_log_miner_br.h"
#include "ob_log_miner_br_producer.h"
#include "ob_log_miner_br_converter.h"
#include "ob_log_miner_logger.h"

namespace oceanbase
{
namespace oblogminer
{

//////////////////////////////////// ColumnBRFilterPlugin ///////////////////////////////////

ColumnBRFilterPlugin::ColumnBRFilterPlugin(ObIAllocator *alloc):
    alloc_(alloc),
    multi_table_column_cond_(alloc_)
    { }

int ColumnBRFilterPlugin::init(const char *table_cond_str)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(multi_table_column_cond_.init(table_cond_str))) {
    LOG_ERROR("multi_table_cond failed to init", K(table_cond_str), K(alloc_));
  }

  return ret;
}

bool ColumnBRFilterPlugin::need_process_(const RecordType type)
{
  bool bret = false;
  switch (type) {
    case EINSERT:
    case EUPDATE:
    case EDELETE:
      bret = true;
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

bool ColumnBRFilterPlugin::is_table_match_(const ObLogMinerTableColumnCond& table_column_cond,
     const char *db_name,
     const char *table_name)
{
  return (table_column_cond.db_name_.is_empty() || 0 == strcmp(table_column_cond.db_name_.ptr(), db_name)) &&
         (table_column_cond.table_name_.is_empty() || 0 == strcmp(table_column_cond.table_name_.ptr(), table_name));
}

bool ColumnBRFilterPlugin::satisfy_column_cond_(
     const ObLogMinerColumnVals &col_cond,
     const binlogBuf *new_cols,
     const int64_t new_col_cnt,
     const binlogBuf *old_cols,
     const int64_t old_col_cnt,
     ITableMeta *tbl_meta)
{
  bool satisfy_new_col_cond_met = true;
  bool satisfy_old_col_cond_met = true;
  const int64_t col_cond_cnt = col_cond.count();
  ARRAY_FOREACH_X(col_cond, col_val_idx, col_cond_cnt,
      (satisfy_old_col_cond_met || satisfy_new_col_cond_met)) {
    const ObLogMinerColVal &col_val = col_cond[col_val_idx];
    int64_t column_idx = tbl_meta->getColIndex(col_val.col_.ptr());
    if (0 > column_idx) {
      satisfy_new_col_cond_met = false;
      satisfy_old_col_cond_met = false;
      LOG_TRACE("col in col_cond doesn't exist in the row", K(column_idx), K(col_val));
    } else {
      const char *new_data_ptr = nullptr, *old_data_ptr = nullptr;
      int64_t new_data_len = 0, old_data_len = 0;
      if (column_idx < new_col_cnt) {
        new_data_ptr = new_cols[column_idx].buf;
        new_data_len = new_cols[column_idx].buf_used_size;
      } else {
        // -1 indicates new_data don't exist
        new_data_len = -1;
      }
      if (column_idx < old_col_cnt) {
        old_data_ptr = old_cols[column_idx].buf;
        old_data_len = old_cols[column_idx].buf_used_size;
      } else {
        // -1 indicates old_data don't exist
        old_data_len = -1;
      }

      if (satisfy_new_col_cond_met) {
        satisfy_new_col_cond_met = is_data_match_column_val_(new_data_ptr, new_data_len, col_val);
      }
      if (satisfy_old_col_cond_met) {
        satisfy_old_col_cond_met = is_data_match_column_val_(old_data_ptr, old_data_len, col_val);
      }
    }
  }

  return satisfy_old_col_cond_met || satisfy_new_col_cond_met;
}

bool ColumnBRFilterPlugin::is_data_match_column_val_(const char *data_ptr,
     const int64_t data_len,
     const ObLogMinerColVal &col_val)
{
  bool bret = true;
  if (data_len != col_val.val_.length()) {
    bret = false;
    LOG_TRACE("the length of new_data is not same as the length of new col_val", K(col_val),
      K(data_ptr), K(data_len));
  } else if (nullptr == data_ptr) {
    if (col_val.is_null_) {
      bret = true;
      LOG_TRACE("data_ptr and the val in col_val are both null", K(col_val),
        K(data_ptr));
    } else {
      bret = false;
    }
  } else {
    if (0 != MEMCMP(data_ptr, col_val.val_.ptr(), min(data_len, col_val.val_.length()))) {
      bret = false;
      LOG_TRACE("new_data doesn't match", K(col_val),
        K(data_ptr));
    } else {
      bret = true;
      LOG_TRACE("new_data match successfully", K(col_val),
        K(data_ptr));
    }
  }
  return bret;
}

int ColumnBRFilterPlugin::filter(ObLogMinerBR &br, bool &need_filter)
{
  int ret = OB_SUCCESS;
  need_filter = false;
  ICDCRecord *cdc_br = br.get_br();
  if (OB_ISNULL(cdc_br)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get a null cdc binlog record, unexpected", K(cdc_br));
  } else if (!need_process_(static_cast<RecordType>(cdc_br->recordType()))) {
    need_filter = false;
    LOG_TRACE("doesn't filter some record needn't to be processed");
  } else {
    ITableMeta *tbl_meta = cdc_br->getTableMeta();
    if (OB_ISNULL(tbl_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null table meta when filter record", "recordType", cdc_br->recordType());
    } else {
      const char *tenant_db_name = cdc_br->dbname();
      const char *tbl_name = cdc_br->tbname();
      unsigned int new_col_cnt = 0, old_col_cnt = 0;
      binlogBuf *new_cols = cdc_br->newCols(new_col_cnt);
      binlogBuf *old_cols = cdc_br->oldCols(old_col_cnt);

      // expect tenant_db_name as tenant_name.db_name
      const char *db_name = nullptr;
      if (OB_ISNULL(tenant_db_name) || OB_ISNULL(tbl_name)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("dbname or tblname is null", K(tenant_db_name), K(tbl_name));
      } else if (0 == multi_table_column_cond_.table_column_conds_.count()) {
        need_filter = false;
      } else if (OB_ISNULL(db_name = STRCHR(tenant_db_name, '.') + 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected tenant_db_name format", K(tenant_db_name), K(tbl_name));
      } else {
        bool satisfy_one_cond = false;

        DbAndTableWrapper db_and_table_wrapper(db_name, tbl_name);
        ObLogMinerTableColumnCond *table_column_cond = nullptr;
        if (OB_FAIL(multi_table_column_cond_.table_column_conds_.get(db_and_table_wrapper, table_column_cond))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("get table column cond failed", K(db_and_table_wrapper));
          }
        } else if (nullptr != table_column_cond) {
          const int64_t column_conds_cnt = table_column_cond->column_conds_.count();
          ARRAY_FOREACH_X(table_column_cond->column_conds_, col_cond_idx, column_conds_cnt,
              OB_SUCC(ret) && ! satisfy_one_cond) {
            const ObLogMinerColumnVals &col_cond = table_column_cond->column_conds_[col_cond_idx];
            satisfy_one_cond = satisfy_column_cond_(col_cond, new_cols,
                new_col_cnt, old_cols, old_col_cnt, tbl_meta);
          } // ARRAY_FOREACH column_conds

          if (table_column_cond->column_conds_.empty()) {
            satisfy_one_cond = true;
          }
        }
        need_filter = !satisfy_one_cond;
      } // else get legal arguments
    }
  }

  return ret;
}

void ColumnBRFilterPlugin::destroy()
{
  multi_table_column_cond_.destroy();
}

//////////////////////////////////// OperationFilterPlugin ///////////////////////////////////
OperationBRFilterPlugin::OperationBRFilterPlugin():
    op_cond_() { }

int OperationBRFilterPlugin::init(const char *op_cond_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_cond_.init(op_cond_str))) {
    LOG_ERROR("op_cond init failed", K(op_cond_str));
  }
  return ret;
}

int OperationBRFilterPlugin::filter(ObLogMinerBR &br, bool &need_filter)
{
  int ret = OB_SUCCESS;
  ICDCRecord *cdc_br = br.get_br();
  need_filter = false;
  if (OB_ISNULL(cdc_br)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("operation filter get a null cdc br", K(cdc_br));
  } else {
    need_filter = ! op_cond_.is_record_type_match(br.get_record_type());
  }
  return ret;
}

void OperationBRFilterPlugin::destroy()
{
  op_cond_.reset();
}

//////////////////////////////////// BRFilter ///////////////////////////////////

const int64_t ObLogMinerBRFilter::BR_FILTER_THREAD_NUM = 1L;
const int64_t ObLogMinerBRFilter::BR_FILTER_QUEUE_SIZE = 100000L;

ObLogMinerBRFilter::ObLogMinerBRFilter():
    is_inited_(false),
    plugin_allocator("FilterPlugin"),
    filter_pipeline_(),
    data_manager_(nullptr),
    resource_collector_(nullptr),
    br_converter_(nullptr),
    err_handle_(nullptr) { }

ObLogMinerBRFilter::~ObLogMinerBRFilter()
{
  destroy();
}

int ObLogMinerBRFilter::init(const char *table_column_cond,
    const char *op_cond,
    ILogMinerDataManager *data_manager,
    ILogMinerResourceCollector *resource_collector,
    ILogMinerBRConverter *br_converter,
    ILogMinerErrorHandler *err_handle)
{
  int ret = OB_SUCCESS;
  ColumnBRFilterPlugin *column_plugin = nullptr;
  OperationBRFilterPlugin *op_plugin = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("oblogminer br_filter is already inited, no need to init again", K(is_inited_),
        K(table_column_cond), K(op_cond), K(data_manager), K(resource_collector), K(br_converter));
  } else if (OB_FAIL(BRFilterThreadPool::init(BR_FILTER_THREAD_NUM, BR_FILTER_QUEUE_SIZE))) {
    LOG_ERROR("BRFilterThreadPool failed to init");
  } else if (OB_ISNULL(column_plugin =
      static_cast<ColumnBRFilterPlugin*>(plugin_allocator.alloc(sizeof(ColumnBRFilterPlugin))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for table filter plugin failed", K(column_plugin));
  } else if (OB_ISNULL(op_plugin =
      static_cast<OperationBRFilterPlugin*>(plugin_allocator.alloc(sizeof(OperationBRFilterPlugin))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for operation filter plugin failed", K(op_plugin));
  } else {
    column_plugin = new (column_plugin) ColumnBRFilterPlugin(&plugin_allocator);
    op_plugin = new (op_plugin) OperationBRFilterPlugin();
    if (OB_FAIL(column_plugin->init(table_column_cond))) {
      LOG_ERROR("table plugin init failed", K(table_column_cond));
    } else if (OB_FAIL(op_plugin->init(op_cond))) {
      LOG_ERROR("op plugin init failed", K(op_cond));
    } else if (OB_FAIL(filter_pipeline_.push_back(op_plugin))) {
      LOG_ERROR("filter pipeline push back op_plugin failed", K(op_plugin), K(filter_pipeline_));
    } else if (OB_FAIL(filter_pipeline_.push_back(column_plugin))) {
      LOG_ERROR("filter pipeline push back column_plugin failed", K(column_plugin), K(filter_pipeline_));
    } else {
      data_manager_ = data_manager;
      resource_collector_ = resource_collector;
      br_converter_ = br_converter;
      err_handle_ = err_handle;
      is_inited_ = true;
      LOG_INFO("ObLogMinerBRFilter finished to init", K(table_column_cond), K(op_cond));
      LOGMINER_STDOUT_V("ObLogMinerBRFilter finished to init\n");
    }
  }
  return ret;
}

int ObLogMinerBRFilter::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRFilter hasn't been inited yet", K(is_inited_));
  } else if (OB_FAIL(BRFilterThreadPool::start())) {
    LOG_ERROR("BRFilterThreadPool failed to start");
  } else {
    LOG_INFO("ObLogMinerBRFilter starts successfully");
  }
  return ret;
}

void ObLogMinerBRFilter::stop()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    BRFilterThreadPool::mark_stop_flag();
    LOG_INFO("ObLogMinerBRFilter stopped");
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogMinerBRFilter is not init or has been stopped");
  }
}

void ObLogMinerBRFilter::wait()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    BRFilterThreadPool::stop();
    LOG_INFO("ObLogMinerBRFilter finished to wait");
  } else {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogMinerBRFilter is not init or has been stopped");
  }
}

void ObLogMinerBRFilter::destroy()
{
  if (IS_INIT) {
    filter_pipeline_.destroy();
    data_manager_ = nullptr;
    resource_collector_ = nullptr;
    br_converter_ = nullptr;
    plugin_allocator.clear();
    err_handle_ = nullptr;
    BRFilterThreadPool::destroy();
    is_inited_ = false;
    LOG_INFO("ObLogMinerBRFilter destroyed");
    LOGMINER_STDOUT_V("ObLogMinerBRFilter destroyed\n");
  }
}

int ObLogMinerBRFilter::push(ObLogMinerBR *logminer_br)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRFilter is not inited", K(this));
  } else if (OB_ISNULL(logminer_br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ObLogMiner get a null logminer br", K(logminer_br));
  } else {
    LOG_TRACE("BRFilter start to process logminer_br", KPC(logminer_br));
    uint64_t hash_val = logminer_br->get_seq_no(); // hash_val always equal to 0
    constexpr int64_t PUSH_TIMEOUT_TIME = 1L * 1000 * 1000; // 1 sec
    // TODO: caculate hash_val for multi-thread
    RETRY_FUNC(is_stoped(), *(static_cast<ObMQThread*>(this)), push, logminer_br, hash_val, PUSH_TIMEOUT_TIME);

    if (OB_FAIL(ret)) {
      LOG_ERROR("push logminer br into logminer brfilter failed", K(logminer_br), K(hash_val));
    }
  }
  return ret;
}

int ObLogMinerBRFilter::get_total_task_count(int64_t &task_count)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRFilter is not inited", K(this));
  } else if (OB_FAIL(BRFilterThreadPool::get_total_task_num(task_count))) {
    LOG_ERROR("failed to get total task num");
  }

  return ret;
}

int ObLogMinerBRFilter::handle(void *data, const int64_t thread_index, volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;
  ObLogMinerBR *logminer_br = static_cast<ObLogMinerBR*>(data);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogMinerBRFilter is not inited", K(is_inited_), K(data));
  } else if (OB_ISNULL(logminer_br)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("BRfilter got a null logminer br", K(logminer_br));
  } else {
    bool need_filter = false;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(filter_br_(*logminer_br, need_filter))) {
      LOG_ERROR("ObLogMinerBRFilter failed to filter br", K(logminer_br), K(need_filter));
    } else if (need_filter) {
      logminer_br->mark_filtered();
    } else {
      // succ
    }

    if (OB_SUCC(ret)) {
      if (logminer_br->is_filtered()) {
        // the RecordType of filtered br must be EINSERT, EUPDATE or EDELETE
        // records of the RecordType above don't impact the progress, i.e. analysis checkpoint.
        LOG_TRACE("br has been filtered", KPC(logminer_br));
        if (OB_FAIL(resource_collector_->revert(logminer_br))) {
          LOG_ERROR("failed to revert logminer br", KPC(logminer_br), K(resource_collector_));
        }
      } else {
        if (OB_FAIL(br_converter_->push(logminer_br))) {
          LOG_ERROR("failed to push logminer br into br converter", K(br_converter_), KPC(logminer_br));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    err_handle_->handle_error(ret, "ObLogMinerBRFilter exit unexpected\n");
  }
  return ret;
}

int ObLogMinerBRFilter::filter_br_(ObLogMinerBR &br, bool &need_filter)
{
  int ret = OB_SUCCESS;
  const int64_t filter_pipeline_size = filter_pipeline_.count();
  need_filter = false;
  ARRAY_FOREACH_X(filter_pipeline_, filter_idx, filter_pipeline_size, OB_SUCC(ret) && ! need_filter) {
    IBRFilterPlugin *filter_plugin = filter_pipeline_.at(filter_idx);
    if (OB_ISNULL(filter_plugin)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get a null filter plugin in filter pipeline", K(filter_plugin));
    } else if (OB_FAIL(filter_plugin->filter(br, need_filter))) {
      LOG_ERROR("filter_plugin failed to filter br", K(filter_plugin), K(need_filter));
    }
  }
  return ret;
}

}
}