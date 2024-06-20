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

#include "observer/virtual_table/ob_all_virtual_load_data_stat.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace observer
{

ObAllVirtualLoadDataStat::ObAllVirtualLoadDataStat()
    : ObVirtualTableScannerIterator(),
      addr_(),
      all_job_status_op_()
{
}

ObAllVirtualLoadDataStat::~ObAllVirtualLoadDataStat()
{
  reset();
}

void ObAllVirtualLoadDataStat::reset()
{
  addr_.reset();
  all_job_status_op_.reset();

  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualLoadDataStat::inner_open()
{
  int ret = OB_SUCCESS;
  sql::ObGlobalLoadDataStatMap *job_status_map = sql::ObGlobalLoadDataStatMap::getInstance();
  if (OB_FAIL(job_status_map->get_all_job_status(all_job_status_op_))) {
    SERVER_LOG(WARN, "fail to get all job status", K(ret));
  }
  return ret;
}

int ObAllVirtualLoadDataStat::inner_close()
{
  int ret = OB_SUCCESS;
  all_job_status_op_.reset();
  return ret;
}

int ObAllVirtualLoadDataStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  sql::ObLoadDataStat *job_status = nullptr;
  if (OB_FAIL(all_job_status_op_.get_next_job_status(job_status))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      SERVER_LOG(WARN, "fail to get next job status", KR(ret));
    }
  } else {
    ObObj *cells = cur_row_.cells_;
    const int64_t col_count = output_column_ids_.count();

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const uint64_t col_id = output_column_ids_.at(i);

      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(job_status->tenant_id_);
          break;
        }
        case SVR_IP: {
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(addr_.get_port());
          break;
        }
        case JOB_ID: {
          cells[i].set_int(job_status->job_id_);
          break;
        }
        case JOB_TYPE: {
          cells[i].set_varchar(job_status->job_type_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TABLE_NAME: {
          cells[i].set_varchar(job_status->table_name_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case FILE_PATH: {
          cells[i].set_varchar(job_status->file_path_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case TABLE_COLUMN: {
          cells[i].set_int(job_status->table_column_);
          break;
        }
        case FILE_COLUMN: {
          cells[i].set_int(job_status->file_column_); 
          break;
        }
        case BATCH_SIZE: {
          cells[i].set_int(job_status->batch_size_);
          break;
        }
        case PARALLEL: {
          cells[i].set_int(job_status->parallel_);
          break;
        }
        case LOAD_MODE: {
          cells[i].set_int(job_status->load_mode_);
          break;
        }
        case LOAD_TIME: {//当前导入数据已经花费的秒数
          int64_t current_time = common::ObTimeUtility::current_time();
          cells[i].set_int((current_time - job_status->start_time_) / 1000000L);
          break;
        }
        case ESTIMATED_REMAINING_TIME: {
          int64_t estimated_remaining_time = calc_remaining_time(*job_status);
          cells[i].set_int(estimated_remaining_time);
          break;
        }
        case TOTAL_BYTES: {
          cells[i].set_int(job_status->total_bytes_);
          break;
        }
        case READ_BYTES: {
          cells[i].set_int(job_status->read_bytes_);
          break;
        }
        case PARSED_BYTES: {
          cells[i].set_int(job_status->parsed_bytes_);
          break;
        }
        case PARSED_ROWS: {
          cells[i].set_int(job_status->parsed_rows_);
          break;
        }
        case TOTAL_SHUFFLE_TASK: {
          cells[i].set_int(job_status->total_shuffle_task_);
          break;
        }
        case TOTAL_INSERT_TASK: {
          cells[i].set_int(job_status->total_insert_task_);
          break;
        }
        case SHUFFLE_RT_SUM: {
          cells[i].set_int(job_status->shuffle_rt_sum_);
          break;
        }
        case INSERT_RT_SUM: {
          cells[i].set_int(job_status->insert_rt_sum_);
          break;
        }
        case TOTAL_WAIT_SECS: {
          cells[i].set_int(job_status->total_wait_secs_);
          break;
        }
        case MAX_ALLOWED_ERROR_ROWS: {
          cells[i].set_int(job_status->max_allowed_error_rows_);
          break;
        }
        case DETECTED_ERROR_ROWS: {
          cells[i].set_int(job_status->detected_error_rows_);
          break;
        }
        case COORDINATOR_RECEIVED_ROWS: {
          cells[i].set_int(job_status->coordinator_.received_rows_);
          break;
        }
        case COORDINATOR_LAST_COMMIT_SEGMENT_ID: {
          cells[i].set_int(job_status->coordinator_.last_commit_segment_id_);
          break;
        }
        case COORDINATOR_STATUS: {
          cells[i].set_varchar(job_status->coordinator_.status_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case COORDINATOR_TRANS_STATUS: {
          cells[i].set_varchar(job_status->coordinator_.trans_status_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STORE_PROCESSED_ROWS: {
          cells[i].set_int(job_status->store_.processed_rows_);
          break;
        }
        case STORE_LAST_COMMIT_SEGMENT_ID: {
          cells[i].set_int(job_status->store_.last_commit_segment_id_);
          break;
        }
        case STORE_STATUS: {
          cells[i].set_varchar(job_status->store_.status_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STORE_TRANS_STATUS: {
          cells[i].set_varchar(job_status->store_.trans_status_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MESSAGE: {
          int64_t pos = 0;
          if (OB_FAIL(databuff_printf(job_status->message_, common::MAX_LOAD_DATA_MESSAGE_LENGTH, pos,
                                      "COMPACT_STAGE_LOAD_ROWS: %ld, COMPACT_STAGE_DUMP_ROWS: %ld, "
                                      "COMPACT_STAGE_PRODUCT_TMP_FILES: %ld, COMPACT_STAGE_CONSUME_TMP_FILES: %ld, "
                                      "COMPACT_STAGE_MERGE_WRITE_ROWS: %ld, MERGE_STAGE_WRITE_ROWS: %ld",
                                      job_status->store_.compact_stage_load_rows_,
                                      job_status->store_.compact_stage_dump_rows_,
                                      job_status->store_.compact_stage_product_tmp_files_,
                                      job_status->store_.compact_stage_consume_tmp_files_,
                                      job_status->store_.compact_stage_merge_write_rows_,
                                      job_status->store_.merge_stage_write_rows_))) {
            SERVER_LOG(WARN, "fail to fill message_", K(ret));
          } else {
            cells[i].set_varchar(job_status->message_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

int64_t ObAllVirtualLoadDataStat::calc_remaining_time(
    sql::ObLoadDataStat &job_status) const
{
  int64_t current_time = common::ObTimeUtility::current_time();
  int64_t load_time = (current_time - job_status.start_time_) / 1000000L;
  int64_t remaining_time = 0;
  // in load data local infile, the total_bytes_ is 0 or -1
  if ((load_time > 0)
      && (job_status.parsed_bytes_ > 0)
      && (job_status.total_bytes_ > 0)) {
    const double min_speed = 1.0; // bytes per second
    double speed = (double)job_status.parsed_bytes_ / load_time;
    if (OB_LIKELY(speed >= min_speed)) {
      int64_t remain_bytes = job_status.total_bytes_ - job_status.parsed_bytes_;
      remaining_time = (int64_t)(remain_bytes / speed);
      if (remaining_time < 0) {
        remaining_time = INT64_MAX;
      }
    } else {
      remaining_time = INT64_MAX;
    }
  }
  return remaining_time;
}

} // namespace observer
} // namespace oceanbase
