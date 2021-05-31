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

#include "observer/virtual_table/ob_gv_partition_info.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
namespace oceanbase {
namespace observer {

ObGVPartitionInfo::ObGVPartitionInfo()
    : ObVirtualTableScannerIterator(), partition_service_(NULL), addr_(), pg_iter_(NULL)
{}

ObGVPartitionInfo::~ObGVPartitionInfo()
{
  reset();
}

void ObGVPartitionInfo::reset()
{
  if (NULL != pg_iter_) {
    if (NULL == partition_service_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      partition_service_->revert_pg_iter(pg_iter_);
      pg_iter_ = NULL;
    }
  }
  partition_service_ = NULL;
  addr_.reset();
  ip_buf_[0] = '\0';
  freeze_status_buf_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

int ObGVPartitionInfo::freeze_status_to_string(int64_t freeze_status, char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  switch (freeze_status) {
    case INIT_STATUS:
      if (NULL != buf && buf_len > 0) {
        if (0 > snprintf(buf, buf_len, "INIT_STATUS")) {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      break;
    case PREPARED_SUCCEED:
      if (NULL != buf && buf_len > 0) {
        if (0 > snprintf(buf, buf_len, "PREPARED_SUCCEED")) {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      break;
    case COMMIT_SUCCEED:
      if (NULL != buf && buf_len > 0) {
        if (0 > snprintf(buf, buf_len, "COMMIT_SUCCEED")) {
          ret = OB_ERR_UNEXPECTED;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObGVPartitionInfo::partition_state_to_string(int64_t partition_state, char* buf, int16_t buf_len)
{
  int ret = OB_SUCCESS;
  if (partition_state <= INVALID_STATE) {
    if (NULL != buf && buf_len > 0) {
      if (0 > snprintf(buf, buf_len, OB_PARTITION_STATE_STR[partition_state])) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }

  return ret;
}

int ObGVPartitionInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObSavedStorageInfoV2 info;

  if (NULL == allocator_ || NULL == partition_service_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "allocator_ or partition_service_ shouldn't be NULL", K(allocator_), K(partition_service_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (NULL == pg_iter_ && NULL == (pg_iter_ = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc partition iter", K(ret));
  } else if (OB_SUCCESS != (ret = pg_iter_->get_next(partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else if (NULL == partition) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get partition failed", K(ret));
  } else if (OB_FAIL(partition->get_all_saved_info(info))) {
    SERVER_LOG(WARN, "fail to get data info", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    const ObPGKey& pkey = partition->get_partition_key();
    ObVersion version;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            SERVER_LOG(WARN, "ip_to_string failed", K(addr_));
          }
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // tenant_id
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // table_id
          cur_row_.cells_[i].set_int(pkey.table_id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // parition_idx
          cur_row_.cells_[i].set_int(pkey.get_partition_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 5: {
          // max_decided_trans_version
          int64_t max_decided_trans_version = -1;
          if (OB_UNLIKELY(
                  OB_SUCCESS != (tmp_ret = partition->get_max_decided_trans_version(max_decided_trans_version)))) {
            // skip
          }
          cur_row_.cells_[i].set_int(max_decided_trans_version);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 6:
          // max_passed_trans_ts(Deprecated)
          cur_row_.cells_[i].set_int(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // freeze_ts
          cur_row_.cells_[i].set_int(partition->get_freeze_snapshot_ts());
          break;
        case OB_APP_MIN_COLUMN_ID + 8: {
          // allow_gc
          bool allow_gc = true;
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = partition->allow_gc(allow_gc)))) {
            SERVER_LOG(WARN, "failed to get allow_gc", K(tmp_ret), K(pkey));
          }
          cur_row_.cells_[i].set_bool(allow_gc);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 9: {
          if (OB_FAIL(partition_state_to_string(
                  partition->get_partition_state(), partition_state_buf_, sizeof(partition_state_buf_)))) {
            SERVER_LOG(WARN, "convert partition_state_to_string failed", K(ret));
          } else {
            cur_row_.cells_[i].set_varchar(partition_state_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 10:
          // ('sstable_read_count_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // ('sstable_read_bytes_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // ('sstable_write_count_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // ('sstable_write_bytes_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // ('log_write_count_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          // ('log_write_bytes_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          // ('memtable_bytes', 'int'),
          cur_row_.cells_[i].set_int(static_cast<int64_t>(0));
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          // ('cpu_utime_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          //('cpu_stime_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          // ('net_in_count_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          //('net_in_bytes_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          //('net_out_count_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 22:
          //('net_out_bytes_15_minute_rate', 'double'),
          cur_row_.cells_[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 23:
          cur_row_.cells_[i].set_int(partition->get_cur_min_log_service_ts());
          break;
        case OB_APP_MIN_COLUMN_ID + 24:
          cur_row_.cells_[i].set_int(partition->get_cur_min_trans_service_ts());
          break;
        case OB_APP_MIN_COLUMN_ID + 25:
          cur_row_.cells_[i].set_int(partition->get_cur_min_replay_engine_ts());
          break;
        case OB_APP_MIN_COLUMN_ID + 26:
          cur_row_.cells_[i].set_bool(partition->is_need_rebuild());
          break;
        case OB_APP_MIN_COLUMN_ID + 27: {
          // pg_key
          ObPartitionArray pkeys;
          int64_t pkey_cnt = -1;
          if (OB_SUCCESS != (tmp_ret = partition->get_all_pg_partition_keys(pkeys))) {
            SERVER_LOG(WARN, "get all pg partition keys error", K(ret));
          } else {
            pkey_cnt = pkeys.count();
          }
          cur_row_.cells_[i].set_int(pkey_cnt);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 28:
          // is_pg
          cur_row_.cells_[i].set_bool(partition->is_pg());
          break;
        case OB_APP_MIN_COLUMN_ID + 29: {
          // slave_wrs
          int64_t timestamp = 0;
          if (OB_FAIL(partition->get_weak_read_timestamp(timestamp))) {
            SERVER_LOG(WARN, "fail to get weak read timestamp", KR(ret));
          } else {
            cur_row_.cells_[i].set_int(timestamp);
          }
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 30:
          // replica_type
          cur_row_.cells_[i].set_int(partition->get_replica_type());
          break;
        case OB_APP_MIN_COLUMN_ID + 31:
          // last_replay_log_id
          cur_row_.cells_[i].set_int(info.get_clog_info().get_last_replay_log_id());
          break;
        case OB_APP_MIN_COLUMN_ID + 32:
          // schema_version
          cur_row_.cells_[i].set_int(info.get_data_info().get_schema_version());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;

  } else {
    // revert pg_iter_ no matter ret is OB_ITER_END or other errors
    if (NULL != pg_iter_) {
      if (NULL == partition_service_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "partition_service_ is null, ", K(ret));
      } else {
        partition_service_->revert_pg_iter(pg_iter_);
        pg_iter_ = NULL;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
