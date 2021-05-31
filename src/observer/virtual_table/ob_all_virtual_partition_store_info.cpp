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

#include "observer/virtual_table/ob_all_virtual_partition_store_info.h"

#include "storage/ob_i_partition_group.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_store.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_pg_storage.h"

using namespace oceanbase;
using namespace common;
using namespace storage;

namespace oceanbase {
namespace observer {

ObAllPartitionStoreInfo::ObAllPartitionStoreInfo()
    : addr_(), partition_service_(nullptr), ptt_iter_(nullptr), pg_(nullptr), iter_(0)
{}

ObAllPartitionStoreInfo::~ObAllPartitionStoreInfo()
{
  reset();
}

void ObAllPartitionStoreInfo::reset()
{
  if (nullptr != ptt_iter_) {
    if (nullptr == partition_service_) {
      SERVER_LOG(ERROR, "partition_service_ is null");
    } else {
      partition_service_->revert_pg_iter(ptt_iter_);
      ptt_iter_ = nullptr;
    }
  }
  partition_service_ = nullptr;
  pkeys_.reset();
  pg_ = nullptr;
  iter_ = 0;
  addr_.reset();
  pkey_.reset();
  ip_buf_[0] = '\0';
}

int ObAllPartitionStoreInfo::get_next_partition_store_info(ObPartitionStoreInfo& info)
{
  int ret = OB_SUCCESS;
  info.reset();

  if (OB_ISNULL(allocator_) || OB_ISNULL(partition_service_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "allocator_ or partition_service_ shouldn't be NULL", K(allocator_), K(partition_service_), K(ret));
  } else if (nullptr == ptt_iter_ && nullptr == (ptt_iter_ = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc partition iter", K(ret));
  } else {
    while (OB_SUCC(ret) && iter_ >= pkeys_.count()) {
      pkeys_.reuse();
      pg_ = nullptr;
      if (OB_FAIL(ptt_iter_->get_next(pg_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "scan next partition failed", K(ret));
        }
      } else if (OB_ISNULL(pg_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "partition must not null", K(ret));
      } else if (OB_FAIL(pg_->get_pg_storage().get_all_pg_partition_keys(pkeys_))) {
        SERVER_LOG(WARN, "failed to get_all_pg_partition_keys", K(ret));
      } else {
        iter_ = 0;
      }
    }
    if (OB_SUCC(ret)) {
      pkey_ = pkeys_.at(iter_);
      if (OB_FAIL(pg_->get_pg_storage().get_partition_store_info(pkey_, info))) {
        SERVER_LOG(WARN, "failed to get partition store info", K(ret), K_(pkey));
      } else {
        iter_++;
      }
    }
  }

  if (OB_FAIL(ret)) {
    // revert ptt_iter_ no matter ret is OB_ITER_END or other errors
    reset();
  }

  return ret;
}

int ObAllPartitionStoreInfo::inner_get_next_row(common::ObNewRow*& row)
{

  int ret = OB_ITER_END;
  ObPartitionStoreInfo info;

  if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (OB_FAIL(get_next_partition_store_info(info))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          if (OB_UNLIKELY(!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_)))) {
            SERVER_LOG(ERROR, "ip to string failed");
            ret = OB_ERR_UNEXPECTED;
          } else {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case SVR_PORT: {
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        }
        case TENANT_ID: {
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey_.table_id_));
          break;
        }
        case TABLE_ID: {
          cur_row_.cells_[i].set_int(pkey_.table_id_);
          break;
        }
        case PARTITION_IDX: {
          cur_row_.cells_[i].set_int(pkey_.get_partition_id());
          break;
        }
        case PARTITION_CNT: {
          cur_row_.cells_[i].set_int(pkey_.get_partition_cnt());
          break;
        }
        case IS_RESTORE: {
          cur_row_.cells_[i].set_int(info.is_restore_);
          break;
        }
        case MIGRATE_STATUS: {
          cur_row_.cells_[i].set_int(info.migrate_status_);
          break;
        }
        case MIGRATE_TIMESTAMP: {
          cur_row_.cells_[i].set_int(info.migrate_timestamp_);
          break;
        }
        case REPLICA_TYPE: {
          cur_row_.cells_[i].set_int(info.replica_type_);
          break;
        }
        case SPLIT_STATE: {
          cur_row_.cells_[i].set_int(info.split_state_);
          break;
        }
        case MULTI_VERSION_START: {
          cur_row_.cells_[i].set_int(info.multi_version_start_);
          break;
        }
        case REPORT_VERSION: {
          cur_row_.cells_[i].set_int(info.report_version_);
          break;
        }
        case REPORT_ROW_COUNT: {
          cur_row_.cells_[i].set_int(info.report_row_count_);
          break;
        }
        case REPORT_DATA_CHECKSUM: {
          cur_row_.cells_[i].set_int(info.report_data_checksum_);
          break;
        }
        case REPORT_DATA_SIZE: {
          cur_row_.cells_[i].set_int(info.report_data_size_);
          break;
        }
        case REPORT_REQUIRED_SIZE: {
          cur_row_.cells_[i].set_int(info.report_required_size_);
          break;
        }
        case READABLE_TS: {
          cur_row_.cells_[i].set_int(info.readable_ts_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", K(ret), K(i), K(output_column_ids_), K(col_id));
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

}  // namespace observer
}  // namespace oceanbase
