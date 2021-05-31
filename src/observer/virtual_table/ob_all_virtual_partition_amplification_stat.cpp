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

#include "observer/virtual_table/ob_all_virtual_partition_amplification_stat.h"

#include "storage/ob_i_partition_group.h"
#include "storage/ob_pg_partition.h"
#include "storage/ob_partition_service.h"

using namespace oceanbase;
using namespace common;

namespace oceanbase {
namespace common {
class ObObj;
}

namespace observer {

ObAllPartitionAmplificationStat::ObAllPartitionAmplificationStat() : addr_(), partition_service_(NULL), ptt_iter_(NULL)
{}

ObAllPartitionAmplificationStat::~ObAllPartitionAmplificationStat()
{
  reset();
}

void ObAllPartitionAmplificationStat::reset()
{
  if (NULL != ptt_iter_) {
    if (NULL == partition_service_) {
      SERVER_LOG(ERROR, "partition service is NULL!");
    } else {
      partition_service_->revert_pg_partition_iter(ptt_iter_);
    }
    ptt_iter_ = NULL;
  }
  partition_service_ = NULL;
  addr_.reset();
}

int ObAllPartitionAmplificationStat::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  storage::ObPGPartition* partition = NULL;
  if (NULL == allocator_ || NULL == partition_service_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(
        WARN, "allocator_ or partition_service_ shouldn't be NULL", K(allocator_), K(partition_service_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cells of cur row is NULL", K(ret));
  } else if (NULL == ptt_iter_ && NULL == (ptt_iter_ = partition_service_->alloc_pg_partition_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc partition iter", K(ret));
  } else if (OB_SUCCESS != (ret = ptt_iter_->get_next(partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "scan next partition failed", K(ret));
    }
  } else if (NULL == partition) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "get partition failed", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObPartitionKey pkey = partition->get_partition_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP: {
          if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            ret = OB_BUF_NOT_ENOUGH;
            SERVER_LOG(WARN, "buffer not enough", K(ret));
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
          cur_row_.cells_[i].set_int(extract_tenant_id(pkey.table_id_));
          break;
        }
        case TABLE_ID: {
          cur_row_.cells_[i].set_int(pkey.table_id_);
          break;
        }
        case PARTITION_ID: {
          cur_row_.cells_[i].set_int(pkey.get_partition_id());
          break;
        }
        case PARTITION_CNT: {
          cur_row_.cells_[i].set_int(pkey.get_partition_cnt());
          break;
        }
        case DIRTY_1: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_3: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_5: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_10: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_15: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_20: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_30: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_50: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_75: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case DIRTY_100: {
          cur_row_.cells_[i].set_int(0);
          break;
        }
        case BLOCK_CNT: {
          cur_row_.cells_[i].set_int(0);
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
  } else {
    // revert ptt_iter_ no matter ret is OB_ITER_END or other errors
    if (NULL != ptt_iter_) {
      if (NULL == partition_service_) {
        SERVER_LOG(ERROR, "partition service is NULL!");
      } else {
        partition_service_->revert_pg_partition_iter(ptt_iter_);
      }
      ptt_iter_ = NULL;
    }
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
