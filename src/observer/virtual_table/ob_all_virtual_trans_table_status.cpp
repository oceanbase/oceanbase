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

#include "ob_all_virtual_trans_table_status.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "share/ob_tenant_mgr.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace observer {
ObAllVirtualTransTableStatus::ObAllVirtualTransTableStatus() : ObVirtualTableIterator(), ipstr_(), index_(0)
{}

ObAllVirtualTransTableStatus::~ObAllVirtualTransTableStatus()
{
  reset();
}

int ObAllVirtualTransTableStatus::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K_(allocator), K(ret));
  } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr_))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_all_partitions(partitions_))) {
    SERVER_LOG(WARN, "Failed to get all partitions", K(ret));
  }
  return ret;
}

void ObAllVirtualTransTableStatus::reset()
{
  ipstr_.reset();
  index_ = 0;
}

int ObAllVirtualTransTableStatus::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  int64_t col_count = output_column_ids_.count();
  ObIPartitionGroup* partition = NULL;
  ObTransTableStatus status;
  if (index_ >= partitions_.count()) {
    ret = OB_ITER_END;
  } else {
    partition = partitions_.at(index_);
    index_++;
    partition->get_pg_storage().get_trans_table_status(status);
  }

  if (OB_SUCC(ret)) {
    ObObj* cells = cur_row_.cells_;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case TENANT_ID: {
            cells[i].set_int(partition->get_partition_key().get_tenant_id());
            break;
          }
          case SVR_IP: {
            cells[i].set_varchar(ipstr_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TABLE_ID: {
            cells[i].set_int(partition->get_partition_key().get_table_id());
            break;
          }
          case PARTITION_ID: {
            cells[i].set_int(partition->get_partition_key().get_partition_id());
            break;
          }
          case END_LOG_ID: {
            cells[i].set_int(status.end_log_ts_);
            break;
          }
          case TRANS_CNT: {
            cells[i].set_int(status.row_count_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  } else {
    // do nothing;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
