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

#include "ob_all_virtual_partition_audit.h"
#include "storage/ob_partition_service.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace storage;

namespace observer {

ObAllVirtualPartitionAudit::ObAllVirtualPartitionAudit() : is_inited_(false), svr_port_(0), partition_itertor_(NULL)
{
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
}

ObAllVirtualPartitionAudit::~ObAllVirtualPartitionAudit()
{
  reset();
}

int ObAllVirtualPartitionAudit::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (!OBSERVER.get_self().ip_to_string(svr_ip_, sizeof(svr_ip_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get server ip", K(ret));
  } else if (OB_ISNULL(partition_itertor_ = ObPartitionService::get_instance().alloc_pg_iter())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to alloc partition allocator", K(ret));
  } else {
    svr_port_ = OBSERVER.get_self().get_port();
    is_inited_ = true;
  }
  return ret;
}

void ObAllVirtualPartitionAudit::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  MEMSET(svr_ip_, 0, sizeof(svr_ip_));
  svr_port_ = 0;
  if (OB_NOT_NULL(partition_itertor_)) {
    ObPartitionService::get_instance().revert_pg_iter(partition_itertor_);
  }
  partition_itertor_ = NULL;
}

int ObAllVirtualPartitionAudit::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObObj* cells = cur_row_.cells_;
  if (OB_ISNULL(cells) || !is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_FAIL(partition_itertor_->get_next(partition))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next partition");
    }
  } else if (OB_ISNULL(partition)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "partition is null");
  } else {
    transaction::ObTransService* trans_service = ObPartitionService::get_instance().get_trans_service();
    transaction::ObPartitionAuditInfo audit_info;
    if (OB_FAIL(trans_service->get_partition_audit_info(partition->get_partition_key(), audit_info))) {
      TRANS_LOG(WARN, "get partition audit info error", K(ret), K(partition));
    }
    // transaction::ObPartitionAuditInfo audit_info = partition->get_audit_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          cells[i].set_varchar(svr_ip_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(svr_port_);
          break;
        }
        case TENANT_ID: {
          cells[i].set_int(partition->get_partition_key().get_tenant_id());
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
        case PARTITION_STATUS: {
          cells[i].set_int(partition->get_partition_state());
          break;
        }
        case BASE_ROW_COUNT: {
          cells[i].set_int(audit_info.base_row_count_);
          break;
        }
        case INSERT_ROW_COUNT: {
          cells[i].set_int(audit_info.insert_row_count_);
          break;
        }
        case DELETE_ROW_COUNT: {
          cells[i].set_int(audit_info.delete_row_count_);
          break;
        }
        case UPDATE_ROW_COUNT: {
          cells[i].set_int(audit_info.update_row_count_);
          break;
        }
        case QUERY_ROW_COUNT: {
          cells[i].set_int(audit_info.query_row_count_);
          break;
        }
        case INSERT_SQL_COUNT: {
          cells[i].set_int(audit_info.insert_sql_count_);
          break;
        }
        case DELETE_SQL_COUNT: {
          cells[i].set_int(audit_info.delete_sql_count_);
          break;
        }
        case UPDATE_SQL_COUNT: {
          cells[i].set_int(audit_info.update_sql_count_);
          break;
        }
        case QUERY_SQL_COUNT: {
          cells[i].set_int(audit_info.query_sql_count_);
          break;
        }
        case TRANS_COUNT: {
          cells[i].set_int(audit_info.trans_count_);
          break;
        }
        case SQL_COUNT: {
          cells[i].set_int(audit_info.sql_count_);
          break;
        }
        case ROLLBACK_INSERT_ROW_COUNT: {
          cells[i].set_int(audit_info.rollback_insert_row_count_);
          break;
        }
        case ROLLBACK_DELETE_ROW_COUNT: {
          cells[i].set_int(audit_info.rollback_delete_row_count_);
          break;
        }
        case ROLLBACK_UPDATE_ROW_COUNT: {
          cells[i].set_int(audit_info.rollback_update_row_count_);
          break;
        }
        case ROLLBACK_INSERT_SQL_COUNT: {
          cells[i].set_int(audit_info.rollback_insert_sql_count_);
          break;
        }
        case ROLLBACK_DELETE_SQL_COUNT: {
          cells[i].set_int(audit_info.rollback_delete_sql_count_);
          break;
        }
        case ROLLBACK_UPDATE_SQL_COUNT: {
          cells[i].set_int(audit_info.rollback_update_sql_count_);
          break;
        }
        case ROLLBACK_TRANS_COUNT: {
          cells[i].set_int(audit_info.rollback_trans_count_);
          break;
        }
        case ROLLBACK_SQL_COUNT: {
          cells[i].set_int(audit_info.rollback_sql_count_);
          break;
        }
        default: {
          ret = OB_NOT_SUPPORTED;
          SERVER_LOG(WARN, "not supported column", K(column_id), K(i));
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
