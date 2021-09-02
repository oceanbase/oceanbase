// Copyright 2010-2020 Oceanbase Inc. All Rights Reserved.
// Author:
//   muwei.ym@antgroup.com
//

#include "observer/virtual_table/ob_all_virtual_backup_clean_info.h"
#include "observer/ob_server.h"
#include "share/backup/ob_tenant_backup_task_updater.h"

using namespace oceanbase;
using namespace common;
using namespace memtable;
using namespace storage;
using namespace observer;

ObAllVirtualBackupCleanInfo::ObAllVirtualBackupCleanInfo()
    : ObVirtualTableScannerIterator(),
      is_inited_(false),
      res_(),
      result_(NULL),
      tenant_ids_(),
      index_(0),
      sql_proxy_(NULL),
      sql_()
{}

ObAllVirtualBackupCleanInfo::~ObAllVirtualBackupCleanInfo()
{
  reset();
}

void ObAllVirtualBackupCleanInfo::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  res_.reset();
  result_ = NULL;
  tenant_ids_.reset();
  index_ = 0;
  sql_proxy_ = NULL;
  sql_.reset();
}

int ObAllVirtualBackupCleanInfo::init(common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSchemaService* schema_service = NULL;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema service should not be NULL", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_ids(tenant_ids_))) {
    SERVER_LOG(WARN, "failed to get tenant ids", K(ret));
  } else if (tenant_ids_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "tenant ids should not be empty", K(ret), K(tenant_ids_));
  } else if (OB_FAIL(sql_.assign_fmt("SELECT * FROM %s ", OB_ALL_TENANT_BACKUP_CLEAN_INFO_TNAME))) {
    SERVER_LOG(WARN, "fail to assign sql", K(ret));
  } else if (OB_FAIL(sql_proxy.read(res_, tenant_ids_.at(0), sql_.ptr()))) {
    SERVER_LOG(WARN, "fail to execute sql", K(ret), K(sql_));
  } else if (OB_ISNULL(result_ = res_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "error unexpected, query result must not be NULL", K(ret));
  } else {
    index_++;
    sql_proxy_ = &sql_proxy;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualBackupCleanInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(result_->next())) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "failed to get next result", K(ret));
        } else if (index_ == tenant_ids_.count()) {
          ret = OB_ITER_END;
          break;
        } else {
          // overwrite ret
          const uint64_t tenant_id = tenant_ids_.at(index_);
          if (OB_FAIL(sql_proxy_->read(res_, tenant_id, sql_.ptr()))) {
            SERVER_LOG(WARN, "fail to execute sql", K(ret), K(sql_));
          } else if (OB_ISNULL(result_ = res_.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "error unexpected, query result must not be NULL", K(ret));
          } else {
            index_++;
          }
        }
      } else {
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(inner_get_next_row_(row))) {
      SERVER_LOG(WARN, "failed to do inner get next row_", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualBackupCleanInfo::inner_get_next_row_(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  clean_info_.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(cells)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(ObTenantBackupCleanInfoOperator::extract_backup_clean_info(result_, clean_info_))) {
    SERVER_LOG(WARN, "failed to extract tenant backup task", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case TENANT_ID: {
          cells[i].set_int(clean_info_.tenant_id_);
          break;
        }
        case JOB_ID: {
          cells[i].set_int(clean_info_.job_id_);
          break;
        }
        case START_TIME: {
          int64_t start_time = 0;
          if (ObBackupCleanInfoStatus::STOP != clean_info_.status_) {
            start_time = clean_info_.start_time_;
          }
          cells[i].set_int(start_time);
          break;
        }
        case END_TIME: {
          const int64_t end_time = 0;
          cells[i].set_int(end_time);
          break;
        }
        case INCARNATION: {
          cells[i].set_int(clean_info_.incarnation_);
          break;
        }
        case TYPE: {
          cells[i].set_varchar(ObBackupCleanType::get_str(clean_info_.type_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STATUS: {
          cells[i].set_varchar(ObBackupCleanInfoStatus::get_str(clean_info_.status_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case PARAMETER: {
          int64_t parameter = 0;
          if (OB_FAIL(clean_info_.get_clean_parameter(parameter))) {
            SERVER_LOG(WARN, "failed to get clean parameter", K(ret), K(clean_info_));
          } else {
            cells[i].set_int(parameter);
          }
          break;
        }
        case ERROR_MSG: {
          cells[i].set_varchar(clean_info_.error_msg_.ptr());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case COMMENT: {
          cells[i].set_varchar(clean_info_.comment_.ptr());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CLOG_GC_SNAPSHOT: {
          cells[i].set_int(clean_info_.clog_gc_snapshot_);
          break;
        }
        case RESULT: {
          cells[i].set_int(clean_info_.result_);
          break;
        }
        case COPY_ID: {
          cells[i].set_int(clean_info_.copy_id_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
        }  // default
      }    // switch
    }      // for
  }        // else
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
