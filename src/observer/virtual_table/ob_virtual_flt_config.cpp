/*
 * Copyright (c) 2022 OceanBase Technology Co.,Ltd.
 * OceanBase is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *          http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 * ---------------------------------------------------------------------------------------
 * Authors:
 *   Juehui <>
 * ---------------------------------------------------------------------------------------
 */
#include "observer/virtual_table/ob_virtual_flt_config.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;
using namespace oceanbase::omt;
using namespace oceanbase::share;
using namespace oceanbase::sql;
namespace oceanbase {
namespace observer {

  ObVirtualFLTConfig::ObVirtualFLTConfig():
    ObVirtualTableScannerIterator(),
    rec_list_(),
    rec_array_idx_(OB_INVALID_ID),
    tenant_id_array_(),
    tenant_id_array_idx_(0)
{
}

ObVirtualFLTConfig::~ObVirtualFLTConfig() {
  reset();
}

void ObVirtualFLTConfig::reset()
{
  ObVirtualTableScannerIterator::reset();
  rec_list_.reset();
  rec_array_idx_ = OB_INVALID_ID;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = 0;
}

int ObVirtualFLTConfig::inner_open()
{
  int ret = OB_SUCCESS;
  // sys tenant show all tenant plan cache stat
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(GCTX.omt_->get_mtl_tenant_ids(tenant_id_array_))) {
      SERVER_LOG(WARN, "failed to add tenant id", K(ret));
    }
  } else {
    tenant_id_array_.reset();
    // user tenant show self tenant stat
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "fail to push back effective_tenant_id_", KR(ret), K(effective_tenant_id_),
          K(tenant_id_array_));
    }
  }
  SERVER_LOG(TRACE,"get tenant_id array", K(effective_tenant_id_), K(is_sys_tenant(effective_tenant_id_)), K(tenant_id_array_));
  return ret;
}

int ObVirtualFLTConfig::get_row_from_tenants()
{
  int ret = OB_SUCCESS;
  bool is_sub_end = false;
  do {
    is_sub_end = false;
    if (tenant_id_array_idx_ < 0) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid tenant_id_array idx", K(ret), K(tenant_id_array_idx_));
    } else if (tenant_id_array_idx_ >= tenant_id_array_.count()) {
      ret = OB_ITER_END;
      tenant_id_array_idx_ = 0;
    } else {
      uint64_t tenant_id = tenant_id_array_.at(tenant_id_array_idx_);
      MTL_SWITCH(tenant_id) {
        if (OB_FAIL(get_row_from_specified_tenant(tenant_id,
                                                  is_sub_end))) {
          SERVER_LOG(WARN,
                     "fail to insert plan by tenant id",
                     K(ret),
                     "tenant id",
                     tenant_id_array_.at(tenant_id_array_idx_),
                     K(tenant_id_array_idx_));
        } else {
          if (is_sub_end) {
            ++tenant_id_array_idx_;
          }
        }
      }
    }
  } while(is_sub_end && OB_SUCCESS == ret);
  return ret;
}

int ObVirtualFLTConfig::get_row_from_specified_tenant(uint64_t tenant_id, bool &is_end)
{
  int ret = OB_SUCCESS;
  is_end = false;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "allocator is null", K(ret));
  } else if (OB_INVALID_ID == static_cast<uint64_t>(rec_array_idx_)) {
    ObFLTControlInfoManager mgr(tenant_id);
    if (OB_FAIL(mgr.init())) {
      SERVER_LOG(WARN,"failed to init full link trace info manager", K(ret));
    } else if (OB_FAIL(mgr.get_all_flt_config(rec_list_, *allocator_))) {
      SERVER_LOG(WARN,"failed to fill flt configs", K(ret));
    } else {
      rec_array_idx_ = 0;
    }
  }

  if (OB_SUCC(ret)) {
    bool is_filled = false;
    while (OB_SUCC(ret) && false == is_filled && false == is_end) {
      if (rec_array_idx_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid rec list index", K(rec_array_idx_));
      } else if (rec_array_idx_ >= rec_list_.count()) {
        is_end = true;
        rec_array_idx_ = OB_INVALID_ID;
        rec_list_.reset();
      } else {
        is_end = false;
        ObFLTConfRec& rec = rec_list_.at(rec_array_idx_);
        ++rec_array_idx_;
        if (OB_FAIL(fill_cells(rec))) {
          SERVER_LOG(WARN, "fail to fill cells", K(rec), K(tenant_id));
        } else {
          is_filled = true;
        }
      }
    } //while end
  }
  SERVER_LOG(TRACE,
             "flt config from a tenant",
             K(ret),
             K(tenant_id));
  return ret;
}

int ObVirtualFLTConfig::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObVirtualFLTConfig::fill_cells(ObFLTConfRec &record)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;

  if (OB_ISNULL(cells)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  } else {
    for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch(col_id) {
      case TENANT_ID: {
        cells[cell_idx].set_int(record.tenant_id_);
      } break;
      case TYPE: {
        if (record.type_ == FLT_TENANT_TYPE) {
          cells[cell_idx].set_varchar("TENANT");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else if (record.type_ == FLT_MOD_ACT_TYPE) {
          cells[cell_idx].set_varchar("MODULE_ACTION");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else if (record.type_ == FLT_CLIENT_ID_TYPE) {
          cells[cell_idx].set_varchar("CLENT_IDENTIFIER");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else {
          // do nothing
          cells[cell_idx].set_varchar("INVALID TYPE");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        }
      } break;
      case MODULE_NAME: {
        cells[cell_idx].set_varchar(record.mod_name_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case ACTION_NAME: {
        cells[cell_idx].set_varchar(record.act_name_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case CLIENT_IDENTIFIER: {
        cells[cell_idx].set_varchar(record.identifier_name_);
        cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
      } break;
      case LEVEL: {
        cells[cell_idx].set_int(record.control_info_.level_);
      } break;
      case SAMPLE_PERCENTAGE: {
        cells[cell_idx].set_int((record.control_info_.sample_pct_ == -1)
                                   ? -1 : record.control_info_.sample_pct_*100);
      } break;
      case RECORD_POLICY: {
        if (record.control_info_.rp_ == sql::FLTControlInfo::RecordPolicy::RP_ALL) {
          cells[cell_idx].set_varchar("ALL");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else if (record.control_info_.rp_ == sql::FLTControlInfo::RecordPolicy::RP_ONLY_SLOW_QUERY) {
          cells[cell_idx].set_varchar("ONLY_SLOW_QUERY");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else if (record.control_info_.rp_ == sql::FLTControlInfo::RecordPolicy::RP_SAMPLE_AND_SLOW_QUERY) {
          cells[cell_idx].set_varchar("SAMPLE_AND_SLOW_QUERY");
          cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                     ObCharset::get_default_charset()));
        } else {
          // do nothing
          cells[cell_idx].set_null();
        }
      } break;
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
      } break;
      }
    }
  }
  return ret;
}
} //namespace observer
} //namespace oceanbase
