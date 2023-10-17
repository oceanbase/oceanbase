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

#include "observer/virtual_table/ob_all_virtual_timestamp_service.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "storage/tx_storage/ob_ls_service.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

namespace oceanbase
{
namespace observer
{

void ObAllVirtualTimestampService::reset()
{
  init_ = false;
  tenant_ids_index_ = 0;
  cur_tenant_id_ = 0;
  ts_value_ = 0;
  service_role_ = ObTimestampAccess::ServiceType::FOLLOWER;
  is_primary_ = false;
  role_ = common::ObRole::FOLLOWER;
  service_epoch_ = 0;
  all_tenants_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTimestampService::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  const int64_t execute_timeout = 10 * 1000 * 1000; // 10s
  if (OB_FAIL(fill_tenant_ids_())) {
    SERVER_LOG(WARN, "fail to fill tenant ids", K(ret));
  } else {
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTimestampService::get_next_tenant_id_info_()
{
  int ret = OB_SUCCESS;
  if (tenant_ids_index_ >= all_tenants_.count()) {
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret)) {
    cur_tenant_id_ = all_tenants_.at(tenant_ids_index_);
    MTL_SWITCH(cur_tenant_id_) {
      bool exist = false;
      if (OB_FAIL(MTL(ObLSService*)->check_ls_exist(IDS_LS, exist))) {
        SERVER_LOG(WARN, "check ls exist fail", K(ret), K_(cur_tenant_id));
      } else if (!exist) {
        ret = OB_LS_NOT_EXIST;
        tenant_ids_index_++;
      } else {
        MTL(ObTimestampAccess *)->get_virtual_info(ts_value_, service_role_, role_, service_epoch_);
        is_primary_ = MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID();
        tenant_ids_index_++;
      }
    } else {
      tenant_ids_index_++;
    }
  }

  return ret;
}

int ObAllVirtualTimestampService::fill_tenant_ids_()
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == effective_tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "invalid tenant_id", KR(ret), K_(effective_tenant_id));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "failed to get multi tenant from GCTX", K(ret));
  } else {
    omt::TenantIdList tmp_all_tenants;
    tmp_all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
    GCTX.omt_->get_tenant_ids(tmp_all_tenants);
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_all_tenants.size(); ++i) {
      uint64_t tenant_id = tmp_all_tenants[i];
      if (!is_virtual_tenant_id(tenant_id) && // skip virtual tenant
          (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
        if (OB_FAIL(all_tenants_.push_back(tenant_id))) {
          SERVER_LOG(WARN, "fail to push back effective_tenant_id", KR(ret), K(tenant_id));
        }
      }
    }
    SERVER_LOG(INFO, "succeed to get tenant ids", K(all_tenants_));
  }

  return ret;
}

int ObAllVirtualTimestampService::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else {
    do {
      if (OB_FAIL(get_next_tenant_id_info_())) {
        if (OB_ITER_END != ret && OB_LS_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "ObAllVirtualTimestampService iter error", K(ret));
        }
      }
    } while (OB_TENANT_NOT_IN_SERVER == ret || OB_LS_NOT_EXIST == ret);
  }
  if (OB_SUCC(ret)) {
    SERVER_LOG(INFO, "ObAllVirtualTimestampService iter success", K(*this));
    const ObAddr self = GCTX.self_addr();
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: { // tenant_id
        cur_row_.cells_[i].set_int(cur_tenant_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: { // svr_ip
        if (false == self.ip_to_string(ip_buf_, common::OB_IP_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret), K(self));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: { // svr_port
        cur_row_.cells_[i].set_int(self.get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: { // ts_value
        cur_row_.cells_[i].set_int(ts_value_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: { // ts_type
        cur_row_.cells_[i].set_varchar(ObTimestampAccess::ts_type_to_cstr(is_primary_));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: { // service_role
        cur_row_.cells_[i].set_varchar((ObTimestampAccess::service_type_to_cstr(service_role_)));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: { // role
        if (OB_FAIL(role_to_string(role_, role_str_, sizeof(role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K_(role));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: { // service_epoch
        cur_row_.cells_[i].set_int(service_epoch_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
        break;
      }
      } // switch
    } // for

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

} // observer
} // oceanbase
