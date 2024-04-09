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

#include "observer/virtual_table/ob_all_virtual_unit.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_meta.h"
#include "observer/omt/ob_multi_tenant.h" 
#include "observer/omt/ob_tenant.h"
#include "share/ob_unit_getter.h"
#include "logservice/ob_log_service.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace logservice;

ObAllVirtualUnit::ObAllVirtualUnit()
    : ObVirtualTableScannerIterator(),
      addr_(),
      zone_type_(ZONE_TYPE_INVALID),
      region_(DEFAULT_REGION_NAME),
      is_zone_type_set_(false),
      is_region_set_(false),
      tenant_idx_(0),
      tenant_meta_arr_()
{
}

ObAllVirtualUnit::~ObAllVirtualUnit()
{
  reset();
}

void ObAllVirtualUnit::reset()
{
  addr_.reset();
  tenant_meta_arr_.reset();
  tenant_idx_ = 0;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualUnit::init(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else {
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualUnit::inner_open()
{
  int ret = OB_SUCCESS;
  is_zone_type_set_ = false;
  is_region_set_ = false;
  ObLocalityManager *locality_manager_ = GCTX.locality_manager_;

  ObTenant *tenant = nullptr;
  if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to get multi tenant from GCTX", K(ret));
  } else if (OB_SYS_TENANT_ID == effective_tenant_id_) {
    common::ObArray<omt::ObTenantMeta> tenant_meta_arr_tmp;
    if (OB_FAIL(GCTX.omt_->get_tenant_metas(tenant_meta_arr_tmp))) {
      SERVER_LOG(WARN, "fail to get tenant metas", K(ret));
    } else {
      // output all tenants unit info: USER, SYS, META
      for (int64_t i = 0; i < tenant_meta_arr_tmp.count() && OB_SUCC(ret); i++) {
        const ObTenantMeta &tenant_meta = tenant_meta_arr_tmp.at(i);
        if (OB_FAIL(tenant_meta_arr_.push_back(tenant_meta))) {
          SERVER_LOG(WARN, "fail to push back tenant meta", K(ret), K(tenant_meta));
        }
      }
    }
  } else if (OB_FAIL(GCTX.omt_->get_tenant(effective_tenant_id_, tenant))) { // not sys
    if (OB_TENANT_NOT_IN_SERVER != ret) {
      SERVER_LOG(WARN, "fail to get tenant handle", K(ret), K_(effective_tenant_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(tenant_meta_arr_.push_back(tenant->get_tenant_meta()))) {
    SERVER_LOG(WARN, "fail to push back tenant meta", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(locality_manager_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to get locality manager from GCTX", KR(ret));
    } else {
      // Loacality cache will be loaded shortly after ObServer starts.
      // Before that, two columns ZONE_TYPE and REGION of the local unit rows will be set to NULL.
      if (OB_FAIL(locality_manager_->get_server_zone_type(addr_, zone_type_))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          SERVER_LOG(WARN, "fail to get zone type from locality manager", KR(ret));
        } else {
          // OB_ENTRY_NOT_EXIST means locality cache not loaded, no warning would be popped.
          ret = OB_SUCCESS;
        }
      } else {
        is_zone_type_set_ = true;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(locality_manager_->get_server_region(addr_, region_))) {
          if (OB_ENTRY_NOT_EXIST != ret) {
            SERVER_LOG(WARN, "fail to get region from locality manager", KR(ret));
          } else {
            // OB_ENTRY_NOT_EXIST means locality cache not loaded, no warning would be popped.
            ret = OB_SUCCESS;
          }
        } else {
          is_region_set_ = true;
        }
      }
    }
  }

  tenant_idx_ = 0;

  return ret;
}

int ObAllVirtualUnit::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (tenant_idx_ >= tenant_meta_arr_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObTenantMeta &tenant_meta = tenant_meta_arr_.at(tenant_idx_++);
    const int64_t col_count = output_column_ids_.count();

    // META tenant CPU and IOPS are shared with USER tenant
    // So, show CPU and IOPS with value NULL to user
    const bool is_meta_tnt = is_meta_tenant(tenant_meta.unit_.tenant_id_);

    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case UNIT_ID:
          cur_row_.cells_[i].set_int(tenant_meta.unit_.unit_id_);
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(tenant_meta.unit_.tenant_id_);
          break;
        case ZONE:
          cur_row_.cells_[i].set_varchar(GCONF.zone.str());
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case ZONE_TYPE:
          if (OB_UNLIKELY(!is_zone_type_set_)) {
            // locality not refreshed yet, set to null
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_varchar(zone_type_to_str(zone_type_));
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case REGION:
          if (OB_UNLIKELY(!is_region_set_)) {
            // locality not refreshed yet, set to null
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_varchar(region_.str());
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case MIN_CPU: {
          if (is_meta_tnt) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_double(tenant_meta.unit_.config_.min_cpu());
          }
          break;
        }
        case MAX_CPU: {
          if (is_meta_tnt) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_double(tenant_meta.unit_.config_.max_cpu());
          }
          break;
        }
        case MEMORY_SIZE:
          cur_row_.cells_[i].set_int(tenant_meta.unit_.config_.memory_size());
          break;
        case MIN_IOPS: {
          if (is_meta_tnt) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_int(tenant_meta.unit_.config_.min_iops());
          }
          break;
        }
        case MAX_IOPS: {
          if (is_meta_tnt) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_int(tenant_meta.unit_.config_.max_iops());
          }
          break;
        }
        case IOPS_WEIGHT: {
          if (is_meta_tnt) {
            cur_row_.cells_[i].set_null();
          } else {
            cur_row_.cells_[i].set_int(tenant_meta.unit_.config_.iops_weight());
          }
          break;
        }
        case LOG_DISK_SIZE:
          cur_row_.cells_[i].set_int(tenant_meta.unit_.config_.log_disk_size());
          break;
        case LOG_DISK_IN_USE: {
          int64_t clog_disk_in_use = 0;
          const uint64_t tenant_id = tenant_meta.unit_.tenant_id_;
          if (OB_FAIL(get_clog_disk_used_size_(tenant_id, clog_disk_in_use))) {
            SERVER_LOG(WARN, "fail to get clog disk in use", K(ret), K(tenant_meta));
          } else {
            cur_row_.cells_[i].set_int(clog_disk_in_use);
          }
          break;
        }
        case DATA_DISK_IN_USE: {
          int64_t data_disk_in_use = 0;
          if (OB_FAIL(static_cast<ObDiskUsageReportTask*>(GCTX.disk_reporter_)->get_data_disk_used_size(tenant_meta.unit_.tenant_id_, data_disk_in_use))) {
            SERVER_LOG(WARN, "fail to get data disk in use", K(ret), K(tenant_meta));
          } else {
            cur_row_.cells_[i].set_int(data_disk_in_use);
          }
          break;
        }
        case DATA_DISK_SIZE: {
          cur_row_.cells_[i].set_null();
          break;
        }
        case STATUS: {
          const char* status_str = share::ObUnitInfoGetter::get_unit_status_str(tenant_meta.unit_.unit_status_);
          cur_row_.cells_[i].set_varchar(status_str);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case CREATE_TIME:
          cur_row_.cells_[i].set_int(tenant_meta.unit_.create_timestamp_);
          break;
        case MAX_NET_BANDWIDTH: {
          cur_row_.cells_[i].set_int(ObUnitResource::DEFAULT_NET_BANDWIDTH);    // not used, keep default
          break;
        }
        case NET_BANDWIDTH_WEIGHT: {
          cur_row_.cells_[i].set_int(ObUnitResource::DEFAULT_NET_BANDWIDTH_WEIGHT);   // not used, keep default
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

int ObAllVirtualUnit::get_clog_disk_used_size_(const uint64_t tenant_id,
                                               int64_t &log_used_size)
{
  int ret = OB_SUCCESS;
  log_used_size = 0;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  if (OB_SUCC(guard.switch_to(tenant_id))) {
    ObLogService *log_service = MTL(ObLogService*);
    int64_t unused_log_disk_total_size = 0;
    if (OB_ISNULL(log_service)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ObLogService is nullptr", KP(log_service), K(tenant_id));
    } else if (OB_FAIL(log_service->get_palf_stable_disk_usage(log_used_size,
                                                               unused_log_disk_total_size))) {
      SERVER_LOG(WARN, "get_palf_stable_disk_usage failed", KP(log_service), K(tenant_id));
    }
  }
  // return OB_SUCCESS whatever.
  return OB_SUCCESS;
}


