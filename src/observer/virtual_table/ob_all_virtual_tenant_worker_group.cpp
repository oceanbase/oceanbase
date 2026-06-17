/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_all_virtual_tenant_worker_group.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualTenantWorkerGroup::ObAllVirtualTenantWorkerGroup()
    : ObVirtualTableScannerIterator(),
      start_to_read_(false)
{
}

ObAllVirtualTenantWorkerGroup::~ObAllVirtualTenantWorkerGroup()
{
  reset();
}

void ObAllVirtualTenantWorkerGroup::reset()
{
  start_to_read_ = false;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualTenantWorkerGroup::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))
              == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

int ObAllVirtualTenantWorkerGroup::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      omt::ObMultiTenant *omt = GCTX.omt_;
      omt::TenantIdList current_ids(nullptr, ObModIds::OMT);
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "omt is null", K(ret));
      } else {
        omt->get_tenant_ids(current_ids);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < current_ids.size(); ++i) {
        uint64_t tenant_id = current_ids.at(i);
        if (is_virtual_tenant_id(tenant_id)
            || (!is_sys_tenant(effective_tenant_id_) && tenant_id != effective_tenant_id_)) {
          continue;
        }
        MTL_SWITCH(tenant_id) {
          omt::ObTenant *tenant = static_cast<omt::ObTenant *>(MTL_CTX());
          if (OB_ISNULL(tenant)) {
            continue;
          }
          if (OB_FAIL(tenant->for_each_group([&](omt::ObResourceGroup *group) {
            int tmp_ret = OB_SUCCESS;
            int64_t req_queue_size = group->get_req_queue().size();
            const int64_t col_count = output_column_ids_.count();
            for (int64_t j = 0; OB_SUCCESS == tmp_ret && j < col_count; ++j) {
              const uint64_t col_id = output_column_ids_.at(j);
              ObObj *cells = cur_row_.cells_;
              switch (col_id) {
                case SVR_IP:
                  cells[j].set_varchar(ip_buf_);
                  cells[j].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                case SVR_PORT:
                  cells[j].set_int(GCONF.self_addr_.get_port());
                  break;
                case TENANT_ID:
                  cells[j].set_int(tenant_id);
                  break;
                case GROUP_ID:
                  cells[j].set_int(group->get_group_id());
                  break;
                case WORKERS_SIZE:
                  cells[j].set_int(group->workers_.get_size());
                  break;
                case REQ_QUEUE_SIZE:
                  cells[j].set_int(req_queue_size);
                  break;
                case RECV_REQ_CNT:
                  cells[j].set_int(group->get_recv_req_cnt());
                  break;
                case DELETED:
                  cells[j].set_bool(group->is_deleted());
                  break;
                case TOKEN_CHANGE_TS:
                  cells[j].set_int(group->get_token_change_ts());
                  break;
                case THROTTLED_TIME_US:
                  cells[j].set_int(group->get_throttled_time_us());
                  break;
                case IDLE_CNT:
                  cells[j].set_int(group->idle_cnt_);
                  break;
                case LAST_NOT_EMPTY_TS:
                  cells[j].set_int(group->last_not_empty_ts_);
                  break;
                default:
                  tmp_ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "unexpected column id", K(col_id), K(j), K(tmp_ret));
                  break;
              }
            }
            if (OB_SUCCESS == tmp_ret && OB_SUCCESS != (tmp_ret = scanner_.add_row(cur_row_))) {
              SERVER_LOG(WARN, "fail to add row", K(tmp_ret), K(cur_row_));
            }
            return tmp_ret;
          }))) {
            SERVER_LOG(WARN, "for_each_group failed", K(ret), K(tenant_id));
          }
        }
      }
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
