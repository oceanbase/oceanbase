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

#include "observer/virtual_table/ob_all_virtual_weak_read_stat.h"
#include "observer/omt/ob_multi_tenant.h"     // TenantIdList
#include "share/rc/ob_context.h"              // WITH_CONTEXT  ObWithTenantContext
#include "share/rc/ob_tenant_base.h"          // MTL
#include "lib/container/ob_array.h"           // ObArray
#include "observer/ob_server_struct.h"


using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::share;

namespace oceanbase
{
namespace observer
{
ObAllVirtualWeakReadStat::ObAllVirtualWeakReadStat()
  :ObVirtualTableScannerIterator(),
   start_to_read_(false)
{}

ObAllVirtualWeakReadStat::~ObAllVirtualWeakReadStat()
{
  reset();
}

void ObAllVirtualWeakReadStat::reset()
{
  start_to_read_ = false;
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualWeakReadStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  if (OB_ISNULL(allocator_)){
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL",
                     K(allocator_), K(ret));
  } else if (!start_to_read_) {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (OB_ISNULL(cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_ID;
      all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
      GCTX.omt_->get_tenant_ids(all_tenants);
      for (int64_t index = 0; OB_SUCCESS == ret && index < all_tenants.size(); index++) {
        //对每个租户，切换租户上下文，获得列数据
        tenant_id = all_tenants[index];
        char self_ip_buf[common::OB_IP_STR_BUFF];
        char master_ip_buf[common::OB_IP_STR_BUFF];
        if (! is_virtual_tenant_id(tenant_id)) {
          MTL_SWITCH(tenant_id) {
            transaction::ObTenantWeakReadService *twrs = MTL(transaction::ObTenantWeakReadService *);
            if (OB_ISNULL(twrs)) {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "MTL ObTenantWeakReadService is NULL", K(ret));
            } else {
        	transaction::ObTenantWeakReadStat wrs_stat;
        	twrs->get_weak_read_stat(wrs_stat);

		for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
		  uint64_t col_id = output_column_ids_.at(i);
		  switch(col_id) {
		    case OB_APP_MIN_COLUMN_ID + TENANT_ID:
		      cells[i].set_int(tenant_id);
		      break;
		    case OB_APP_MIN_COLUMN_ID + SERVER_IP:
		      (void)wrs_stat.self_.ip_to_string(self_ip_buf, sizeof(self_ip_buf));
		      cells[i].set_varchar(self_ip_buf);
		      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
		      break;
		    case OB_APP_MIN_COLUMN_ID + SERVER_PORT:
		      cells[i].set_int(wrs_stat.self_.get_port());
		      break;
		    case OB_APP_MIN_COLUMN_ID + SERVER_VERSION: {
					uint64_t v = wrs_stat.server_version_.is_valid() ? wrs_stat.server_version_.get_val_for_inner_table_field() : 0;
		      cells[i].set_uint64(v);
		      break;
				}
		    case OB_APP_MIN_COLUMN_ID + SERVER_VERSION_DELTA:
		      cells[i].set_int(wrs_stat.server_version_delta_);
		      break;
        case OB_APP_MIN_COLUMN_ID + LOCAL_CLUSTER_VERSION: {
					uint64_t v = wrs_stat.local_cluster_version_.is_valid() ? wrs_stat.local_cluster_version_.get_val_for_inner_table_field() : 0;
		      cells[i].set_uint64(v);
          break;
				}
        case OB_APP_MIN_COLUMN_ID + LOCAL_CLUSTER_VERSION_DELTA:
          cells[i].set_int(wrs_stat.local_cluster_version_delta_);
          break;
		    case OB_APP_MIN_COLUMN_ID + TOTAL_PART_COUNT:
		      cells[i].set_int(wrs_stat.total_part_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + VALID_INNER_PART_COUNT:
		      cells[i].set_int(wrs_stat.valid_inner_part_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + VALID_USER_PART_COUNT:
		      cells[i].set_int(wrs_stat.valid_user_part_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_MASTER_IP:
		      (void)wrs_stat.cluster_master_.ip_to_string(master_ip_buf, sizeof(master_ip_buf));
		      cells[i].set_varchar(master_ip_buf);
		      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_MASTER_PORT:
		      cells[i].set_int(wrs_stat.cluster_master_.get_port());
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_HEART_BEAT_TS:
		      cells[i].set_int(wrs_stat.cluster_heartbeat_post_tstamp_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_HEART_BEAT_COUNT:
		      cells[i].set_int(wrs_stat.cluster_heartbeat_post_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_HEART_BEAT_SUCC_TS:
		      cells[i].set_int(wrs_stat.cluster_heartbeat_succ_tstamp_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_HEART_BEAT_SUCC_COUNT:
		      cells[i].set_int(wrs_stat.cluster_heartbeat_succ_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + SELF_CHECK_TS:
		      cells[i].set_int(wrs_stat.self_check_tstamp_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + LOCAL_CURRENT_TS:
		      cells[i].set_int(wrs_stat.local_current_tstamp_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + IN_CLUSTER_SERVICE:
		      cells[i].set_int(wrs_stat.in_cluster_service_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + IS_CLUSTER_MASTER:
		      cells[i].set_int(wrs_stat.is_cluster_master_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_MASTER_EPOCH:
		      cells[i].set_int(wrs_stat.cluster_service_epoch_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_SERVERS_COUNT:
		      cells[i].set_int(wrs_stat.cluster_servers_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_SKIPPED_SERVERS_COUNT:
		      cells[i].set_int(wrs_stat.cluster_skipped_servers_count_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_VERSION_GEN_TS:
		      cells[i].set_int(wrs_stat.cluster_version_gen_tstamp_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_VERSION: {
					uint64_t v = wrs_stat.cluster_version_.is_valid() ? wrs_stat.cluster_version_.get_val_for_inner_table_field() : 0;
		      cells[i].set_uint64(v);
		      break;
				}
		    case OB_APP_MIN_COLUMN_ID + CLUSTER_VERSION_DELTA:
		      cells[i].set_int(wrs_stat.cluster_version_delta_);
		      break;
		    case OB_APP_MIN_COLUMN_ID + MIN_CLUSTER_VERSION: {
					uint64_t v = wrs_stat.min_cluster_version_.is_valid() ? wrs_stat.min_cluster_version_.get_val_for_inner_table_field() : 0;
		      cells[i].set_uint64(v);
		      break;
				}
		    case OB_APP_MIN_COLUMN_ID + MAX_CLUSTER_VERSION: {
					uint64_t v = wrs_stat.max_cluster_version_.is_valid() ? wrs_stat.max_cluster_version_.get_val_for_inner_table_field() : 0;
		      cells[i].set_uint64(v);
		      break;
				}
		    default:
		      ret = OB_ERR_UNEXPECTED;
		      SERVER_LOG(WARN, "unexpected column id", K(ret));
		      break;
		  }
		} //for columns

		if (OB_SUCCESS == ret && OB_FAIL(scanner_.add_row(cur_row_))) {
		  SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
		}
            } //else
          }  //WITH_CONTEXT
        } // if (is_virtual_tenant_id)
      }  // for all_tenants
      if (OB_SUCC(ret)) {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }
  }
  if (OB_SUCCESS == ret && start_to_read_) {
    if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}
} // observer
} // oceanbase
