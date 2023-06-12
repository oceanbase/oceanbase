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

#include "observer/ob_server_struct.h"
#include "ob_all_virtual_dump_tenant_info.h"
#include "share/ob_errno.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase
{
using namespace lib;
namespace observer
{
ObAllVirtualDumpTenantInfo::ObAllVirtualDumpTenantInfo()
  : is_inited_(false)
{
}

ObAllVirtualDumpTenantInfo::~ObAllVirtualDumpTenantInfo()
{
}

int ObAllVirtualDumpTenantInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    auto func = [&] (omt::ObTenant &t) {
      int ret = OB_SUCCESS;
      const int64_t col_count = output_column_ids_.count();
      ObObj *cells = cur_row_.cells_;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          //svr_ip
          if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          //svr_port
          cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // id
          cells[i].set_int(t.id_);
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          //compat_mode
          cells[i].set_int(static_cast<int64_t>(t.get_compat_mode()));
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          //unit_min_cpu
          cells[i].set_double(t.unit_min_cpu_);
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          //unit_max_cpu
          cells[i].set_double(t.unit_max_cpu_);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          //slice
          cells[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          //slice_remain
          cells[i].set_double(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          //token_cnt
          cells[i].set_int(t.worker_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          //ass_token_cnt
          cells[i].set_int(t.worker_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          //lq_tokens
          cells[i].set_int(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          //used_lq_tokens
          cells[i].set_int(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          //stopped
          cells[i].set_int(t.stopped_);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          //idle_us
          cells[i].set_int(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          //recv_hp_rpc_cnt
          cells[i].set_int(t.recv_hp_rpc_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 15:
          //recv_np_rpc_cnt
          cells[i].set_int(t.recv_np_rpc_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 16:
          //recv_lp_rpc_cnt
          cells[i].set_int(t.recv_lp_rpc_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 17:
          //recv_mysql_cnt
          cells[i].set_int(t.recv_mysql_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 18:
          //recv_task_cnt
          cells[i].set_int(t.recv_task_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 19:
          //recv_large_req_cnt
          cells[i].set_int(t.recv_large_req_cnt_);
          break;
        case OB_APP_MIN_COLUMN_ID + 20:
          //tt_large_quries
          cells[i].set_int(t.tt_large_quries_);
          break;
        case OB_APP_MIN_COLUMN_ID + 21:
          //actives
          cells[i].set_int(t.workers_.get_size());
          break;
        case OB_APP_MIN_COLUMN_ID + 22:
          //workers
          cells[i].set_int(t.workers_.get_size());
          break;
        case OB_APP_MIN_COLUMN_ID + 23:
          //lq_waiting_workers
          cells[i].set_int(0);
          break;
        case OB_APP_MIN_COLUMN_ID + 24:
          //req_queue_total_size
          cells[i].set_int(t.req_queue_.size());
          break;
        case OB_APP_MIN_COLUMN_ID + 25:
          //queue_0
          cells[i].set_int(t.req_queue_.queue_size(0));
          break;
        case OB_APP_MIN_COLUMN_ID + 26:
          //queue_1
          cells[i].set_int(t.req_queue_.queue_size(1));
          break;
        case OB_APP_MIN_COLUMN_ID + 27:
          //queue_2
          cells[i].set_int(t.req_queue_.queue_size(2));
          break;
        case OB_APP_MIN_COLUMN_ID + 28:
          //queue_3
          cells[i].set_int(t.req_queue_.queue_size(3));
          break;
        case OB_APP_MIN_COLUMN_ID + 29:
          //queue_4
          cells[i].set_int(t.req_queue_.queue_size(4));
          break;
        case OB_APP_MIN_COLUMN_ID + 30:
          //queue_5
          cells[i].set_int(t.req_queue_.queue_size(5));
          break;
        case OB_APP_MIN_COLUMN_ID + 31:
          //large_queued
          cells[i].set_int(t.lq_retry_queue_size());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
        }
      }
      if (OB_SUCC(ret)) {
        // The scanner supports up to 64M, so the overflow situation is not considered for the time being
        if (OB_FAIL(scanner_.add_row(cur_row_))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
      return ret;
    };

    omt::ObMultiTenant *omt = GCTX.omt_;
    if (OB_ISNULL(omt)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "nullptr", K(ret));
    } else if (OB_FAIL(omt->for_each(func))){
      SERVER_LOG(WARN, "omt for each failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      is_inited_ = true;
    }
  }

  if (OB_SUCC(ret)) {
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

} /* namespace observer */
} /* namespace oceanbase */
