/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "ob_all_virtual_log_transport_dest_stat.h"
#include "logservice/cdcservice/ob_cdc_service.h"
#include "logservice/ob_log_service.h"

namespace oceanbase
{
namespace observer
{

bool ObAllVirtualLogTransportDestStat::ClientLSCtxMapStatFunctor::operator()(
      const cdc::ClientLSKey &key,
      const cdc::ClientLSCtx *ctx)
{
  bool bret = true;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    EXTLOG_LOG_RET(WARN, OB_ERR_UNEXPECTED, "get null ctx in ClientLSCtx", K(key));
  } else if (obrpc::ObCdcFetchLogProtocolType::UnknownProto != ctx->get_proto_type()) {
    if (OB_FAIL(stat_vtable_.insert_log_transport_dest_stat_(key, *ctx))) {
      EXTLOG_LOG(WARN, "failed to insert log_transport", K(key));
    } else {
      EXTLOG_LOG(TRACE, "insert one record", K(key));
    }
  } else {
    // ignore ctx which is not inited
  }
  if (OB_FAIL(ret)) {
    bret = false;
  }
  return bret;
}

int ObAllVirtualLogTransportDestStat::TenantStatFunctor::operator()()
{
  int ret = OB_SUCCESS;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
  cdc::ObCdcService *cdc_service = nullptr;
  if (OB_ISNULL(log_service)) {
    SERVER_LOG(INFO, "tenant has no ObLogService", K(MTL_ID()));
  } else if (OB_ISNULL(cdc_service = log_service->get_cdc_service())) {
    SERVER_LOG(INFO, "tenant has no CdcService", K(MTL_ID()));
  } else {
    cdc::ClientLSCtxMap &map = cdc_service->get_ls_ctx_map();
    if (OB_FAIL(map.for_each(functor_))) {
      SERVER_LOG(WARN, "get stat from ClientLSCtxMap failed", K(MTL_ID()));
    }
  }

  return ret;
}

ObAllVirtualLogTransportDestStat::ObAllVirtualLogTransportDestStat(omt::ObMultiTenant *omt):
    omt_(omt) { }

ObAllVirtualLogTransportDestStat::~ObAllVirtualLogTransportDestStat()
{
  destroy();
}

int ObAllVirtualLogTransportDestStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    TenantStatFunctor functor(*this);
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(functor))) {
      SERVER_LOG(WARN, "failed to operate_each_tenant_for_sys_or_self");
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }

  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get_next_row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

void ObAllVirtualLogTransportDestStat::destroy()
{
  omt_ = nullptr;
}

int ObAllVirtualLogTransportDestStat::insert_log_transport_dest_stat_(
    const cdc::ClientLSKey &key,
    const cdc::ClientLSCtx &ctx)
{
  int ret = OB_SUCCESS;
  const int64_t column_count = output_column_ids_.count();
  int64_t avg_process_time = 0;
  int64_t avg_queue_time = 0;
  int64_t avg_read_log_time = 0;
  int64_t avg_read_log_size = 0;
  int64_t avg_log_transport_bandwidth = 0;

  ctx.get_traffic_stat_info().calc_avg_traffic_stat(avg_process_time, avg_queue_time,
    avg_read_log_time, avg_read_log_size, avg_log_transport_bandwidth);

  for (int64_t i = 0; OB_SUCC(ret) && i < column_count; i++) {
    const uint64_t column_id = output_column_ids_.at(i);
    switch (column_id) {
      case TENANT_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }

      case SVR_IP: {
        if (! GCTX.self_addr().ip_to_string(self_ip_, MAX_IP_ADDR_LENGTH)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "self_ip_ ip_to_string failed");
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(self_ip_));
          cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }

      case SVR_PORT: {
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      }

      case LS_ID: {
        cur_row_.cells_[i].set_int(key.get_ls_id().id());
        break;
      }

      case CLIENT_IP: {
        if (! key.get_client_addr().ip_to_string(client_ip_, MAX_IP_ADDR_LENGTH)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "client_ip_ ip_to_string failed");
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(client_ip_));
          cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }

      case CLIENT_PID: {
        cur_row_.cells_[i].set_int(key.get_client_pid());
        break;
      }

      case CLIENT_TENANT_ID: {
        cur_row_.cells_[i].set_int(key.get_tenant_id());
        break;
      }

      case CLIENT_TYPE: {
        cur_row_.cells_[i].set_int(ctx.get_client_type());
        break;
      }

      case START_SERVE_TIME: {
        cur_row_.cells_[i].set_timestamp(ctx.get_create_ts());
        break;
      }

      case LAST_SERVE_TIME: {
        cur_row_.cells_[i].set_timestamp(ctx.get_touch_ts());
        break;
      }

      case LAST_READ_SOURCE: {
        cur_row_.cells_[i].set_int(ctx.get_fetch_mode());
        break;
      }

      case LAST_REQUEST_TYPE: {
        cur_row_.cells_[i].set_int(ctx.get_proto_type());
        break;
      }

      case LAST_REQUEST_LOG_LSN: {
        cur_row_.cells_[i].set_uint64(ctx.get_req_lsn().val_);
        break;
      }

      case LAST_REQUEST_LOG_SCN: {
        cur_row_.cells_[i].set_uint64(ctx.get_progress());
        break;
      }

      case LAST_FAILED_REQUEST: {
        const cdc::ClientLSCtx::RpcRequestInfo &failed_rpc_info = ctx.get_failed_rpc_info();
        if (! failed_rpc_info.is_valid()) {
          cur_row_.cells_[i].set_null();
        } else if (OB_FAIL(failed_rpc_info.to_string_rdlock(last_failed_reqeuest_, MAX_RPC_REQUEST_INFO_LEN))) {
          SERVER_LOG(WARN, "failed to stringify failed rpc info");
        } else {
          ObString last_failed_reqeuest_str = ObString::make_string(last_failed_reqeuest_);
          cur_row_.cells_[i].set_lob_value(common::ObLongTextType, last_failed_reqeuest_str.ptr(),
              last_failed_reqeuest_str.length());
          cur_row_.cells_[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }

      case AVG_REQUEST_PROCESS_TIME: {
        cur_row_.cells_[i].set_int(avg_process_time);
        break;
      }

      case AVG_REQUEST_QUEUE_TIME: {
        cur_row_.cells_[i].set_int(avg_queue_time);
        break;
      }

      case AVG_REQUEST_READ_LOG_TIME: {
        cur_row_.cells_[i].set_int(avg_read_log_time);
        break;
      }

      case AVG_REQUEST_READ_LOG_SIZE: {
        cur_row_.cells_[i].set_int(avg_read_log_size);
        break;
      }

      case AVG_LOG_TRANSPORT_BANDWIDTH: {
        cur_row_.cells_[i].set_int(avg_log_transport_bandwidth);
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    scanner_.add_row(cur_row_);
  }

  return ret;
}

}
}