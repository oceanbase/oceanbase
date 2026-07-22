/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_all_virtual_ai_gateway_endpoint_stat.h"
#include "observer/omt/ob_tenant_ai_service.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ai_service/ob_ai_endpoint_circuit_state.h"
#include "share/ai_service/ob_ai_gateway_circuit_state.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;

namespace observer
{

// inc_ref inside the bucket read lock closes the UAF window against drain's erase_if.
class GatewayStatReadOp
{
public:
  GatewayStatReadOp() : entry_(NULL) {}
  void operator()(common::hash::HashMapPair<uint64_t, share::ObAiGatewayCircuitState *> &kv)
  {
    entry_ = kv.second;
    if (OB_NOT_NULL(entry_)) {
      entry_->inc_ref();
    }
  }
  share::ObAiGatewayCircuitState *entry_;
};

ObAllVirtualAiGatewayEndpointStat::ObAllVirtualAiGatewayEndpointStat()
  : is_inited_(false),
    addr_(),
    stat_infos_(),
    stat_pos_(0),
    data_collected_(false)
{
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

ObAllVirtualAiGatewayEndpointStat::~ObAllVirtualAiGatewayEndpointStat()
{
}

int ObAllVirtualAiGatewayEndpointStat::init(const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already initialized", K(ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid address", K(ret), K(addr));
  } else {
    addr_ = addr;
    MEMSET(ip_buf_, 0, sizeof(ip_buf_));
    if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ip to string failed", K(ret), K(addr_));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

void ObAllVirtualAiGatewayEndpointStat::reset()
{
  ObVirtualTableScannerIterator::reset();
  is_inited_ = false;
  addr_.reset();
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
  stat_infos_.reset();
  stat_pos_ = 0;
  data_collected_ = false;
}

// foreach_refactored holds a per-bucket read lock; only collect keys, no heavy work here.
class CollectGatewayIdsFunc
{
public:
  explicit CollectGatewayIdsFunc(ObIArray<uint64_t> &ids) : ids_(ids) {}
  int operator()(hash::HashMapPair<uint64_t, ObAiGatewayCircuitState *> &kv)
  {
    return ids_.push_back(kv.first);
  }
private:
  ObIArray<uint64_t> &ids_;
};

int ObAllVirtualAiGatewayEndpointStat::collect_data_()
{
  int ret = OB_SUCCESS;
  omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService *);
  if (OB_ISNULL(ai_service)) {
    data_collected_ = true;
  } else {
    const uint64_t tenant_id = MTL_ID();
    stat_infos_.set_attr(ObMemAttr(MTL_ID(), "AiGwEpStat"));

    ObSEArray<uint64_t, 16> gateway_ids;
    gateway_ids.set_attr(ObMemAttr(MTL_ID(), "AiGwEpStat"));
    CollectGatewayIdsFunc func(gateway_ids);
    ObSchemaGetterGuard schema_guard;
    omt::ObAiGatewayCircuitManager &mgr = ai_service->get_gateway_circuit_mgr();
    if (OB_FAIL(mgr.gateway_circuit_map_.foreach_refactored(func))) {
      LOG_WARN("failed to iterate gateway circuit map", K(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema service is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < gateway_ids.count(); ++i) {
      const uint64_t gateway_id = gateway_ids.at(i);
      const ObAIGatewaySchema *gateway_schema = nullptr;
      int tmp_ret = schema_guard.get_ai_gateway_schema(tenant_id, gateway_id, gateway_schema);
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to get gateway schema", K(tmp_ret), K(tenant_id), K(gateway_id));
      } else if (OB_ISNULL(gateway_schema)) {
        int push_ret = mgr.push_stale_gateway(gateway_id);
        if (OB_SUCCESS != push_ret) {
          LOG_WARN("failed to push stale gateway", K(push_ret), K(gateway_id));
        }
      } else {
        const ObString gateway_name = gateway_schema->get_name();

        ObAiGatewayCircuitState *entry = nullptr;
        ObAiGatewayStateRefGuard ref_guard;
        {
          GatewayStatReadOp read_op;
          int read_ret = mgr.gateway_circuit_map_.read_atomic(gateway_id, read_op);
          if (OB_SUCCESS == read_ret && OB_NOT_NULL(read_op.entry_)) {
            entry = read_op.entry_;
            ref_guard.adopt(entry);
          }
        }

        if (OB_NOT_NULL(entry)) {
          ObSpinLockGuard entry_guard(entry->lock_);
          const int64_t current_schema_version = gateway_schema->get_schema_version();
          if (entry->cached_schema_version_ < current_schema_version) {
            int refresh_ret = entry->refresh_from_schema(
                gateway_schema->get_endpoints(),
                gateway_schema->get_circuit_breaker(),
                current_schema_version);
            if (OB_SUCCESS != refresh_ret) {
              LOG_WARN("failed to refresh gateway state in virtual table query",
                       K(refresh_ret), K(gateway_id), K(current_schema_version));
            }
          }

          for (int64_t j = 0; OB_SUCC(ret) && j < entry->endpoints_.count(); ++j) {
            const ObAiGatewayEndpoint &ep = entry->endpoints_.at(j);
            ObAiEndpointCircuitState *ep_state = nullptr;
            if (j < entry->endpoint_states_.count()) {
              ep_state = entry->endpoint_states_.at(j);
              if (OB_NOT_NULL(ep_state)) {
                ep_state->get_sliding_window_mut().evict_expired_slots();
              }
            }

            EndpointStatInfo info;
            build_endpoint_stat_info(tenant_id, gateway_name, ep, ep_state,
                                       entry->cb_params_, info);
            if (OB_FAIL(stat_infos_.push_back(info))) {
              LOG_WARN("failed to push back endpoint status info", K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    data_collected_ = true;
  }

  return ret;
}

void ObAllVirtualAiGatewayEndpointStat::build_endpoint_stat_info(
    uint64_t tenant_id,
    const common::ObString &gateway_name,
    const share::ObAiGatewayEndpoint &ep,
    share::ObAiEndpointCircuitState *ep_state,
    const share::ObAiCircuitBreakerParams &cb_params,
    EndpointStatInfo &info)
{
  info.tenant_id_ = tenant_id;

  const int64_t gw_name_len = MIN(gateway_name.length(),
                                   static_cast<int64_t>(sizeof(info.gateway_name_) - 1));
  MEMCPY(info.gateway_name_, gateway_name.ptr(), gw_name_len);
  info.gateway_name_[gw_name_len] = '\0';

  const int64_t ep_name_len = MIN(ep.endpoint_name_.length(),
                                   static_cast<int64_t>(sizeof(info.endpoint_name_) - 1));
  MEMCPY(info.endpoint_name_, ep.endpoint_name_.ptr(), ep_name_len);
  info.endpoint_name_[ep_name_len] = '\0';

  if (OB_NOT_NULL(ep_state)) {
    const ObAiCircuitState st = ep_state->get_state();
    switch (st) {
      case ObAiCircuitState::CLOSED:    info.routing_status_ = "SERVING"; break;
      case ObAiCircuitState::OPEN:      info.routing_status_ = "BLOCKED"; break;
      case ObAiCircuitState::HALF_OPEN: info.routing_status_ = "PROBING"; break;
      default:                          info.routing_status_ = "SERVING"; break;
    }
    const int64_t total_success = ep_state->get_sliding_window().get_total_success();
    const int64_t total_fail = ep_state->get_sliding_window().get_total_fail();
    info.total_requests_ = total_success + total_fail;
    info.failure_rate_ = info.total_requests_ > 0
        ? total_fail * share::ObAiCircuitBreakerParams::FAILURE_RATE_PERCENT_BASE / info.total_requests_
        : 0;
    info.block_until_us_ = (st == ObAiCircuitState::OPEN)
        ? ep_state->get_open_ts()
              + cb_params.break_duration_seconds_ * share::ObAiCircuitBreakerParams::US_PER_SECOND
        : 0;
    info.last_failure_time_us_ = ep_state->get_last_failure_time_us();
  } else {
    info.routing_status_ = "SERVING";
    info.failure_rate_ = 0;
    info.total_requests_ = 0;
    info.block_until_us_ = 0;
    info.last_failure_time_us_ = 0;
  }
}

int ObAllVirtualAiGatewayEndpointStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;

  if (OB_UNLIKELY(!is_inited_ || nullptr == cells)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), KP(cur_row_.cells_), K(is_inited_));
  } else {
    if (!data_collected_ && OB_FAIL(collect_data_())) {
      LOG_WARN("failed to collect endpoint status data", K(ret));
    } else if (stat_pos_ >= stat_infos_.count()) {
      row = nullptr;
      ret = OB_ITER_END;
    } else {
      const EndpointStatInfo &info = stat_infos_.at(stat_pos_);
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        const uint64_t column_id = output_column_ids_.at(i);
        switch (column_id) {
          case SVR_IP: {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(addr_.get_port());
            break;
          }
          case TENANT_ID: {
            cells[i].set_int(info.tenant_id_);
            break;
          }
          case GATEWAY_NAME: {
            cells[i].set_varchar(ObString::make_string(info.gateway_name_));
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ENDPOINT_NAME: {
            cells[i].set_varchar(ObString::make_string(info.endpoint_name_));
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ROUTING_STATUS: {
            cells[i].set_varchar(ObString::make_string(info.routing_status_));
            cells[i].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case FAILURE_RATE: {
            cells[i].set_int(info.failure_rate_);
            break;
          }
          case TOTAL_REQUESTS: {
            cells[i].set_int(info.total_requests_);
            break;
          }
          case BLOCKED_UNTIL_TS: {
            if (0 == info.block_until_us_) {
              cells[i].set_null();
            } else {
              cells[i].set_timestamp(info.block_until_us_);
            }
            break;
          }
          case LAST_FAILED_TS: {
            if (0 == info.last_failure_time_us_) {
              cells[i].set_null();
            } else {
              cells[i].set_timestamp(info.last_failure_time_us_);
            }
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column id", K(ret), K(column_id), K(i),
                     K(output_column_ids_));
            break;
          }
        } // end switch
      } // end for-loop
      if (OB_SUCC(ret)) {
        row = &cur_row_;
      }
      ++stat_pos_;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
