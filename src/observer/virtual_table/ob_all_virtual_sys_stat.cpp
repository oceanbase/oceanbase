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

#define USING_LOG_PREFIX SERVER

#include "ob_all_virtual_sys_stat.h"
#include "lib/ob_running_mode.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/cache/ob_kv_storecache.h"
#include "storage/tx_storage/ob_tenant_freezer.h"

namespace oceanbase
{
namespace observer
{

ObAllVirtualSysStat::ObAllVirtualSysStat()
    : ObVirtualTableScannerIterator(),
    ObMultiTenantOperator(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    stat_iter_(0),
    tenant_id_(OB_INVALID_TENANT_ID),
    diag_info_(allocator_)
{
}

ObAllVirtualSysStat::~ObAllVirtualSysStat()
{
  reset();
}

void ObAllVirtualSysStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  stat_iter_ = 0;
  tenant_id_ = OB_INVALID_ID;
  diag_info_.reset();
}

int ObAllVirtualSysStat::set_ip(common::ObAddr *addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr){
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObAllVirtualSysStat::update_all_stats(const int64_t tenant_id, ObStatEventSetStatArray &stat_events)
{
  int ret = OB_SUCCESS;
  if (is_virtual_tenant_id(tenant_id)) {
    if (OB_FAIL(update_all_stats_(tenant_id, stat_events))) {
      SERVER_LOG(WARN, "Fail to update_all_stats_ for virtual tenant", K(ret), K(tenant_id));
    }
  } else {
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(update_all_stats_(tenant_id, stat_events))) {
        SERVER_LOG(WARN, "Fail to update_all_stats_ for tenant", K(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObAllVirtualSysStat::update_all_stats_(const int64_t tenant_id, ObStatEventSetStatArray &stat_events)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_cache_size_(tenant_id, stat_events))) {
    SERVER_LOG(WARN, "Fail to get cache size", K(ret));
  } else {
    int64_t unused = 0;
    //ignore ret
    if (is_virtual_tenant_id(tenant_id)) {
      ObVirtualTenantManager &tenant_mgr = common::ObVirtualTenantManager::get_instance();
      tenant_mgr.get_tenant_mem_limit(tenant_id,
          stat_events.get(ObStatEventIds::MIN_MEMORY_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
          stat_events.get(ObStatEventIds::MAX_MEMORY_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_);
    } else {
        storage::ObTenantFreezer *freezer = MTL(storage::ObTenantFreezer *);
        freezer->get_tenant_memstore_cond(
            stat_events.get(ObStatEventIds::ACTIVE_MEMSTORE_USED - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
            stat_events.get(ObStatEventIds::TOTAL_MEMSTORE_USED - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
            stat_events.get(ObStatEventIds::MAJOR_FREEZE_TRIGGER - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
            stat_events.get(ObStatEventIds::MEMSTORE_LIMIT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
            unused);
        freezer->get_tenant_mem_limit(
            stat_events.get(ObStatEventIds::MIN_MEMORY_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_,
            stat_events.get(ObStatEventIds::MAX_MEMORY_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_);
    }

    // not supported now.
    stat_events.get(ObStatEventIds::DISK_USAGE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ = 0;

    stat_events.get(ObStatEventIds::OBLOGGER_WRITE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_write_size() : 0;
    stat_events.get(ObStatEventIds::ELECTION_WRITE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_elec_write_size() : 0;
    stat_events.get(ObStatEventIds::ROOTSERVICE_WRITE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_rs_write_size() : 0;
    stat_events.get(ObStatEventIds::OBLOGGER_TOTAL_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_total_write_count() : 0;
    stat_events.get(ObStatEventIds::ELECTION_TOTAL_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_elec_total_write_count() : 0;
    stat_events.get(ObStatEventIds::ROOTSERVICE_TOTAL_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_rs_total_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_ERROR_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_error_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_WARN_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_warn_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_INFO_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_info_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_TRACE_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_trace_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_DEBUG_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_debug_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_LOG_FLUSH_SPEED - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_async_flush_log_speed() : 0;


    stat_events.get(ObStatEventIds::ASYNC_GENERIC_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_generic_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_USER_REQUEST_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_user_request_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_DATA_MAINTAIN_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_data_maintain_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_ROOT_SERVICE_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_root_service_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_SCHEMA_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_schema_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_FORCE_ALLOW_LOG_WRITE_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_force_allow_log_write_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_GENERIC_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_generic_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_USER_REQUEST_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_user_request_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_DATA_MAINTAIN_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_data_maintain_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_ROOT_SERVICE_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_root_service_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_SCHEMA_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_schema_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_FORCE_ALLOW_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_force_allow_log_dropped_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_ERROR_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_error_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_WARN_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_warn_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_INFO_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_info_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_TRACE_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_trace_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_DEBUG_LOG_DROPPED_COUNT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_dropped_debug_log_count() : 0;
    stat_events.get(ObStatEventIds::ASYNC_LOG_FLUSH_SPEED - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
      (OB_SYS_TENANT_ID == tenant_id) ? OB_LOGGER.get_async_flush_log_speed() : 0;
    stat_events.get(ObStatEventIds::MEMORY_HOLD_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? lib::AChunkMgr::instance().get_hold() : 0;
    stat_events.get(ObStatEventIds::MEMORY_USED_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? lib::AChunkMgr::instance().get_used() : 0;
    stat_events.get(ObStatEventIds::MEMORY_FREE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? lib::AChunkMgr::instance().get_freelist_hold() : 0;
    stat_events.get(ObStatEventIds::IS_MINI_MODE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? (lib::is_mini_mode() ? 1 : 0) : -1;
    stat_events.get(ObStatEventIds::STANDBY_FETCH_LOG_BYTES - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? global_poc_server.get_ratelimit_rxbytes() : -1;
    stat_events.get(ObStatEventIds::STANDBY_FETCH_LOG_BANDWIDTH_LIMIT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? global_poc_server.get_ratelimit() : -1;
    stat_events.get(ObStatEventIds::MEMORY_LIMIT - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? GMEMCONF.get_server_memory_limit() : 0;
    stat_events.get(ObStatEventIds::SYSTEM_MEMORY - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? GMEMCONF.get_reserved_server_memory() : 0;
    stat_events.get(ObStatEventIds::HIDDEN_SYS_MEMORY - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_ =
        (OB_SYS_TENANT_ID == tenant_id) ? GMEMCONF.get_hidden_sys_memory() : 0;

    int ret_bk = ret;
    if (NULL != GCTX.omt_) {
      double cpu_usage = .0;
      if (OB_SUCC(GCTX.omt_->get_tenant_cpu_usage(tenant_id, cpu_usage))) {
        stat_events.get(ObStatEventIds::CPU_USAGE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = static_cast<int64_t>(cpu_usage * 100);
        stat_events.get(ObStatEventIds::MEMORY_USAGE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = lib::get_tenant_memory_hold(tenant_id);
        stat_events.get(ObStatEventIds::KV_CACHE_HOLD - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = lib::get_tenant_cache_hold(tenant_id);
      } else {
        // it is ok to not have any records
      }

      int64_t worker_time = 0;
      if (OB_SUCC(GCTX.omt_->get_tenant_worker_time(tenant_id, worker_time))) {
        stat_events.get(ObStatEventIds::WORKER_TIME - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = worker_time;
      } else {
        // it is ok to not have any records
      }

      double min_cpu = .0;
      double max_cpu = .0;
      if (OB_SUCC(GCTX.omt_->get_tenant_cpu(tenant_id, min_cpu, max_cpu))) {
        stat_events.get(ObStatEventIds::MIN_CPUS - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = static_cast<int64_t>(min_cpu * 100);
        stat_events.get(ObStatEventIds::MAX_CPUS - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = static_cast<int64_t>(max_cpu * 100);
      } else {
        // it is ok to not have any records
      }
    }
    if (NULL != GCTX.ob_service_) {
      stat_events.get(ObStatEventIds::OBSERVER_PARTITION_TABLE_UPATER_USER_QUEUE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
          = 0;
      stat_events.get(ObStatEventIds::OBSERVER_PARTITION_TABLE_UPATER_SYS_QUEUE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
          = 0;
      stat_events.get(ObStatEventIds::OBSERVER_PARTITION_TABLE_UPATER_CORE_QUEUE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
          = 0;
    }

    int64_t cpu_time = 0;
    if (OB_SUCC(GCTX.omt_->get_tenant_cpu_time(tenant_id, cpu_time))) {
      stat_events.get(ObStatEventIds::CPU_TIME - ObStatEventIds::STAT_EVENT_ADD_END - 1)->stat_value_
          = cpu_time;
    } else {
      // it is ok to not have any records
    }

    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        MTL_SWITCH(tenant_id) {
          auto *tenant_base = MTL_CTX();
          int64_t max_sess_num = tenant_base->get_max_session_num(tenant_config->_resource_limit_max_session_num);
          stat_events.get(ObStatEventIds::MAX_SESSION_NUM - ObStatEventIds::STAT_EVENT_ADD_END - 1)->stat_value_
              = max_sess_num;
        }
      }
    }

    ret = ret_bk;
  }
  return ret;
}

int ObAllVirtualSysStat::get_the_diag_info(
    const uint64_t tenant_id,
    common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  diag_info.reset();
  if (OB_FAIL(common::ObDIGlobalTenantCache::get_instance().get_the_diag_info(tenant_id, diag_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "Fail to get tenant stat event", KR(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObAllVirtualSysStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObAllVirtualSysStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  const int64_t col_count = output_column_ids_.count();

  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    if (MTL_ID() != tenant_id_) {
      tenant_id_ = MTL_ID(); // new tenant, init diag info
      if (OB_FAIL(set_ip(addr_))){
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (OB_FAIL(get_the_diag_info(tenant_id_, diag_info_))) {
        SERVER_LOG(WARN, "get diag info fail", K(ret), K(tenant_id_));
      } else {
        stat_iter_ = 0;
        ObStatEventSetStatArray &stat_events = diag_info_.get_set_stat_stats();
        if (OB_FAIL(update_all_stats_(tenant_id_, stat_events))) {
          SERVER_LOG(WARN, "update all stats fail", K(ret), K(tenant_id_));
        }
      }
    }

    if (OB_SUCC(ret) && stat_iter_ >= ObStatEventIds::STAT_EVENT_SET_END) {
      // current tenant stat finish
      ret = OB_ITER_END;
    }

    if (OB_SUCC(ret)) {
      uint64_t cell_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch(col_id) {
          case TENANT_ID: {
            cells[cell_idx].set_int(tenant_id_);
            break;
          }
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(port_);
            break;
          }
          case STATISTIC: {
            if (stat_iter_ < ObStatEventIds::STAT_EVENT_ADD_END) {
              cells[cell_idx].set_int(stat_iter_);
            } else {
              cells[cell_idx].set_int(stat_iter_ - 1);
            }
            break;
          }
          case VALUE: {
            if (stat_iter_ < ObStatEventIds::STAT_EVENT_ADD_END) {
              ObStatEventAddStat *stat = diag_info_.get_add_stat_stats().get(stat_iter_);
              if (NULL == stat) {
                ret = OB_INVALID_ARGUMENT;
                SERVER_LOG(WARN, "The argument is invalid, ", K(stat_iter_), K(ret));
              } else {
                cells[cell_idx].set_int(stat->stat_value_);
              }
            } else {
              ObStatEventSetStat *stat = diag_info_.get_set_stat_stats().get(stat_iter_ - ObStatEventIds::STAT_EVENT_ADD_END -1);
              if (NULL == stat) {
                ret = OB_INVALID_ARGUMENT;
                SERVER_LOG(WARN, "The argument is invalid, ", K(stat_iter_), K(ret));
              } else {
                cells[cell_idx].set_int(stat->stat_value_);
              }
            }
            break;
          }
          case VALUE_TYPE: {
            if (stat_iter_ < ObStatEventIds::STAT_EVENT_ADD_END) {
              cells[cell_idx].set_varchar("ADD_VALUE");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } else {
              cells[cell_idx].set_varchar("SET_VALUE");
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            }
            break;
          }
          case STAT_ID: {
            cells[cell_idx].set_int(OB_STAT_EVENTS[stat_iter_].stat_id_);
            break;
          }
          case NAME: {
            cells[cell_idx].set_varchar(OB_STAT_EVENTS[stat_iter_].name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case CLASS: {
            cells[cell_idx].set_int(OB_STAT_EVENTS[stat_iter_].stat_class_);
            break;
          }
          case CAN_VISIBLE: {
            cells[cell_idx].set_bool(OB_STAT_EVENTS[stat_iter_].can_visible_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      stat_iter_++;
      row = &cur_row_;
      if (ObStatEventIds::STAT_EVENT_ADD_END == stat_iter_) {
        stat_iter_++;
      }
    }
  }
  return ret;
}

int ObAllVirtualSysStat::get_cache_size_(const int64_t tenant_id, ObStatEventSetStatArray &stat_events)
{
  int ret = OB_SUCCESS;
  ObArray<ObKVCacheInstHandle> inst_handles;
  if (OB_FAIL(ObKVGlobalCache::get_instance().get_cache_inst_info(tenant_id, inst_handles))) {
    SERVER_LOG(WARN, "Fail to get tenant cache infos, ", K(ret));
  } else {
    ObKVCacheInst * inst = NULL;
    for (int64_t i = 0; i < inst_handles.count(); ++i) {
      inst = inst_handles.at(i).get_inst();
      if (OB_ISNULL(inst)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "ObKVCacheInstHandle with NULL ObKVCacheInst", K(ret));
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "opt_table_stat_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::OPT_TAB_STAT_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "opt_column_stat_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::OPT_TAB_COL_STAT_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "tablet_ls_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::TABLET_LS_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "index_block_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::INDEX_BLOCK_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "user_block_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::USER_BLOCK_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "user_row_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::USER_ROW_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "bf_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::BLOOM_FILTER_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else if (0 == STRNCMP(inst->status_.config_->cache_name_, "log_kv_cache", MAX_CACHE_NAME_LENGTH)) {
        stat_events.get(ObStatEventIds::LOG_KV_CACHE_SIZE - ObStatEventIds::STAT_EVENT_ADD_END -1)->stat_value_
            = inst->status_.map_size_ + inst->status_.store_size_;
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

void ObAllVirtualSysStat::release_last_tenant()
{
  diag_info_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
}

bool ObAllVirtualSysStat::is_need_process(uint64_t tenant_id)
{
  return (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_);
}

int ObAllVirtualSysStatI1::get_the_diag_info(
    const uint64_t tenant_id,
    common::ObDiagnoseTenantInfo &diag_info)
{
  int ret = OB_SUCCESS;
  diag_info.reset();
  if (!is_contain(get_index_ids(), (int64_t)tenant_id)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(common::ObDIGlobalTenantCache::get_instance().get_the_diag_info(tenant_id, diag_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "Fail to get tenant stat event", KR(ret), K(tenant_id));
    }
  }
  return ret;
}
} /* namespace observer */
} /* namespace oceanbase */
