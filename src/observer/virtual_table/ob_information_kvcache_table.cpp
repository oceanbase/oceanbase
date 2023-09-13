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

#include "observer/virtual_table/ob_information_kvcache_table.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace observer
{

ObInfoSchemaKvCacheTable::ObInfoSchemaKvCacheTable()
    : ObVirtualTableScannerIterator(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    cache_iter_(0),
    str_buf_(),
    arenallocator_(),
    tenant_di_info_(allocator_),
    tenant_dis_()
{
}

ObInfoSchemaKvCacheTable::~ObInfoSchemaKvCacheTable()
{
  reset();
}

void ObInfoSchemaKvCacheTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  cache_iter_ = 0;
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  inst_handles_.reset();
  str_buf_.reset();
  for (int64_t i = 0; i  < OB_ROW_MAX_COLUMNS_COUNT; i++) {
    cells_[i].reset();
  }
  arenallocator_.reset();
  tenant_di_info_.reset();
  tenant_dis_.reset();
}

int ObInfoSchemaKvCacheTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  row = nullptr;
  ObKVCacheInst * inst = NULL;
  ObDiagnoseTenantInfo *tenant_info = nullptr;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Invalid allocator, not init", K(ret), KP(allocator_));
  } else if (OB_FAIL(get_handles(inst, tenant_info))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "Fail to get cache inst or tenant diagnose info", K(ret));
    }
  } else if (OB_FAIL(set_diagnose_info(inst, tenant_info))) {
    SERVER_LOG(WARN, "Fail to set diagnose info for cache inst", K(ret));
  } else if (OB_FAIL(process_row(inst))) {
    SERVER_LOG(WARN, "Fail to process current row", K(ret));
  } else {
    row = &cur_row_;
  }

  return ret;
}

int ObInfoSchemaKvCacheTable::set_ip()
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (nullptr == addr_) {
    ret = OB_ENTRY_NOT_EXIST;
    SERVER_LOG(WARN, "Null address", K(ret), KP(addr_));
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "Fail to cast ip to string", K(ret));
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "Failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObInfoSchemaKvCacheTable::inner_open()
{
  int ret = OB_SUCCESS;

  inst_handles_.reuse();
  if (OB_FAIL(set_ip())) {
    SERVER_LOG(WARN, "Fail to set ip from addr", K(ret), K(addr_));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_cache_inst_info(effective_tenant_id_, inst_handles_))) {
    SERVER_LOG(WARN, "Fail to get cache info", K(ret), K(effective_tenant_id_));
  } else if (OB_FAIL(get_tenant_info())) {
    SERVER_LOG(WARN, "Fail to get tenant info", K(ret));
  }

  return ret;
}

int ObInfoSchemaKvCacheTable::get_tenant_info()
{
  int ret = OB_SUCCESS;

  if (oceanbase::lib::is_diagnose_info_enabled()) {
    if (is_sys_tenant(effective_tenant_id_)) {
      arenallocator_.reuse();
      tenant_dis_.reuse();
      if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_all_stat_event(arenallocator_, tenant_dis_))) {
        SERVER_LOG(WARN, "Fail to get all stat event", K(ret));
      }
    } else {
      tenant_di_info_.reset();
      if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_the_diag_info(effective_tenant_id_, tenant_di_info_))) {
        SERVER_LOG(WARN, "Fail to get tenant stat event", K(ret), K(effective_tenant_id_));
      }
    }
  }

  return ret;
}

int ObInfoSchemaKvCacheTable::get_handles(ObKVCacheInst *&inst, ObDiagnoseTenantInfo *&tenant_info)
{
  int ret = OB_SUCCESS;

  inst = nullptr;
  tenant_info = nullptr;
  if (cache_iter_ >= inst_handles_.count()) {
    ret = OB_ITER_END;
  } else {
    inst = inst_handles_.at(cache_iter_++).get_inst();
  }
  if (OB_FAIL(ret)) {
  } else if (!oceanbase::lib::is_diagnose_info_enabled()) {
  } else if (is_sys_tenant(effective_tenant_id_)) {
    for (int64_t i = 0 ; i < tenant_dis_.count() ; ++i) {
      if (tenant_dis_.at(i).first == inst->tenant_id_) {
        tenant_info = tenant_dis_.at(i).second;
        break;
      }
    }
  } else {
    tenant_info = &tenant_di_info_;
  }

  return ret;
}

int ObInfoSchemaKvCacheTable::set_diagnose_info(ObKVCacheInst *inst, ObDiagnoseTenantInfo *tenant_info)
{
  int ret = OB_SUCCESS;

  if (!oceanbase::lib::is_diagnose_info_enabled()) {
  } else if (nullptr == tenant_info || nullptr == inst) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid argument", K(ret), KP(inst), KP(tenant_info));
  } else if (nullptr == inst->status_.config_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "Unexpected null cache inst config", KP(inst->status_.config_));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"user_block_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_MISS);
    inst->status_.total_hit_cnt_.set( GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"user_row_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"bf_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"fuse_row_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"tablet_ls_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::LOCATION_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::LOCATION_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"schema_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"schema_history_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_HISTORY_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_HISTORY_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"opt_table_stat_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::OPT_TABLE_STAT_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::OPT_TABLE_STAT_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"opt_column_stat_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::OPT_COLUMN_STAT_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::OPT_COLUMN_STAT_CACHE_HIT));
  } else if (0 == strcmp(inst->status_.config_->cache_name_,"opt_ds_stat_cache")) {
    inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::OPT_DS_STAT_CACHE_MISS);
    inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::OPT_DS_STAT_CACHE_HIT));
  }

  return ret;
}

int ObInfoSchemaKvCacheTable::process_row(const ObKVCacheInst *inst)
{
  int ret = OB_SUCCESS;

  uint64_t cell_idx = 0;
  cur_row_.cells_ = cells_;
  cur_row_.count_ = reserved_column_cnt_;
  for (int64_t i = 0 ; OB_SUCC(ret) && i < output_column_ids_.count() ; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch(col_id) {
      case TENANT_ID: {
        cells_[cell_idx].set_int(inst->tenant_id_);
        break;
      }
      case SVR_IP: {
        cells_[cell_idx].set_varchar(ipstr_);
        cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells_[cell_idx].set_int(port_);
        break;
      }
      case CACHE_NAME: {
        if (NULL != inst->status_.config_) {
          cells_[cell_idx].set_varchar(inst->status_.config_->cache_name_);
          cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      }
      case CACHE_ID: {
        cells_[cell_idx].set_int(inst->cache_id_);
        break;
      }
      case CACHE_SIZE: {
        cells_[cell_idx].set_int(inst->status_.store_size_ + inst->status_.map_size_);
        break;
      }
      case PRIORITY: {
        if (NULL != inst->status_.config_) {
          cells_[cell_idx].set_int(inst->status_.config_->priority_);
        } else {
          cells_[cell_idx].set_int(0);
        }
        break;
      }
      case CACHE_STORE_SIZE: {
        cells_[cell_idx].set_int(inst->status_.store_size_);
        break;
      }
      case CACHE_MAP_SIZE: {
        cells_[cell_idx].set_int(inst->status_.map_size_);
        break;
      }
      case KV_CNT: {
        cells_[cell_idx].set_int(inst->status_.kv_cnt_);
        break;
      }
      case HIT_RATIO: {
        str_buf_.reset();
        number::ObNumber num;
        double value = inst->status_.get_hit_ratio() * 100;
        static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;
        char buf[MAX_DOUBLE_PRINT_SIZE];
        memset(buf, 0, MAX_DOUBLE_PRINT_SIZE);
        if (OB_UNLIKELY(0 > snprintf(buf, MAX_DOUBLE_PRINT_SIZE, "%lf", value))) {
          ret = OB_IO_ERROR;
          SERVER_LOG(WARN, "Fail to snprintf hit ratio", K(ret), K(errno), KERRNOMSG(errno));
        } else if (OB_FAIL(num.from(buf, str_buf_))) {
          SERVER_LOG(WARN, "Fail to cast to number", K(ret));
        } else {
          cells_[cell_idx].set_number(num);
        }
        break;
      }
      case TOTAL_PUT_CNT: {
        cells_[cell_idx].set_int(inst->status_.total_put_cnt_.value());
        break;
      }
      case TOTAL_HIT_CNT: {
        cells_[cell_idx].set_int(inst->status_.total_hit_cnt_.value());
        break;
      }
      case TOTAL_MISS_CNT: {
        cells_[cell_idx].set_int(inst->status_.total_miss_cnt_);
        break;
      }
      case HOLD_SIZE: {
        cells_[cell_idx].set_int(inst->status_.hold_size_);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "Invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
        break;
      }
    }
    ++cell_idx;
  }

  return ret;
}


}/* ns observer*/
}/* ns oceanbase */
