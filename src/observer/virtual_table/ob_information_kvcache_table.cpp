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

namespace oceanbase {
namespace observer {

ObInfoSchemaKvCacheTable::ObInfoSchemaKvCacheTable()
    : ObVirtualTableScannerIterator(),
      addr_(NULL),
      ipstr_(),
      port_(0),
      cache_iter_(0),
      str_buf_(),
      arenallocator_(),
      tenant_dis_()
{}

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
  for (int64_t i = 0; i < OB_ROW_MAX_COLUMNS_COUNT; i++) {
    cells_[i].reset();
  }
  arenallocator_.reset();
  tenant_dis_.reset();
}

int ObInfoSchemaKvCacheTable::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
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

int ObInfoSchemaKvCacheTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    ObKVCacheInst* inst = NULL;
    const int64_t col_count = output_column_ids_.count();
    cur_row_.cells_ = cells_;
    cur_row_.count_ = reserved_column_cnt_;

    if (0 == cache_iter_) {
      inst_handles_.reuse();
      if (OB_SUCCESS != (ret = set_ip(addr_))) {
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_all_cache_info(inst_handles_))) {
        SERVER_LOG(WARN, "Fail to get all cache info, ", K(ret));
      }
      if (oceanbase::lib::is_diagnose_info_enabled()) {
        arenallocator_.reuse();
        tenant_dis_.reuse();
        if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_all_stat_event(arenallocator_, tenant_dis_))) {
          SERVER_LOG(WARN, "Fail to get all_stat_event when diagnose_info_enabled, ", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (cache_iter_ >= inst_handles_.count()) {
        ret = OB_ITER_END;
      } else {
        inst = inst_handles_.at(cache_iter_).get_inst();
        if (OB_ISNULL(inst)) {
          ret = OB_ERR_SYS;
          SERVER_LOG(WARN, "kvcache inst should not be null", K(ret));
        }
      }
    }
    if (OB_SUCCESS == ret && oceanbase::lib::is_diagnose_info_enabled()) {
      // use diagnose_info to set cache miss_cnt and hit_cnt
      ObDiagnoseTenantInfo* tenant_info = NULL;
      for (int64_t i = 0; i < tenant_dis_.count(); ++i) {
        if (tenant_dis_.at(i).first == inst_handles_.at(cache_iter_).get_inst()->tenant_id_) {
          tenant_info = tenant_dis_.at(i).second;
          break;
        }
      }
      if (OB_LIKELY(NULL != tenant_info && NULL != inst->status_.config_)) {
        if (0 == strcmp(inst->status_.config_->cache_name_, "block_index_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_INDEX_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_INDEX_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "user_block_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::BLOCK_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "user_row_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::ROW_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "bf_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::BLOOM_FILTER_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "fuse_row_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::FUSE_ROW_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "location_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::LOCATION_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::LOCATION_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "schema_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::SCHEMA_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "clog_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::CLOG_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::CLOG_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "index_clog_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::INDEX_CLOG_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::INDEX_CLOG_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "user_tab_col_stat_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::USER_TAB_COL_STAT_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::USER_TAB_COL_STAT_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "user_table_stat_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::USER_TABLE_STAT_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::USER_TABLE_STAT_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "opt_table_stat_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::OPT_TABLE_STAT_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::OPT_TABLE_STAT_CACHE_HIT));
        } else if (0 == strcmp(inst->status_.config_->cache_name_, "opt_column_stat_cache")) {
          inst->status_.total_miss_cnt_ = GLOBAL_EVENT_GET(ObStatEventIds::OPT_COLUMN_STAT_CACHE_MISS);
          inst->status_.total_hit_cnt_.set(GLOBAL_EVENT_GET(ObStatEventIds::OPT_COLUMN_STAT_CACHE_HIT));
        }
      }
    }
    if (OB_SUCCESS == ret) {
      str_buf_.reset();
      uint64_t cell_idx = 0;
      double value = 0;
      char buf[MAX_DOUBLE_PRINT_SIZE];
      memset(buf, 0, MAX_DOUBLE_PRINT_SIZE);
      number::ObNumber num;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch (col_id) {
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
            memset(buf, 0, MAX_DOUBLE_PRINT_SIZE);
            value = inst->status_.get_hit_ratio() * 100;
            if (OB_UNLIKELY(0 > snprintf(buf, MAX_DOUBLE_PRINT_SIZE, "%lf", value))) {
              ret = OB_IO_ERROR;
              SERVER_LOG(WARN, "snprintf fail", K(ret), K(errno), KERRNOMSG(errno));
            } else if (OB_SUCCESS == (ret = num.from(buf, str_buf_))) {
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
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        } else if (OB_ERR_UNEXPECTED != ret) {
          SERVER_LOG(WARN, "failed cast to number", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
        }
      }
    }

    if (OB_SUCC(ret)) {
      cache_iter_++;
      row = &cur_row_;
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
