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

#include "observer/virtual_table/ob_all_virtual_kvcache_handle_leak_info.h"
namespace oceanbase
{
namespace observer
{
ObAllVirtualKVCacheHandleLeakInfo::ObAllVirtualKVCacheHandleLeakInfo()
  : addr_(nullptr),
    ipstr_(),
    port_(0),
    cache_name_(),
    backtrace_(),
    opened_(false),
    map_info_(),
    map_iter_()
{
  MEMSET(cache_name_, 0, MAX_CACHE_NAME_LENGTH);
  MEMSET(backtrace_, 0, 512);
}
ObAllVirtualKVCacheHandleLeakInfo::~ObAllVirtualKVCacheHandleLeakInfo()
{
  reset();
}
void ObAllVirtualKVCacheHandleLeakInfo::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = nullptr;
  ipstr_.reset();
  port_ = 0;
  MEMSET(cache_name_, 0, MAX_CACHE_NAME_LENGTH);
  opened_ = false;
  map_info_.reuse();
}
int ObAllVirtualKVCacheHandleLeakInfo::inner_get_next_row(ObNewRow *&row)
{
  INIT_SUCC(ret);
  if (OB_FAIL(process_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "Fail to process row", K(ret));
    }
  } else {
    row = &cur_row_;
    ++map_iter_;
  }
  return ret;
}
int ObAllVirtualKVCacheHandleLeakInfo::set_ip()
{
  INIT_SUCC(ret);
  char ipbuf[common::OB_IP_STR_BUFF];
  if (nullptr == addr_) {
    ret = OB_ENTRY_NOT_EXIST;
    SERVER_LOG(WARN, "Null address", K(ret), KP(addr_));
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "Fail to cast ip to string", K(ret));
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    port_ = addr_->get_port();
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "Failed to write string", K(ret));
    }
  }
  return ret;
}
int ObAllVirtualKVCacheHandleLeakInfo::inner_open()
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(opened_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "Unexpected opened", K(opened_));
  } else if (OB_FAIL(set_ip())) {
    SERVER_LOG(WARN, "Fail to set ip", K(ret));
  } else if (OB_FAIL(map_info_.create(MAP_BUCKET_NUM, "CACHE_CHECKER_T", "CACHE_CHECKER_T"))) {
    SERVER_LOG(WARN, "Fail to create map info", K(ret));
  } else if (OB_FAIL(ObKVCacheHandleRefChecker::get_instance().get_aggregate_bt_info(map_info_))) {
    SERVER_LOG(WARN, "Fail to get aggregated backtrace info", K(ret));
  } else {
    opened_ = true;
    map_iter_ = map_info_.begin();
  }
  return ret;
}
int ObAllVirtualKVCacheHandleLeakInfo::process_row()
{
  INIT_SUCC(ret);
  if (OB_UNLIKELY(!opened_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "Unexpected error : unopened hashmap", K(ret), K(opened_));
  } else if (map_info_.end() == map_iter_) {
    ret = OB_ITER_END;
  } else {
    cur_row_.count_ = reserved_column_cnt_;
    for (int64_t cell_idx = 0 ; OB_SUCC(ret) && cell_idx < output_column_ids_.count() ; ++cell_idx) {
      uint64_t col_id = output_column_ids_.at(cell_idx);
      switch (col_id) {
        case SVR_IP : {
          cur_row_.cells_[cell_idx].set_varchar(ipstr_);
          cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case SVR_PORT : {
          cur_row_.cells_[cell_idx].set_int(port_);
          break;
        }
        case TENANT_ID : {
          cur_row_.cells_[cell_idx].set_int(map_iter_->first.tenant_id_);
          break;
        }
        case CACHE_ID : {
          cur_row_.cells_[cell_idx].set_int(map_iter_->first.cache_id_);
          break;
        }
        case CACHE_NAME : {
          if (OB_FAIL(ObKVGlobalCache::get_instance().get_cache_name(map_iter_->first.cache_id_, cache_name_))) {
            SERVER_LOG(WARN, "Fail to get cache name", K(ret), K(map_iter_->first.cache_id_));
          } else {
            cur_row_.cells_[cell_idx].set_varchar(cache_name_);
            cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case HOLD_COUNT : {
          cur_row_.cells_[cell_idx].set_int(map_iter_->second);
          break;
        }
        case BACKTRACE : {
          MEMCPY(backtrace_, map_iter_->first.bt_, 512);
          cur_row_.cells_[cell_idx].set_varchar(backtrace_);
          cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "Invalid column id", K(ret), K(cell_idx), K(col_id), K(output_column_ids_));
          break;
      }
    }
  }
  return ret;
}
};  // observer
};  // oceanbase