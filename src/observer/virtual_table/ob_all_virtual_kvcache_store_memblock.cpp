/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "observer/virtual_table/ob_all_virtual_kvcache_store_memblock.h"

namespace oceanbase
{
namespace observer
{

ObAllVirtualKVCacheStoreMemblock::ObAllVirtualKVCacheStoreMemblock()
  : ObVirtualTableScannerIterator(),
    memblock_iter_(0),
    addr_(nullptr),
    ipstr_(),
    port_(0),
    memblock_infos_(),
    str_buf_()
{
}

ObAllVirtualKVCacheStoreMemblock::~ObAllVirtualKVCacheStoreMemblock()
{
  reset();
}

void ObAllVirtualKVCacheStoreMemblock::reset()
{
  ObVirtualTableScannerIterator::reset();
  memblock_iter_ = 0;
  addr_ = nullptr;
  port_ = 0;
  ipstr_.reset();
  str_buf_.reset();
  memblock_infos_.reset();
}

int ObAllVirtualKVCacheStoreMemblock::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  row = nullptr;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (memblock_iter_ >= memblock_infos_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(process_row(memblock_infos_.at(memblock_iter_++)))) {
    SERVER_LOG(WARN, "Fail to process current row", K(ret), K(memblock_iter_));
  } else {
    row = &cur_row_;
  }

  return ret;
}

int ObAllVirtualKVCacheStoreMemblock::set_ip()
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
    port_ = addr_->get_port();
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "Failed to write string", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualKVCacheStoreMemblock::inner_open()
{
  int ret = OB_SUCCESS;

  memblock_infos_.reset();
  if (OB_FAIL(set_ip())) {
    SERVER_LOG(WARN, "Fail to get ip in ObAllVirtualKVCacheStoreMemblock", K(ret));
  } else if (OB_FAIL(ObKVGlobalCache::get_instance().get_memblock_info(effective_tenant_id_, memblock_infos_))) {  // get memblock info from kvcache
    SERVER_LOG(WARN, "Fail to get memblock information from global cache", K(ret));
  }

  return ret;
}

int ObAllVirtualKVCacheStoreMemblock::process_row(const ObKVCacheStoreMemblockInfo &info)
{
  int ret = OB_SUCCESS;

  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "Invalid argument", K(ret), K(info));
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
          cur_row_.cells_[cell_idx].set_int(info.tenant_id_);
          break;
        }
        case CACHE_ID : {
          cur_row_.cells_[cell_idx].set_int(info.cache_id_);
          break;
        }
        case CACHE_NAME : {
          cur_row_.cells_[cell_idx].set_varchar(info.cache_name_);
          cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case MEMBLOCK_PTR : {
          cur_row_.cells_[cell_idx].set_varchar(info.memblock_ptr_);
          cur_row_.cells_[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case REF_COUNT : {
          cur_row_.cells_[cell_idx].set_int(info.ref_count_);
          break;
        }
        case STATUS : {
          cur_row_.cells_[cell_idx].set_int(info.using_status_);
          break;
        }
        case POLICY : {
          cur_row_.cells_[cell_idx].set_int(info.policy_);
          break;
        }
        case KV_CNT : {
          cur_row_.cells_[cell_idx].set_int(info.kv_cnt_);
          break;
        }
        case GET_CNT : {
          cur_row_.cells_[cell_idx].set_int(info.get_cnt_);
          break;
        }
        case RECENT_GET_CNT : {
          cur_row_.cells_[cell_idx].set_int(info.recent_get_cnt_);
          break;
        }
        case PRIORITY : {
          cur_row_.cells_[cell_idx].set_int(info.priority_);
          break;
        }
        case SCORE : {
          static const int64_t MAX_DOUBLE_PRINT_SIZE = 64;
          char buf[MAX_DOUBLE_PRINT_SIZE];
          memset(buf, 0, MAX_DOUBLE_PRINT_SIZE);
          str_buf_.reset();
          number::ObNumber num;
          double value = info.score_;
          if (OB_UNLIKELY(0 > snprintf(buf, MAX_DOUBLE_PRINT_SIZE, "%lf", value))) {
            ret = OB_IO_ERROR;
            SERVER_LOG(WARN, "snprintf fail", K(ret), K(errno), KERRNOMSG(errno));
          } else if (OB_FAIL(num.from(buf, str_buf_))) {
            SERVER_LOG(WARN, "Fail to cast to number", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
          } else {
            cur_row_.cells_[cell_idx].set_number(num);
          }
          break;
        }
        case ALIGN_SIZE : {
          cur_row_.cells_[cell_idx].set_int(info.align_size_);
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "Invalid column id", K(ret), K(cell_idx), K(col_id), K(output_column_ids_));
          break;
        }
      }
    }
  }
  return ret;
}

} // observer
} // oceanbase