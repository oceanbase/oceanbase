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

#include "ob_all_concurrency_object_pool.h"
#include "common/object/ob_object.h"
#include "share/config/ob_server_config.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "observer/ob_server_utils.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::observer;

ObAllConcurrencyObjectPool::ObAllConcurrencyObjectPool()
    : ObVirtualTableScannerIterator(), addr_(NULL), flls_(NULL, ObModIds::OB_VT_ALL_CONCUR_OBJ_POOL), fl_(NULL), idx_(0)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObObjFreeListList::get_freelists().get_info(flls_))) {
    SERVER_LOG(ERROR, "get freelists failed", K(ret));
  }
}

ObAllConcurrencyObjectPool::~ObAllConcurrencyObjectPool()
{
  reset();
}

void ObAllConcurrencyObjectPool::reset()
{
  addr_ = NULL;
  fl_ = NULL;
  idx_ = 0;
  flls_.reset();
}
int ObAllConcurrencyObjectPool::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (idx_ >= flls_.size()) {
    ret = OB_ITER_END;
  } else if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is not init", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    ObObj* cells = NULL;
    if (0 == col_count) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "col count could not be zero", K(ret));
    } else if (OB_ISNULL(cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
    } else if (cur_row_.count_ < output_column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "cur_row count is invalid", K(ret));
    } else {
      fl_ = flls_.at(idx_);
      if (OB_ISNULL(fl_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "fl_ is null", K(ret));
      } else {
        // ip
        ObString ipstr;
        for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); i++) {
          uint64_t col_id = output_column_ids_.at(i);
          switch (col_id) {
            case 16: {
              if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
                SERVER_LOG(ERROR, "get server ip failed", K(ret));
              } else {
                cells[i].set_varchar(ipstr);
                cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
            } break;
            case 17: {
              const int32_t port = addr_->get_port();
              cells[i].set_int(port);
            } break;
            case 18: {
              cells[i].set_varchar(fl_->get_name());
              cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            } break;
            case 19: {
              cells[i].set_int((fl_->get_allocated() - fl_->get_allocated_base()) * fl_->get_type_size());
            } break;
            case 20: {
              cells[i].set_int((fl_->get_used() - fl_->get_used_base()) * fl_->get_type_size());
            } break;
            case 21: {
              cells[i].set_int(fl_->get_used() - fl_->get_used_base());
            } break;
            case 22: {
              cells[i].set_int(fl_->get_type_size());
            } break;
            case 23: {
              cells[i].set_int(fl_->get_chunk_count());
            } break;
            case 24: {
              cells[i].set_int(fl_->get_chunk_byte_size());
            } break;
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(i));
              break;
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        row = &cur_row_;
        ++idx_;
      }
    }
  }
  return ret;
}
