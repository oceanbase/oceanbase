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

#include "ob_all_virtual_storage_meta_memory_status.h"
#include "observer/ob_server.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace observer
{
ObAllVirtualStorageMetaMemoryStatus::ObAllVirtualStorageMetaMemoryStatus()
  : ObVirtualTableScannerIterator(),
    addr_(),
    pool_idx_(0)
{
}

ObAllVirtualStorageMetaMemoryStatus::~ObAllVirtualStorageMetaMemoryStatus()
{
  reset();
}

void ObAllVirtualStorageMetaMemoryStatus::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualStorageMetaMemoryStatus::init(ObIAllocator *allocator, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(start_to_read_)) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret));
  } else {
    allocator_ = allocator;
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualStorageMetaMemoryStatus::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualStorageMetaMemoryStatus::release_last_tenant()
{
  pool_idx_ = 0;
  status_arr_.reset();
}

bool ObAllVirtualStorageMetaMemoryStatus::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    return true;
  }
  return false;
}

int ObAllVirtualStorageMetaMemoryStatus::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!start_to_read_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not inited", K(start_to_read_), K(ret));
  } else if (OB_ISNULL(cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (0 == status_arr_.count() && OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_meta_mem_status(status_arr_))) {
    SERVER_LOG(WARN, "fail to get obj pools' status", K(ret));
  } else if (pool_idx_ == status_arr_.count()) {
    ret = OB_ITER_END;
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case SVR_IP:
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cur_row_.cells_[i].set_varchar(ip_buf_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case NAME:
          cur_row_.cells_[i].set_varchar(status_arr_[pool_idx_].name_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case USED_SIZE:
          cur_row_.cells_[i].set_int(status_arr_[pool_idx_].used_size_);
          break;
        case TOTAL_SIZE:
          cur_row_.cells_[i].set_int(status_arr_[pool_idx_].total_size_);
          break;
        case USED_OBJ_CNT:
          cur_row_.cells_[i].set_int(status_arr_[pool_idx_].used_obj_cnt_);
          break;
        case FREE_OBJ_CNT:
          cur_row_.cells_[i].set_int(status_arr_[pool_idx_].free_obj_cnt_);
          break;
        case EACH_OBJ_SIZE:
          cur_row_.cells_[i].set_int(status_arr_[pool_idx_].each_obj_size_);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
    pool_idx_++;
  }
  return ret;
}

}
}