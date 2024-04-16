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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_tenant_resource_limit_detail.h"
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;
namespace oceanbase
{
namespace observer
{

ObResourceLimitDetailTable::ObResourceLimitDetailTable()
  : ObVirtualTableScannerIterator(),
    addr_(),
    iter_(),
    constraint_iter_()
{
}

ObResourceLimitDetailTable::~ObResourceLimitDetailTable()
{
  reset();
}

void ObResourceLimitDetailTable::reset()
{
  // Note that cross-tenant resources must be released by ObMultiTenantOperator, it should be called at first.
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ObVirtualTableScannerIterator::reset();
}

int ObResourceLimitDetailTable::set_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  addr_ = addr;
  if (!addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get ip buffer failed", K(ret), K(addr_));
  }
  return ret;
}

void ObResourceLimitDetailTable::release_last_tenant()
{
  constraint_iter_.reset();
  iter_.reset();
}

int ObResourceLimitDetailTable::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    LOG_WARN("execute fail", K(ret));
  }
  return ret;
}

bool ObResourceLimitDetailTable::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObResourceLimitDetailTable::get_next_resource_limit_val_(int64_t &val)
{
  int ret = OB_SUCCESS;
  bool iter_next_res = false;
  while (OB_SUCC(ret)) {
    if (!iter_.is_ready()
        && OB_FAIL(iter_.set_ready())) {
      LOG_WARN("iterator is not ready", K(ret));
    } else if ((iter_.get_curr_type() == 0 || iter_next_res)
               && OB_FAIL(iter_.get_next_type())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next resource type failed", K(ret), K(iter_.get_curr_type()));
      }
    } else if (!constraint_iter_.is_ready()
               && OB_FAIL(constraint_iter_.set_ready(iter_.get_curr_type()))) {
      LOG_WARN("constraint iterator is not ready", K(ret), K(iter_.get_curr_type()));
    } else if (OB_FAIL(constraint_iter_.get_next(val))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next constraint value failed", K(ret), K(constraint_iter_.get_curr_type()));
      } else {
        // iter next resource
        constraint_iter_.reset();
        iter_next_res = true;
        ret = OB_SUCCESS;
      }
    } else {
      // get succeed
      break;
    }
  }
  return ret;
}

int ObResourceLimitDetailTable::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  int64_t limit_value;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (FALSE_IT(start_to_read_ = true)) {
  } else if (OB_FAIL(get_next_resource_limit_val_(limit_value))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get_next_resource_info failed", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case SVR_IP:
          cur_row_.cells_[i].set_varchar(ip_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case SVR_PORT:
          cur_row_.cells_[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cur_row_.cells_[i].set_int(MTL_ID());
          break;
        case RESOURCE_NAME: {
          cur_row_.cells_[i].set_varchar(get_logic_res_type_name(iter_.get_curr_type()));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case LIMIT_TYPE:
          cur_row_.cells_[i].set_varchar(get_constraint_type_name(constraint_iter_.get_curr_type()));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case LIMIT_VALUE:
          cur_row_.cells_[i].set_int(limit_value);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid col_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}



} // end namespace observer
} // end namespace oceanbase
