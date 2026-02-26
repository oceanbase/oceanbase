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

#include "observer/virtual_table/ob_all_virtual_kv_client_info.h"
using namespace oceanbase::common;
using namespace oceanbase::table;

namespace oceanbase
{
namespace observer
{
int ObGetAllClientInfoOp::operator()(common::hash::HashMapPair<uint64_t, table::ObTableClientInfo*> &entry) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is NULL", K(ret));
  } else {
    ObTableClientInfo cli_info = *entry.second; // shallow copy
    if (OB_FAIL(ob_write_string(*allocator_, entry.second->client_info_str_, cli_info.client_info_str_))) {
      LOG_WARN("ob write client info str failed", K(ret));
    } else if (OB_FAIL(cli_infos_.push_back(cli_info))) {
      LOG_WARN("fail to push back client info", K(ret), K(cli_info));
    }
  }

  return ret;
}

int ObAllVirtualKvClientInfo::set_svr_addr(common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  svr_addr_ = addr;
  if (!svr_addr_.ip_to_string(svr_ip_buf_, sizeof(svr_ip_buf_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get server ip buffer failed", K(ret), K(svr_addr_));
  }
  return ret;
}

void ObAllVirtualKvClientInfo::reset()
{
  cur_idx_ = 0;
  MEMSET(svr_ip_buf_, 0, common::OB_IP_STR_BUFF);
  cli_infos_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualKvClientInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    LOG_WARN("execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualKvClientInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualKvClientInfo::release_last_tenant()
{
  cur_idx_ = 0;
  cli_infos_.reset();
}

int ObAllVirtualKvClientInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObGetAllClientInfoOp get_all_op(cli_infos_);
  start_to_read_ = true;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator_ should not be NULL", K(ret));
  } else if (FALSE_IT(get_all_op.set_allocator(allocator_))) {
  } else if (cli_infos_.count() == 0 &&
      OB_FAIL(TABLEAPI_CLI_INFO_MGR->get_cli_info_map().foreach_refactored(get_all_op))) {
    LOG_WARN("fail to get cli info map", K(ret), K(cli_infos_));
  } else if (cur_idx_ >= cli_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    ObTableClientInfo &cli_info = cli_infos_.at(cur_idx_);
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case CLI_INFO_COLUMN::CLIENT_ID:
          cells[i].set_uint64(cli_info.client_id_);
          break;
        case CLI_INFO_COLUMN::CLIENT_IP: {
          char *cli_ip_buf = nullptr;
          if (OB_ISNULL(cli_ip_buf = static_cast<char *>(allocator_->alloc(OB_IP_STR_BUFF)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc client ip buf fail", K(ret));
          } else if (!cli_info.client_addr_.ip_to_string(cli_ip_buf, OB_IP_STR_BUFF)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get client ip string", K(ret), K(cli_info.client_addr_));
          } else {
            cells[i].set_varchar(ObString::make_string(cli_ip_buf));
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        case CLI_INFO_COLUMN::CLIENT_PORT:
          cells[i].set_int(cli_info.client_addr_.get_port());
          break;
        case CLI_INFO_COLUMN::SVR_IP:
          cells[i].set_varchar(ObString::make_string(svr_ip_buf_));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case CLI_INFO_COLUMN::SVR_PORT:
          cells[i].set_int(svr_addr_.get_port());
          break;
        case CLI_INFO_COLUMN::TENANT_ID:
          cells[i].set_int(MTL_ID());
          break;
        case CLI_INFO_COLUMN::USER_NAME:
          cells[i].set_varchar(cli_info.user_name_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case CLI_INFO_COLUMN::FIRST_LOGIN_TS:
          cells[i].set_timestamp(cli_info.first_login_ts_);
          break;
        case CLI_INFO_COLUMN::LAST_LOGIN_TS:
          cells[i].set_timestamp(cli_info.last_login_ts_);
          break;
        case CLI_INFO_COLUMN::CLIENT_INFO:
          cells[i].set_varchar(cli_info.client_info_str_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid column id", K(ret), K(col_id), K(i), K(output_column_ids_));
          break;
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
      cur_idx_++;
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase