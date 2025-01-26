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

#define USING_LOG_PREFIX SERVER

#include "observer/virtual_table/ob_all_virtual_kv_group_commit_info.h"
using namespace oceanbase::common;
using namespace oceanbase::table;

namespace oceanbase
{
namespace observer
{
void ObAllVirtualKvGroupCommitInfo::reset()
{
  cur_idx_ = 0;
  group_infos_.reset();
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualKvGroupCommitInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    LOG_WARN("execute fail", K(ret));
  }
  return ret;
}

bool ObAllVirtualKvGroupCommitInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualKvGroupCommitInfo::release_last_tenant()
{
  cur_idx_ = 0;
  group_infos_.reset();
}

int ObAllVirtualKvGroupCommitInfo::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  ObGetAllGroupInfoOp get_all_op(group_infos_);
  start_to_read_ = true;
  if (group_infos_.count() == 0 &&
      OB_FAIL(TABLEAPI_GROUP_COMMIT_MGR->get_all_group_info(get_all_op))) {
    LOG_WARN("fail to get group info map", K(ret), K(group_infos_));
  } else if (cur_idx_ >= group_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    ObTableGroupInfo &group_info = group_infos_.at(cur_idx_);
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
        case GROUP_COLUMN::SVR_IP:
          if (!group_info.client_addr_.ip_to_string(ipbuf_, common::OB_IP_STR_BUFF)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to get ip string", K(ret), K(group_info.client_addr_));
          } else {
            cells[i].set_varchar(ObString::make_string(ipbuf_));
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case GROUP_COLUMN::SVR_PORT:
          cells[i].set_int(group_info.client_addr_.get_port());
          break;
        case GROUP_COLUMN::TENANT_ID:
          cells[i].set_int(group_info.tenant_id_);
          break;
        case GROUP_COLUMN::GROUP_TYPE:
          cells[i].set_varchar(group_info.get_group_type_str());
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case GROUP_COLUMN::LS_ID:
          cells[i].set_int(group_info.ls_id_.id());
          break;
        case GROUP_COLUMN::TABLE_ID:
          cells[i].set_int(group_info.table_id_);
          break;
        case GROUP_COLUMN::SCHEMA_VERSION:
          cells[i].set_int(group_info.schema_version_);
          break;
        case GROUP_COLUMN::QUEUE_SIZE:
          cells[i].set_int(group_info.queue_size_);
          break;
        case GROUP_COLUMN::BATCH_SIZE:
          cells[i].set_int(group_info.batch_size_);
          break;
        case GROUP_COLUMN::CREATE_TIME:
          cells[i].set_timestamp(group_info.gmt_created_);
          break;
        case GROUP_COLUMN::UPDATE_TIME:
          cells[i].set_timestamp(group_info.gmt_modified_);
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