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

#include "ob_all_virtual_dag_warning_history.h"
#include "share/ob_errno.h"
#include "share/scheduler/ob_dag_scheduler.h"

namespace oceanbase {
using namespace storage;
using namespace common;
namespace observer {
ObAllVirtualDagWarningHistory::ObAllVirtualDagWarningHistory()
    : dag_warning_info_(), dag_warning_info_iter_(), is_inited_(false)
{}
ObAllVirtualDagWarningHistory::~ObAllVirtualDagWarningHistory()
{
  reset();
}

int ObAllVirtualDagWarningHistory::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualDagWarningHistory has been inited, ", K(ret));
  } else if (OB_FAIL(dag_warning_info_iter_.open())) {
    SERVER_LOG(WARN, "Fail to open merge info iter, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualDagWarningHistory::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualDagWarningHistory has been inited, ", K(ret));
  } else if (OB_FAIL(dag_warning_info_iter_.get_next_info(dag_warning_info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next merge info, ", K(ret));
    }
  } else if (OB_FAIL(fill_cells(dag_warning_info_))) {
    STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret), K(dag_warning_info_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualDagWarningHistory::fill_cells(ObDagWarningInfo& dag_warning_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj* cells = cur_row_.cells_;
  int64_t n = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case SVR_IP:
        // svr_ip
        if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case SVR_PORT:
        // svr_port
        cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
        break;
      case TENANT_ID:
        // tenant_id
        cells[i].set_int(dag_warning_info.tenant_id_);
        break;
      case TASK_ID:
        // table_id
        n = dag_warning_info.task_id_.to_string(task_id_buf_, sizeof(task_id_buf_));
        if (n < 0 || n >= sizeof(task_id_buf_)) {
          ret = OB_BUF_NOT_ENOUGH;
          SERVER_LOG(WARN, "buffer not enough", K(ret));
        } else {
          cells[i].set_varchar(task_id_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case MODULE:
        // module
        cells[i].set_varchar(ObDagModuleStr[dag_warning_info.dag_type_]);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case TYPE: {
        // dag_type
        cells[i].set_varchar(share::ObIDag::ObIDagTypeStr[dag_warning_info.dag_type_]);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case RET: {
        // dag_ret
        cells[i].set_varchar(common::ob_error_name(dag_warning_info.dag_ret_));
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case STATUS:
        // dag_status
        // dag_ret
        cells[i].set_varchar(ObDagStatusStr[dag_warning_info.dag_status_]);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case GMT_CREATE:
        // table_type
        cells[i].set_timestamp(dag_warning_info.gmt_create_);
        break;
      case GMT_MODIFIED:
        // major_table_id
        cells[i].set_timestamp(dag_warning_info.gmt_modified_);
        break;
      case WARNING_INFO: {
        // merge_type
        cells[i].set_varchar(dag_warning_info.warning_info_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
    }
  }

  return ret;
}
void ObAllVirtualDagWarningHistory::reset()
{
  ObVirtualTableScannerIterator::reset();
  dag_warning_info_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(task_id_buf_, 0, sizeof(task_id_buf_));
}

} /* namespace observer */
} /* namespace oceanbase */
