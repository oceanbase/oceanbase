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
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
using namespace share;
using namespace common;
namespace observer
{
ObAllVirtualDagWarningHistory::ObAllVirtualDagWarningHistory()
    : ip_buf_("\0"),
      task_id_buf_("\0"),
      dag_warning_info_(),
      dag_warning_info_iter_(),
      is_inited_(false),
      comment_("\0")
{
}
ObAllVirtualDagWarningHistory::~ObAllVirtualDagWarningHistory()
{
  reset();
}

int ObAllVirtualDagWarningHistory::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "execute fail", K(ret));
    }
  }
  return ret;
}

bool ObAllVirtualDagWarningHistory::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObAllVirtualDagWarningHistory::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t compression_ratio = 0;
  int n = 0;
  if (!dag_warning_info_iter_.is_opened()) {
    if (OB_FAIL(MTL(ObDagWarningHistoryManager *)->open_iter(dag_warning_info_iter_))) {
      STORAGE_LOG(WARN, "fail to begin ObTenantSSTableMergeInfoMgr::Iterator", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (FALSE_IT(MEMSET(comment_, '\0', sizeof(comment_)))) {
    } else if (OB_FAIL(dag_warning_info_iter_.get_next(&dag_warning_info_, comment_, sizeof(comment_)))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next sstable merge info", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      //svr_ip
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      //svr_port
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case TENANT_ID:
      //tenant_id
      cells[i].set_int(dag_warning_info_.tenant_id_);
      break;
    case TASK_ID:
      //table_id
      n = dag_warning_info_.task_id_.to_string(task_id_buf_, sizeof(task_id_buf_));
      if (n < 0 || n >= sizeof(task_id_buf_)) {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "buffer not enough", K(ret));
      } else {
        cells[i].set_varchar(task_id_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case MODULE:
      //module
      cells[i].set_varchar(share::ObIDag::get_dag_module_str(dag_warning_info_.dag_type_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case TYPE: {
      //dag_type
      cells[i].set_varchar(share::ObIDag::get_dag_type_str(dag_warning_info_.dag_type_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    case RET: {
      // dag_ret
      cells[i].set_varchar(common::ob_error_name(dag_warning_info_.dag_ret_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    case STATUS:
      // dag_status
      cells[i].set_varchar(ObDagWarningInfo::get_dag_status_str(dag_warning_info_.dag_status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case GMT_CREATE:
      //table_type
      cells[i].set_timestamp(dag_warning_info_.gmt_create_);
      break;
    case GMT_MODIFIED:
      //major_table_id
      cells[i].set_timestamp(dag_warning_info_.gmt_modified_);
      break;
    case RETRY_CNT:
      cells[i].set_int(dag_warning_info_.retry_cnt_);
      break;
    case WARNING_INFO: {
      MEMSET(warning_info_buf_, '\0', sizeof(warning_info_buf_));
      int64_t pos = 0;
      if (dag_warning_info_.location_.is_valid()) {
        common::databuff_printf(warning_info_buf_, sizeof(warning_info_buf_), pos, "%s:%d(%s),",
          dag_warning_info_.location_.filename_,
          dag_warning_info_.location_.line_,
          dag_warning_info_.location_.function_);
      }
      common::databuff_printf(warning_info_buf_, sizeof(warning_info_buf_), pos, "%s",
        comment_);
      cells[i].set_varchar(warning_info_buf_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}
void ObAllVirtualDagWarningHistory::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(task_id_buf_, 0, sizeof(task_id_buf_));
  memset(warning_info_buf_, 0, sizeof(warning_info_buf_));
  memset(comment_, 0, sizeof(comment_));
}


} /* namespace observer */
} /* namespace oceanbase */
