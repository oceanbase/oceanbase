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

#include "ob_all_virtual_compaction_diagnose_info.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualCompactionDiagnoseInfo::ObAllVirtualCompactionDiagnoseInfo()
    : diagnose_info_(),
      diagnose_info_iter_(),
      is_inited_(false)
{
}

ObAllVirtualCompactionDiagnoseInfo::~ObAllVirtualCompactionDiagnoseInfo()
{
  reset();
}

int ObAllVirtualCompactionDiagnoseInfo::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualCompactionDiagnoseInfo has been inited", K(ret));
  } else if (OB_FAIL(diagnose_info_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open suggestion iter", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualCompactionDiagnoseInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualCompactionDiagnoseInfo has been inited", K(ret));
  } else if (OB_FAIL(diagnose_info_iter_.get_next_info(diagnose_info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next suggestion info", K(ret));
    }
  } else if (OB_FAIL(fill_cells())) {
    STORAGE_LOG(WARN, "Fail to fill cells", K(ret), K(diagnose_info_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualCompactionDiagnoseInfo::fill_cells()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
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
      cells[i].set_int(diagnose_info_.tenant_id_);
      break;
    case MERGE_TYPE:
      cells[i].set_varchar(merge_type_to_str(diagnose_info_.merge_type_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case LS_ID:
      cells[i].set_int(diagnose_info_.ls_id_);
      break;
    case TABLET_ID:
      cells[i].set_int(diagnose_info_.tablet_id_);
      break;
    case STATUS:
      cells[i].set_varchar(diagnose_info_.get_diagnose_status_str(diagnose_info_.status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case CREATE_TIME:
      cells[i].set_timestamp(diagnose_info_.timestamp_);
      break;
    case DIAGNOSE_INFO: {
      cells[i].set_varchar(diagnose_info_.diagnose_info_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }

  return ret;
}
void ObAllVirtualCompactionDiagnoseInfo::reset()
{
  ObVirtualTableScannerIterator::reset();
  diagnose_info_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  is_inited_ = false;
}


} /* namespace observer */
} /* namespace oceanbase */
