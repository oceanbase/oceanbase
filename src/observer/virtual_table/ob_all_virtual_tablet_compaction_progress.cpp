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

#include "ob_all_virtual_tablet_compaction_progress.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualTabletCompactionProgress::ObAllVirtualTabletCompactionProgress()
    : progress_(),
      progress_iter_(),
      is_inited_(false)
{
}

ObAllVirtualTabletCompactionProgress::~ObAllVirtualTabletCompactionProgress()
{
  reset();
}

int ObAllVirtualTabletCompactionProgress::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualTabletCompactionProgress has been inited", K(ret));
  } else if (OB_FAIL(progress_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open suggestion iter", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualTabletCompactionProgress::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualTabletCompactionProgress has been inited", K(ret));
  } else if (OB_FAIL(progress_iter_.get_next_info(progress_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next suggestion info", K(ret));
    }
  } else if (OB_FAIL(fill_cells())) {
    STORAGE_LOG(WARN, "Fail to fill cells", K(ret), K(progress_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualTabletCompactionProgress::fill_cells()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t n = 0;
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
      cells[i].set_int(progress_.tenant_id_);
      break;
    case MERGE_TYPE:
      cells[i].set_varchar(merge_type_to_str(progress_.merge_type_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case MERGE_VERSION:
      //TODO:SCN
      cells[i].set_uint64(progress_.merge_version_ < 0 ? 0 : progress_.merge_version_);
      break;
    case TASK_ID:
      n = progress_.dag_id_.to_string(dag_id_buf_, sizeof(dag_id_buf_));
      if (n < 0 || n >= sizeof(dag_id_buf_)) {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "buffer not enough", K(ret));
      } else {
        cells[i].set_varchar(dag_id_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case STATUS:
      cells[i].set_varchar(share::ObIDag::get_dag_status_str(progress_.status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case LS_ID:
      cells[i].set_int(progress_.ls_id_);
      break;
    case TABLET_ID:
      cells[i].set_int(progress_.tablet_id_);
      break;
    case DATA_SIZE:
      cells[i].set_int(progress_.data_size_);
      break;
    case UNFINISHED_DATA_SIZE:
      cells[i].set_int(progress_.unfinished_data_size_);
      break;
    case PROGRESSIVE_MERGE_ROUND:
      cells[i].set_int(progress_.progressive_merge_round_);
      break;
    case CREATE_TIME:
      cells[i].set_timestamp(progress_.create_time_);
      break;
    case START_TIME:
      cells[i].set_timestamp(progress_.start_time_);
      break;
    case ESTIMATED_FINISH_TIME:
      cells[i].set_timestamp(progress_.estimated_finish_time_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }

  return ret;
}

void ObAllVirtualTabletCompactionProgress::reset()
{
  ObVirtualTableScannerIterator::reset();
  progress_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  is_inited_ = false;
}

} /* namespace observer */
} /* namespace oceanbase */
