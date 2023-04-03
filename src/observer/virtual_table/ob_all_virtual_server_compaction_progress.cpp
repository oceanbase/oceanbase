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

#include "ob_all_virtual_server_compaction_progress.h"
#include "storage/compaction/ob_server_compaction_event_history.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualServerCompactionProgress::ObAllVirtualServerCompactionProgress()
    : progress_(),
      progress_iter_(),
      is_inited_(false)
{
}

ObAllVirtualServerCompactionProgress::~ObAllVirtualServerCompactionProgress()
{
  reset();
}

int ObAllVirtualServerCompactionProgress::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualServerCompactionProgress has been inited", K(ret));
  } else if (OB_FAIL(progress_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open suggestion iter", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualServerCompactionProgress::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualServerCompactionProgress has been inited", K(ret));
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

int ObAllVirtualServerCompactionProgress::fill_cells()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t compression_ratio = 0;
  compaction::ObServerCompactionEvent tmp_event;
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
    case ZONE:
      //zone
      cells[i].set_varchar(GCONF.zone);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case COMPACTION_SCN:
      cells[i].set_uint64(progress_.merge_version_ < 0 ? 0 : progress_.merge_version_);
      break;
    case STATUS:
      cells[i].set_varchar(share::ObIDag::get_dag_status_str(progress_.status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case TOTAL_PARTITION_CNT:
      cells[i].set_int(progress_.total_tablet_cnt_);
      break;
    case UNFIINISHED_PARTITION_CNT:
      cells[i].set_int(progress_.unfinished_tablet_cnt_);
      break;
    case DATA_SIZE:
      cells[i].set_int(progress_.data_size_);
      break;
    case UNFINISHED_DATA_SIZE:
      cells[i].set_int(progress_.unfinished_data_size_);
      break;
    case COMPRESSION_RATIO:
      if (0 == progress_.original_size_) {
        cells[i].set_double(1);
      } else {
        compression_ratio = progress_.compressed_size_ * 1.0 / progress_.original_size_ * 100;
        cells[i].set_double(compression_ratio * 1.0 / 100);
      }
      break;
    case START_TIME:
      cells[i].set_timestamp(progress_.start_time_);
      break;
    case ESTIMATED_FINISH_TIME:
      if (share::ObIDag::DAG_STATUS_FINISH != progress_.status_) {
        int64_t current_time = ObTimeUtility::fast_current_time();
        // update before select, update estimated_finish_time
        if (progress_.estimated_finish_time_ < current_time) {
          progress_.estimated_finish_time_ = current_time + SERVER_PROGRESS_EXTRA_TIME;
        }
      }
      cells[i].set_timestamp(progress_.estimated_finish_time_);
      break;
    case COMMENTS:
      cells[i].set_varchar("");
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      if (share::ObIDag::DAG_STATUS_FINISH != progress_.status_) {
        MTL_SWITCH(progress_.tenant_id_) {
          MTL(compaction::ObServerCompactionEventHistory *)->get_last_event(tmp_event);
          if (OB_FAIL(tmp_event.generate_event_str(event_buf_, sizeof(event_buf_)))) {
            SERVER_LOG(WARN, "failed to generate event str", K(ret), K(tmp_event));
          }
        }
        if (OB_SUCC(ret)) {
          cells[i].set_varchar(event_buf_);
        }
      } else {
        progress_.sum_time_guard_.to_string(event_buf_, sizeof(event_buf_));
        cells[i].set_varchar(event_buf_);
      }
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }

  return ret;
}

void ObAllVirtualServerCompactionProgress::reset()
{
  ObVirtualTableScannerIterator::reset();
  progress_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(event_buf_, 0, sizeof(event_buf_));
  is_inited_ = false;
}

} /* namespace observer */
} /* namespace oceanbase */
