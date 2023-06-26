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

#include "ob_all_virtual_tablet_compaction_history.h"
#include "storage/ob_sstable_struct.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualTabletCompactionHistory::ObAllVirtualTabletCompactionHistory()
    : ip_buf_(),
      parallel_merge_info_buf_(),
      dag_id_buf_(),
      participant_table_str_(),
      macro_id_list_(),
      comment_(),
      merge_info_(),
      major_merge_info_iter_(),
      minor_merge_info_iter_()
{
}

ObAllVirtualTabletCompactionHistory::~ObAllVirtualTabletCompactionHistory()
{
  reset();
}

int ObAllVirtualTabletCompactionHistory::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "execute fail", K(ret));
    }
  }
  return ret;
}

bool ObAllVirtualTabletCompactionHistory::is_need_process(uint64_t tenant_id)
{
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    return true;
  }
  return false;
}

int ObAllVirtualTabletCompactionHistory::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t compression_ratio = 0;
  int n = 0;
  if (!major_merge_info_iter_.is_opened() && !minor_merge_info_iter_.is_opened()) {
    if (OB_FAIL(MTL(ObTenantSSTableMergeInfoMgr *)->open_iter(major_merge_info_iter_, minor_merge_info_iter_))) {
      STORAGE_LOG(WARN, "fail to open ObTenantSSTableMergeInfoMgr::Iterator", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (FALSE_IT(MEMSET(comment_, '\0', sizeof(comment_)))) {
    } else if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_merge_info_iter_,
                minor_merge_info_iter_,
                merge_info_, comment_, sizeof(comment_)))){
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next sstable merge info", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case TENANT_ID:
      cells[i].set_int(merge_info_.tenant_id_);
      break;
    case LS_ID:
      // index_id
      cells[i].set_int(merge_info_.ls_id_.id());
      break;
    case TABLET_ID:
      cells[i].set_int(merge_info_.tablet_id_.id());
      break;
    case MERGE_TYPE: {
      cells[i].set_varchar(merge_type_to_str(merge_info_.merge_type_));
      cells[i].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    case MERGE_VERSION:
      //TODO:SCN
      cells[i].set_uint64(merge_info_.compaction_scn_ < 0 ? 0 : merge_info_.compaction_scn_);
      break;
    case MERGE_START_TIME:
      cells[i].set_timestamp(merge_info_.merge_start_time_);
      break;
    case MERGE_FINISH_TIME:
      cells[i].set_timestamp(merge_info_.merge_finish_time_);
      break;
    case TASK_ID:
      //dag_id
      n = merge_info_.dag_id_.to_string(dag_id_buf_, sizeof(dag_id_buf_));
      if (n < 0 || n >= sizeof(dag_id_buf_)) {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "buffer not enough", K(ret));
      } else {
        cells[i].set_varchar(dag_id_buf_);
        cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case OCCUPY_SIZE:
      cells[i].set_int(merge_info_.occupy_size_);
      break;
    case MACRO_BLOCK_COUNT:
      cells[i].set_int(merge_info_.macro_block_count_);
      break;
    case MULTIPLEXED_MACRO_BLOCK_COUNT:
      cells[i].set_int(merge_info_.multiplexed_macro_block_count_);
      break;
    case NEW_MICRO_COUNT_IN_NEW_MACRO:
      cells[i].set_int(merge_info_.new_micro_count_in_new_macro_);
      break;
    case MULTIPLEXED_MICRO_COUNT_IN_NEW_MACRO:
      cells[i].set_int(merge_info_.multiplexed_micro_count_in_new_macro_);
      break;
    case TOTAL_ROW_COUNT:
      cells[i].set_int(merge_info_.total_row_count_);
      break;
    case INCREMENTAL_ROW_COUNT:
      cells[i].set_int(merge_info_.incremental_row_count_);
      break;
    case COMPRESSION_RATIO:
      if (0 == merge_info_.original_size_) {
        cells[i].set_double(1);
      } else {
        compression_ratio = merge_info_.compressed_size_ * 1.0 / merge_info_.original_size_ * 100;
        cells[i].set_double(compression_ratio * 1.0 / 100);
      }
      break;
    case NEW_FLUSH_DATA_RATE:
      // calc flush data speed
      cells[i].set_int(merge_info_.new_flush_data_rate_);
      break;
    case PROGRESSIVE_MREGE_ROUND:
      if (merge_info_.is_full_merge_) {
        cells[i].set_int(-1);
      } else {
        cells[i].set_int(merge_info_.progressive_merge_round_);
      }
      break;
    case PROGRESSIVE_MREGE_NUM:
      cells[i].set_int(merge_info_.progressive_merge_num_);
      break;
    case PARALLEL_DEGREE:
      cells[i].set_int(merge_info_.concurrent_cnt_);
      break;
    case PARALLEL_INFO:
      if (merge_info_.concurrent_cnt_ > 1) {
        merge_info_.parallel_merge_info_.to_paral_info_string(
            parallel_merge_info_buf_,
            common::OB_PARALLEL_MERGE_INFO_LENGTH);
        cells[i].set_varchar(parallel_merge_info_buf_);
      } else {
        cells[i].set_varchar("-");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case PARTICIPANT_TABLE_INFO:
      MEMSET(participant_table_str_, '\0', sizeof(participant_table_str_));
      merge_info_.participant_table_info_.fill_info(participant_table_str_, sizeof(participant_table_str_));
      cells[i].set_varchar(participant_table_str_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case MACRO_ID_LIST:
      MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
      MEMCPY(macro_id_list_, merge_info_.macro_id_list_, strlen(merge_info_.macro_id_list_));
      cells[i].set_varchar(macro_id_list_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case COMMENT:
      merge_info_.fill_comment(comment_, sizeof(comment_));
      cells[i].set_varchar(comment_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
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
void ObAllVirtualTabletCompactionHistory::reset()
{
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(parallel_merge_info_buf_, 0, sizeof(parallel_merge_info_buf_));
  memset(dag_id_buf_, 0, sizeof(dag_id_buf_));
  memset(participant_table_str_, 0, sizeof(participant_table_str_));
  memset(macro_id_list_, 0, sizeof(macro_id_list_));
  memset(comment_, 0, sizeof(comment_));
}


} /* namespace observer */
} /* namespace oceanbase */
