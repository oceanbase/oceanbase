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

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace compaction;
namespace observer
{
ObAllVirtualTabletCompactionHistory::ObAllVirtualTabletCompactionHistory()
    : ip_buf_(),
      parallel_merge_info_buf_(),
      dag_id_buf_(),
      participant_table_str_(),
      macro_id_list_(),
      other_info_(),
      comment_(),
      merge_history_(),
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
    } else if (FALSE_IT(MEMSET(other_info_, '\0', sizeof(other_info_)))) {
    } else if (OB_FAIL(ObTenantSSTableMergeInfoMgr::get_next_info(major_merge_info_iter_,
                minor_merge_info_iter_,
                merge_history_, other_info_, sizeof(other_info_)))) {
      if (OB_ITER_END != ret) {
        STORAGE_LOG(WARN, "fail to get next sstable merge info", K(ret));
      }
    }
  }
  const ObMergeStaticInfo &static_info = merge_history_.static_info_;
  const ObMergeRunningInfo &running_info = merge_history_.running_info_;
  const ObMergeBlockInfo &block_info = merge_history_.block_info_;
  const ObMergeDiagnoseInfo &diagnose_info = merge_history_.diagnose_info_;
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
      cells[i].set_int(MTL_ID());
      break;
    case LS_ID:
      // index_id
      cells[i].set_int(static_info.ls_id_.id());
      break;
    case TABLET_ID:
      cells[i].set_int(static_info.tablet_id_.id());
      break;
    case START_CG_ID:
      cells[i].set_int(running_info.start_cg_idx_);
      break;
    case END_CG_ID:
      // start_cg_id == end_cg_id == 0 means row store merge
      cells[i].set_int(running_info.end_cg_idx_);
      break;
    case MERGE_TYPE: {
      cells[i].set_varchar(merge_type_to_str(static_info.merge_type_));
      cells[i].set_collation_type(
          ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    case MERGE_VERSION:
      //TODO:SCN
      cells[i].set_uint64(static_info.compaction_scn_ < 0 ? 0 : static_info.compaction_scn_);
      break;
    case MERGE_START_TIME:
      cells[i].set_timestamp(running_info.merge_start_time_);
      break;
    case MERGE_FINISH_TIME:
      cells[i].set_timestamp(running_info.merge_finish_time_);
      break;
    case TASK_ID:
      //dag_id
      n = running_info.dag_id_.to_string(dag_id_buf_, sizeof(dag_id_buf_));
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
      cells[i].set_int(block_info.occupy_size_);
      break;
    case MACRO_BLOCK_COUNT:
      cells[i].set_int(block_info.macro_block_count_);
      break;
    case MULTIPLEXED_MACRO_BLOCK_COUNT:
      cells[i].set_int(block_info.multiplexed_macro_block_count_);
      break;
    case NEW_MICRO_COUNT_IN_NEW_MACRO:
      cells[i].set_int(block_info.new_micro_count_in_new_macro_);
      break;
    case MULTIPLEXED_MICRO_COUNT_IN_NEW_MACRO:
      cells[i].set_int(block_info.multiplexed_micro_count_in_new_macro_);
      break;
    case TOTAL_ROW_COUNT:
      cells[i].set_int(block_info.total_row_count_);
      break;
    case INCREMENTAL_ROW_COUNT:
      cells[i].set_int(block_info.incremental_row_count_);
      break;
    case COMPRESSION_RATIO:
      if (0 == block_info.original_size_) {
        cells[i].set_double(1);
      } else {
        compression_ratio = block_info.compressed_size_ * 1.0 / block_info.original_size_ * 100;
        cells[i].set_double(compression_ratio * 1.0 / 100);
      }
      break;
    case NEW_FLUSH_DATA_RATE:
      // calc flush data speed
      cells[i].set_int(block_info.new_flush_data_rate_);
      break;
    case PROGRESSIVE_MREGE_ROUND:
      if (static_info.is_full_merge_) {
        cells[i].set_int(-1);
      } else {
        cells[i].set_int(static_info.progressive_merge_round_);
      }
      break;
    case PROGRESSIVE_MREGE_NUM:
      cells[i].set_int(static_info.progressive_merge_num_);
      break;
    case PARALLEL_DEGREE:
      cells[i].set_int(static_info.concurrent_cnt_);
      break;
    case PARALLEL_INFO:
      if (static_info.concurrent_cnt_ > 1) {
        running_info.parallel_merge_info_.to_paral_info_string(
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
      static_info.participant_table_info_.fill_info(participant_table_str_, sizeof(participant_table_str_));
      cells[i].set_varchar(participant_table_str_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case MACRO_ID_LIST:
      MEMSET(macro_id_list_, '\0', sizeof(macro_id_list_));
      MEMCPY(macro_id_list_, block_info.macro_id_list_, strlen(block_info.macro_id_list_));
      cells[i].set_varchar(macro_id_list_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case COMMENT:
      merge_history_.fill_comment(comment_, sizeof(comment_), other_info_);
      cells[i].set_varchar(comment_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case KEPT_SNAPSHOT:
      if (static_info.kept_snapshot_info_.is_valid()) {
        MEMSET(kept_snapshot_info_, '\0', sizeof(kept_snapshot_info_));
        static_info.kept_snapshot_info_.to_string(kept_snapshot_info_, sizeof(kept_snapshot_info_));
        cells[i].set_varchar(kept_snapshot_info_);
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case MERGE_LEVEL:
      if (is_valid_merge_level(static_info.merge_level_)) {
        cells[i].set_varchar(merge_level_to_str(static_info.merge_level_));
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case EXEC_MODE:
      if (is_valid_exec_mode(static_info.exec_mode_)) {
        cells[i].set_varchar(exec_mode_to_str(static_info.exec_mode_));
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case IS_FULL_MERGE:
      cells[i].set_bool(static_info.is_full_merge_);
      break;
    case IO_COST_TIME_PERCENTAGE:
      cells[i].set_int(running_info.io_percentage_);
      break;
    case MERGE_REASON:
      if (ObAdaptiveMergePolicy::is_valid_merge_reason(static_info.merge_reason_)) {
        cells[i].set_varchar(ObAdaptiveMergePolicy::merge_reason_to_str(static_info.merge_reason_));
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case BASE_MAJOR_STATUS:
      if (is_valid_co_major_sstable_status(static_info.base_major_status_)) {
        cells[i].set_varchar(co_major_sstable_status_to_str(static_info.base_major_status_));
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case CO_MERGE_TYPE:
      if (ObCOMajorMergePolicy::is_valid_major_merge_type(static_info.co_major_merge_type_)) {
        cells[i].set_varchar(ObCOMajorMergePolicy::co_major_merge_type_to_str(static_info.co_major_merge_type_));
      } else {
        cells[i].set_varchar("");
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case MDS_FILTER_INFO:
      cells[i].set_varchar("");
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
  memset(other_info_, 0, sizeof(other_info_));
}


} /* namespace observer */
} /* namespace oceanbase */
