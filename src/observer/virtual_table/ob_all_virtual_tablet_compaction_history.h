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

#ifndef OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_H_
#define OB_ALL_VIRTUAL_TABLET_COMPACTION_HISTORY_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
#include "storage/ob_sstable_struct.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTabletCompactionHistory : public common::ObVirtualTableScannerIterator,
                                            public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    MERGE_TYPE,
    MERGE_VERSION,
    MERGE_START_TIME,
    MERGE_FINISH_TIME,
    TASK_ID,
    OCCUPY_SIZE,
    MACRO_BLOCK_COUNT,
    MULTIPLEXED_MACRO_BLOCK_COUNT,
    NEW_MICRO_COUNT_IN_NEW_MACRO,
    MULTIPLEXED_MICRO_COUNT_IN_NEW_MACRO,
    TOTAL_ROW_COUNT,
    INCREMENTAL_ROW_COUNT,
    COMPRESSION_RATIO,
    NEW_FLUSH_DATA_RATE,
    PROGRESSIVE_MREGE_ROUND,
    PROGRESSIVE_MREGE_NUM,
    PARALLEL_DEGREE,
    PARALLEL_INFO,
    PARTICIPANT_TABLE_INFO,
    MACRO_ID_LIST,
    COMMENT
  };
  ObAllVirtualTabletCompactionHistory();
  virtual ~ObAllVirtualTabletCompactionHistory();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells(ObSSTableMergeInfo *merge_info);
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override
  {
    major_info_idx_ = 0;
    major_info_cnt_ = 0;
    minor_info_idx_ = 0;
    minor_info_cnt_ = 0;
    get_info_cnt_flag_ = true;
  }
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char parallel_merge_info_buf_[common::OB_PARALLEL_MERGE_INFO_LENGTH];
  char dag_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  char participant_table_str_[common::OB_PART_TABLE_INFO_LENGTH];
  char macro_id_list_[common::OB_MACRO_ID_INFO_LENGTH];
  char comment_[common::OB_COMPACTION_EVENT_STR_LENGTH];
  int64_t major_info_idx_;
  int64_t major_info_cnt_;
  int64_t minor_info_idx_;
  int64_t minor_info_cnt_;
  ObSSTableMergeInfo merge_info_;
  bool get_info_cnt_flag_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletCompactionHistory);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
