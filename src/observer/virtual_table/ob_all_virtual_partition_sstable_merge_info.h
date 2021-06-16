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

#ifndef OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_H_
#define OB_ALL_VIRTUAL_PARTITION_SSTABLE_MERGE_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/container/ob_array.h"
#include "storage/ob_sstable_merge_info_mgr.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObSSTable;
}  // namespace storage
namespace observer {

class ObAllVirtualPartitionSSTableMergeInfo : public common::ObVirtualTableScannerIterator {
public:
  enum COLUMN_ID_LIST {
    VERSION = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    TENANT_ID,
    TABLE_ID,
    PARTITION_ID,
    MERGE_TYPE,
    SNAPSHOT_VERSION,
    TABLE_TYPE,
    MAJOR_TABLE_ID,
    MERGE_START_TIME,
    MERGE_FINISH_TIME,
    MERGE_COST_TIME,
    ESTIMATE_COST_TIME,
    OCCUPY_SIZE,
    MACRO_BLOCK_COUNT,
    USE_OLD_MACRO_BLOCK_COUNT,
    BUILD_BLOOMFILTER_COUNT,
    TOTAL_ROW_COUNT,
    DELETE_ROW_COUNT,
    INSERT_ROW_COUNT,
    UPDATE_ROW_COUNT,
    BASE_ROW_COUNT,
    USE_BASE_ROW_COUNT,
    MEMTABLE_ROW_COUNT,
    PURGED_ROW_COUNT,
    OUTPUT_ROW_COUNT,
    MERGE_LEVEL,
    REWRITE_MACRO_OLD_MICRO_BLOCK_COUNT,
    REWRITE_MACRO_TOTAL_MICRO_BLOCK_COUNT,
    TOTAL_CHILD_TASK,
    FINISH_CHILD_TASK,
    STEP_MERGE_PERCENTAGE,
    MERGE_PERCENTAGE,
    ERROR_CODE,
    TABLE_COUNT,
  };
  ObAllVirtualPartitionSSTableMergeInfo();
  virtual ~ObAllVirtualPartitionSSTableMergeInfo();
  int init();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

protected:
  int fill_cells(storage::ObSSTableMergeInfo* merge_info);

private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char version_buf_[common::OB_SYS_TASK_TYPE_LENGTH];
  storage::ObSSTableMergeInfo merge_info_;
  storage::ObSSTableMergeInfoIterator merge_info_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionSSTableMergeInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
