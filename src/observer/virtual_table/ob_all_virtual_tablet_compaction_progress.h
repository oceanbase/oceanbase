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

#ifndef OB_ALL_VIRTUAL_PARITION_COMPACTION_PROGRESS_H_
#define OB_ALL_VIRTUAL_PARITION_COMPACTION_PROGRESS_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTabletCompactionProgress : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    MERGE_TYPE,
    LS_ID,
    TABLET_ID,
    MERGE_VERSION,
    TASK_ID,
    STATUS,
    DATA_SIZE,
    UNFINISHED_DATA_SIZE,
    PROGRESSIVE_MERGE_ROUND,
    CREATE_TIME,
    START_TIME,
    ESTIMATED_FINISH_TIME,
  };
  ObAllVirtualTabletCompactionProgress();
  virtual ~ObAllVirtualTabletCompactionProgress();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  char dag_id_buf_[common::OB_TRACE_STAT_BUFFER_SIZE];
  compaction::ObTabletCompactionProgress progress_;
  compaction::ObTabletCompactionProgressIterator progress_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletCompactionProgress);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
