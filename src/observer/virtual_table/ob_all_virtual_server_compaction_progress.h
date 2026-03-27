/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_H_
#define OB_ALL_VIRTUAL_SERVER_COMPACTION_PROGRESS_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/compaction/ob_tenant_compaction_progress.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualServerCompactionProgress : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    ZONE,
    TENANT_ID,
    MERGE_TYPE,
    COMPACTION_SCN,
    STATUS,
    TOTAL_PARTITION_CNT,
    UNFIINISHED_PARTITION_CNT,
    DATA_SIZE,
    UNFINISHED_DATA_SIZE,
    COMPRESSION_RATIO,
    START_TIME,
    ESTIMATED_FINISH_TIME,
    COMMENTS,
  };
  ObAllVirtualServerCompactionProgress();
  virtual ~ObAllVirtualServerCompactionProgress();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells();
private:
  static const int64_t SERVER_PROGRESS_EXTRA_TIME = 120 * 1000 * 1000; // 120 seconds

  char ip_buf_[common::OB_IP_STR_BUFF];
  char event_buf_[common::OB_COMPACTION_EVENT_STR_LENGTH];
  compaction::ObTenantCompactionProgress progress_;
  compaction::ObTenantCompactionProgressIterator progress_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualServerCompactionProgress);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
