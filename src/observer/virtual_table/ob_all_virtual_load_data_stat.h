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

#ifndef OB_ALL_VIRTUAL_LOAD_DATA_STAT_H_
#define OB_ALL_VIRTUAL_LOAD_DATA_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualLoadDataStat : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualLoadDataStat();
  virtual ~ObAllVirtualLoadDataStat();

public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  virtual int inner_open();
  virtual int inner_close();
  inline void set_addr(common::ObAddr &addr)
  {
    addr_ = addr;
  }

private:
  enum TABLE_COLUMN
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    JOB_ID,
    JOB_TYPE,
    TABLE_NAME,
    FILE_PATH,
    TABLE_COLUMN,
    FILE_COLUMN,
    BATCH_SIZE,
    PARALLEL,
    LOAD_MODE,
    LOAD_TIME,
    ESTIMATED_REMAINING_TIME,
    TOTAL_BYTES,
    READ_BYTES,
    PARSED_BYTES,
    PARSED_ROWS,
    TOTAL_SHUFFLE_TASK,
    TOTAL_INSERT_TASK,
    SHUFFLE_RT_SUM,
    INSERT_RT_SUM,
    TOTAL_WAIT_SECS,
    MAX_ALLOWED_ERROR_ROWS,
    DETECTED_ERROR_ROWS,
    COORDINATOR_RECEIVED_ROWS,
    COORDINATOR_LAST_COMMIT_SEGMENT_ID,
    COORDINATOR_STATUS,
    COORDINATOR_TRANS_STATUS,
    STORE_PROCESSED_ROWS,
    STORE_LAST_COMMIT_SEGMENT_ID,
    STORE_STATUS,
    STORE_TRANS_STATUS
  };
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  sql::ObGetAllJobStatusOp all_job_status_op_;

  TO_STRING_KV(K(addr_));

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualLoadDataStat);
};

}
}

#endif  /* OB_ALL_VIRTUAL_LOAD_DATA_STAT_H_*/