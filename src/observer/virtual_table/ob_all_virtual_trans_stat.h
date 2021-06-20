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

#ifndef OB_ALL_VIRTUAL_TRANS_STAT_H_
#define OB_ALL_VIRTUAL_TRANS_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/container/ob_se_array.h"
#include "storage/transaction/ob_trans_ctx_mgr.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
namespace memtable {
class ObMemtable;
}
namespace transaction {
class ObTransService;
class ObTransID;
class ObStartTransParam;
class ObTransStat;
}  // namespace transaction
namespace observer {
class ObGVTransStat : public common::ObVirtualTableScannerIterator {
public:
  explicit ObGVTransStat(transaction::ObTransService* trans_service) : trans_service_(trans_service)
  {}
  virtual ~ObGVTransStat()
  {
    destroy();
  }

public:
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();
  virtual void destroy();

private:
  int prepare_start_to_read_();
  int get_next_trans_info_(transaction::ObTransStat& trans_stat);

private:
  enum {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    INC_NUM,
    SESSION_ID,
    PROXY_ID,
    TRANS_TYPE,
    TRANS_ID,
    IS_EXITING,
    IS_READONLY,
    IS_DECIDED,
    TRANS_MODE,
    ACTIVE_MEMSTORE_VERSION,
    PARTITION,
    PARTICIPANTS,
    AUTOCOMMIT,
    TRANS_CONSISTENCY,
    CTX_CREATE_TIME,
    EXPIRED_TIME,
    REFER,
    SQL_NO,
    STATE,
    PART_TRANS_ACTION,
    LOCK_FOR_READ_RETRY_COUNT,
    CTX_ADDR,
    PREV_TRANS_ARR,
    PREV_TRANS_COUNT,
    NEXT_TRANS_ARR,
    NEXT_TRANS_COUNT,
    PREV_TRANS_COMMIT_COUNT,
    CTX_ID,
    PENDING_LOG_SIZE,
    FLUSHED_LOG_SIZE
  };

  // define max buffer size for participants
  static const int64_t OB_MAX_BUFFER_SIZE = 1024;
  // define min buffer size for partition and addr
  static const int64_t OB_MIN_BUFFER_SIZE = 128;
  char ip_buffer_[common::OB_IP_STR_BUFF];
  char memstore_version_buffer_[common::MAX_VERSION_LENGTH];
  char partition_buffer_[OB_MIN_BUFFER_SIZE];
  char participants_buffer_[OB_MAX_BUFFER_SIZE];
  char trans_id_buffer_[OB_MIN_BUFFER_SIZE];
  char proxy_sessid_buffer_[OB_MIN_BUFFER_SIZE];
  char prev_trans_info_buffer_[OB_MAX_BUFFER_SIZE];
  char next_trans_info_buffer_[OB_MAX_BUFFER_SIZE];

private:
  transaction::ObTransService* trans_service_;
  transaction::ObPartitionIterator partition_iter_;
  transaction::ObTransStatIterator trans_stat_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGVTransStat);
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_TRANS_STAT_H */
