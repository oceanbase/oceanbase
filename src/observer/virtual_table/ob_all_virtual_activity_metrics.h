/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_ACTIVITY_METRICS_H_
#define OB_ALL_VIRTUAL_ACTIVITY_METRICS_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx_storage/ob_tenant_freezer.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace observer
{
class ObAllVirtualActivityMetric : public common::ObVirtualTableScannerIterator,
                                   public omt::ObMultiTenantOperator
{
  enum ACTIVITY_METRIC_COLUMN {
    SERVER_IP = common::OB_APP_MIN_COLUMN_ID,
    SERVER_PORT,
    TENANT_ID,
    ACTIVITY_TIMESTAMP,
    MODIFICATION_SIZE,
    FREEZE_TIMES,
    MINI_MERGE_COST,
    MINI_MERGE_TIMES,
    MINOR_MERGE_COST,
    MINOR_MERGE_TIMES,
    MAJOR_MERGE_COST,
    MAJOR_MERGE_TIMES,
  };
public:
  ObAllVirtualActivityMetric();
  virtual ~ObAllVirtualActivityMetric();
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }
private:
  bool is_need_process(uint64_t tenant_id) override;
  int process_curr_tenant(common::ObNewRow *&row) override;
  void release_last_tenant() override;
  int get_next_freezer_stat_(storage::ObTenantFreezerStat& stat);
  int prepare_start_to_read_();

private:
  int64_t current_pos_;
  int64_t length_;
  common::ObAddr addr_;
  char ip_buffer_[common::OB_IP_STR_BUFF];
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualActivityMetric);
};

} // namespace observer
} // namespace oceanbase
#endif /* OB_ALL_VIRTUAL_ACTIVITY_METRICS_H */
