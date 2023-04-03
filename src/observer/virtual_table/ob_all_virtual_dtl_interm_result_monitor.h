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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DTL_INTERM_RESULT_MONITOR_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DTL_INTERM_RESULT_MONITOR_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/net/ob_addr.h"
namespace oceanbase
{
namespace sql
{
namespace dtl
{
class ObDTLIntermResultKey;
class ObDTLIntermResultInfo;
}
}
namespace observer
{

class ObDTLIntermResultMonitorInfoGetter
{
public:
  ObDTLIntermResultMonitorInfoGetter(common::ObScanner &scanner,
                                     common::ObIAllocator &allocator,
                                     common::ObIArray<uint64_t> &output_column_ids,
                                     common::ObNewRow &cur_row,
                                     common::ObAddr &addr,
                                     common::ObString &addr_ip,
                                     uint64_t effective_tenant_id)
    : scanner_(scanner),
      allocator_(allocator),
      output_column_ids_(output_column_ids),
      cur_row_(cur_row),
      addr_(addr),
      addr_ip_(addr_ip),
      effective_tenant_id_(effective_tenant_id)
  {}
  virtual ~ObDTLIntermResultMonitorInfoGetter() = default;
  int operator() (common::hash::HashMapPair<sql::dtl::ObDTLIntermResultKey, sql::dtl::ObDTLIntermResultInfo *> &entry);
public:
  uint64_t get_effective_tenant_id() const { return effective_tenant_id_; }
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultMonitorInfoGetter);
private:
  common::ObScanner &scanner_;
  common::ObIAllocator &allocator_;
  common::ObIArray<uint64_t> &output_column_ids_;
  common::ObNewRow &cur_row_;
  common::ObAddr &addr_;
  common::ObString &addr_ip_;
  uint64_t effective_tenant_id_;
};

class ObAllDtlIntermResultMonitor : public common::ObVirtualTableScannerIterator
{
  friend ObDTLIntermResultMonitorInfoGetter;
public:
  ObAllDtlIntermResultMonitor();
  virtual ~ObAllDtlIntermResultMonitor();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  inline void set_addr(common::ObAddr &addr) { addr_ = &addr; }
private:
  common::ObAddr *addr_;
  enum INSPECT_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    TRACE_ID,
    OWNER,
    START_TIME,
    EXPIRE_TIME,
    HOLD_MEMORY,
    DUMP_SIZE,
    DUMP_COST,
    DUMP_TIME,
    DUMP_FD,
    DUMP_DIR_ID,
    CHANNEL_ID,
    QC_ID,
    DFO_ID,
    SQC_ID,
    BATCH_ID,
    MAX_HOLD_MEM,
  };
private:
  int fill_scanner();
  DISALLOW_COPY_AND_ASSIGN(ObAllDtlIntermResultMonitor);
};


} // namespace observer
} // namespace oceanbase
#endif // OCEANBASE_OBSERVER_VIRTUAL_TABLE_ALL_DTL_INTERM_RESULT_MONITOR_

