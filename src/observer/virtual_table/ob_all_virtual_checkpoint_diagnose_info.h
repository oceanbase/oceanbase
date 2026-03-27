/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_CHEKPOINT_DIAGNOSE_INFO_H
#define OB_ALL_VIRTUAL_CHEKPOINT_DIAGNOSE_INFO_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "src/storage/checkpoint/ob_checkpoint_diagnose.h"
namespace oceanbase
{
namespace observer
{

class ObAllVirtualCheckpointDiagnoseInfo : public common::ObVirtualTableScannerIterator,
                                           public omt::ObMultiTenantOperator
{
friend class GenerateTraceRow;
public:
  virtual ~ObAllVirtualCheckpointDiagnoseInfo() { omt::ObMultiTenantOperator::reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row) { return execute(row); }
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  common::ObAddr addr_;
};

struct GenerateTraceRow
{
public:
  GenerateTraceRow() = delete;
  GenerateTraceRow(const GenerateTraceRow&) = delete;
  GenerateTraceRow& operator=(const GenerateTraceRow&) = delete;
  GenerateTraceRow(ObAllVirtualCheckpointDiagnoseInfo &virtual_table)
    : virtual_table_(virtual_table)
  {}
  int operator()(const storage::checkpoint::ObTraceInfo &trace_info) const;

private:
  ObAllVirtualCheckpointDiagnoseInfo &virtual_table_;
};


} // observer
} // oceanbase
#endif
