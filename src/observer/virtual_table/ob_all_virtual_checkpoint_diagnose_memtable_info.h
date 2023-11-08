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

#ifndef OB_ALL_VIRTUAL_CHEKPOINT_DIAGNOSE_MEMTABLE_INFO_H
#define OB_ALL_VIRTUAL_CHEKPOINT_DIAGNOSE_MEMTABLE_INFO_H

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualCheckpointDiagnoseMemtableInfo : public common::ObVirtualTableScannerIterator
{
friend class GenerateMemtableRow;
public:
  virtual int inner_get_next_row(common::ObNewRow *&row);
  inline void set_addr(common::ObAddr &addr) { addr_ = addr; }

protected:
  int get_primary_key_();
  common::ObAddr addr_;
  uint64_t tenant_id_;
  int64_t trace_id_;
};

class ObAllVirtualCheckpointDiagnoseCheckpointUnitInfo : public ObAllVirtualCheckpointDiagnoseMemtableInfo
{
friend class GenerateCheckpointUnitRow;
  virtual int inner_get_next_row(common::ObNewRow *&row);
};

struct GenerateMemtableRow
{
public:
  GenerateMemtableRow() = delete;
  GenerateMemtableRow(const GenerateMemtableRow&) = delete;
  GenerateMemtableRow& operator=(const GenerateMemtableRow&) = delete;
  GenerateMemtableRow(ObAllVirtualCheckpointDiagnoseMemtableInfo &virtual_table)
    : virtual_table_(virtual_table)
  {}
  int operator()(const storage::checkpoint::ObTraceInfo &trace_info,
      const storage::checkpoint::ObCheckpointDiagnoseKey &key,
      const storage::checkpoint::ObMemtableDiagnoseInfo &memtable_diagnose_info) const;

private:
  ObAllVirtualCheckpointDiagnoseMemtableInfo &virtual_table_;
};

struct GenerateCheckpointUnitRow
{
public:
  GenerateCheckpointUnitRow() = delete;
  GenerateCheckpointUnitRow(const GenerateCheckpointUnitRow&) = delete;
  GenerateCheckpointUnitRow& operator=(const GenerateCheckpointUnitRow&) = delete;
  GenerateCheckpointUnitRow(ObAllVirtualCheckpointDiagnoseCheckpointUnitInfo &virtual_table)
    : virtual_table_(virtual_table)
  {}
  int operator()(const storage::checkpoint::ObTraceInfo &trace_info,
      const storage::checkpoint::ObCheckpointDiagnoseKey &key,
      const storage::checkpoint::ObCheckpointUnitDiagnoseInfo &checkpoint_unit_diagnose_info) const;

private:
  ObAllVirtualCheckpointDiagnoseCheckpointUnitInfo &virtual_table_;
};

} // observer
} // oceanbase
#endif
