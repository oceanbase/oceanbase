/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DUMMY_TABLE_ROW_ITER_H
#define OB_DUMMY_TABLE_ROW_ITER_H

#include "share/ob_i_tablet_scan.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase {
namespace sql {

class ObDummyTableRowIterator : public ObExternalTableRowIterator{
public:
  ObDummyTableRowIterator() {}
  virtual ~ObDummyTableRowIterator() {};

  int init(const storage::ObTableScanParam *scan_param) override;
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;

  virtual int get_next_row(ObNewRow *&row) override {
    UNUSED(row);
    return common::OB_ERR_UNEXPECTED;
  }
  virtual void reset() override;
};

}
}

#endif // OB_DUMMY_TABLE_ROW_ITER_H
