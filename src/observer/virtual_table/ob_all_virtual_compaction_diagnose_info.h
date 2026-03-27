/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_H_
#define OB_ALL_VIRTUAL_COMPACTION_DIAGNOSE_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualCompactionDiagnoseInfo : public common::ObVirtualTableScannerIterator
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
    STATUS,
    CREATE_TIME,
    DIAGNOSE_INFO,
  };
  ObAllVirtualCompactionDiagnoseInfo();
  virtual ~ObAllVirtualCompactionDiagnoseInfo();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  compaction::ObCompactionDiagnoseInfo diagnose_info_;
  compaction::ObCompactionDiagnoseIterator diagnose_info_iter_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualCompactionDiagnoseInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
