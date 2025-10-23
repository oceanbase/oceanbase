/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALL_VIRTUAL_SS_DIAGNOSE_INFO_H_
#define OB_ALL_VIRTUAL_SS_DIAGNOSE_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/share/ob_ss_diagnose.h"
#endif

namespace oceanbase
{
namespace observer
{

class ObAllVirtualSSDiagnoseInfo : public common::ObVirtualTableScannerIterator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP  = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    DIAG_TYPE,
    LS_ID,
    TABLET_ID,
    CREATE_TIME,
    DIAGNOSE_INFO,
  };
  ObAllVirtualSSDiagnoseInfo();
  virtual ~ObAllVirtualSSDiagnoseInfo();
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
protected:
  int fill_cells();
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
#ifdef OB_BUILD_SHARED_STORAGE
  storage::ObSSDiagnoseInfo diagnose_info_;
  storage::ObSSDiagnoseIterator diagnose_info_iter_;
#endif
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSSDiagnoseInfo);
};

} /* namespace observer */
} /* namespace oceanbase */
#endif
