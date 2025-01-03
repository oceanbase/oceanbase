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

#ifndef OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_
#define OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_

#include "lib/ob_define.h"
#include "lib/container/ob_array.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_ddl_common.h"
#include "storage/ob_i_store.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"
#include "observer/virtual_table/ob_all_virtual_diag_index_scan.h"

namespace oceanbase
{
namespace observer
{
class ObAllVirtualDDLDiagnoseInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualDDLDiagnoseInfo()
    : is_inited_(false)
     {}
  virtual ~ObAllVirtualDDLDiagnoseInfo() = default;
  int init();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLDiagnoseInfo);
  bool is_inited_;
};

class ObAllVirtualDDLDiagnoseInfoI1 : public ObAllVirtualDDLDiagnoseInfo, public ObAllVirtualDiagIndexScan
{
public:
  ObAllVirtualDDLDiagnoseInfoI1() = default;
  virtual ~ObAllVirtualDDLDiagnoseInfoI1() = default;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualDDLDiagnoseInfoI1);
};

}  // end namespace observer
}  // end namespace oceanbase

#endif  // OB_ALL_VIRTUAL_DDL_DIAGNOSE_INFO_H_
