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

#ifndef _OB_ALL_VIRTUAL_ROOTSERVICE_STAT_H
#define _OB_ALL_VIRTUAL_ROOTSERVICE_STAT_H 1

#include "share/ob_virtual_table_projector.h"
#include "lib/stat/ob_diagnose_info.h"

namespace oceanbase {
namespace rootserver {
class ObRootService;
class ObAllVirtualRootserviceStat : public common::ObSimpleVirtualTableIterator {
public:
  ObAllVirtualRootserviceStat();
  virtual ~ObAllVirtualRootserviceStat()
  {}

  int init(ObRootService& rootservice);
  virtual int init_all_data() override;
  virtual int get_next_full_row(const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns) override;

private:
  // data members
  ObRootService* rootservice_;
  common::ObDiagnoseTenantInfo sys_tenant_di_;
  int32_t stat_iter_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualRootserviceStat);
};

}  // end namespace rootserver
}  // end namespace oceanbase
#endif /* _OB_ALL_VIRTUAL_ROOTSERVICE_STAT_H */
