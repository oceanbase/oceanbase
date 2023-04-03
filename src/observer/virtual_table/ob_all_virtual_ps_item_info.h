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

#ifndef _OB_ALL_VIRTUAL_PS_ITEM_INFO_H
#define _OB_ALL_VIRTUAL_PS_ITEM_INFO_H 1

#include "observer/virtual_table/ob_all_plan_cache_stat.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"

namespace oceanbase
{
using common::ObPsStmtId;
namespace observer
{

class ObAllVirtualPsItemInfo: public ObAllPlanCacheBase
{
public:
  ObAllVirtualPsItemInfo()
    : ObAllPlanCacheBase(),
      stmt_id_array_idx_(OB_INVALID_ID),
      stmt_id_array_(),
      ps_cache_(NULL) {}
  virtual ~ObAllVirtualPsItemInfo() {}

  virtual int inner_get_next_row() override;
  virtual int inner_open() override;
  virtual void reset() override;
private:
  int fill_cells(uint64_t tenant_id,
                 ObPsStmtId stmt_id,
                 sql::ObPsStmtItem *stmt_item,
                 sql::ObPsStmtInfo *stmt_info);
  int get_next_row_from_specified_tenant(uint64_t tenant_id, bool &is_end);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPsItemInfo);

private:
  int64_t stmt_id_array_idx_;
  common::ObSEArray<ObPsStmtId, 1024> stmt_id_array_;
  sql::ObPsCache *ps_cache_;
};

}
}

#endif /* _OB_ALL_VIRTUAL_PS_ITEM_INFO_H */
