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

#ifndef _OB_ALL_VIRTUAL_PS_STAT_H
#define _OB_ALL_VIRTUAL_PS_STAT_H 1

#include "observer/virtual_table/ob_all_plan_cache_stat.h"

namespace oceanbase
{
namespace sql
{
class ObPsCache;
}
namespace observer
{

class ObAllVirtualPsStat : public ObAllPlanCacheBase
{
public:
  ObAllVirtualPsStat() : ObAllPlanCacheBase() {}
  virtual ~ObAllVirtualPsStat() {}

  virtual int inner_get_next_row() override;
  virtual int inner_open() override;
private:
  int fill_cells(sql::ObPsCache &ps_cache, uint64_t tenant_id);
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPsStat);
};

}
}

#endif /* _OB_ALL_VIRTUAL_PS_STAT_H */
