/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
