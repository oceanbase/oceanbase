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

#pragma once

#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"

namespace oceanbase
{
namespace table
{

struct ObTableLoadSqlStatistics
{
  OB_UNIS_VERSION(1);
public:
  ObTableLoadSqlStatistics() : allocator_("TLD_Opstat")
  {
    table_stat_array_.set_tenant_id(MTL_ID());
    col_stat_array_.set_tenant_id(MTL_ID());
    allocator_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadSqlStatistics() { reset(); }
  void reset();
  bool is_empty() const
  {
    return table_stat_array_.count() == 0 || col_stat_array_.count() == 0;
  }
  int allocate_table_stat(ObOptTableStat *&table_stat);
  int allocate_col_stat(ObOptOSGColumnStat *&col_stat);
  int merge(const ObTableLoadSqlStatistics& other);
  int get_table_stat_array(ObIArray<ObOptTableStat*> &table_stat_array) const;
  int get_col_stat_array(ObIArray<ObOptColumnStat*> &col_stat_array) const;
  int persistence_col_stats();
  ObIArray<ObOptTableStat*> & get_table_stat_array() {return table_stat_array_; }
  ObIArray<ObOptOSGColumnStat*> & get_col_stat_array() {return col_stat_array_; }
  TO_STRING_KV(K_(col_stat_array), K_(table_stat_array));
public:
  common::ObArray<ObOptTableStat *> table_stat_array_;
  common::ObArray<ObOptOSGColumnStat *> col_stat_array_;
  common::ObArenaAllocator allocator_;
};

} // namespace table
} // namespace oceanbase
