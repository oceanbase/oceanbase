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

#include "share/stat/ob_opt_column_stat.h"
#include "share/stat/ob_opt_osg_column_stat.h"
#include "share/stat/ob_opt_table_stat.h"
#include "share/stat/ob_stat_define.h"

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
  OB_INLINE int64_t get_table_stat_count() const { return table_stat_array_.count(); }
  OB_INLINE int64_t get_col_stat_count() const { return col_stat_array_.count(); }
  OB_INLINE bool is_empty() const { return table_stat_array_.empty() && col_stat_array_.empty(); }
  int create(int64_t column_count);
  int merge(const ObTableLoadSqlStatistics &other);
  int get_table_stat(int64_t idx, ObOptTableStat *&table_stat);
  int get_col_stat(int64_t idx, ObOptOSGColumnStat *&osg_col_stat);
  int get_table_stat_array(ObIArray<ObOptTableStat *> &table_stat_array) const;
  int get_col_stat_array(ObIArray<ObOptColumnStat *> &col_stat_array) const;
  int get_table_stats(TabStatIndMap &table_stats) const;
  int get_col_stats(ColStatIndMap &col_stats) const;
  ObOptOSGSampleHelper& get_sample_helper() { return sample_helper_; }
  TO_STRING_KV(K_(col_stat_array), K_(table_stat_array));
private:
  int allocate_table_stat(ObOptTableStat *&table_stat);
  int allocate_col_stat(ObOptOSGColumnStat *&col_stat);
public:
  common::ObArray<ObOptTableStat *> table_stat_array_;
  common::ObArray<ObOptOSGColumnStat *> col_stat_array_;
  common::ObArenaAllocator allocator_;
  ObOptOSGSampleHelper sample_helper_;
};

} // namespace table
} // namespace oceanbase
