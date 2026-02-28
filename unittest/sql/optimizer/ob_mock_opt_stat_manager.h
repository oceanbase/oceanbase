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

#ifndef _OB_OPT_MOCK_STAT_MANAGER_H
#define _OB_OPT_MOCK_STAT_MANAGER_H
#include "share/stat/ob_opt_column_stat_cache.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "share/stat/ob_opt_stat_service.h"
#include "share/stat/ob_opt_table_stat.h"

#include "sql/resolver/ob_schema_checker.h"

#include "sql/optimizer/ob_opt_default_stat.h"
#include "lib/timezone/ob_time_convert.h"

namespace test
{

class MockOptStatManager : public oceanbase::common::ObOptStatManager
{
 public:
  virtual int check_opt_stat_validity(oceanbase::sql::ObExecContext &ctx,
                                      const uint64_t tenant_id,
                                      const uint64_t table_ref_id,
                                      const ObIArray<int64_t> &part_ids,
                                      bool &is_opt_stat_valid) override
  {
    UNUSED(ctx);
    UNUSED(tenant_id);
    UNUSED(table_ref_id);
    UNUSED(part_ids);
    is_opt_stat_valid = false;
    return OB_SUCCESS;
  }
  virtual int check_system_stat_validity(oceanbase::sql::ObExecContext *ctx,
                                         const uint64_t tenant_id,
                                         bool &is_valid) override
  {
    UNUSED(ctx);
    UNUSED(tenant_id);
    is_valid = false;
    return OB_SUCCESS;
  }
  virtual int check_opt_stat_validity(oceanbase::sql::ObExecContext &ctx,
                              const uint64_t tenant_id,
                              const uint64_t tab_ref_id,
                              const int64_t global_part_id,
                              bool &is_opt_stat_valid) override
  {
    UNUSED(ctx);
    UNUSED(tenant_id);
    UNUSED(tab_ref_id);
    UNUSED(global_part_id);
    is_opt_stat_valid = false;
    return OB_SUCCESS;
  }
  virtual int batch_get_column_stats(const uint64_t tenant_id,
                                     const uint64_t table_id,
                                     const ObIArray<int64_t> &part_ids,
                                     const ObIArray<uint64_t> &column_ids,
                                     const int64_t row_cnt,
                                     const double scale_ratio,
                                     ObIArray<ObGlobalColumnStat> &stat,
                                     ObIAllocator *alloc) override
  {
    UNUSED(tenant_id);
    UNUSED(table_id);
    UNUSED(part_ids);
    UNUSED(column_ids);
    UNUSED(row_cnt);
    UNUSED(scale_ratio);
    UNUSED(alloc);
    stat.reset();
    return OB_SUCCESS;
  }
  virtual int get_column_stat(const uint64_t tenant_id,
                              const uint64_t table_id,
                              const ObIArray<int64_t> &part_ids,
                              const uint64_t column_id,
                              const int64_t row_cnt,
                              const double scale_ratio,
                              ObGlobalColumnStat &stat,
                              ObIAllocator *alloc) override
  {
    UNUSED(tenant_id);
    UNUSED(table_id);
    UNUSED(part_ids);
    UNUSED(column_id);
    UNUSED(row_cnt);
    UNUSED(scale_ratio);
    UNUSED(stat);
    UNUSED(alloc);
    return OB_SUCCESS;
  }
  virtual int get_column_stat(const uint64_t tenant_id,
                              const uint64_t ref_id,
                              const int64_t part_id,
                              const uint64_t col_id,
                              ObOptColumnStatHandle &handle) override
  {
    UNUSED(tenant_id);
    mock_col_stat_.set_table_id(ref_id);
    mock_col_stat_.set_partition_id(part_id);
    mock_col_stat_.set_column_id(col_id);
    handle.stat_ = &mock_col_stat_;
    return OB_SUCCESS;
  }
  virtual int get_table_stat(const uint64_t tenant_id,
                             const uint64_t table_ref_id,
                             const int64_t part_id,
                             const double scale_ratio,
                             ObGlobalTableStat &stat) override
  {
    UNUSED(tenant_id);
    UNUSED(table_ref_id);
    UNUSED(part_id);
    UNUSED(scale_ratio);
    stat.set_last_analyzed(0);
    return OB_SUCCESS;
  }
  virtual int get_table_stat(const uint64_t tenant_id,
                             const uint64_t tab_ref_id,
                             const ObIArray<int64_t> &part_ids,
                             const double scale_ratio,
                             ObGlobalTableStat &stat) override
  {
    UNUSED(tenant_id);
    UNUSED(tab_ref_id);
    UNUSED(part_ids);
    UNUSED(scale_ratio);
    stat.set_last_analyzed(0);
    return OB_SUCCESS;
  }
  virtual int get_table_stat(const uint64_t tenant_id,
                             const uint64_t table_id,
                             const ObIArray<int64_t> &part_ids,
                             ObIArray<ObOptTableStat> &tstats) override
  {
    UNUSED(tenant_id);
    UNUSED(table_id);
    UNUSED(part_ids);
    tstats.reset();
    return OB_SUCCESS;
  }
  ObOptColumnStat mock_col_stat_;
};

} // end namespace test

#endif /* _OB_MOCK_OPT_STAT_MANAGER_H */

