/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG

#include "share/stat/catalog/ob_catalog_topk_hist_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
namespace common
{

ObCatalogTopkHistEstimator::ObCatalogTopkHistEstimator(
    sql::ObExecContext &ctx, ObIAllocator &allocator)
    : ObCatalogStatsEstimator(ctx, allocator)
{
}

int ObCatalogTopkHistEstimator::estimate(
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_stat)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(opt_stat);
  LOG_WARN("catalog topk histogram estimation is not supported yet", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog topk histogram estimation");
  return ret;
}

int ObCatalogTopkHistEstimator::fill_hints(
    common::ObIAllocator &alloc,
    const ObString &table_name,
    int64_t gather_vectorize)
{
  int ret = OB_SUCCESS;
  ObSqlString default_hints;
  if (OB_UNLIKELY(table_name.empty() || gather_vectorize < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_name), K(gather_vectorize));
  } else if (OB_FAIL(default_hints.append_fmt(
                 "NO_REWRITE DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', %ld)",
                 gather_vectorize))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(add_hint(default_hints.string(), alloc))) {
    LOG_WARN("failed to add hint", K(ret));
  }
  return ret;
}

int ObCatalogTopkHistEstimator::add_topk_hist_stat_items(
    const ObIArray<ObCatalogColumnStatParam> &column_params,
    ObOptCatalogStat &opt_stat)
{
  int ret = OB_SUCCESS;
  // TODO: histogram not yet supported for catalog tables
  UNUSED(column_params);
  UNUSED(opt_stat);
  return ret;
}

} // namespace common
} // namespace oceanbase
