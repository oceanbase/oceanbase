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
#include "lib/container/ob_array.h"
namespace oceanbase
{
namespace table
{
class ObTableLoadDmlStat;
class ObTableLoadSqlStatistics;
} // namespace table
namespace observer
{
class ObTableLoadStoreTableCtx;
class ObTableLoadStoreCtx;
class ObTableLoadMergerManager
{
public:
  ObTableLoadMergerManager(ObTableLoadStoreCtx *store_ctx) : store_ctx_(store_ctx), index_merger_idx_(0), is_inited_(false) {}
  void stop();
  int start();
  int init();
  int handle_merge_finish();
  int commit(table::ObTableLoadDmlStat &dml_stats, table::ObTableLoadSqlStatistics &sql_statistics);
private:
  ObTableLoadStoreCtx * store_ctx_;
  common::ObArray<ObTableLoadStoreTableCtx*> index_store_array_;
  int index_merger_idx_;
  bool is_inited_;
};

}
}