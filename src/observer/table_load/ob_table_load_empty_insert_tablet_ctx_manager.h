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

#ifndef _OB_TABLE_LOAD_EMPTY_INSERT_TABLET_CTX_MANAGER_H_
#define _OB_TABLE_LOAD_EMPTY_INSERT_TABLET_CTX_MANAGER_H_

#include "lib/container/ob_iarray.h"
#include "observer/table_load/ob_table_load_partition_location.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace table
{
struct ObTableLoadPartitionId;
} // namespace table
namespace observer
{

class ObTableLoadEmptyInsertTabletCtxManager
{
  using LeaderInfo = ObTableLoadPartitionLocation::LeaderInfo;
  static const int64_t TABLET_COUNT_PER_TASK = 20;
public:
  ObTableLoadEmptyInsertTabletCtxManager();
  ~ObTableLoadEmptyInsertTabletCtxManager();
  int init(
      const common::ObIArray<table::ObTableLoadPartitionId> &partition_ids,
      const common::ObIArray<table::ObTableLoadPartitionId> &target_partition_ids);
  int get_next_task(ObAddr &addr,
                    ObIArray<table::ObTableLoadLSIdAndPartitionId> &partition_ids,
                    ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_ids);
  int set_thread_count(const int64_t thread_count);
  int handle_thread_finish(bool &is_finish);
  static int execute(const uint64_t &table_id,
                     const ObTableLoadDDLParam &ddl_param,
                     const ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_part_ids,
                     const ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_part_ids);
  static int execute_for_dag(const uint64_t &table_id,
                             const ObTableLoadDDLParam &ddl_param,
                             const ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_part_ids,
                             const ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_part_ids);
private:
  ObTableLoadPartitionLocation partition_location_;
  ObTableLoadPartitionLocation target_partition_location_;
  table::ObTableLoadArray<LeaderInfo> all_leader_info_array_;
  table::ObTableLoadArray<LeaderInfo> target_all_leader_info_array_;
  int64_t thread_count_ CACHE_ALIGNED;
  lib::ObMutex op_lock_;
  int64_t idx_;
  int64_t start_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase
#endif // _OB_TABLE_LOAD_EMPTY_INSERT_TABLET_CTX_MANAGER_H_