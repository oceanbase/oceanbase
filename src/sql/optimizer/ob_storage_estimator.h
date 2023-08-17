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

#ifndef OB_STORAGE_ESTIMATOR_H
#define OB_STORAGE_ESTIMATOR_H
#include "share/stat/ob_opt_stat_manager.h"
namespace oceanbase
{
using namespace common;
namespace common {
struct ObSimpleBatch;
struct ObEstRowCountRecord;
}
namespace storage {
class ObIPartitionGroupGuard;
class ObTableScanParam;
}
namespace obrpc {
struct ObEstPartArg;
struct ObEstPartRes;
struct ObEstPartArgElement;
struct ObEstPartResElement;
}
namespace sql
{

class ObStorageEstimator
{
public:
  ObStorageEstimator() {};

  static int estimate_row_count(const obrpc::ObEstPartArg &arg,
                                obrpc::ObEstPartRes &res);

  static int estimate_block_count_and_row_count(const obrpc::ObEstBlockArg &arg,
                                                obrpc::ObEstBlockRes &res);
private:

  // compute memtable whole range row counts
  static int estimate_memtable_row_count(const obrpc::ObEstPartArg &arg,
                                         int64_t &logical_row_count,
                                         int64_t &physical_row_count);

  /**
  * @brief storage_estimate_rowcount
  * estimate rowcount for an index access path using storage interface
  */
  static int storage_estimate_rowcount(const uint64_t tenant_id,
                                       const storage::ObTableScanParam &param,
                                       const ObSimpleBatch &batch,
                                       obrpc::ObEstPartResElement &res);

  // do compute query range row counts
  // 通过存储层接口获取逻辑行和物理行信息
  //@param[in] batch : query range集合
  //@param[in] table_scan_param: table scan 参数
  //@param[in] range_columns_count: 索引列数
  //@param[in] part_service: partition service
  static int storage_estimate_partition_batch_rowcount(
      const uint64_t tenant_id,
      const ObSimpleBatch &batch,
      const storage::ObTableScanParam &table_scan_param,
      ObIArray<common::ObEstRowCountRecord> &est_records,
      double &logical_row_count,
      double &physical_row_count);

  /**
  * @brief storage_estimate_block_count_and_row_count
  * estimate the blockcount of tablet by using storage interface
  */
  static int storage_estimate_block_count_and_row_count(const obrpc::ObEstBlockArgElement &arg,
                                                        obrpc::ObEstBlockResElement &res);
};

}
}

#endif // OB_STORAGE_ESTIMATOR_H
