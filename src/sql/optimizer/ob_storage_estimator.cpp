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

#define USING_LOG_PREFIX SQL_OPT
#include "ob_storage_estimator.h"
#include "storage/tx_storage/ob_access_service.h"
#include "storage/access/ob_table_scan_range.h"
#include "share/ob_simple_batch.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase {
using namespace storage;
using namespace share;

namespace sql {

int ObStorageEstimator::estimate_row_count(const obrpc::ObEstPartArg &arg,
                                           obrpc::ObEstPartRes &res)
{
  int ret = OB_SUCCESS;
  //est path rows
  ObTableScanParam param;
  param.schema_version_ = arg.schema_version_;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_params_.count(); i++) {
    obrpc::ObEstPartResElement est_res;
    param.index_id_ = arg.index_params_.at(i).index_id_;
    param.scan_flag_ = arg.index_params_.at(i).scan_flag_;
    param.tablet_id_ = arg.index_params_.at(i).tablet_id_;
    param.ls_id_ = arg.index_params_.at(i).ls_id_;
    param.tx_id_ = arg.index_params_.at(i).tx_id_;
    if (OB_FAIL(storage_estimate_rowcount(
                  arg.index_params_.at(i).tenant_id_,
                  param,
                  arg.index_params_.at(i).batch_,
                  est_res))) {
      LOG_WARN("failed to estimate index row count", K(ret));
    } else if (OB_FAIL(res.index_param_res_.push_back(est_res))) {
      LOG_WARN("failed to push back result", K(ret));
    } else {
      LOG_TRACE("[OPT EST]: row count stat", K(est_res), K(i), K(param));
    }
  }
#if !defined(NDEBUG)
  if (OB_SUCC(ret)) {
    LOG_INFO("[OPT EST] rowcount estimation result", K(arg), K(res));
  }
#endif
  return ret;
}

int ObStorageEstimator::estimate_block_count_and_row_count(const obrpc::ObEstBlockArg &arg,
                                                           obrpc::ObEstBlockRes &res)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_params_arg_.count(); ++i) {
    obrpc::ObEstBlockResElement est_res;
    if (OB_FAIL(storage_estimate_block_count_and_row_count(arg.tablet_params_arg_.at(i), est_res))) {
      LOG_WARN("failed to estimate tablet block count and row count", K(ret));
    } else if (OB_FAIL(res.tablet_params_res_.push_back(est_res))) {
      LOG_WARN("failed to push back result", K(ret));
    } else {
      LOG_TRACE("[OPT EST]: block count and row count stat", K(est_res), K(i), "param", arg.tablet_params_arg_.at(i));
    }
  }
#if !defined(NDEBUG)
  if (OB_SUCC(ret)) {
    LOG_INFO("[OPT EST] block count and row count estimation result", K(arg), K(res));
  }
#endif
  return ret;
}

// estimate scan rowcount
int ObStorageEstimator::storage_estimate_rowcount(const uint64_t tenant_id,
                                                  const ObTableScanParam &param,
                                                  const ObSimpleBatch &batch,
                                                  obrpc::ObEstPartResElement &res)
{
  int ret = OB_SUCCESS;
  double rc_logical = 0;
  double rc_physical = 0;
  if (!batch.is_valid()) {
    // do nothing when there is no scan range
    res.logical_row_count_ = static_cast<int64_t>(rc_logical);
    res.physical_row_count_ = static_cast<int64_t>(rc_physical);
    res.reliable_ = true;
  } else if (OB_FAIL(storage_estimate_partition_batch_rowcount(
                       tenant_id,
                       batch,
                       param,
                       res.est_records_,
                       rc_logical,
                       rc_physical))) {
    LOG_WARN("fail to get partition batch rowcount", K(param.tablet_id_), K(batch), K(ret));
    res.reset();
    ret = OB_SUCCESS;
  } else {
    res.logical_row_count_ = static_cast<int64_t>(rc_logical);
    res.physical_row_count_ = static_cast<int64_t>(rc_physical);
    res.reliable_ = true;
  }
  LOG_TRACE("[OPT EST]:estimate partition scan batch rowcount", K(res), K(batch), K(ret));
  return ret;
}

//@shanyan.g 调整层按照parition级别来操作
int ObStorageEstimator::storage_estimate_partition_batch_rowcount(
    const uint64_t tenant_id,
    const ObSimpleBatch &batch,
    const storage::ObTableScanParam &table_scan_param,
    ObIArray<ObEstRowCountRecord> &est_records,
    double &logical_row_count,
    double &physical_row_count)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    int64_t rc_logical = 0;
    int64_t rc_physical = 0;
    ObArenaAllocator allocator;
    ObAccessService *access_service = NULL;
    storage::ObTableScanRange table_scan_range;
    if (OB_ISNULL(access_service = MTL(ObAccessService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(access_service));
    } else if (OB_FAIL(table_scan_range.init(batch, allocator))) {
      STORAGE_LOG(WARN, "Failed to init table scan range", K(ret), K(batch));
    } else if (OB_FAIL(access_service->estimate_row_count(table_scan_param,
                                                          table_scan_range,
                                                          est_records,
                                                          rc_logical,
                                                          rc_physical))) {
      LOG_TRACE("OPT:[STORAGE EST FAILED, USE STAT EST]", "storage_ret", ret);
    } else {
      LOG_TRACE("storage estimate row count result", K(rc_logical), K(rc_physical),
                K(table_scan_param), K(table_scan_range), K(ret));
        logical_row_count = rc_logical < 0 ? 1.0 : static_cast<double>(rc_logical);
        physical_row_count = rc_physical < 0 ? 1.0 : static_cast<double>(rc_physical);
    }
  }
  
  return ret;
}

int ObStorageEstimator::storage_estimate_block_count_and_row_count(
    const obrpc::ObEstBlockArgElement &arg,
    obrpc::ObEstBlockResElement &res)
{
  int ret = OB_SUCCESS;
  int64_t macro_block_count = 0;
  int64_t micro_block_count = 0;
  int64_t sstable_row_count = 0;
  int64_t memtable_row_count = 0;

  if (!arg.is_valid()) {
    res.macro_block_count_ = macro_block_count;
    res.micro_block_count_ = micro_block_count;
    res.sstable_row_count_ = sstable_row_count;
    res.memtable_row_count_ = memtable_row_count;
  } else {
    const uint64_t tenant_id = arg.tenant_id_;
    MTL_SWITCH(tenant_id) {
      ObAccessService *access_service = NULL;
      if (OB_ISNULL(access_service = MTL(ObAccessService *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(access_service));
      } else if (OB_FAIL(access_service->estimate_block_count_and_row_count(arg.ls_id_,
                                                                            arg.tablet_id_,
                                                                            macro_block_count,
                                                                            micro_block_count,
                                                                            sstable_row_count,
                                                                            memtable_row_count))) {
        LOG_WARN("OPT:[STORAGE EST BLOCK COUNT AND ROW COUNT FAILED]", "storage_ret", ret);
      } else {
        LOG_TRACE("storage estimate block count and row count result", K(macro_block_count),
                K(micro_block_count), K(sstable_row_count), K(memtable_row_count), K(ret));
        res.macro_block_count_ = macro_block_count;
        res.micro_block_count_ = micro_block_count;
        res.sstable_row_count_ = sstable_row_count;
        res.memtable_row_count_ = memtable_row_count;
      }
    }
  }
  return ret;
}

} // end of sql
} // end of oceanbase
