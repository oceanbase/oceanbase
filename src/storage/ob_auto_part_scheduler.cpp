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

#include "ob_auto_part_scheduler.h"
#include "common/rowkey/ob_rowkey.h"
#include "lib/ob_define.h"
#include "storage/ob_pg_storage.h"
#include "ob_partition_service.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
namespace storage {
ObAutoPartScheduler::ObAutoPartScheduler() : partition_service_(NULL)
{}

ObAutoPartScheduler::~ObAutoPartScheduler()
{
  destroy();
}

int ObAutoPartScheduler::init(
    storage::ObPartitionService* partition_service, share::schema::ObMultiVersionSchemaService* schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_service) || OB_ISNULL(schema_service)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid arguments", K(ret), KP(partition_service), KP(schema_service));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(ERROR, "ObAutoPartScheduler has already been inited", K(ret));
  } else {
    partition_service_ = partition_service;
    schema_service_ = schema_service;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  STORAGE_LOG(INFO, "ObAutoPartScheduler is inited", K(ret));
  return ret;
}

int ObAutoPartScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObThreadPool::start())) {
    STORAGE_LOG(ERROR, "ObAutoPartScheduler thread failed to start", K(ret));
  } else {
    STORAGE_LOG(INFO, "ObAutoPartScheduler thread start finished", K(ret));
  }
  return ret;
}

void ObAutoPartScheduler::stop()
{
  share::ObThreadPool::stop();
  STORAGE_LOG(INFO, "ObAutoPartScheduler stop finished");
}

void ObAutoPartScheduler::wait()
{
  share::ObThreadPool::wait();
  STORAGE_LOG(INFO, "ObAutoPartScheduler wait finished");
}

void ObAutoPartScheduler::destroy()
{
  is_inited_ = false;
  stop();
  wait();
  partition_service_ = NULL;
  STORAGE_LOG(INFO, "ObAutoPartScheduler destroy");
}

void ObAutoPartScheduler::run1()
{
  lib::set_thread_name("AutoPartSche");
  STORAGE_LOG(INFO, "ObAutoPartScheduler start to run");
  while (!has_set_stop()) {
    const int64_t start_time = ObTimeUtility::current_time();
    // do_work_();
    const int64_t round_cost_time = ObTimeUtility::current_time() - start_time;
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      STORAGE_LOG(INFO, "ObAutoPartScheduler is running", K(round_cost_time));
    }
    int32_t sleep_ts = DO_WORK_INTERVAL - static_cast<const int32_t>(round_cost_time);
    if (sleep_ts < 0) {
      sleep_ts = 0;
    }
    usleep(sleep_ts);
  }
}

void ObAutoPartScheduler::do_work_()
{
  int ret = OB_SUCCESS;
  storage::ObIPartitionGroupIterator* partition_iter = NULL;
  if (NULL == (partition_iter = partition_service_->alloc_pg_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(ERROR, "alloc_scan_iter failed", K(ret));
  } else {
    storage::ObIPartitionGroup* partition = NULL;
    while (!has_set_stop() && OB_SUCC(ret)) {
      if (OB_FAIL(partition_iter->get_next(partition))) {
        // do nothing
      } else if (NULL == partition) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "Unexpected error, partition is NULL", K(ret));
      } else if (!partition->is_valid()) {
        STORAGE_LOG(INFO, "partition is invalid", K(ret));
      } else {
        handle_partition_(partition);
      }
    }

    if (NULL != partition_iter) {
      partition_service_->revert_pg_iter(partition_iter);
      partition_iter = NULL;
    }
  }
}

void ObAutoPartScheduler::handle_partition_(storage::ObIPartitionGroup* partition)
{
  if (need_auto_split_(partition)) {
    execute_range_part_split_(partition);
  }
}

bool ObAutoPartScheduler::need_auto_split_(storage::ObIPartitionGroup* partition)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (check_partition_is_leader_(partition) && !partition->is_splitting()) {
    const common::ObPartitionKey& partition_key = partition->get_partition_key();
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema* table_schema = nullptr;

    if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
      STORAGE_LOG(WARN, "fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.check_formal_guard())) {
      STORAGE_LOG(WARN, "schema_guard is not formal", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(partition_key.get_table_id(), table_schema))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        STORAGE_LOG(INFO, "table is dropped", K(ret), K(partition_key));
      } else {
        STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(partition_key));
      }
    } else if (NULL == table_schema) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "table_schema must not null", K(ret), K(partition_key));
    } else {
      bool_ret = check_partition_is_auto_part_(partition, table_schema) &&
                 check_partition_is_enough_large_(partition, table_schema);
    }
  }
  return bool_ret;
}

bool ObAutoPartScheduler::check_partition_is_auto_part_(
    storage::ObIPartitionGroup* partition, const share::schema::ObTableSchema* table_schema)
{
  UNUSED(partition);
  return table_schema->is_auto_partitioned_table();
}

bool ObAutoPartScheduler::check_partition_is_enough_large_(
    storage::ObIPartitionGroup* partition, const share::schema::ObTableSchema* table_schema)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  const common::ObPartitionKey& partition_key = partition->get_partition_key();
  ObTablesHandle sstables_handle;
  ObTablesHandle memtables_handle;
  common::ObArray<ObSSTable*> sstables;
  common::ObArray<memtable::ObMemtable*> memtables;
  int64_t total_size = 0;
  if (OB_FAIL(partition->get_reference_tables(partition_key, partition_key.get_table_id(), sstables_handle))) {
    STORAGE_LOG(WARN, "get_reference_tables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(partition->get_reference_memtables(memtables_handle))) {
    STORAGE_LOG(WARN, "get_reference_memtables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(sstables_handle.get_all_sstables(sstables))) {
    STORAGE_LOG(WARN, "sstables_handle get_all_sstables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(memtables_handle.get_all_memtables(memtables))) {
    STORAGE_LOG(WARN, "memtables_handle get_all_memtables failed", K(ret), K(partition_key));
  } else {
    for (int64_t idx = 0; idx < sstables.count(); idx++) {
      total_size += sstables[idx]->get_occupy_size();
    }
    for (int64_t idx = 0; idx < memtables.count(); idx++) {
      total_size += memtables[idx]->get_occupied_size();
    }
    bool_ret = (total_size >= table_schema->get_auto_part_size());
  }

  return bool_ret;
}

bool ObAutoPartScheduler::check_partition_is_leader_(storage::ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  clog::ObIPartitionLogService* pls = NULL;
  common::ObRole role = common::INVALID_ROLE;
  if (NULL == (pls = partition->get_log_service())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "partition_log_service is NULL, unexpected", K(ret));
  } else if (OB_FAIL(pls->get_role(role)) || LEADER != role) {
    // do nothing
  }
  return OB_SUCCESS == ret;
}

void ObAutoPartScheduler::execute_range_part_split_(storage::ObIPartitionGroup* partition)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKey& partition_key = partition->get_partition_key();
  obrpc::ObExecuteRangePartSplitArg arg;
  arg.partition_key_ = partition_key;
  arg.exec_tenant_id_ = partition_key.get_tenant_id();
  const int64_t RPC_TIMEOUT = 60 * 1000 * 1000;
  ObArenaAllocator allocator;
  if (OB_FAIL(get_split_rowkey_(partition, arg.rowkey_, allocator))) {
    STORAGE_LOG(WARN, "get_split_rowkey_ failed", K(ret), K(partition_key));
  } else if (OB_FAIL(partition_service_->get_rs_rpc_proxy().timeout(RPC_TIMEOUT).execute_range_part_split(arg))) {
    STORAGE_LOG(WARN, "rpc execute_range_part_split failed", K(ret), K(partition_key), K(arg));
  } else {
    STORAGE_LOG(INFO, "execute_range_part_split finished", K(ret), K(partition_key), K(arg));
  }
}

int ObAutoPartScheduler::get_split_rowkey_(
    storage::ObIPartitionGroup* partition, ObRowkey& rowkey, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKey& partition_key = partition->get_partition_key();
  ObTablesHandle sstables_handle;
  ObTablesHandle memtables_handle;
  common::ObArray<ObSSTable*> sstables;
  common::ObArray<memtable::ObMemtable*> memtables;
  RowkeyArray rowkey_array;

  if (OB_FAIL(partition->get_reference_tables(partition_key, partition_key.get_table_id(), sstables_handle))) {
    STORAGE_LOG(WARN, "get_reference_tables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(partition->get_reference_memtables(memtables_handle))) {
    STORAGE_LOG(WARN, "get_reference_memtables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(sstables_handle.get_all_sstables(sstables))) {
    STORAGE_LOG(WARN, "get_all_sstables failed", K(ret), K(partition_key));
  } else if (OB_FAIL(memtables_handle.get_all_memtables(memtables))) {
    STORAGE_LOG(WARN, "get_all_memtables failed", K(ret), K(partition_key));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < sstables.count(); idx++) {
      if (OB_FAIL(build_rowkey_array_(partition_key, sstables[idx], rowkey_array, allocator))) {
        STORAGE_LOG(WARN, "sstable build_rowkey_array_ failed", K(ret), K(partition_key));
      }
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < memtables.count(); idx++) {
      if (OB_FAIL(build_rowkey_array_(partition_key, memtables[idx], rowkey_array, allocator))) {
        STORAGE_LOG(WARN, "memtable build_rowkey_array_ failed", K(ret), K(partition_key));
      }
    }

    // if (OB_SUCC(ret) && rowkey_array.count() >= 512) {
    if (OB_SUCC(ret) && rowkey_array.count() >= 1) {
      std::sort(rowkey_array.begin(), rowkey_array.end());

      if (OB_FAIL(rowkey_array[rowkey_array.count() / 2].deep_copy(rowkey, allocator))) {
        STORAGE_LOG(WARN, "rowkey deep_copy failed", K(ret), K(partition_key));
      }
    }
  }
  return ret;
}

int ObAutoPartScheduler::build_rowkey_array_(const common::ObPartitionKey& partition_key, storage::ObSSTable* sstable,
    RowkeyArray& rowkey_array, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sstable->get_all_macro_block_endkey(rowkey_array, allocator))) {
    STORAGE_LOG(WARN, "sstable get_all_macro_block_endkey failed", K(ret), K(partition_key));
  }
  return ret;
}

int ObAutoPartScheduler::build_rowkey_array_(const common::ObPartitionKey& partition_key,
    memtable::ObMemtable* memtable, RowkeyArray& rowkey_array, common::ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObStoreRange, 2> range_array;
  const int64_t range_cnt = memtable->get_occupied_size() / (2 * 1024 * 1024) + 2;
  if (OB_FAIL(memtable->get_split_ranges(partition_key.get_table_id(), nullptr, nullptr, range_cnt, range_array))) {
    STORAGE_LOG(WARN, "memtable get_split_ranges failed", K(ret), K(partition_key));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < range_cnt - 1; idx++) {
      ObRowkey tmp_rowkey1;
      ObRowkey tmp_rowkey2;
      tmp_rowkey1 = range_array[idx].get_end_key().get_rowkey();
      if (OB_FAIL(tmp_rowkey1.deep_copy(tmp_rowkey2, allocator))) {
        STORAGE_LOG(WARN, "failed to deep copy rowkey", K(ret), K(partition_key));
      } else if (OB_FAIL(rowkey_array.push_back(tmp_rowkey2))) {
        STORAGE_LOG(WARN, "rowkey_array push_back failed", K(ret), K(partition_key));
      }
    }
  }
  return ret;
}
}  // namespace storage
}  // namespace oceanbase
