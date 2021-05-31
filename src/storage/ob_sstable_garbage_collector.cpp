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

#define USING_LOG_PREFIX STORAGE

#include "ob_sstable_garbage_collector.h"
#include "storage/ob_partition_service.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_pg_storage.h"
#include "storage/ob_partition_scheduler.h"

namespace oceanbase {
using namespace common;
using namespace transaction;
namespace storage {

ObSSTableGarbageCollector::ObSSTableGarbageCollector() : trans_service_(nullptr), is_inited_(false)
{}

ObSSTableGarbageCollector::~ObSSTableGarbageCollector()
{
  destroy();
}

void ObSSTableGarbageCollector::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    trans_service_ = nullptr;
    is_inited_ = false;
  }
}

int ObSSTableGarbageCollector::init(ObTransService* trans_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(trans_service)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid argument", K(ret), KP(trans_service));
  } else {
    trans_service_ = trans_service;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableGarbageCollector::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableGarbageCollector is not inited", K(ret));
  } else if (OB_FAIL(lib::ThreadPool::start())) {
    LOG_WARN("failed to start thread", K(ret));
  } else {
    LOG_INFO("ObSSTableGarbageCollector starts to run");
  }
  return ret;
}

void ObSSTableGarbageCollector::run1()
{
  lib::set_thread_name("ObSSTableGC");
  while (!has_set_stop()) {
    int ret = OB_SUCCESS;
    const int64_t start_ts = ObTimeUtility::current_time();
    ObIPartitionGroupIterator* pg_iter = nullptr;
    const int64_t frozen_version = ObPartitionScheduler::get_instance().get_frozen_version();
    if (OB_ISNULL(pg_iter = ObPartitionService::get_instance().alloc_pg_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc_pg_iter", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        ObIPartitionGroup* pg = nullptr;
        if (OB_FAIL(pg_iter->get_next(pg))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next partition", K(ret));
          }
        } else if (OB_ISNULL(pg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pg should not be null", K(ret));
        } else if (OB_FAIL(pg->get_pg_storage().update_upper_trans_version_and_gc_sstable(
                       *trans_service_, frozen_version))) {
          LOG_WARN("failed to gc_pg_sstables_", K(ret), "pg_key", pg->get_partition_key());
          // reset ret code, continue processing next partition
          ret = OB_SUCCESS;
        }
      }
    }
    if (nullptr != pg_iter) {
      ObPartitionService::get_instance().revert_pg_iter(pg_iter);
      pg_iter = nullptr;
    }
    const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    if (cost_ts < SSTABLE_GC_INTERVAL) {
      usleep(SSTABLE_GC_INTERVAL - cost_ts);
    }
    if (TC_REACH_COUNT_INTERVAL(5)) {
      FLOG_INFO("sstable garbage collector is running", K(cost_ts));
    }
  }
}

}  // namespace storage
}  // namespace oceanbase
