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
 *
 * This file defines test_ob_cdc_part_trans_resolver.cpp
 */

#define USING_LOG_PREFIX OBLOG

#include "gtest/gtest.h"

#include "log_generator.h" // must at last of header list
#define private public
#include "logservice/libobcdc/src/ob_cdc_part_trans_resolver.h"
#undef private
#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/tx/ob_trans_log.h"
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"
#include "logservice/libobcdc/src/ob_log_utils.h"
#include "logservice/libobcdc/src/ob_log_ls_fetch_ctx.h"
#include "logservice/libobcdc/src/ob_log_ls_fetch_mgr.h"
#include "logservice/libobcdc/src/ob_log_entry_task_pool.h"
#include "logservice/libobcdc/src/ob_log_part_progress_controller.h"
#include "logservice/libobcdc/src/ob_log_part_trans_resolver_factory.h"
#include "logservice/libobcdc/src/ob_log_sys_ls_task_handler.h"
#include "logservice/libobcdc/src/ob_log_cluster_id_filter.h"
#include "logservice/libobcdc/src/ob_log_committer.h"
#include "logservice/libobcdc/src/ob_log_fetcher_dispatcher.h"
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_resource_collector.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;
using namespace transaction;
using namespace storage;
using namespace logfetcher;

#define PREPARE_ENV(tenant_id, ls_id, tx_id, cluster_id) \
    bool stop_flag = false; \
    logservice::TenantLSID tls_id(tenant_id, share::ObLSID(ls_id)); \
    EXPECT_TRUE(tls_id.is_valid()); \
    IObCDCPartTransResolver::MissingLogInfo missing_info; \
    logfetcher::TransStatInfo tsi; \
    int64_t start_ts_ns = 1; \
    palf::LSN start_lsn(0); \
    EXPECT_TRUE(start_lsn.is_valid());


#define GET_LS_FETCH_MGR(ls_fetch_mgr) \
    ObLogInstance *instance = ObLogInstance::get_instance(); \
    ObConcurrentFIFOAllocator fifo_allocator; \
    PartProgressController progress_controller; \
    EXPECT_EQ(OB_SUCCESS, progress_controller.init(10)); \
    ObLogPartTransResolverFactory resolver_factory; \
    ObLogTransTaskPool<PartTransTask> task_pool; \
    EXPECT_EQ(OB_SUCCESS, fifo_allocator.init(16 * _G_, 16 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE)); \
    EXPECT_EQ(OB_SUCCESS, task_pool.init(&fifo_allocator, PREALLOC_POOL_SIZE, true, PREALLOC_PAGE_COUNT)); \
    ObLogEntryTaskPool log_entry_task_pool; \
    EXPECT_EQ(OB_SUCCESS, log_entry_task_pool.init(10/* fixed_log_entry_task_count */)); \
    MockFetcherDispatcher fetcher_dispatcher; \
    MockResourceCollector resource_collector; \
    instance->resource_collector_ = &resource_collector; \
    ObLogSysLsTaskHandler sys_ls_handler; \
    ObLogCommitter committer; \
    EXPECT_EQ(OB_SUCCESS, fetcher_dispatcher.init(&sys_ls_handler, &committer, 0)); \
    ObLogClusterIDFilter cluster_id_filter; \
    const char *cluster_id_black_list = "2147473648"; \
    double a = 1.0; \
    void *fetcher = &a; \
    EXPECT_EQ(OB_SUCCESS, cluster_id_filter.init(cluster_id_black_list, 2147473648, 2147483647)); \
    EXPECT_EQ(OB_SUCCESS, resolver_factory.init(task_pool, log_entry_task_pool, fetcher_dispatcher, cluster_id_filter)); \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.init(1, progress_controller, resolver_factory, fetcher));


#define PREPARE_LS_FETCH_CTX() \
    const uint64_t tenant_id = 1002; /*should be user tenant but not meta_tenant or sys_tenant*/ \
    const int64_t ls_id = 1001; \
    const int64_t tx_id = 111111; \
    const uint64_t cluster_id = 1; \
    PREPARE_ENV(tenant_id, ls_id, tx_id, cluster_id); \
    ObLogLSFetchMgr ls_fetch_mgr; \
    GET_LS_FETCH_MGR(ls_fetch_mgr); \
    LSFetchCtx *ls_fetch_ctx = NULL; \
    ObLogFetcherStartParameters start_paras; \
    start_paras.reset(start_ts_ns, start_lsn); \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.add_ls(tls_id, start_paras, false, false, \
        ClientFetchingMode::FETCHING_MODE_INTEGRATED, "|")); \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.get_ls_fetch_ctx(tls_id, ls_fetch_ctx)); \
    ObTxLogGenerator log_generator(tenant_id, ls_id, tx_id, cluster_id);


#define GET_PART_TRANS_TASK() \
  ObCDCPartTransResolver *resolver = static_cast<ObCDCPartTransResolver*>(ls_fetch_ctx->get_part_trans_resolver()); \
  PartTransTask *part_trans_task = NULL; \
  EXPECT_EQ(OB_SUCCESS, resolver->obtain_task_(tx_id, part_trans_task, false));


#define DESTROY_OBLOG_INSTANCE() \
    instance->resource_collector_ = NULL; \
    ObLogInstance::destroy_instance();

namespace oceanbase
{
namespace libobcdc
{
class MockFetcherDispatcher : public ObLogFetcherDispatcher
{
  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag) override
  {
    return OB_SUCCESS;
  }
};
class MockResourceCollector : public ObLogResourceCollector
{
  virtual int revert(PartTransTask *task)
  {
    return OB_SUCCESS;
  }
};
}
namespace unittest
{

// Task Pool
static const int64_t PREALLOC_POOL_SIZE = 10 * 1024;
static const int64_t TRANS_TASK_BLOCK_SIZE = 4 * 1024 *1024;
static const int64_t PREALLOC_PAGE_COUNT = 1024;

// test trans count
static const int64_t TRANS_COUNT = 100;
// redo log count
static const int64_t TRANS_REDO_LOG_COUNT = 100;

void call_sort_and_unique_missing_log_ids(IObCDCPartTransResolver::MissingLogInfo &missing_info)
{
  LOG_DEBUG("MISSING LOG [BEGIN]", K(missing_info));
  EXPECT_EQ(OB_SUCCESS, missing_info.sort_and_unique_missing_log_lsn());
  LOG_DEBUG("MISSING LOG [END]", K(missing_info));
}

TEST(ObCDCPartTransResolver, test_misslog_info_basic)
{
  int ret = OB_SUCCESS;
  IObCDCPartTransResolver::MissingLogInfo missing_info;
  ObLogLSNArray &missing_log_id = missing_info.get_miss_redo_lsn_arr();

  // prepare data
  palf::LSN lsn_1(1);
  palf::LSN lsn_2(2);
  palf::LSN lsn_3(3);
  palf::LSN lsn_4(4);
  palf::LSN lsn_5(5);
  palf::LSN record_lsn(6);
  ObLogLSNArray local_lsn_arr;
  local_lsn_arr.reset();
  local_lsn_arr.push_back(lsn_1);
  local_lsn_arr.push_back(lsn_2);
  local_lsn_arr.push_back(lsn_3);
  local_lsn_arr.push_back(lsn_4);

  // 1. one miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 2. two miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 3. repeatable miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 4. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // // 5. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_lsn_arr(local_lsn_arr));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 6. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }


  // // 7. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 8. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 9. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }
}


TEST(ObCDCPartTransResolver, test_ls_fetch_ctx)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-0", K(log_entry), K(lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-1", K(log_entry), K(lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-2", K(log_entry), K(lsn));
  LogEntry offline_ls_entry;
  LSN offline_lsn;
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_ls_offline_log_entry(offline_ls_entry, offline_lsn));
  EXPECT_EQ(OB_INVALID_DATA, ls_fetch_ctx->read_log(offline_ls_entry, offline_lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(offline_ls_entry, offline_lsn, missing_info, tsi, stop_flag));

  EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.remove_ls(tls_id));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ls_fetch_mgr.get_ls_fetch_ctx(tls_id, ls_fetch_ctx));
  DESTROY_OBLOG_INSTANCE();
}

// seq1: redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq1)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// seq2: redo | redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq2)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq2
TEST(ObCDCPartTransResolver, test_sp_tx_seq2_miss)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  LOG_DEBUG("read log2", K(lsn), K(lsn2), K(missing_info));
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  missing_info.reset();
  missing_info.set_resolving_miss_log();
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// seq3: redo + commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq3)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
//  GET_PART_TRANS_TASK();
//  LOG_DEBUG("part_trans_task", KPC(part_trans_task), K(lsn), K(log_entry), K(lsn2), K(log_entry2));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(0, missing_info.get_total_misslog_cnt());

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq3
TEST(ObCDCPartTransResolver, test_sp_tx_seq3_miss)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));

  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  LOG_DEBUG("test_sp_tx_seq3_miss", K(lsn), K(log_entry), K(lsn2), K(log_entry2));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  missing_info.set_resolving_miss_log();
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  EXPECT_EQ(0, reconsume_miss_info.get_total_misslog_cnt());

  DESTROY_OBLOG_INSTANCE();
}

// seq4: redo | redo + commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq4)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);
  LOG_DEBUG("test_sp_tx_seq4");

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq4(before commit_info_log)
TEST(ObCDCPartTransResolver, test_sp_tx_seq4_miss_1)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry()); // commit_info entry is not miss_log, need reconsume
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq4(before commit_log)
TEST(ObCDCPartTransResolver, test_sp_tx_seq4_miss_2)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn2, missing_info.miss_record_or_state_log_lsn_);
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));
  EXPECT_EQ(1, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(lsn, new_miss_log.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(new_miss_log.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log_2;
  new_miss_log_2.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log_2));

  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, reconsume_miss_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// seq5: redo | commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq5)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch middle of seq5(before log_entry2)
TEST(ObCDCPartTransResolver, test_sp_tx_seq5_miss)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry()); // commit_info entry is not miss_log, need reconsume
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// seq6: redo | rollback_to | redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq6)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq6(before log_entry3)
TEST(ObCDCPartTransResolver, test_sp_tx_seq6_miss)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_EQ(lsn2, missing_info.get_miss_redo_lsn_arr().at(1));
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));

  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, reconsume_miss_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq1: redo + commit_info | prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry2;
  palf::LSN lsn2;
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of dist_seq1(before prepare)
TEST(ObCDCPartTransResolver, test_sp_tx_dist_miss1)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry2;
  palf::LSN lsn2;
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of dist_seq1(before commit)
TEST(ObCDCPartTransResolver, test_sp_tx_dist_miss2)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry2;
  palf::LSN lsn2;
  LogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn2, missing_info.miss_record_or_state_log_lsn_);
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));
  EXPECT_EQ(1, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(lsn, new_miss_log.miss_record_or_state_log_lsn_);
  new_miss_log.reset();
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  missing_info.reset();
  missing_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq2: redo + commit_info + prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist2)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq3: commit_info + prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist3)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// dist_seq4: commit(may occour while transfer case)
// currently disabled cause transfer is not implied yet. will add case for transfer.
TEST(ObCDCPartTransResolver, DISABLED_test_sp_tx_dist4)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry, lsn);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_record)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  LogEntry log_entry1;
  palf::LSN lsn1;
  LogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  LogEntry log_entry2;
  palf::LSN lsn2;
  LogEntry log_entry_rc2;
  palf::LSN lsn_rc2;
  // generate and log_entry below
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc0, lsn_rc0);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry1, lsn1);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc1, lsn_rc1);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc2, lsn_rc2);
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc0, lsn_rc0, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry1, lsn1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc1, lsn_rc1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc2, lsn_rc2, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_record_miss)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  LogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  LogEntry log_entry1;
  palf::LSN lsn1;
  LogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  LogEntry log_entry2;
  palf::LSN lsn2;
  LogEntry log_entry_rc2;
  palf::LSN lsn_rc2;
  // generate and log_entry below
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc0, lsn_rc0);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry1, lsn1);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc1, lsn_rc1);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc2, lsn_rc2);
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry1, lsn1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry_rc1, lsn_rc1, missing_info, tsi, stop_flag));
  EXPECT_TRUE(missing_info.miss_record_or_state_log_lsn_.is_valid());
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  LOG_INFO("", K(lsn), K(lsn_rc0), K(lsn1), K(lsn_rc1), K(lsn2), K(lsn_rc2), K(missing_info));
//  EXPECT_EQ(lsn, missing_info.miss_redo_or_state_lsn_arr_.at(0));
  IObCDCPartTransResolver::MissingLogInfo missing_info1;
  missing_info1.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry_rc0, lsn_rc0, missing_info1, tsi, stop_flag));
  EXPECT_EQ(lsn, missing_info1.get_miss_redo_lsn_arr().at(0));
  IObCDCPartTransResolver::MissingLogInfo missing_info2;
  missing_info2.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info2, tsi, stop_flag));
  IObCDCPartTransResolver::MissingLogInfo missing_info3;
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info3, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc2, lsn_rc2, missing_info3, tsi, stop_flag));
  LOG_INFO("missing_infos", K(missing_info), K(missing_info1), K(missing_info2), K(missing_info3));

  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_seq_example)
{
  PREPARE_LS_FETCH_CTX();
  LogEntry log_entry;
  palf::LSN lsn;
  // generate and log_entry below

  DESTROY_OBLOG_INSTANCE();
}

}
}

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_cdc_part_trans_resolver.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_ob_cdc_part_trans_resolver.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG;TLOG.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
