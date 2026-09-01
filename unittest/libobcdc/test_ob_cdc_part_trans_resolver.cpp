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


#include "log_generator.h" // must at last of header list
#define private public
#include "logservice/libobcdc/src/ob_cdc_part_trans_resolver.h"
#undef private
#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/tx/ob_trans_log.h"
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"
#include "logservice/libobcdc/src/ob_log_utils.h"
#include "logservice/libobcdc/src/ob_log_ls_fetch_mgr.h"
#include "logservice/libobcdc/src/ob_log_part_progress_controller.h"
#include "logservice/libobcdc/src/ob_log_part_trans_resolver_factory.h"
#include "logservice/libobcdc/src/ob_log_sys_ls_task_handler.h"
#include "logservice/libobcdc/src/ob_log_cluster_id_filter.h"
#include "logservice/libobcdc/src/ob_log_committer.h"
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_resource_collector.h"
#include "logservice/libobcdc/src/ob_log_lsn_filter.h"
#include "logservice/libobcdc/src/ob_log_trans_id_filter.h"

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
    ObLogLsnFilter lsn_filter; \
    ObLogTransIDFilter trans_id_filter; \
    double a = 1.0; \
    void *fetcher = &a; \
    const int64_t source_cluster_id = 1; \
    EXPECT_EQ(OB_SUCCESS, cluster_id_filter.init(cluster_id_black_list, 2147473648, 2147483647)); \
    EXPECT_EQ(OB_SUCCESS, lsn_filter.init("|")); \
    EXPECT_EQ(OB_SUCCESS, trans_id_filter.init("|")); \
    EXPECT_EQ(OB_SUCCESS, resolver_factory.init(task_pool, log_entry_task_pool, fetcher_dispatcher, cluster_id_filter, lsn_filter, trans_id_filter, source_cluster_id)); \
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
    logservice::ObLogserviceModelInfo logservice_model_info; \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.add_ls(tls_id, start_paras, false, false, \
        ClientFetchingMode::FETCHING_MODE_INTEGRATED, "|", logservice_model_info)); \
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
  ipalf::ILogEntry log_entry;
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
  ipalf::ILogEntry offline_ls_entry;
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
  ipalf::ILogEntry log_entry;
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
  ipalf::ILogEntry log_entry;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  ipalf::ILogEntry log_entry2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  ipalf::ILogEntry log_entry2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));

  ipalf::ILogEntry log_entry2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry, lsn);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_record)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  ipalf::ILogEntry log_entry1;
  palf::LSN lsn1;
  ipalf::ILogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry_rc2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  ipalf::ILogEntry log_entry1;
  palf::LSN lsn1;
  ipalf::ILogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry_rc2;
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
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  // generate and log_entry below

  DESTROY_OBLOG_INSTANCE();
}

TEST(MemtableMutatorRow, test_deserialize_matches_storage_row)
{
  // 验证完整格式的存储层 Row 经 CDC 反序列化后，各字段、rowkey 和 pos
  // 与原始数据一致；同时验证重复反序列化会被拒绝，以及 first/second
  // 两段式解析能得到相同的 table id 和 table version。
  //
  // TDE 构建中首个变长字段表示加密信息索引，因此这里使用 0，确保测试
  // 覆盖普通未加密 Row 路径。
  const uint64_t table_id = 0;
  const int64_t table_version = 12345;
  const uint32_t update_seq = 17;
  const uint32_t acc_checksum = 0x12345678;
  const int64_t version = 987654321;
  const int32_t flag = 11;
  const ObTxSEQ seq_no(456, 2);
  const int64_t column_cnt = 9;
  const int64_t update_split_trace_id = 2468;
  ObObj key_objs[3];
  key_objs[0].set_int(101);
  key_objs[1].set_int(-202);
  key_objs[2].set_int(303);
  ObStoreRowkey source_rowkey;
  ASSERT_EQ(OB_SUCCESS, source_rowkey.assign(key_objs, ARRAYSIZEOF(key_objs)));

  char new_data_buf[] = "new-row-payload";
  char old_data_buf[] = "old-row-payload";
  memtable::ObRowData new_row;
  memtable::ObRowData old_row;
  new_row.set(new_data_buf, sizeof(new_data_buf));
  old_row.set(old_data_buf, sizeof(old_data_buf));
  memtable::ObMemtableMutatorRow source_row(
      table_id, source_rowkey, table_version, new_row, old_row,
      blocksstable::ObDmlFlag::DF_UPDATE, update_seq, acc_checksum, version,
      flag, seq_no, column_cnt, update_split_trace_id);

  char serialized_buf[4096];
  int64_t serialized_len = sizeof(serialized_buf);
  int64_t serialize_pos = 0;
  ObCLogEncryptInfo encrypt_info;
  encrypt_info.init();
  ASSERT_EQ(OB_SUCCESS, source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));

  ObArenaAllocator allocator;
  MemtableMutatorRow row(allocator);
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      row.deserialize(serialized_buf, serialize_pos, deserialize_pos));
  EXPECT_EQ(serialize_pos, deserialize_pos);
  EXPECT_EQ(static_cast<uint32_t>(serialize_pos), row.row_size_);
  EXPECT_EQ(table_id, row.table_id_);
  EXPECT_EQ(table_version, row.table_version_);
  EXPECT_EQ(blocksstable::ObDmlFlag::DF_UPDATE, row.dml_flag_);
  EXPECT_EQ(update_seq, row.update_seq_);
  EXPECT_EQ(acc_checksum, row.acc_checksum_);
  EXPECT_EQ(version, row.version_);
  EXPECT_EQ(flag, row.flag_);
  EXPECT_EQ(seq_no, row.seq_no_);
  EXPECT_EQ(column_cnt, row.column_cnt_);
  EXPECT_EQ(update_split_trace_id, row.get_update_split_trace_id());
  EXPECT_EQ(new_row, row.new_row_);
  EXPECT_EQ(old_row, row.old_row_);
  ASSERT_EQ(ARRAYSIZEOF(key_objs), row.rowkey_.get_obj_cnt());
  for (int64_t idx = 0; idx < ARRAYSIZEOF(key_objs); ++idx) {
    EXPECT_EQ(key_objs[idx], row.rowkey_.get_obj_ptr()[idx]);
  }
  EXPECT_EQ(OB_STATE_NOT_MATCH,
      row.deserialize(serialized_buf, serialize_pos, deserialize_pos));

  MemtableMutatorRow staged_row(allocator);
  int64_t staged_pos = 0;
  int32_t row_size = 0;
  int64_t staged_table_version = 0;
  ASSERT_EQ(OB_SUCCESS, staged_row.deserialize_first(
      serialized_buf, serialize_pos, staged_pos, row_size));
  EXPECT_EQ(table_id, staged_row.table_id_);
  ASSERT_EQ(OB_SUCCESS, staged_row.deserialize_second(
      serialized_buf, serialize_pos, staged_pos, staged_table_version));
  EXPECT_EQ(table_version, staged_table_version);
  EXPECT_GT(row_size, staged_pos);
}

TEST(MemtableMutatorRow, test_deserialize_legacy_optional_tail)
{
  // 验证兼容各个历史版本的 Row：数据可以分别在 old_row、checksum/version、
  // flag、seq_no、column_cnt 和 update-split trace 后结束。已写入的尾字段
  // 必须正确解析，尚不存在的尾字段必须保持默认值，且不能越过当前 Row frame。
  const uint64_t table_id = 0;
  const int64_t table_version = 100;
  const uint32_t update_seq = 7;
  const uint32_t acc_checksum = 0x12345678;
  const int64_t version = 987654321;
  const int32_t flag = 11;
  const ObTxSEQ seq_no(456, 2);
  const int64_t column_cnt = 9;
  const int64_t update_split_trace_id = 2468;
  ObObj key_obj;
  key_obj.set_int(1);
  ObStoreRowkey source_rowkey;
  ASSERT_EQ(OB_SUCCESS, source_rowkey.assign(&key_obj, 1));
  char new_data_buf[] = "new-row";
  char old_data_buf[] = "old-row";
  memtable::ObRowData new_row;
  memtable::ObRowData old_row;
  new_row.set(new_data_buf, sizeof(new_data_buf));
  old_row.set(old_data_buf, sizeof(old_data_buf));

  char serialized_buf[4096];
  int64_t serialize_pos = serialization::encoded_length_i32(0);
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      serialized_buf, sizeof(serialized_buf), serialize_pos, table_id));
  ASSERT_EQ(OB_SUCCESS, source_rowkey.serialize(
      serialized_buf, sizeof(serialized_buf), serialize_pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      serialized_buf, sizeof(serialized_buf), serialize_pos, table_version));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i8(
      serialized_buf, sizeof(serialized_buf), serialize_pos,
      blocksstable::ObDmlFlag::DF_INSERT));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi32(
      serialized_buf, sizeof(serialized_buf), serialize_pos, update_seq));
  ASSERT_EQ(OB_SUCCESS, new_row.serialize(
      serialized_buf, sizeof(serialized_buf), serialize_pos));
  ASSERT_EQ(OB_SUCCESS, old_row.serialize(
      serialized_buf, sizeof(serialized_buf), serialize_pos));
  int64_t tail_end_pos[6];
  tail_end_pos[0] = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi32(
      serialized_buf, sizeof(serialized_buf), serialize_pos, acc_checksum));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      serialized_buf, sizeof(serialized_buf), serialize_pos, version));
  tail_end_pos[1] = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi32(
      serialized_buf, sizeof(serialized_buf), serialize_pos, flag));
  tail_end_pos[2] = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, seq_no.serialize(
      serialized_buf, sizeof(serialized_buf), serialize_pos));
  tail_end_pos[3] = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      serialized_buf, sizeof(serialized_buf), serialize_pos, column_cnt));
  tail_end_pos[4] = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      serialized_buf, sizeof(serialized_buf), serialize_pos,
      update_split_trace_id));
  tail_end_pos[5] = serialize_pos;

  for (int64_t tail_idx = 0; tail_idx < ARRAYSIZEOF(tail_end_pos); ++tail_idx) {
    int64_t header_pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(
        serialized_buf, sizeof(serialized_buf), header_pos,
        tail_end_pos[tail_idx]));

    ObArenaAllocator allocator;
    MemtableMutatorRow row(allocator);
    int64_t deserialize_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
        row.deserialize(serialized_buf, tail_end_pos[tail_idx],
            deserialize_pos));
    EXPECT_EQ(tail_end_pos[tail_idx], deserialize_pos);
    EXPECT_EQ(static_cast<uint32_t>(tail_end_pos[tail_idx]), row.row_size_);
    EXPECT_EQ(table_id, row.table_id_);
    EXPECT_EQ(table_version, row.table_version_);
    EXPECT_EQ(blocksstable::ObDmlFlag::DF_INSERT, row.dml_flag_);
    EXPECT_EQ(update_seq, row.update_seq_);
    EXPECT_EQ(tail_idx >= 1 ? acc_checksum : 0, row.acc_checksum_);
    EXPECT_EQ(tail_idx >= 1 ? version : 0, row.version_);
    EXPECT_EQ(tail_idx >= 2 ? flag : 0, row.flag_);
    if (tail_idx >= 3) {
      EXPECT_EQ(seq_no, row.seq_no_);
    } else {
      EXPECT_FALSE(row.seq_no_.is_valid());
    }
    EXPECT_EQ(tail_idx >= 4 ? column_cnt : 0, row.column_cnt_);
    EXPECT_EQ(tail_idx >= 5 ? update_split_trace_id : 0,
        row.get_update_split_trace_id());
  }
}

TEST(MemtableMutatorRow, test_compact_size_and_reset_reuse)
{
  // 验证 Compact Row 的对象大小显著小于存储层 Row，防止后续修改重新引入
  // 固定 ObObj[128]；同时验证同一对象 reset 后可以解析下一条不同 Row，
  // 旧 rowkey、尾字段和反序列化状态都不会残留。
  EXPECT_LE(sizeof(MemtableMutatorRow), 384U);
  EXPECT_GT(sizeof(memtable::ObMemtableMutatorRow),
      sizeof(MemtableMutatorRow) * 4);

  ObObj first_key_objs[3];
  first_key_objs[0].set_int(101);
  first_key_objs[1].set_int(102);
  first_key_objs[2].set_int(103);
  ObStoreRowkey first_rowkey;
  ASSERT_EQ(OB_SUCCESS,
      first_rowkey.assign(first_key_objs, ARRAYSIZEOF(first_key_objs)));
  ObObj second_key_obj;
  second_key_obj.set_int(201);
  ObStoreRowkey second_rowkey;
  ASSERT_EQ(OB_SUCCESS, second_rowkey.assign(&second_key_obj, 1));

  memtable::ObRowData empty_row;
  memtable::ObMemtableMutatorRow first_source_row(
      0, first_rowkey, 100, empty_row, empty_row,
      blocksstable::ObDmlFlag::DF_UPDATE, 7, 101, 102, 103,
      ObTxSEQ(104, 1), 105, 106);
  memtable::ObMemtableMutatorRow second_source_row(
      0, second_rowkey, 200, empty_row, empty_row,
      blocksstable::ObDmlFlag::DF_DELETE, 8, 201, 202, 203,
      ObTxSEQ(204, 2), 205, 206);

  char serialized_buf[8192];
  int64_t serialized_len = sizeof(serialized_buf);
  int64_t serialize_pos = 0;
  ObCLogEncryptInfo encrypt_info;
  encrypt_info.init();
  ASSERT_EQ(OB_SUCCESS, first_source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));
  const int64_t first_row_end = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, second_source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));

  ObArenaAllocator allocator;
  MemtableMutatorRow row(allocator);
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      row.deserialize(serialized_buf, serialize_pos, deserialize_pos));
  EXPECT_EQ(first_row_end, deserialize_pos);
  ASSERT_EQ(ARRAYSIZEOF(first_key_objs), row.rowkey_.get_obj_cnt());
  EXPECT_EQ(first_key_objs[2], row.rowkey_.get_obj_ptr()[2]);
  EXPECT_EQ(106, row.get_update_split_trace_id());

  row.reset();
  EXPECT_EQ(0, row.rowkey_.get_obj_cnt());
  EXPECT_EQ(OB_INVALID_ID, row.table_id_);
  EXPECT_EQ(0, row.get_update_split_trace_id());
  EXPECT_EQ(nullptr, row.encrypted_row_holder_);

  ASSERT_EQ(OB_SUCCESS,
      row.deserialize(serialized_buf, serialize_pos, deserialize_pos));
  EXPECT_EQ(serialize_pos, deserialize_pos);
  ASSERT_EQ(1, row.rowkey_.get_obj_cnt());
  EXPECT_EQ(second_key_obj, row.rowkey_.get_obj_ptr()[0]);
  EXPECT_EQ(200, row.table_version_);
  EXPECT_EQ(blocksstable::ObDmlFlag::DF_DELETE, row.dml_flag_);
  EXPECT_EQ(203, row.flag_);
  EXPECT_EQ(ObTxSEQ(204, 2), row.seq_no_);
  EXPECT_EQ(205, row.column_cnt_);
  EXPECT_EQ(206, row.get_update_split_trace_id());
}

TEST(MemtableMutatorRow, test_max_rowkey_and_consecutive_rows)
{
  // 验证协议允许的最大 rowkey 列数，以及同一缓冲区内连续两个 Row 的位置
  // 推进。后续 Compact Row 按实际列数分配时，必须完整支持 128 列上界，
  // 也不能把第一个 Row 的长度或对象状态带到第二个 Row。
  ObObj key_objs[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  for (int64_t idx = 0; idx < ARRAYSIZEOF(key_objs); ++idx) {
    key_objs[idx].set_int(idx + 1);
  }
  ObStoreRowkey source_rowkey;
  ASSERT_EQ(OB_SUCCESS, source_rowkey.assign(key_objs, ARRAYSIZEOF(key_objs)));
  memtable::ObRowData empty_row;
  memtable::ObMemtableMutatorRow source_row(
      0, source_rowkey, 4567, empty_row, empty_row,
      blocksstable::ObDmlFlag::DF_DELETE, 1, 0, 0, 0,
      ObTxSEQ(789, 0), ARRAYSIZEOF(key_objs));

  char serialized_buf[32768];
  int64_t serialized_len = sizeof(serialized_buf);
  int64_t serialize_pos = 0;
  ObCLogEncryptInfo encrypt_info;
  encrypt_info.init();
  ASSERT_EQ(OB_SUCCESS, source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));
  const int64_t first_row_end = serialize_pos;
  ASSERT_EQ(OB_SUCCESS, source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));

  ObArenaAllocator allocator;
  MemtableMutatorRow first_row(allocator);
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      first_row.deserialize(serialized_buf, serialize_pos, deserialize_pos));
  EXPECT_EQ(first_row_end, deserialize_pos);
  ASSERT_EQ(ARRAYSIZEOF(key_objs), first_row.rowkey_.get_obj_cnt());
  for (int64_t idx = 0; idx < ARRAYSIZEOF(key_objs); ++idx) {
    EXPECT_EQ(key_objs[idx], first_row.rowkey_.get_obj_ptr()[idx]);
  }

  MemtableMutatorRow second_row(allocator);
  ASSERT_EQ(OB_SUCCESS,
      second_row.deserialize(serialized_buf, serialize_pos, deserialize_pos));
  EXPECT_EQ(serialize_pos, deserialize_pos);
  EXPECT_EQ(ARRAYSIZEOF(key_objs), second_row.rowkey_.get_obj_cnt());
}

TEST(MemtableMutatorRow, test_invalid_frame_and_tde_compatibility)
{
  // 验证异常输入和安全边界：空指针、非法 pos、截断 frame、伪造超长
  // row_size 和超过协议上限的 rowkey 数量都不能被成功解析；TDE 构建中，
  // CDC 对加密 Row 的返回码和消费位置必须与存储层兼容路径一致。
  ObObj key_obj;
  key_obj.set_int(1);
  ObStoreRowkey source_rowkey;
  ASSERT_EQ(OB_SUCCESS, source_rowkey.assign(&key_obj, 1));
  memtable::ObRowData empty_row;
  memtable::ObMemtableMutatorRow source_row(
      0, source_rowkey, 100, empty_row, empty_row,
      blocksstable::ObDmlFlag::DF_INSERT, 1, 0, 0, 0,
      ObTxSEQ(1, 0), 1);
  char serialized_buf[4096];
  int64_t serialized_len = sizeof(serialized_buf);
  int64_t serialize_pos = 0;
  ObCLogEncryptInfo encrypt_info;
  encrypt_info.init();
  ASSERT_EQ(OB_SUCCESS, source_row.serialize(
      serialized_buf, serialized_len, serialize_pos, NULL, encrypt_info));
  const auto deserialize_storage_row =
      [&encrypt_info](const char *buf, const int64_t data_len, int64_t &pos) {
    memtable::ObMemtableMutatorRow storage_row;
    memtable::ObEncryptRowBuf decrypt_buf;
    share::ObEncryptMeta encrypt_meta;
    share::ObCLogEncryptStatMap encrypt_stat_map;
    return storage_row.deserialize(
        buf, data_len, pos, decrypt_buf, encrypt_info,
        false, encrypt_meta, encrypt_stat_map);
  };

  ObArenaAllocator allocator;
  MemtableMutatorRow null_row(allocator);
  int64_t pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, null_row.deserialize(NULL, serialize_pos, pos));
  pos = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      null_row.deserialize(serialized_buf, serialize_pos, pos));
  pos = serialize_pos + 1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      null_row.deserialize(serialized_buf, serialize_pos, pos));

  MemtableMutatorRow truncated_row(allocator);
  int64_t storage_pos = 0;
  const int truncated_storage_ret = deserialize_storage_row(
      serialized_buf, serialize_pos - 1, storage_pos);
  pos = 0;
  EXPECT_EQ(truncated_storage_ret,
      truncated_row.deserialize(serialized_buf, serialize_pos - 1, pos));
  EXPECT_EQ(storage_pos, pos);

  char oversized_buf[4096];
  MEMCPY(oversized_buf, serialized_buf, serialize_pos);
  int64_t header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(
      oversized_buf, sizeof(oversized_buf), header_pos, serialize_pos + 1));
  MemtableMutatorRow oversized_row(allocator);
  storage_pos = 0;
  const int oversized_storage_ret = deserialize_storage_row(
      oversized_buf, serialize_pos, storage_pos);
  pos = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, oversized_storage_ret);
  EXPECT_EQ(oversized_storage_ret,
      oversized_row.deserialize(oversized_buf, serialize_pos, pos));
  EXPECT_EQ(storage_pos, pos);

  // 超过协议最大值的 rowkey 数量必须被拒绝。
  char invalid_count_buf[4096];
  int64_t invalid_pos = serialization::encoded_length_i32(0);
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      invalid_count_buf, sizeof(invalid_count_buf), invalid_pos, 0));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      invalid_count_buf, sizeof(invalid_count_buf), invalid_pos,
      common::OB_MAX_ROWKEY_COLUMN_NUMBER + 1));
  header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(
      invalid_count_buf, sizeof(invalid_count_buf), header_pos, invalid_pos));
  MemtableMutatorRow invalid_count_row(allocator);
  storage_pos = 0;
  const int invalid_count_storage_ret = deserialize_storage_row(
      invalid_count_buf, invalid_pos, storage_pos);
  pos = 0;
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, invalid_count_storage_ret);
  EXPECT_EQ(invalid_count_storage_ret,
      invalid_count_row.deserialize(invalid_count_buf, invalid_pos, pos));
  EXPECT_EQ(storage_pos, pos);

#ifdef OB_BUILD_TDE_SECURITY
  // 正数外层 ID 会被解释为加密信息索引。当前 CDC 接口没有传入事务级
  // 加密元数据，因此这里只验证 fallback 的失败返回码和消费位置与存储层
  // 一致，并验证失败路径创建的 holder 可以安全 reset。
  memtable::ObMemtableMutatorRow encrypted_source_row(
      500001, source_rowkey, 100, empty_row, empty_row,
      blocksstable::ObDmlFlag::DF_INSERT, 1, 0, 0, 0,
      ObTxSEQ(1, 0), 1);
  int64_t encrypted_pos = 0;
  ASSERT_EQ(OB_SUCCESS, encrypted_source_row.serialize(
      serialized_buf, serialized_len, encrypted_pos, NULL, encrypt_info));

  storage_pos = 0;
  const int storage_ret = deserialize_storage_row(
      serialized_buf, encrypted_pos, storage_pos);

  MemtableMutatorRow cdc_row(allocator);
  int64_t cdc_pos = 0;
  EXPECT_NE(OB_SUCCESS, storage_ret);
  EXPECT_EQ(storage_ret,
      cdc_row.deserialize(serialized_buf, encrypted_pos, cdc_pos));
  EXPECT_EQ(storage_pos, cdc_pos);
  EXPECT_NE(nullptr, cdc_row.encrypted_row_holder_);
  cdc_row.reset();
  EXPECT_EQ(nullptr, cdc_row.encrypted_row_holder_);

  // 外层字段在协议中是 uint64_t。损坏数据编码出负数时，存储层会按
  // 无符号值解释并进入 TDE fallback；Compact dispatch 必须保持一致。
  char negative_index_buf[64];
  int64_t negative_index_pos = serialization::encoded_length_i32(0);
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      negative_index_buf, sizeof(negative_index_buf), negative_index_pos, -1));
  header_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(
      negative_index_buf, sizeof(negative_index_buf), header_pos,
      negative_index_pos));
  storage_pos = 0;
  const int negative_index_storage_ret = deserialize_storage_row(
      negative_index_buf, negative_index_pos, storage_pos);
  MemtableMutatorRow negative_index_row(allocator);
  cdc_pos = 0;
  EXPECT_EQ(negative_index_storage_ret,
      negative_index_row.deserialize(
          negative_index_buf, negative_index_pos, cdc_pos));
  EXPECT_EQ(storage_pos, cdc_pos);
  EXPECT_NE(nullptr, negative_index_row.encrypted_row_holder_);
#endif
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
