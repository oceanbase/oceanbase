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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_SEQUENCER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_SEQUENCER_H_

#include "share/ob_thread_pool.h"          // ObThreadPool
#include "common/ob_queue_thread.h"        // ObCond
#include "lib/utility/ob_print_utils.h"    // Print*
#include "ob_archive_define.h"             // DEFAULT_THREAD_RUN_INTERVAL
#include "share/ob_ls_id.h"        // ObLSID
#include "logservice/palf/lsn.h"           // LSN
#include "share/scn.h"           // SCN
#include <cstdint>

namespace oceanbase
{
namespace share
{
class ObLSID;
}

namespace logservice
{
class ObLogService;
}

namespace archive
{
class ObArchiveFetcher;
class ObArchiveLSMgr;
class ObArchiveRoundMgr;
class ObLSArchiveTask;
class ObArchiveLogFetchTask;
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
using oceanbase::logservice::ObLogService;

/*
 * Sequencer是为各个日志流产生归档任务的模块, 也是处理各日志流间定序模块
 *
 * Sequencer遍历日志流并为每个日志流产生归档日志范围[start_offset, end_offset)任务,
 * 其中start_offset和end_offset分别为某条完全日志的起始偏移, 并且每个任务数据范围为一个
 * 完整归档文件大小, 比如64M/128M..;
 *
 * 根据每个日志流日志积压数量以及落后程度, 决定每次为日志流产生任务量多少,
 * 由于Sequencer模块本身无法感知到日志流日志offset与scn关系, sequencer根据fetcher模块进度
 * 以及已分配任务数量作为归档进度的参考
 *
 * NB: Sequencer产生的单个任务可以包含当前不存在的日志范围, 既start_offset < commit_lsn < end_offset
 * */
class ObArchiveSequencer : public share::ObThreadPool
{
  // sequencer线程默认工作周期
  static const int64_t DEFAULT_THREAD_RUN_INTERVAL = 5 * 1000 * 1000L;
  // 单个日志流一次产生LogFetchTask数量
  static const int64_t MAX_TASK_COUNT_SINGLE_CYCLE = 4;

public:
  ObArchiveSequencer();
  ~ObArchiveSequencer();

public:
  int init(const uint64_t tenant_id,
           ObLogService *log_service,
           ObArchiveFetcher *fetcher,
           ObArchiveLSMgr *mgr,
           ObArchiveRoundMgr *round_mgr);
  void destroy();
  int start();
  void stop();
  void wait();

  // 通知sequencer开始为归档服务
  void notify_start();
  void signal();

private:

private:
  void run1();
  void do_thread_task_();
  int produce_log_fetch_task_();

private:
  bool                         inited_;
  uint64_t                     tenant_id_;
  int64_t                      round_;
  ObLogService                 *log_service_;
  ObArchiveFetcher             *archive_fetcher_;
  ObArchiveLSMgr               *ls_mgr_;
  ObArchiveRoundMgr            *round_mgr_;
  // 全部日志流最小定序任务scn
  share::SCN                      min_scn_;

  // 添加开启归档/日志流归档任务/以及消费LogFetchTask唤醒sequencer
  common::ObCond               seq_cond_;
};

/*
 * 迭代一遍全部日志流, 确定出最落后日志流的sequence进度以及日志流
 * 该日志流作为本轮次产生归档任务的参考
 * */
class QueryMinLogTsFunctor
{
public:
  explicit QueryMinLogTsFunctor(const int64_t incarnation, const int64_t round)
    : incarnation_(incarnation),
    round_(round),
    succ_count_(0),
    total_count_(0),
    min_scn_(),
    id_() {}
  bool operator()(const ObLSID &id, ObLSArchiveTask *ls_archive_task);
  void get_min_log_info(ObLSID &id, share::SCN &min_scn);
  TO_STRING_KV(K_(incarnation), K_(round), K_(succ_count), K_(total_count), K_(min_scn), K_(id));

private:
  int64_t incarnation_;
  int64_t round_;
  int64_t succ_count_;
  int64_t total_count_;
  share::SCN min_scn_;
  //MaxLogFileInfo  min_log_info_;
  ObLSID id_;
};

class GenFetchTaskFunctor
{
public:
  explicit GenFetchTaskFunctor(const uint64_t tenant_id,
                               const ArchiveKey &key,
                               const ObLSID &id,
                               ObLogService *log_service,
                               ObArchiveFetcher *fetcher,
                               ObArchiveLSMgr *ls_mgr)
    : tenant_id_(tenant_id),
    key_(key),
    max_delayed_id_(id),
    log_service_(log_service),
    archive_fetcher_(fetcher),
    ls_mgr_(ls_mgr) {}
  bool operator()(const ObLSID &id, ObLSArchiveTask *ls_archive_task);

private:
  void cal_end_lsn_(const LSN &seq_lsn, const LSN &fetch_lsn, const LSN &commit_lsn, LSN &end_lsn);
  int get_commit_index_(const ObLSID &id, LSN &commit_lsn);
  int generate_log_fetch_task_(const ObLSID &id,
      const ArchiveWorkStation &station,
      const LSN &start_lsn,
      const LSN &end_lsn,
      ObArchiveLogFetchTask *&task);
private:
  uint64_t tenant_id_;
  ArchiveKey key_;
  ObLSID max_delayed_id_;
  ObLogService *log_service_;
  ObArchiveFetcher *archive_fetcher_;
  ObArchiveLSMgr *ls_mgr_;

private:
  DISALLOW_COPY_AND_ASSIGN(GenFetchTaskFunctor);
};

} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_SEQUENCER_H_ */
