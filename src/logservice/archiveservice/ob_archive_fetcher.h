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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_FETCHER_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_FETCHER_H_

#include "lib/queue/ob_lighty_queue.h"      // ObLightyQueue
#include "lib/compress/ob_compress_util.h"  // ObCompressorType
#include "common/ob_queue_thread.h"         // ObCond
#include "share/ob_thread_pool.h"           // ObThreadPool
#include "share/ob_ls_id.h"                 // ObLSID
#include "share/backup/ob_archive_piece.h"  // ObArchivePiece
#include "logservice/palf/lsn.h"            // LSN
#include "share/scn.h"            // SCN
#include "logservice/palf/palf_iterator.h"  // PalfGroupBufferIterator
#include "ob_archive_define.h"

namespace oceanbase
{
namespace logservice
{
class ObLogService;
}

namespace share
{
class SCN;
}
namespace palf
{
struct LSN;
class PalfHandleGuard;
class LogGroupEntry;
}

namespace archive
{
class ObArchiveAllocator;
class ObArchiveSender;
class ObArchiveSequencer;
class ObArchiveLSMgr;
class ObArchiveRoundMgr;
class ArchiveWorkStation;
class ObArchiveSendTask;
class ObLSArchiveTask;
using oceanbase::logservice::ObLogService;
using oceanbase::share::ObLSID;
using oceanbase::share::ObArchivePiece;
using oceanbase::palf::LSN;
using oceanbase::palf::PalfGroupBufferIterator;
using oceanbase::palf::LogGroupEntry;

struct ObArchiveLogFetchTask;

/*
 * 归档读取clog文件模块, 消费ObArchiveLogFetchTask, 产生SendTask
 * 具体到单个日志流, ObArchiveFetcher是并发服务的
 * 另外在为ObArchiveSender模块产生SendTask同时, 会构造连续顺序任务队列, 方便单个日志流顺序归档clog文件
 * */
class ObArchiveFetcher : public share::ObThreadPool
{
  static const int64_t THREAD_RUN_INTERVAL = 100 * 1000L;    // 100ms
  static const int64_t MAX_CONSUME_TASK_NUM = 5;
public:
  ObArchiveFetcher();
  ~ObArchiveFetcher();

public:
  // sequencer线程构造日志读取任务, 提交到fetcher任务队列, 线程安全
  //
  // @param [out], task 日志读取任务
  // @retval       OB_SUCCESS    success
  // @retval       OB_EAGAIN     retry later
  // @retval       other code    fail
  int submit_log_fetch_task(ObArchiveLogFetchTask *task);

  // 开启关闭归档接口
  int set_archive_info(const int64_t piece_interval_us,
                       const share::SCN &genesis_scn,
                       const int64_t base_piece_id,
                       const int64_t unit_size,
                       const bool need_compress,
                       const ObCompressorType type,
                       const bool need_encrypt);
  void clear_archive_info();
  void signal();

  // 分配LogFetch任务
  ObArchiveLogFetchTask *alloc_log_fetch_task();
  // 释放LogFetch任务
  void free_log_fetch_task(ObArchiveLogFetchTask *task);

  int64_t get_log_fetch_task_count() const;

  int modify_thread_count(const int64_t thread_count);
public:
  int init(const uint64_t tenant_id,
      ObLogService *log_service,
      ObArchiveAllocator *allocator,
      ObArchiveSender *archive_sender,
      ObArchiveSequencer *archive_sequencer,
      ObArchiveLSMgr *mgr,
      ObArchiveRoundMgr *round_mgr);
  void destroy();
  int start();
  void wait();
  void stop();

private:
  class TmpMemoryHelper;
private:
  void run1();
  // fetcher线程工作内容: 1. 消费sequencer构造的LogFetchTask; 2. 根据聚合策略压缩加密以及构建SendTask
  void do_thread_task_();
  int handle_single_task_();
  void handle_log_fetch_ret_(const ObLSID &id, const ArchiveKey &key, const int ret_code);

  // ============================ 读取数据构造send_task =============================== //
  //
  // NB: 由于相同日志流不同LogOffset任务并发处理, 产生SendTask需要push回定序队列, 单线程消费任务
  //
  // 1. 消费sequencer构造的LogFetchTask, 读取ob日志, 提交到日志流归档管理任务fetch_log队列
  int handle_log_fetch_task_(ObArchiveLogFetchTask &task);

  // 1.0 get palf max lsn and scn
  int get_max_lsn_scn_(const ObLSID &id, palf::LSN &lsn, share::SCN &scn);

  // 1.1 检查任务是否delay处理
  int check_need_delay_(const ObArchiveLogFetchTask &task, const LSN &commit_lsn, bool &need_delay);

  // 1.1.1 检查ob日志是否有产生满足处理单元大小的数据
  void check_capacity_enough_(const LSN &commit_lsn, const LSN &cur_lsn,
      const LSN &end_offset, bool &data_full);

  // 1.1.2 检查日志流落后程度是否需要触发归档
  bool check_scn_enough_(const share::ObLSID &id, const bool new_block, const palf::LSN &lsn,
      const palf::LSN &max_no_limit_lsn, const share::SCN &base_scn, const share::SCN &fetch_scn,
      const int64_t last_fetch_timestamp);

  // 1.2 初始化TmpMemoryHelper
  int init_helper_(ObArchiveLogFetchTask &task, const LSN &commit_lsn, TmpMemoryHelper &helper);

  // 1.3 初始化日志迭代器
  int init_iterator_(const ObLSID &id, const TmpMemoryHelper &helper,
      palf::PalfHandleGuard &palf_handle_guard, PalfGroupBufferIterator &iter);

  // 1.4 产生归档数据
  int generate_send_buffer_(PalfGroupBufferIterator &iter,  TmpMemoryHelper &helper);

  // 1.4.1 获取受控归档位点
  int get_max_archive_point_(share::SCN &max_scn);

  // 1.4.2 helper piece信息为空，则使用LogFetchTask第一条日志piece填充
  int fill_helper_piece_if_empty_(const ObArchivePiece &piece, TmpMemoryHelper &helper);

  // 1.4.3 检查piece是否变化
  bool check_piece_inc_(const ObArchivePiece &p1, const ObArchivePiece &p2);

  // 1.4.3.1 设置下一个piece
  int set_next_piece_(TmpMemoryHelper &helper);

  // 1.4.4 聚合压缩加密单元
  int append_log_entry_(const char *buffer, LogGroupEntry &entry, TmpMemoryHelper &helper);

  // 1.4.5 是否聚合到足够压缩加密单元大小数据
  bool cached_buffer_full_(TmpMemoryHelper &helper);

  // 1.5 处理加密压缩
  int handle_origin_buffer_(TmpMemoryHelper &helper);

  // 1.5.1 压缩
  int do_compress_(TmpMemoryHelper &helper);

  // 1.5.2 加密
  int do_encrypt_(TmpMemoryHelper &helper);

  // 1.6 创建send_task
  int build_send_task_(const ObLSID &id,
      const ArchiveWorkStation &station, TmpMemoryHelper &helper, ObArchiveSendTask *&task);

  // 1.7 更新LogFetchTask, 并且为LogFetchTask下一个piece回填piece信息
  // NB: 回填next_piece信息, 方便为下一个piece归档数据
  int update_log_fetch_task_(ObArchiveLogFetchTask &fetch_task,
      TmpMemoryHelper &helper, ObArchiveSendTask *send_task);

  // 1.8 提交fetch log
  int submit_fetch_log_(ObArchiveLogFetchTask &task, bool &submitted);

  // ==================================== 顺序消费LogFetchTask读取到数据 ================== //
  //
  // NB: [start_offset, end_offset)可能包含多个piece的数据, 因此在消费完某个LogFetchTask,
  // 如果是没有没有覆盖全部日志范围, 需要重新push回fetcher task queue
  //
  // 2. 尝试消费读取到数据
  int try_consume_fetch_log_(const ObLSID &id);

  // 2.1 获取顺序消费turn的LogFetchTask
  int get_sorted_fetch_log_(ObLSArchiveTask &ls_archive_task,
      ObArchiveLogFetchTask *&task, bool &task_exist);

  // 2.2 提交SendTask
  int submit_send_task_(ObArchiveSendTask *task);

  // 2.3 推高fetcher模块归档进度
  int update_fetcher_progress_(ObLSArchiveTask &ls_archive_task, ObArchiveLogFetchTask &task);

  // 2.3 检查LogFetchTask是否处理完全部数据
  bool check_log_fetch_task_consume_complete_(ObArchiveLogFetchTask &task);

  // 2.4 提交LogFetchTask剩余任务
  int submit_residual_log_fetch_task_(ObArchiveLogFetchTask &task);

  void inner_free_task_(ObArchiveLogFetchTask *task);

  bool is_retry_ret_(const int ret) const;

  bool in_normal_status_(const ArchiveKey &key) const;

  bool in_doing_status_(const ArchiveKey &key) const;

  void statistic(const int64_t log_size, const int64_t ts);

  bool iterator_need_retry_(const int ret) const;
private:
  class TmpMemoryHelper
  {
  public:
    TmpMemoryHelper(const int64_t unit_size, ObArchiveAllocator *allocator);
    ~TmpMemoryHelper();
  public:
    // 若piece指针为空, 表示未指定piece
    int init(const uint64_t tenant_id, const ObLSID &id,
        const LSN &start_offset, const LSN &end_offset, const LSN &commit_lsn, ObArchivePiece *piece);
    const ObLSID &get_ls_id() const { return id_;}
    bool is_piece_set() { return cur_piece_.is_valid(); }
    const ObArchivePiece &get_piece() const { return cur_piece_; }
    int set_piece(const ObArchivePiece &piece);
    const ObArchivePiece &get_next_piece() const { return next_piece_; }
    int set_next_piece();
    const LSN &get_start_offset() const { return start_offset_; }
    const LSN &get_end_offset() const { return end_offset_; }
    const LSN &get_cur_offset() const { return cur_offset_; }
    const share::SCN &get_unitized_scn() const { return unitized_scn_; }
    int64_t get_capaicity() const { return end_offset_ - cur_offset_; }
    bool is_log_out_of_range(const int64_t size) const;
    bool original_buffer_enough(const int64_t size);
    int get_original_buf(const char *&buf, int64_t &buf_size);
    int append_handled_buf(char *buf, const int64_t buf_size);
    int get_handled_buf(char *&buf, int64_t &buf_size);
    int append_log_entry(const char *buffer, LogGroupEntry &entry);
    void freeze_log_entry();
    void reset_original_buffer();
    int64_t get_log_fetch_size() const { return cur_offset_ - start_offset_; }
    bool is_empty() const { return NULL == ec_buf_ || 0 == ec_buf_pos_; }
    bool reach_end() { return cur_offset_ == end_offset_ || cur_offset_ == commit_offset_; }
    ObArchiveSendTask *gen_send_task();

    TO_STRING_KV(K_(tenant_id),
                 K_(id),
                 K_(start_offset),
                 K_(end_offset),
                 K_(commit_offset),
                 K_(origin_buf_pos),
                 K_(cur_offset),
                 K_(cur_scn),
                 //K_(ec_buf),
                 K_(ec_buf_size),
                 K_(ec_buf_pos),
                 K_(unitized_offset),
                 K_(unitized_scn),
                 K_(cur_piece),
                 K_(next_piece),
                 K_(unit_size));
  private:
    int get_send_buffer_(const int64_t size);
    void inner_free_send_buffer_();
    int64_t get_reserved_buf_size_() const;
  private:
    bool inited_;
    uint64_t tenant_id_;
    ObLSID id_;
    // 任务起始和结束offset
    LSN start_offset_;
    LSN end_offset_;
    LSN commit_offset_;
    // 处理原始数据buff, 由于可能单次处理凑不够unit_size数据, 这部分下次会重读同时log_ts/offset进度需要回滚
    const char *origin_buf_;
    int64_t origin_buf_pos_;
    // 当前处理原数日志最大offset/scn
    LSN cur_offset_;
    share::SCN cur_scn_;
    // 中间缺少加密用buffer
    char *ec_buf_;
    int64_t ec_buf_size_;        // 需要根据数据量计算
    int64_t ec_buf_pos_;
    // 做了单元化处理的日志最大offset/scn
    LSN unitized_offset_;
    share::SCN unitized_scn_;
    // 当前正在处理piece, 单个helper包含从start_offset到当前piece的全部数据
    ObArchivePiece cur_piece_;
    // 读取日志过程中, 遇到更大piece说明当前piece已经结束
    // 设置next_piece, 方便下一次处理
    ObArchivePiece next_piece_;
    int64_t unit_size_;
    ObArchiveAllocator *allocator_;
  };

private:
  bool               inited_;
  uint64_t           tenant_id_;
  // 压缩加密处理单元
  int64_t            unit_size_;
  int64_t            piece_interval_;
  share::SCN            genesis_scn_;
  int64_t            base_piece_id_;
  bool               need_compress_;
  // 压缩算法
  ObCompressorType   compress_type_;
  bool               need_encrypt_;
  ObLogService       *log_service_;
  ObArchiveAllocator *allocator_;
  ObArchiveSender    *archive_sender_;
  ObArchiveSequencer *archive_sequencer_;
  ObArchiveLSMgr     *ls_mgr_;
  ObArchiveRoundMgr  *round_mgr_;
  // 全局ObArchiveLogFetchTask队列
  common::ObLightyQueue     task_queue_;
  common::ObCond            fetch_cond_;
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_FETCHER_H_ */
