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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_TASK_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_TASK_H_

#include "lib/queue/ob_link.h"                     // ObLink
#include "share/ob_ls_id.h"                // ObLSID
#include "ob_archive_define.h"                     // ArchiveWorkStation
#include "share/backup/ob_archive_piece.h"         // ObArchivePiece
#include "logservice/palf/lsn.h"         // LSN
#include "share/scn.h"         // SCN
#include <cstdint>

namespace oceanbase
{
namespace archive
{
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
using oceanbase::share::ObArchivePiece;
class ObArchiveSendTask;
/*
 * ObArchiveLogFetchTask是日志文件的读取任务, 由ObArchiveSequencer产生, ObArchiveFetcher来消费.
 * 由于归档无法感知Palf文件系统内部结构, 因此LogFetchTask以一个归档文件为粒度产生任务
 *
 * NB: 它是一段log文件范围的描述(A, B) means (start_offset, end_offset), 其中start_offset和end_offset
 * 分别是归档文件的起始和终点偏移, 归档文件理论上可以为任意大小, 通常等于一个日志文件大小
 *
 * 日志文件大小是固定的, 当最后一条日志超过该日志文件剩余空间时, 该条日志会放置到下一个日志文件;
 * 而归档文件中某条日志由其起始LSN决定, 上述例子中该条日志位于上一个归档文件
 *
 * 当一个日志流的归档出现落后时, 单个日志流会产生多个LogFetchTask, 分别对应多个归档文件
 * NB: 当一个日志流归档到最新日志时, LogFetchTask任务仅有一个, 其end_offset依然是对应归档文件的最大偏移
 * 自然也包含尚未产生日志offset
 *
 * For example,
 * There are two lss, ls1是归档落后日志流, ls2是归档到最新的日志流
 * ls1 -> {[x * N * M, (x + 1) * N * M), [(x + 1) * N * M, (x + 2) * N * M), ...}
 * ls2 -> {[y * N * M, (y + 1) * N * M)}
 * 其中x, y是随意数字, 表示归档文件数, M为ob日志文件大小
 *
 * 另外由于每个observer可能为多个日志流归档服务, 所以多个日志流之间保存整体齐步走是有必要的.
 * ObArchiveSequencer根据ObArchiveLogFetchTask消费进度协调不同日志流归档进度.
 *
 * NB: 由于ObArchiveSequencer无法感知到日志log scn信息, 因此初始LogFetchTask其piece info是空的,
 * 由ObArchiveFetcher在消费时, 为跨piece任务归档数据到多个piece下
 * 对于跨piece的归档文件, 相同编号的归档文件可能会出现在多个piece, 其中每个piece仅包含该piece所属日志范围
 *
 * NB: start_offset 和 end_offset是左闭右开区间, 如[0, 64M)
 *
 * */
class ObArchiveLogFetchTask : public common::ObLink
{
public:
  ObArchiveLogFetchTask();
  virtual ~ObArchiveLogFetchTask();
public:
  int init(const uint64_t tenant_id,
           const ObLSID &id,
           const ArchiveWorkStation &station,
           const share::SCN &base_scn,
           const LSN &start_lsn,
           const LSN &end_lsn);
  uint64_t get_tenant_id() const { return tenant_id_; }
  ObLSID get_ls_id() const { return id_; }
  const ArchiveWorkStation &get_station() const { return station_; }
  const share::SCN &get_base_scn() const { return base_scn_; }
  const LSN &get_start_offset() const { return start_offset_; }
  const LSN &get_cur_offset() const { return cur_offset_; }
  const LSN &get_end_offset() const { return end_offset_; }
  const share::SCN &get_max_scn() const { return max_scn_; }
  int64_t get_log_fetch_size() const { return (int64_t)(end_offset_ - cur_offset_); }
  const ObArchivePiece &get_piece() const { return cur_piece_; }
  int set_next_piece(const ObArchivePiece &piece);
  const ObArchivePiece &get_next_piece() const { return next_piece_; }
  ObArchiveSendTask *get_send_task() { return send_task_; }
  int clear_send_task();
  bool is_valid() const;
  bool is_finish() const;
  int back_fill(const ObArchivePiece &cur_piece,
                  const LSN &start_offset,
                  const LSN &end_offset,
                  const share::SCN &max_scn,
                  ObArchiveSendTask *send_task);
  bool has_fetch_log() const { return NULL != send_task_; }
  bool is_continuous_with(const LSN &lsn) const;
  TO_STRING_KV(K_(tenant_id),
               K_(id),
               K_(station),
               K_(cur_piece),
               K_(next_piece),
               K_(base_scn),
               K_(start_offset),
               K_(end_offset),
               K_(cur_offset),
               "unfinished_data_size", get_log_fetch_size(),
               K_(max_scn),
               K_(send_task),
               KP(this));

private:
  uint64_t tenant_id_;
  ObLSID id_;
  ArchiveWorkStation station_;
  ObArchivePiece cur_piece_;    // 该份数据属于ObArchivePiece目录
  ObArchivePiece next_piece_;
  share::SCN base_scn_;
  LSN start_offset_; // 起始拉日志文件起始offset
  LSN end_offset_;   // 拉取日志文件结束offset(如果是拉取完整文件任务，该值为infoblock起点)
  LSN cur_offset_;
  share::SCN max_scn_;        // 任务包含最大日志log scn

  ObArchiveSendTask *send_task_;
};

/*
 * 归档文件与ob日志文件大小不强绑定, 归档文件可以是ob日志文件的整数倍N, 因此需要有以下约束:
 * 1. 在完整归档文件大小日志没有完全备份完成之前, 归档进度不能推进, 否则切piece会丢失N-1个ob日志文件数据
 * 2. 归档补日志, 以保证归档进度可以满足需求(WORM), 需要根据增加LSN与归档文件大小差值补, 而非ob日志文件大小
 * 3. 非WORM场景下, 由于1的限制, 实际上归档进度在归档完归档文件大小数据之前, 也是不能推进的
 * 4. 归档文件LSN与ob日志LSN对等, 由于支持加密压缩需求, 需要一层转换关系, 由归档文件id计算LSN规则等同ob;
 *    内部表记录ob归档进度LSN, 可以以此计算归档文件file id
 * 5. 归档文件是日志文件N倍, 是指包含日志范围是ob日志文件的N倍; 由于加密压缩, 归档文件实际大小很难是真正的ob日志文件大小N倍
 *
 * LogFetchTask设计:
 * 1. 不可见ob日志文件file_id信息, 仅有LSN信息, start_offset为归档文件包含ob日志范围的起始LSN, end_offset是归档文件包含ob日志的最大LSN
 * 2. 由于归档文件包含日志范围是正常ob文件整数倍, 因此start_offset和end_offset也是ob日志文件的起始和结束
 * 3. 压缩加密单元在归档文件内部以unit为单位, 归档文件截止以剩余长度为单位
 *
 * 优化:
 * 1. 归档文件大小默认等于ob日志文件大小
 * 2. 归档进度推进不考虑归档文件不等于ob日志文件场景, 归档进度可以实时随着归档数据推进
 * */

/*
 * ObArchiveSendTask是日志数据的发送任务, 由ObArchiveFetcher产生, ObArchiveSender来消费.
 * 类似于ObArchiveLogFetchTask, 它也是一段clog文件的描述, 不同的是它是已经读取到内存的数据buffer.
 * 它可以是一个完整文件数据, 也可以是一个文件的部分数据
 *
 * 不同于ObArchiveLogFetchTask, 单个日志流内的ObArchiveSendTask必须被顺序消费, 在ObArchiveSender模块单个日志流处理是串行的.
 * 单个日志流的并发归档能力由底层存储接口模块保证
 *
 * */
class ObArchiveSendTask : public common::ObLink
{
public:
  ObArchiveSendTask();
  virtual ~ObArchiveSendTask();
public:
  int init(const uint64_t tenant_id,
           const ObLSID &id,
           const ArchiveWorkStation &station,
           const ObArchivePiece &piece,
           const LSN &start_offset,
           const LSN &end_offset,
           const share::SCN &max_scn);
  bool is_valid() const;
  uint64_t get_tenant_id() const { return tenant_id_;}
  ObLSID get_ls_id() const { return id_; }
  const ArchiveWorkStation &get_station() const { return station_; }
  const ObArchivePiece &get_piece() const { return piece_; }
  const LSN &get_start_lsn() const { return start_offset_; }
  const LSN &get_end_lsn() const { return end_offset_; };
  int get_buffer(char *&data, int64_t &data_len) const;
  int get_origin_buffer(char *&buf, int64_t &buf_size) const;
  int64_t get_buf_size() const { return data_len_;}
  const share::SCN &get_max_scn() const { return max_scn_; }
  int set_buffer(char *buf, const int64_t buf_size);
  bool is_continuous_with(const ObArchiveSendTask &pre_task) const;
  // @brief issue this task to a sender thread, only if its status is initial
  // @ret_code  flase  issue failed, maybe this task is issued by other sender thread
  //            true   issue successfully
  bool issue_task();
  // @brief after the task is issued to a sender thread and archived successfully, finish the task
  // @ret_code  false  finish failed, should not happend
  //            true   finish successfully
  bool finish_task();
  // @brief mark task stale forcely, cases:
  //  1) archive is stop
  //  2) stale archive task after ls archive task station changed
  void mark_stale();
  // @brief after the task is issued to a sender thread and archived failed, retire the task and wait next turn
  // @ret_code  false  retire failed, should not happend
  //            true   retire successfully
  bool retire_task_with_retry();
  bool is_task_finish() const;
  bool is_task_stale() const;
  int update_file(const int64_t file_id, const int64_t file_offset);
  void get_file(int64_t &file_id, int64_t &file_offset);
  TO_STRING_KV(K_(status),
               K_(tenant_id),
               K_(id),
               K_(station),
               K_(piece),
               K_(start_offset),
               K_(end_offset),
               K_(max_scn),
               K_(file_id),
               K_(file_offset),
               K_(data),
               K_(data_len),
               KP(this));
private:
  static const int8_t INITAL_STATUS = 0;
  static const int8_t ISSUE_STATUS = 1;
  static const int8_t FINISH_STATUS = 2;
  static const int8_t STALE_STATUS = 3;

private:
  int8_t status_;           // low 1st bit means issued(1) or not(0), low 2nd bit means finished(1) or not(0)
  uint64_t tenant_id_;
  ObLSID id_;               // ls id
  ArchiveWorkStation station_;     // archive work station, include incarnation/dest_id/round/lease
  ObArchivePiece piece_;
  LSN start_offset_;       // 归档数据在文件起始offset
  LSN end_offset_;         // 归档数据在文件终止offset
  share::SCN max_scn_;     // 该task包含数据最大scn
  int64_t file_id_;
  int64_t file_offset_;
  char *data_;             // 发送数据
  int64_t data_len_;       // 发送数据长度
};

struct LogFetchTaskCompare final
{
public:
  LogFetchTaskCompare() {}
  ~LogFetchTaskCompare() {}
  bool operator()(const ObArchiveLogFetchTask *left, const ObArchiveLogFetchTask *right)
  {
    return left->get_start_offset() < right->get_start_offset();
  }
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_TASK_H_ */
