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

#ifndef OCEANBASE_LOGSERVICE_OB_APPEND_CALLBACK_
#define OCEANBASE_LOGSERVICE_OB_APPEND_CALLBACK_
#include "palf/lsn.h"
#include "lib/utility/utility.h"
#include "share/scn.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace logservice
{
class AppendCb;

struct ObLogTsInfo
{
  int64_t log_id_;       // 标识唯一的log task
  int64_t gen_ts_;       // 初始化log task的时间
  int64_t freeze_ts_;    // group buffer冻结完成时间
  int64_t submit_ts_;    // palf提交同步任务的时间
  int64_t flushed_ts_;   // 本地写盘完成时间
  int64_t slide_out_ts_; // 滑动窗口滑出时间

  ObLogTsInfo()
  {
    reset();
  }

  ~ObLogTsInfo()
  {
    reset();
  }

  void reset()
  {
    log_id_ = OB_INVALID_LOG_ID;
    gen_ts_ = OB_INVALID_TIMESTAMP;
    freeze_ts_ = OB_INVALID_TIMESTAMP;
    submit_ts_ = OB_INVALID_TIMESTAMP;
    flushed_ts_ = OB_INVALID_TIMESTAMP;
    slide_out_ts_ = OB_INVALID_TIMESTAMP;
  }

  bool is_valid() const
  {
    return OB_INVALID_LOG_ID != log_id_;
  }

  TO_STRING_KV(K_(log_id), K_(gen_ts), K_(freeze_ts), K_(submit_ts),
               K_(flushed_ts), K_(slide_out_ts),
               "gen_to_freeze", freeze_ts_ - gen_ts_,
               "freeze_to_submit", submit_ts_ - freeze_ts_,
               "submit_to_flush", flushed_ts_ - submit_ts_,
               "flush_to_slide", slide_out_ts_ - flushed_ts_);
};
class AppendCbBase {
public:
  AppendCbBase() : __next_(NULL),
                   __start_lsn_(),
                   __scn_()
  {
  }
  virtual ~AppendCbBase()
  {
    __reset();
  }
  void __reset() { __start_lsn_.reset(); __next_ = NULL; __scn_.reset();}
  const palf::LSN &__get_lsn() const { return __start_lsn_; }
  void __set_lsn(const palf::LSN &lsn) { __start_lsn_ = lsn; }
  const share::SCN& __get_scn() const { return __scn_; }
  void __set_scn(const share::SCN& scn) { __scn_ = scn; }
  static AppendCb* __get_class_address(ObLink *ptr);
  static ObLink* __get_member_address(AppendCb *ptr);
  ObLink *__next_;
  VIRTUAL_TO_STRING_KV(KP(__next_), K(__start_lsn_), K(__scn_));
private:
  palf::LSN __start_lsn_;
  share::SCN __scn_;
};

class AppendCb : public AppendCbBase
{
public:
  AppendCb(): append_start_ts_(OB_INVALID_TIMESTAMP), append_finish_ts_(OB_INVALID_TIMESTAMP),
      cb_first_handle_ts_(OB_INVALID_TIMESTAMP), ts_info_() {};
  ~AppendCb()
  {
    reset();
  }
  void reset() {
    AppendCbBase::__reset();
    append_start_ts_ = OB_INVALID_TIMESTAMP;
    append_finish_ts_ = OB_INVALID_TIMESTAMP;
    cb_first_handle_ts_ = OB_INVALID_TIMESTAMP;
    ts_info_.reset();
  }
  virtual int on_success() = 0;
  virtual int on_failure() = 0;
  void set_append_start_ts(const int64_t ts) { append_start_ts_ = ts; }
  void set_append_finish_ts(const int64_t ts) { append_finish_ts_ = ts; }
  void set_cb_first_handle_ts(const int64_t ts);
  int64_t get_append_start_ts() const { return append_start_ts_; }
  int64_t get_append_finish_ts() const { return append_finish_ts_; }
  int64_t get_cb_first_handle_ts() const { return cb_first_handle_ts_; }
  virtual const char *get_cb_name() const = 0;

  void set_ts_info(const ObLogTsInfo &ts_info) { ts_info_ = ts_info; }
  const ObLogTsInfo &get_ts_info() const { return ts_info_; }
  ObLogTsInfo get_ts_info() { return ts_info_; }
  void set_log_id(const int64_t log_id) { ts_info_.log_id_ = log_id; }
  int64_t get_log_id() const { return ts_info_.log_id_; }
  void set_gen_ts(const int64_t ts) { ts_info_.gen_ts_ = ts; }
  void set_freeze_ts(const int64_t ts) { ts_info_.freeze_ts_ = ts; }
  void set_submit_ts(const int64_t ts) { ts_info_.submit_ts_ = ts; }
  void set_flushed_ts(const int64_t ts) { ts_info_.flushed_ts_ = ts; }
  void set_slide_out_ts(const int64_t ts) { ts_info_.slide_out_ts_ = ts; }
  int64_t get_gen_ts() const { return ts_info_.gen_ts_; }
  int64_t get_freeze_ts() const { return ts_info_.freeze_ts_; }
  int64_t get_palf_submit_ts() const { return ts_info_.submit_ts_; }
  int64_t get_flushed_ts() const { return ts_info_.flushed_ts_; }
  int64_t get_slide_out_ts() const { return ts_info_.slide_out_ts_; }

public:
  int64_t append_start_ts_; //提交到palf的起始时刻
  int64_t append_finish_ts_; //palf提交完成时刻,即提交到apply service起始时刻
  int64_t cb_first_handle_ts_; //cb第一次被处理的时刻,不一定调用on_success
  ObLogTsInfo ts_info_;      //PALF层各阶段时间戳信息
};

} // end namespace logservice
} // end namespace oceanbase
#endif
