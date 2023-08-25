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

namespace oceanbase
{
namespace logservice
{
class AppendCb;
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
      cb_first_handle_ts_(OB_INVALID_TIMESTAMP) {};
  ~AppendCb()
  {
    reset();
  }
  void reset() {
    AppendCbBase::__reset();
    append_start_ts_ = OB_INVALID_TIMESTAMP;
    append_finish_ts_ = OB_INVALID_TIMESTAMP;
    cb_first_handle_ts_ = OB_INVALID_TIMESTAMP;
  }
  virtual int on_success() = 0;
  virtual int on_failure() = 0;
  void set_append_start_ts(const int64_t ts) { append_start_ts_ = ts; }
  void set_append_finish_ts(const int64_t ts) { append_finish_ts_ = ts; }
  void set_cb_first_handle_ts(const int64_t ts);
  int64_t get_append_start_ts() const { return append_start_ts_; }
  int64_t get_append_finish_ts() const { return append_finish_ts_; }
  int64_t get_cb_first_handle_ts() const { return cb_first_handle_ts_; }
public:
  int64_t append_start_ts_; //提交到palf的起始时刻
  int64_t append_finish_ts_; //palf提交完成时刻,即提交到apply service起始时刻
  int64_t cb_first_handle_ts_; //cb第一次被处理的时刻,不一定调用on_success
};

} // end namespace logservice
} // end namespace oceanbase
#endif
