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

#ifndef OCEANBASE_LOGSERVICE_LOG_LOG_BUFFER_
#define OCEANBASE_LOGSERVICE_LOG_LOG_BUFFER_

#include "lib/atomic/atomic128.h"
#include "lib/utility/ob_macro_utils.h"
#include "log_define.h"
#include "lsn.h"

namespace oceanbase
{
namespace palf
{
class LogWriteBuf;
class LogGroupBuffer
{
public:
  LogGroupBuffer();
  ~LogGroupBuffer();
public:
  int init(const LSN &start_lsn);
  void reset();
  void destroy();

  //
  // 功能: 将日志体填充到聚合buffer
  //
  // @param [in] lsn, 聚合buffer中填充起始偏移量
  // @param [in] data, 数据内容指针
  // @param [in] data_len, 数据长度
  // @param [in] cb, 回调对象指针
  //
  // return code:
  //      OB_SUCCESS
  int fill(const LSN &lsn,
           const char *data,
           const int64_t data_len);
  int fill_padding_body(const LSN &lsn,
                        const char *data,
                        const int64_t data_len,
                        const int64_t log_body_size);
  int get_log_buf(const LSN &lsn, const int64_t total_len, LogWriteBuf &log_buf);
  bool can_handle_new_log(const LSN &lsn,
                          const int64_t total_len) const;
  bool can_handle_new_log(const LSN &lsn,
                          const int64_t total_len,
                          const LSN &ref_reuse_lsn) const;
  int check_log_buf_wrapped(const LSN &lsn, const int64_t log_len, bool &is_buf_wrapped) const;
  int64_t get_available_buffer_size() const;
  int64_t get_reserved_buffer_size() const;
  int to_leader();
  int to_follower();
  // inc update reuse_lsn, used for flush log cb case.
  int inc_update_reuse_lsn(const LSN &new_reuse_lsn);
  // set reuse_lsn, used for truncate case(trucate/rebuild)
  int set_reuse_lsn(const LSN &new_reuse_lsn);
  void get_reuse_lsn(LSN &reuse_lsn) const { return get_reuse_lsn_(reuse_lsn); }
  TO_STRING_KV("log_group_buffer: start_lsn", start_lsn_, "reuse_lsn", reuse_lsn_, "reserved_buffer_size",
      reserved_buffer_size_, "available_buffer_size", available_buffer_size_);
private:
  int get_buffer_pos_(const LSN &lsn, int64_t &start_pos) const;
  void get_buffer_start_lsn_(LSN &start_lsn) const;
  void get_reuse_lsn_(LSN &reuse_lsn) const;
  int fill_(const LSN &lsn,
           const char *data,
           const int64_t data_len);
private:
  // buffer起始位置对应的lsn
  LSN start_lsn_;
  // buffer可复用起点对应的lsn, 与max_flushed_end_lsn预期最终是相等的.
  // 所有更新max_flushed_end_lsn的逻辑都要考虑一并更新该值.
  LSN reuse_lsn_;
  // 分配的buffer size
  int64_t reserved_buffer_size_;
  // 当前可用的buffer size
  int64_t available_buffer_size_;
  // buffer指针
  char *data_buf_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogGroupBuffer);
};
}
}
#endif // OCEANBASE_LOGSERVICE_LOG_LOG_BUFFER_
