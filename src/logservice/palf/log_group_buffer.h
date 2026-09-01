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
  // tail reset 后回填尾页中已持久化的有效前缀. prefix_begin_lsn 必须
  // 4K 对齐, [prefix_begin_lsn, tail_lsn) 不超过一页且长度等于 data_len.
  // 本接口只同步复制字节, 不推进 readable/data-end/reuse 位点.
  int fill_tail_prefix_after_reset(const LSN &prefix_begin_lsn,
                                   const LSN &tail_lsn,
                                   const char *data,
                                   int64_t data_len);
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
  // 直接落盘的数据不在 group buffer 中: 同时推进可读下界和数据尾,
  // 使这段内存保持不可读, 直到后续 fill 写入新的连续数据.
  int inc_update_readable_begin_lsn(const LSN &new_lsn);
  // Advance the lower bound of memory that the upper buffer may reuse.
  int inc_update_reuse_lsn(const LSN &new_reuse_lsn);
  void get_reuse_lsn(LSN &reuse_lsn) const { return get_reuse_lsn_(reuse_lsn); }
  // Used for truncating log / truncating for rebuild.
  int truncate(const LSN &new_lsn);
  //
  // read log data from group buffer
  //
  // @param [in] read_begin_lsn, the read begin lsn
  // @param [in] in_read_size, the expected read size
  // @param [in] buf, the data buf for read
  // @param [out] out_read_size, the successful read size of data
  //
  // return code:
  //    - OB_INVALID_ARGUMENT, the lsn is invalid or unexpected
  //    - OB_ERR_OUT_OF_LOWER_BOUND, read_begin_lsn < readable_begin_lsn_
  //    - OB_SUCCESS, read successfully
  int read_data(const LSN &read_begin_lsn,
                const int64_t in_read_size,
                char *buf,
                int64_t &out_read_size) const;
  TO_STRING_KV("log_group_buffer: start_lsn", start_lsn_, "buffer_start_lsn", buffer_start_lsn_,
      "buffer_reuse_lsn", buffer_reuse_lsn_, "data_end_lsn", data_end_lsn_,
      "reserved_buffer_size", reserved_buffer_size_, "available_buffer_size",
      available_buffer_size_, "readable_begin_lsn", readable_begin_lsn_);
private:
  int get_buffer_pos_(const LSN &lsn, int64_t &start_pos) const;
  void get_buffer_start_lsn_(LSN &start_lsn) const;
  void get_reuse_lsn_(LSN &reuse_lsn) const;
  void get_start_lsn_(LSN &lsn) const;
  void get_data_end_lsn_(LSN &lsn) const;
  void gen_readable_begin_lsn_for_filling_(const LSN &lsn,
                                           LSN &new_readable_begin_lsn) const;
  void inc_update_readable_begin_lsn_(const LSN &new_readable_begin_lsn);
  int inc_update_data_end_lsn_(const LSN &new_data_end_lsn);
  void get_readable_begin_lsn_(LSN &readable_begin_lsn) const;
  int fill_(const LSN &lsn,
            const int64_t start_pos,
            const char *data,
            const int64_t data_len);
private:
  // 真实逻辑起点, 由 init() 传入. 写入路径用它判断新日志是否早于本轮 buffer 生命周期.
  LSN start_lsn_;
  // 4K 对齐后的环形 buffer 起点. 只用于把 LSN 映射到 data_buf_ 偏移, 可能早于 start_lsn_.
  LSN buffer_start_lsn_;
  // 内存可复用下界. async zero-copy 写盘需要保护尾部 4K 页, 因此它可能落后于已持久化位点.
  LSN buffer_reuse_lsn_;
  // lock for truncate operation.
  mutable common::ObSpinLock truncate_lock_;
  // 当前可读数据下界. 在一次 init 生命周期内只单调推进, truncate 不回退;
  // reset/destroy 会将它清空.
  LSN readable_begin_lsn_;
  // 当前可读数据上界. 它和 buffer_reuse_lsn_ 分离, 避免把“数据已填充”误当成“内存可复用”.
  LSN data_end_lsn_;
  // 分配的buffer size
  int64_t reserved_buffer_size_;
  // 当前可用的buffer size
  int64_t available_buffer_size_;
  // buffer指针, LOG_DIO_ALIGN_SIZE 对齐.
  char *data_buf_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(LogGroupBuffer);
};
}
}
#endif // OCEANBASE_LOGSERVICE_LOG_LOG_BUFFER_
