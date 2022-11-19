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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_ITEM_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_ITEM_H_

#include <stdint.h>
#include "lib/ob_errno.h"
#include "lib/oblog/ob_base_log_writer.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "common/log/ob_log_cursor.h"
#include "ob_storage_log_struct.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogItem : public common::ObIBaseLogItem
{
public:
  ObStorageLogItem();
  virtual ~ObStorageLogItem();
  int init(char *buf, const int64_t buf_size, const int64_t align_size, const int64_t num);
  bool is_valid() const;
  void destroy();

  int wait_flush_log(const uint64_t max_wait_time);
  void finish_flush(const int flush_ret);
  virtual const char *get_buf() const override { return buf_; }
  virtual char *get_buf() override { return buf_; }
  virtual int64_t get_data_len() const override { return len_; }
  int set_data_len(const int64_t len);
  int64_t get_buf_size() const { return buf_size_; }
  int64_t get_log_data_len() const { return log_data_len_; }
  int64_t get_seq() const { return seq_; }
  int set_log_data_len(const int64_t log_data_len);
  bool is_local() const { return is_local_; }

  int64_t get_offset(const int index) const { return local_offset_arr_[index]; }
  uint64_t get_log_cnt() const { return log_cnt_; }
  static int64_t get_align_padding_size(const int64_t x, const int64_t align);

  int fill_log(const int64_t seq, const ObStorageLogParam &log_param, const int index);
  int fill_batch_header(
      const int32_t data_len,
      const int16_t cnt,
      int64_t pos);
  TO_STRING_KV(K_(start_cursor), K_(end_cursor), K_(is_inited), K_(is_local),
      K_(buf_size), KP_(buf), K_(len), K_(log_data_len), K_(seq),
      K_(flush_finish), K_(flush_ret));

  common::ObLogCursor start_cursor_;
  common::ObLogCursor end_cursor_;

private:
  static const int64_t OFFSET_ARR_SIZE = 32;

private:
  bool is_inited_;
  bool is_local_;
  int64_t buf_size_;
  char *buf_;
  int64_t len_;
  int64_t log_data_len_;
  int64_t seq_;
  common::ObThreadCond flush_cond_;
  bool flush_finish_;
  int flush_ret_;

  int64_t offset_arr_[OFFSET_ARR_SIZE];
  int64_t *local_offset_arr_;
  uint64_t log_cnt_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageLogItem);
};

OB_INLINE int64_t ObStorageLogItem::get_align_padding_size(const int64_t x, const int64_t align)
{
  return -x & (align - 1);
}

}
}

#endif
