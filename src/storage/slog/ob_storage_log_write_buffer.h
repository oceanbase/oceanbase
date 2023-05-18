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

#ifndef OCEANBASE_STORAGE_OB_STORAGE_LOG_WRITE_BUFFER_H_
#define OCEANBASE_STORAGE_OB_STORAGE_LOG_WRITE_BUFFER_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
class ObStorageLogItem;

class ObStorageLogWriteBuffer
{
public:
  ObStorageLogWriteBuffer();
  ~ObStorageLogWriteBuffer();

  int init(
      const int64_t align_size,
      const int64_t buf_size,
      const int64_t tenant_id);
  void destroy();
  const char *get_buf() const { return buf_; }
  int64_t get_write_len() const { return write_len_; }
  int64_t get_log_data_len() const { return log_data_len_; }
  int64_t get_left_space() const { return buf_size_ - log_data_len_; }
  int copy_log_item(const ObStorageLogItem *item);
  int move_buffer(int64_t &backward_size);
  void reuse();

  TO_STRING_KV(K_(is_inited), K_(buf), K_(buf_size), K_(write_len), K_(log_data_len));

private:
  bool is_inited_;
  char *buf_;
  int64_t buf_size_;
  // represents the total length (data length + nop length)
  int64_t write_len_;
  // represents the data length
  int64_t log_data_len_;
  int64_t align_size_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_STORAGE_LOG_WRITE_BUFFER_H_
