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

#ifndef OCEANBASE_ARCHIVE_DYNAMIC_BUFFER_H_
#define OCEANBASE_ARCHIVE_DYNAMIC_BUFFER_H_

#include "lib/container/ob_array.h"
#include "lib/ob_define.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include <cstdint>
namespace oceanbase
{
namespace archive
{
// Cache and purge large memory buffer to support dynamic diverse large memory usage
// For small memory(not bigger than the basic block), alloc and free in real-time
// For large memory(bigger than the basic), cache it after usage and purge it with strategy base on usage statistics
class DynamicBuffer final
{
  static const int64_t MAX_BUF_SIZE_TYPE = 8;
  static const int64_t BASIC_BUF_SIZE = OB_MALLOC_BIG_BLOCK_SIZE;    // min_buffer
  static const int64_t MAX_BUF_SIZE = 64 * 1024 * 1024L + 20 * 1024;       // default max buf size value, 64M + 20K
  static const int64_t WASH_INTERVAL = 10 * 1000 * 1000L;      // 10s
  static const int64_t WASH_THREASHOLD = 90;
public:
  explicit DynamicBuffer(const int64_t MAX_SIZE = MAX_BUF_SIZE);
  DynamicBuffer(const char *label, const int64_t MAX_SIZE = MAX_BUF_SIZE);
  ~DynamicBuffer();

  void destroy();
  // for small buffer, alloc pointed size, for big size, alloc upper_align size
  char *acquire(const int64_t size);
  // free small buffer(size <= 1.875M), cache large buffer(size > 1.875M)
  void reclaim(void *ptr);
  int reserve(const int64_t size);
  void purge();
  bool alloc_from(void *ptr);
  int assign(const DynamicBuffer &other);

  TO_STRING_KV(K_(buf_size), K_(buf), K_(ref), K_(buf_limit), K_(buf_gen_timestamp), K_(buf_size_usage), K_(label));

private:
  void alloc_(const int64_t size);
  void free_();
  // slot: 0, 1, 2.. max, buf_size: 1 basic_buf_size, 2 basic_buf_size, 4 basic_buf_size... min(buf_limit, 2^n basic_buf_size)
  int64_t upper_align_(const int64_t input) const;
  void init_statstic_();
  void add_statistic_(const int64_t size);
  void purge_statistic_();
  int64_t get_usage_index_(const int64_t input) const;
  void wash_();
  bool need_wash_() const;
  int64_t get_target_buf_size_() const;

private:
  int64_t buf_size_;
  char *buf_;
  int64_t ref_;
  int64_t buf_limit_;
  int64_t buf_gen_timestamp_;
  ObArray<int64_t> buf_size_usage_;
  int64_t total_usage_;
  common::ObFixedLengthString<lib::AOBJECT_TAIL_SIZE> label_;

private:
  DISALLOW_COPY_AND_ASSIGN(DynamicBuffer);
};
} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_DYNAMIC_BUFFER_H_ */
