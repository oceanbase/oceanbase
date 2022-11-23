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

#ifndef OCEANBASE_COMMON_OB_SEGMENTED_BUFFER_
#define OCEANBASE_COMMON_OB_SEGMENTED_BUFFER_

#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace lib {}
namespace common
{
class ObSegmentedBufffer
{
friend class ObSegmentedBuffferIterator;
public:
  ObSegmentedBufffer(const int block_size,
                     const lib::ObMemAttr attr)
    : block_size_(block_size), attr_(attr),
      head_(nullptr), block_(nullptr), pos_(0) {}
  ~ObSegmentedBufffer() { destory(); }
  int append(char *ptr, int64_t len);
  int64_t size() const;
  int padding(int64_t len);
  void destory();
  int dump_to_file(const char *file_name);
private:
  const int block_size_;
  const lib::ObMemAttr attr_;
  char *head_;
  char *block_;
  int64_t pos_;
};

class ObSegmentedBuffferIterator
{
public:
  ObSegmentedBuffferIterator(ObSegmentedBufffer &sb)
    : next_buf_(sb.head_), sb_(sb) {}
  char *next(int64_t &len);
private:
  char *next_buf_;
  const ObSegmentedBufffer &sb_;
};

} // namespace common
} // namespace oceanbase

#endif //OCEANBASE_COMMON_OB_SEGMENTED_BUFFER_
