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

#ifndef OCEANBASE_ELECTION_OB_ELECTION_PART_ARRAY_BUF_
#define OCEANBASE_ELECTION_OB_ELECTION_PART_ARRAY_BUF_

#include "common/ob_partition_key.h"
#include <stdint.h>

namespace oceanbase {
namespace election {

static const int64_t MAX_EG_PARTITION_NUM = 100;  // limits of election group size

class ObPartArrayBuffer {
public:
  ObPartArrayBuffer() : cur_eg_version_(-1), serialize_size_(0)
  {}
  ~ObPartArrayBuffer()
  {}
  const char* get_data_buf() const
  {
    return buf_;
  }
  int64_t get_eg_version() const
  {
    return cur_eg_version_;
  }
  void set_eg_version(const int64_t eg_version)
  {
    cur_eg_version_ = eg_version;
  }
  int64_t get_buf_len() const
  {
    return ARRAY_BUF_SIZE;
  }
  int64_t get_serialize_size() const
  {
    return serialize_size_;
  }
  int update_content(const common::ObPartitionArray& part_array);

public:
  static const int64_t ARRAY_BUF_SIZE = 4096;

private:
  char buf_[ARRAY_BUF_SIZE];
  int64_t cur_eg_version_;
  int64_t serialize_size_;
};

}  // namespace election
}  // namespace oceanbase

#endif  // OCEANBASE_ELECTION_OB_ELECTION_PART_ARRAY_BUF_
