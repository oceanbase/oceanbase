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

#include "ob_election_part_array_buf.h"
#include "ob_election_async_log.h"

namespace oceanbase {
namespace election {
using namespace common;

int ObPartArrayBuffer::update_content(const ObPartitionArray& part_array)
{
  int ret = OB_SUCCESS;
  const int64_t pkey_cnt = part_array.count();
  int64_t pos = 0;
  if (OB_FAIL(serialization::encode_i64(buf_, ARRAY_BUF_SIZE, pos, pkey_cnt))) {
    ELECT_ASYNC_LOG(WARN, "serialize pkey_cnt error", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pkey_cnt; ++i) {
      const ObPartitionKey pkey = part_array.at(i);
      if (pos + pkey.get_serialize_size() > ARRAY_BUF_SIZE) {
        ret = OB_SIZE_OVERFLOW;
        ELECT_ASYNC_LOG(WARN, "part_array size exceeds buf size", K(ret));
      } else if (OB_FAIL(pkey.serialize(buf_, ARRAY_BUF_SIZE, pos))) {
        ELECT_ASYNC_LOG(WARN, "pkey.serialize error", K(ret));
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret)) {
      serialize_size_ = pos;
    }
  }
  return ret;
}

}  // namespace election
}  // namespace oceanbase
