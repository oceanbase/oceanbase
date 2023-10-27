/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_COMPILE_UTILITY_MDS_DUMMYKEY_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_COMPILE_UTILITY_MDS_DUMMYKEY_H
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

struct DummyKey
{
  DummyKey() = default;
  int serialize(char *, const int64_t, int64_t &) const { return  OB_SUCCESS; }
  int deserialize(const char *, const int64_t, int64_t &) { return  OB_SUCCESS; }
  int64_t get_serialize_size() const { return 0; }
  int64_t to_string(char *buf, const int64_t buf_len) const {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "Dummy");
    return pos;
  }
  bool operator<(const DummyKey& rhs) const { return false; }
  bool operator==(const DummyKey& rhs) const { return true; }
};

}
}
}

#endif
