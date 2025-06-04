//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "storage/multi_data_source/mds_key_serialize_util.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{

int ObMdsSerializeUtil::mds_key_serialize(const int64_t key, char *buf,
                                          const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp = key;
  if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t idx = 0; idx < 8 && OB_SUCC(ret); ++idx) {
      if (pos >= buf_len) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        buf[pos++] = ((tmp >> (56 - 8 * idx)) & 0x00000000000000FF);
      }
    } // for
  }
  return ret;
}

int ObMdsSerializeUtil::mds_key_deserialize(const char *buf, const int64_t buf_len,
                               int64_t &pos, int64_t &key)
{
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  for (int64_t idx = 0; idx < 8 && OB_SUCC(ret); ++idx) {
    if (pos >= buf_len) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      tmp <<= 8;
      tmp |= (0x00000000000000FF & buf[pos++]);
    }
  }
  if (OB_SUCC(ret)) {
    key = tmp;
  }
  return ret;
}

} // namespace mds
} // namespace storage
} // namespace oceanbase
