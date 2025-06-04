//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_MULTI_DATA_SOURCE_MDS_KEY_SERIALIZE_UTIL_H_
#define OB_STORAGE_MULTI_DATA_SOURCE_MDS_KEY_SERIALIZE_UTIL_H_
#include "/usr/include/stdint.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{

struct ObMdsSerializeUtil final
{
  static int mds_key_serialize(const int64_t key, char *buf,
                               const int64_t buf_len, int64_t &pos);
  static int mds_key_deserialize(const char *buf, const int64_t buf_len, int64_t &pos, int64_t &key);
  template <typename T>
  static int64_t mds_key_get_serialize_size(const T key) { return sizeof(T); }
};

} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_MULTI_DATA_SOURCE_MDS_KEY_SERIALIZE_UTIL_H_
