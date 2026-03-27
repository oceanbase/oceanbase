// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
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
