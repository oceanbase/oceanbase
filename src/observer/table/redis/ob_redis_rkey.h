/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TABLE_REDIS_OB_REDIS_RKEY_H
#define OCEANBASE_TABLE_REDIS_OB_REDIS_RKEY_H

#include "share/table/redis/ob_redis_common.h"
#include "share/table/ob_table.h"

namespace oceanbase
{
namespace table
{
class ObRedisRKeyUtil;

class ObRedisRKey
{
public:
  ObRedisRKey(ObIAllocator &alloc, const ObString &key, bool is_data, const ObString &subkey)
      : allocator_(alloc), key_(key), is_data_(is_data), subkey_(subkey)
  {}
  virtual ~ObRedisRKey()
  {}
  int encode(ObString &encoded_rkey);
  int encode_next_prefix(ObString &encoded_rkey);
  // static int encode(const ObRedisRKey &rkey, ObString &encoded_rkey);

  TO_STRING_KV(K_(key), K_(is_data), K_(subkey));

private:
  int key_length_to_hex(ObString &key_length_str);
  int encode_inner(bool is_data, const ObString &key, ObString &encoded_rkey);

  ObIAllocator &allocator_;
  ObString key_;
  bool is_data_;
  ObString subkey_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisRKey);
};

class ObRedisRKeyUtil
{
public:
  static const int32_t KEY_LENGTH_HEX_LENGTH = 8;
  static const int32_t IS_DATA_LENGTH = 1;

  static int decode(ObIAllocator &allocator, const ObString &encoded_rkey, ObRedisRKey *&rkey);
  // only decode part of encoded_rkey
  static int decode_is_data(const ObString &encoded_rkey, bool &is_data);
  static int decode_subkey(const ObString &encoded_rkey, ObString &subkey);
  static int decode_key(const ObString &encoded_rkey, ObString &key);
  static int gen_partition_key_by_rowkey(ObRedisModel model, ObIAllocator &allocator, const ObRowkey &rowkey, ObRowkey &partition_key);

private:
  static int hex_to_key_length(const ObString &hex_str, uint32_t &key_length);
};
}  // namespace table
}  // namespace oceanbase
#endif /* OCEANBASE_TABLE_REDIS_OB_REDIS_RKEY_H */
