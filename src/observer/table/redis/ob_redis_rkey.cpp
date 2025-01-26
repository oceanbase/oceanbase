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

#define USING_LOG_PREFIX SERVER

#include "ob_redis_rkey.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_fast_convert.h"

namespace oceanbase
{
namespace table
{
int ObRedisRKey::encode_inner(bool is_data, const ObString &key, ObString &encoded_rkey)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator_);
  int64_t reserved_size = key.length() + subkey_.length() + ObRedisRKeyUtil::KEY_LENGTH_HEX_LENGTH
                          + ObRedisRKeyUtil::IS_DATA_LENGTH;
  ObString key_length_str;
  ObFastFormatInt ffi(is_data ? 1 : 0);
  ObString is_data_str(ffi.length(), ffi.ptr());
  if (OB_FAIL(buffer.reserve(reserved_size))) {
    LOG_WARN("fail to reserve buffer", K(ret), K(reserved_size));
  } else if (OB_FAIL(key_length_to_hex(key_length_str))) {
    LOG_WARN("fail to get key length str", K(ret), K(key_length_str));
  } else if (OB_FAIL(buffer.append(key_length_str))) {
    LOG_WARN("fail to append key length str", K(ret), K(key_length_str));
  } else if (OB_FAIL(buffer.append(key))) {
    LOG_WARN("fail to append key", K(ret), K(key));
  } else if (OB_FAIL(buffer.append(is_data_str))) {
    LOG_WARN("fail to append is data", K(ret), K(is_data_str));
  } else if (OB_FAIL(buffer.append(subkey_))) {
    LOG_WARN("fail to append subkey", K(ret), K(subkey_));
  } else {
    encoded_rkey = buffer.string();
  }
  return ret;
}

int ObRedisRKey::encode(ObString &encoded_rkey)
{
  return encode_inner(is_data_, key_, encoded_rkey);
}

// prefix: 00000006myhash1 -> next prefix: 00000006myhasi0
int ObRedisRKey::encode_next_prefix(ObString &encoded_rkey)
{
  int ret = OB_SUCCESS;
  ObString next_prefix;
  if (OB_FAIL(ob_write_string(allocator_, key_, next_prefix))) {
    LOG_WARN("fail to write string", K(ret));
  } else {
    char *key_chars = next_prefix.ptr();
    bool need_carry = true;
    for (int i = next_prefix.length() - 1; i >= 0 && need_carry; i--) {
      key_chars[i]++;
      if (key_chars[i] != 0) {
        need_carry = false;
      }
    }
  }
  return encode_inner(false /*is_data*/, next_prefix, encoded_rkey);
}

int ObRedisRKey::key_length_to_hex(ObString &key_length_str)
{
  int ret = OB_SUCCESS;
  const char hexChars[] = "0123456789abcdef";
  char *cret = nullptr;
  uint32_t length = static_cast<uint32_t>(key_.length());
  if (OB_ISNULL(cret = reinterpret_cast<char *>(
                    allocator_.alloc(sizeof(char) * ObRedisRKeyUtil::KEY_LENGTH_HEX_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else {
    MEMSET(cret, '0', ObRedisRKeyUtil::KEY_LENGTH_HEX_LENGTH);
    for (int i = ObRedisRKeyUtil::KEY_LENGTH_HEX_LENGTH - 1; i >= 0 && length != 0; i--) {
      cret[i] = hexChars[length & 0xf];
      length >>= 4;
    }
    key_length_str = ObString(ObRedisRKeyUtil::KEY_LENGTH_HEX_LENGTH, cret);
  }
  return ret;
}

/*******************/
/* ObRedisRKeyUtil */
/*******************/

// rowkey: (db, rkey)
// part_key: list, string: (db, rkey)
//           zset, set, list: (db, vk)
int ObRedisRKeyUtil::gen_partition_key_by_rowkey(ObRedisModel model,
                                                 ObIAllocator &allocator,
                                                 const ObRowkey &rowkey,
                                                 ObRowkey &partition_key)
{
  int ret = OB_SUCCESS;
  if (!rowkey.is_valid() || rowkey.get_obj_cnt() < 2 || model == ObRedisModel::INVALID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument rowkey", K(ret), K(rowkey), K(model));
  } else if (model == ObRedisModel::STRING || model == ObRedisModel::LIST) {
    partition_key = rowkey;
  } else {
    ObString rkey;
    ObString key;
    if (OB_FAIL(rowkey.deep_copy(partition_key, allocator))) {
      LOG_WARN("fail to deep copy rowkey", K(ret));
    } else if (OB_FAIL(partition_key.get_obj_ptr()[ObRedisUtil::COL_IDX_RKEY].get_varbinary(rkey))) {
      LOG_WARN("fail to get varbinary", K(ret), K(partition_key));
    } else if (OB_FAIL(decode_key(rkey, key))) {
      LOG_WARN("fail to decode subkey", K(ret), K(rkey));
    } else {
      partition_key.get_obj_ptr()[ObRedisUtil::COL_IDX_RKEY].set_varbinary(key);
    }
  }

  return ret;
}

int ObRedisRKeyUtil::decode_subkey(const ObString &encoded_rkey, ObString &subkey)
{
  int ret = OB_SUCCESS;
  ObString key_length(KEY_LENGTH_HEX_LENGTH, encoded_rkey.ptr());
  uint32_t length = 0;
  if (OB_FAIL(hex_to_key_length(key_length, length))) {
    LOG_WARN("fail to get key length", K(ret), K(key_length));
  } else if (encoded_rkey.length() == length + KEY_LENGTH_HEX_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoded_rkey length", K(ret), K(encoded_rkey));
  } else {
    int diff = KEY_LENGTH_HEX_LENGTH + length + IS_DATA_LENGTH;
    subkey = ObString(encoded_rkey.length() - diff, encoded_rkey.ptr() + diff);
  }
  return ret;
}

int ObRedisRKeyUtil::decode_key(const ObString &encoded_rkey, ObString &key)
{
  int ret = OB_SUCCESS;
  ObString key_length(KEY_LENGTH_HEX_LENGTH, encoded_rkey.ptr());
  uint32_t length = 0;
  if (OB_FAIL(hex_to_key_length(key_length, length))) {
    LOG_WARN("fail to get key length", K(ret), K(key_length));
  } else if (encoded_rkey.length() == length + KEY_LENGTH_HEX_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoded_rkey length", K(ret), K(encoded_rkey));
  } else {
    key = ObString(length, encoded_rkey.ptr() + KEY_LENGTH_HEX_LENGTH);
  }
  return ret;
}

int ObRedisRKeyUtil::decode_is_data(const ObString &encoded_rkey, bool &is_data)
{
  int ret = OB_SUCCESS;
  ObString key_length(KEY_LENGTH_HEX_LENGTH, encoded_rkey.ptr());
  uint32_t length = 0;
  if (OB_FAIL(hex_to_key_length(key_length, length))) {
    LOG_WARN("fail to get key length", K(ret), K(key_length));
  } else if (encoded_rkey.length() == length + KEY_LENGTH_HEX_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoded_rkey length", K(ret), K(encoded_rkey));
  } else {
    int diff = KEY_LENGTH_HEX_LENGTH + length;
    ObString is_data_str(IS_DATA_LENGTH, encoded_rkey.ptr() + diff);
    is_data = is_data_str == "1" ? true : false;
  }
  return ret;
}

int ObRedisRKeyUtil::decode(ObIAllocator &allocator, const ObString &encoded_rkey,
                            ObRedisRKey *&rkey)
{
  int ret = OB_SUCCESS;
  ObString key_length(KEY_LENGTH_HEX_LENGTH, encoded_rkey.ptr());
  uint32_t length = 0;
  if (OB_FAIL(hex_to_key_length(key_length, length))) {
    LOG_WARN("fail to get key length", K(ret), K(key_length));
  } else if (encoded_rkey.length() == length + KEY_LENGTH_HEX_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid encoded_rkey length", K(ret), K(encoded_rkey));
  } else {
    int diff = KEY_LENGTH_HEX_LENGTH;
    ObString key(length, encoded_rkey.ptr() + diff);
    diff += length;
    ObString is_data_str(IS_DATA_LENGTH, encoded_rkey.ptr() + diff);
    bool is_data = is_data_str == "1" ? true : false;
    diff += IS_DATA_LENGTH;
    // subkey may be empty, e.g. command 'SET k ""'
    ObString subkey;
    if (encoded_rkey.length() > diff) {
      subkey = ObString(encoded_rkey.length() - diff, encoded_rkey.ptr() + diff);
    }
    ObRedisRKey *tmp_rkey = nullptr;
    if (OB_ISNULL(tmp_rkey = OB_NEWx(ObRedisRKey, &allocator, allocator, key, is_data, subkey))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(ret), K(key), K(is_data), K(subkey));
    } else {
      rkey = tmp_rkey;
    }
  }
  return ret;
}

int ObRedisRKeyUtil::hex_to_key_length(const ObString &hex_str, uint32_t &key_length)
{
  int ret = OB_SUCCESS;
  if (hex_str.length() != KEY_LENGTH_HEX_LENGTH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hex string", K(ret), K(hex_str));
  } else {
    key_length = 0;
    for (int i = 0; OB_SUCC(ret) && i < hex_str.length(); ++i) {
      uint32_t cur_num = 0;
      const char &c = hex_str[i];
      if (c >= '0' && c <= '9') {
        cur_num = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        cur_num = c - 'a' + 10;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid hex string", K(ret), K(i), K(c));
      }
      if (OB_SUCC(ret)) {
        key_length = (key_length << 4) + cur_num;
      }
    }
  }
  return ret;
}

}  // namespace table
}  // namespace oceanbase