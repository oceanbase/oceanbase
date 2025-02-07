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

#ifndef OCEANBASE_SHARE_TABLE_OB_REDIS_PARSER_H_
#define OCEANBASE_SHARE_TABLE_OB_REDIS_PARSER_H_

#include "src/share/table/ob_redis_common.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string_buffer.h"

namespace oceanbase
{
namespace table
{
class ObRedisParser
{
public:
  /**
   * @brief Decodes Redis messages
   *
   * @param redis_msg Redis message
   * @param row_strs decoded array of row strings
   * @return int Returns 0 for success and any other value for failure
   */
  static int decode(
      ObIAllocator &allocator,
      const ObString &redis_msg,
      ObString &cmd_name,
      ObArray<ObString> &args);

  /**
   * @brief encodes the error message
   * @param err_msg error message
   * @param encoded_msg encoded message
   * @return Returns the encoded result, 0 on success or some other value on failure
   */
  static int encode_error(ObIAllocator &allocator, const ObString &err_msg, ObString &encoded_msg);
  /**
   * @brief encodes a simple string
   * @param simpe_str Simple string
   * @param encoded_msg encoded message
   * @return Returns the encoded result, 0 on success or some other value on failure
   */
  static int encode_simple_string(
      ObIAllocator &allocator,
      const ObString &simpe_str,
      ObString &encoded_msg);
  /**
   * @brief encodes long strings
   * @param bulk_str Long string
   * @param encoded_msg encoded message
   * @return Returns the encoded result, 0 on success or some other value on failure
   */
  static int encode_bulk_string(
      ObIAllocator &allocator,
      const ObString &bulk_str,
      ObString &encoded_msg);
  /**
   * @brief encodes integers
   * @param integer Integer
   * @param encoded_msg encoded message
   * @return Returns the encoded result, 0 on success or some other value on failure
   */
  static int encode_integer(ObIAllocator &allocator, const int64_t integer, ObString &encoded_msg);

  static int encode_array(
      ObIAllocator &allocator,
      const ObIArray<ObString> &array,
      ObString &encoded_msg);

private:
  static int encode_with_flag(const char flag, const ObString &msg, ObStringBuffer &buffer);
  static int inner_encode_bulk_string(const ObString &bulk_str, ObStringBuffer &buffer);
  DISALLOW_COPY_AND_ASSIGN(ObRedisParser);
};

class ObRedisDecoder
{
public:
  ObRedisDecoder(ObIAllocator &allocator, const ObString &redis_msg)
      : redis_msg_(redis_msg.ptr()),
        length_(redis_msg.length()),
        cur_pos_(0),
        args_(OB_MALLOC_NORMAL_BLOCK_SIZE, ModulePageAllocator(allocator, "RedisDecode")),
        allocator_(&allocator)
  {}
  ~ObRedisDecoder() {}

  int decode();

  inline int get_result(ObString &cmd_name, ObArray<ObString> &args);

  TO_STRING_KV(K(length_), K(cur_pos_));

private:
  int read_until_crlf(ObString &splited);
  int decode_bulk_string(ObString &bulk_str);
  int decode_array(const ObString &header);

  const char *redis_msg_;
  int32_t length_;
  int32_t cur_pos_;
  ObString cmd_name_;
  ObArray<ObString> args_;
  ObIAllocator *allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObRedisDecoder);
};

}  // end namespace table
}  // end namespace oceanbase

#endif /* OCEANBASE_SHARE_TABLE_OB_REDIS_PARSER_H_ */
