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

#ifndef OCEABASE_STORAGE_OB_LOB_WRITE_BUFFER_
#define OCEABASE_STORAGE_OB_LOB_WRITE_BUFFER_

#include "storage/lob/ob_lob_meta.h"
#include "storage/lob/ob_lob_util.h"

namespace oceanbase
{
namespace storage
{

struct ObLobWriteBuffer
{
public:
  static size_t max_bytes_charpos(
      const ObCollationType collation_type,
      const char *str,
      const int64_t str_len,
      const int64_t max_bytes, /* max buffer size can be writed */
      int64_t &char_len);

public:
  ObLobWriteBuffer(ObCollationType coll_type, int64_t max_byte_len, bool is_store_char_len):
    coll_type_(coll_type),
    max_byte_len_(max_byte_len),
    inner_buffer_(),
    use_buffer_(false),
    is_full_(false),
    is_store_char_len_(is_store_char_len),
    is_char_len_valid_(false),
    char_len_(0),
    max_bytes_in_char_(16)
  {}

  bool is_full() const { return is_full_; }
  bool use_buffer() const { return use_buffer_; }
  bool is_char() const { return coll_type_ != common::ObCollationType::CS_TYPE_BINARY; }
  int64_t byte_length() const { return inner_buffer_.length(); }

  int64_t remain() const { return inner_buffer_.remain(); }
  char* buffer_ptr() { return inner_buffer_.ptr(); }
  const char* buffer_ptr() const { return inner_buffer_.ptr(); }
  int64_t buffer_size() const { return inner_buffer_.size(); }

  int align_write_postion(const char *data_ptr, const int64_t data_byte_len, const int64_t max_byte_can_write, int64_t &byte_len, int64_t &char_len);

  int to_lob_meta_info(ObLobMetaInfo &meta_info) const;

private:
  ObString get_const_str() const;
  int get_byte_range(
      const int64_t char_offset,
      const int64_t char_len,
      int64_t &byte_offset,
      int64_t &byte_len) const;
  int get_byte_range(
      const char *data_ptr,
      const int64_t data_len,
      const int64_t char_offset,
      const int64_t char_len,
      int64_t &byte_offset,
      int64_t &byte_len) const;

  int move_to_remain_buffer(
      ObString &remain_buf,
      int64_t move_src_byte_offset,
      int64_t total_move_byte_len,
      int64_t &real_move_byte_len/*in out param*/,
      int64_t &real_move_char_len);

  int move_data_for_write(
      int64_t write_byte_offset,
      int64_t write_old_byte_len,
      int64_t write_new_byte_len,
      ObString &remain_buf);

  int move_data_for_write(
      int64_t write_byte_offset,
      int64_t write_old_byte_len,
      int64_t write_new_byte_len);

  int do_write(
      const int64_t write_byte_offset,
      ObString &write_data);

  int set_char_len(const int64_t char_len);
  int get_char_len(uint32_t &char_len) const;

public:
  int byte_write(
      int64_t write_byte_offset,
      int64_t write_old_byte_len,
      ObString &write_data,
      ObString &remain_buf);

  int char_write(
      int64_t write_char_offset,
      int64_t write_char_len,
      ObString &write_data,
      ObString &remain_buf);

  int fill_zero_data(
      const int64_t byte_offset,
      const int64_t byte_len,
      const int64_t char_len,
      ObString &space);

  int byte_fill_zero(
      int64_t fill_byte_offset,
      int64_t fill_old_byte_len,
      int64_t fill_char_len);

  int char_fill_zero(
      int64_t fill_char_offset,
      int64_t fill_char_len);

  int padding(int64_t char_len, int64_t &real_write_byte_len);

  int set_data(char *data_ptr, int64_t data_byte_len, int64_t &real_set_byte_len);
  int set_data(char *data_ptr, int64_t data_byte_len);
  int set_buffer(char *buf_ptr, int64_t buf_len);

  int append(char *data_ptr, int64_t data_byte_len, int64_t &real_write_byte_len);
  int append(char *data_ptr, int64_t data_byte_len);
  int char_append(ObString &data, int64_t write_char_offset, int64_t write_char_len);

  ObCollationType coll_type_;
  int64_t max_byte_len_;
  ObString inner_buffer_;
  bool use_buffer_;
  bool is_full_;
  bool is_store_char_len_;
  bool is_char_len_valid_;
  int64_t char_len_;
  int64_t max_bytes_in_char_;

  TO_STRING_KV(K_(coll_type), K_(max_byte_len), K_(use_buffer), K_(is_full), K_(inner_buffer), K_(is_store_char_len),
       K_(is_char_len_valid), K_(char_len));
};


} // storage
} // oceanbase

#endif
