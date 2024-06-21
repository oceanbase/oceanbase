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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_write_buffer.h"

namespace oceanbase
{
namespace storage
{

size_t ObLobWriteBuffer::max_bytes_charpos(
    const ObCollationType collation_type,
    const char *str,
    const int64_t str_len,
    const int64_t max_bytes, /* max buffer size can be writed */
    int64_t &char_len)
{
  int64_t byte_len = max_bytes;
  byte_len = ObCharset::max_bytes_charpos(collation_type, str, str_len, byte_len, char_len);
  byte_len = ob_lob_writer_length_validation(collation_type, str_len, byte_len, char_len);
  return byte_len;
}

int ObLobWriteBuffer::align_write_postion(const char *data_ptr, const int64_t data_byte_len, const int64_t max_byte_can_write, int64_t &byte_len, int64_t &char_len)
{
  int ret = OB_SUCCESS;
  if (data_byte_len > max_byte_can_write) {
    byte_len = ObCharset::max_bytes_charpos(coll_type_,
                                          data_ptr,
                                          data_byte_len,
                                          max_byte_can_write,
                                          char_len);
    byte_len = ob_lob_writer_length_validation(coll_type_, data_byte_len, byte_len, char_len);
  } else {
    byte_len = data_byte_len;
    char_len = -1;
  }
  return ret;
}

int ObLobWriteBuffer::get_byte_range(
    const char *data_ptr,
    const int64_t data_len,
    const int64_t char_offset,
    const int64_t char_len,
    int64_t &byte_offset,
    int64_t &byte_len) const
{
  int ret = OB_SUCCESS;
  byte_offset = ObCharset::charpos(coll_type_, data_ptr, data_len, char_offset);
  byte_len = ObCharset::charpos(coll_type_, data_ptr + byte_offset, data_len - byte_offset, char_len);
  if (byte_offset < char_offset || byte_len < char_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("oversize", K(ret), K(byte_offset), K(char_offset), K(byte_len), K(char_len), K(data_len), KP(data_ptr));
  }
  return ret;
}

int ObLobWriteBuffer::get_byte_range(
    const int64_t char_offset,
    const int64_t char_len,
    int64_t &byte_offset,
    int64_t &byte_len) const
{
  return get_byte_range(buffer_ptr(), byte_length(), char_offset, char_len, byte_offset, byte_len);
}

int ObLobWriteBuffer::move_to_remain_buffer(
    ObString &remain_buf,
    int64_t move_src_byte_offset,
    int64_t total_move_byte_len,
    int64_t &real_move_byte_len/*in out param*/,
    int64_t &real_move_char_len)
{
  int ret = OB_SUCCESS;
  int64_t remain_byte_len = total_move_byte_len - real_move_byte_len;
  if (remain_byte_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("remain_byte_len invalid", K(ret), K(remain_byte_len), K(total_move_byte_len), K(real_move_byte_len));
  } else if (remain_byte_len == 0) {
    // no need move
    LOG_DEBUG("remain_byte_len is zero, not move", K(remain_byte_len), K(total_move_byte_len), K(real_move_byte_len));
  } else {
    // need ensure remain buffer data is full char
    real_move_byte_len = ObLobWriteBuffer::max_bytes_charpos(
        coll_type_,
        buffer_ptr() + move_src_byte_offset,
        total_move_byte_len,
        real_move_byte_len,
        real_move_char_len);
    // move_byte_len may be less after max_bytes_charpos, so need update remain_byte_len
    remain_byte_len = total_move_byte_len - real_move_byte_len;
    if (remain_byte_len > remain_buf.size()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("remain buffer oversize", K(ret), K(remain_byte_len), "remain_buffer size", remain_buf.size(), K(total_move_byte_len), K(real_move_byte_len));
    } else if (remain_buf.write(buffer_ptr() + move_src_byte_offset + real_move_byte_len, remain_byte_len) != remain_byte_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write data to remain buffer fail", K(ret), K(move_src_byte_offset), K(real_move_byte_len), K(remain_byte_len), K(remain_buf));
    }
  }
  return ret;
}

int ObLobWriteBuffer::move_data_for_write(
    int64_t write_byte_offset,
    int64_t write_old_byte_len,
    int64_t write_new_byte_len,
    ObString &remain_buf)
{
  int ret = OB_SUCCESS;
  int64_t move_src_byte_offset = write_byte_offset + write_old_byte_len;
  int64_t move_dst_byte_offset = write_byte_offset + write_new_byte_len;
  int64_t total_move_byte_len = byte_length() - move_src_byte_offset;
  int64_t real_move_byte_len = move_dst_byte_offset <= move_src_byte_offset ? total_move_byte_len : (byte_length() <= move_dst_byte_offset ? 0 : byte_length() - move_dst_byte_offset);
  int64_t real_move_char_len = real_move_byte_len;

  if (total_move_byte_len < 0 || real_move_byte_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid length", K(ret), K(total_move_byte_len), K(real_move_byte_len), K(write_byte_offset), K(write_old_byte_len), K(write_new_byte_len), K(byte_length()));
  } else if (move_src_byte_offset == move_dst_byte_offset) {
    // src and dst is same, no need move
    LOG_DEBUG("src and dst is same, so not move", K(move_src_byte_offset), K(move_dst_byte_offset), K(total_move_byte_len));
  } else if (OB_FAIL(move_to_remain_buffer(remain_buf, move_src_byte_offset, total_move_byte_len, real_move_byte_len, real_move_char_len))) {
    LOG_WARN("move_to_remain_buffer fail", K(ret), K(move_src_byte_offset), K(total_move_byte_len), K(real_move_byte_len), K(real_move_char_len));
  } else {
    if (real_move_byte_len > 0) {
      MEMMOVE(buffer_ptr() + move_dst_byte_offset, buffer_ptr() + move_src_byte_offset, real_move_byte_len);
    } else {
      LOG_DEBUG("no data move", K(total_move_byte_len), K(real_move_byte_len), K(write_byte_offset), K(write_old_byte_len), K(write_new_byte_len), K(byte_length()));
    }
    if (OB_FAIL(ret)) {
    } else if (inner_buffer_.set_length(move_dst_byte_offset + real_move_byte_len) != move_dst_byte_offset + real_move_byte_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("set_length fail", K(ret), K(inner_buffer_.size()), K(inner_buffer_.length()), K(move_dst_byte_offset), K(real_move_byte_len));
    }
  }
  return ret;
}

int ObLobWriteBuffer::move_data_for_write(
    int64_t write_byte_offset,
    int64_t write_old_byte_len,
    int64_t write_new_byte_len)
{
  ObString remain_buf;
  return move_data_for_write(write_byte_offset, write_old_byte_len, write_new_byte_len, remain_buf);
}

int ObLobWriteBuffer::do_write(
    const int64_t write_byte_offset,
    ObString &write_data)
{
  int ret = OB_SUCCESS;
  if (write_data.empty()) {
    LOG_DEBUG("not write because write_data is empty");
  } else if (write_byte_offset + write_data.length() > buffer_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("out of buffer range", K(ret), K(write_byte_offset), K(buffer_size()), K(write_data));
  } else {
    MEMCPY(buffer_ptr() + write_byte_offset, write_data.ptr(), write_data.length());
    if (write_byte_offset + write_data.length() > inner_buffer_.length()) {
      if (inner_buffer_.set_length(write_byte_offset + write_data.length()) != write_byte_offset + write_data.length()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set_length fail", K(ret), K(inner_buffer_.size()), K(inner_buffer_.length()), K(write_byte_offset), K(write_data.length()));
      }
    }
  }
  return ret;
}

int ObLobWriteBuffer::byte_write(
    int64_t write_byte_offset,
    int64_t write_old_byte_len,
    ObString &write_data,
    ObString &remain_buf)
{
  int ret = OB_SUCCESS;
  if (! use_buffer() ) {
    if (write_byte_offset > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write not at start pos", K(ret), K(write_byte_offset), K(write_old_byte_len), K(write_data));
    } else if (OB_FAIL(set_data(write_data.ptr(), write_data.length()))) {
      LOG_WARN("set_data fail", K(ret));
    }
  } else if (write_byte_offset + write_data.length() > buffer_size()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("out of buffer range", K(ret), K(write_byte_offset), K(buffer_size()), K(write_data));
  } else if (OB_FAIL(move_data_for_write(write_byte_offset, write_old_byte_len, write_data.length(), remain_buf))) {
    LOG_WARN("move_data_for_write fail", K(ret), K(write_byte_offset), K(write_old_byte_len), K(write_data));
  } else if (OB_FAIL(do_write(write_byte_offset, write_data))) {
    LOG_WARN("do_write fail", K(ret), K(write_byte_offset));
  }
  return ret;
}


int ObLobWriteBuffer::char_write(
    int64_t write_char_offset,
    int64_t write_char_len,
    ObString &write_data,
    ObString &remain_buf)
{
  int ret = OB_SUCCESS;
  int64_t write_byte_offset = write_char_offset;
  int64_t write_old_byte_len = write_char_len;
  if (! is_char()) {
    if (OB_FAIL(byte_write(write_byte_offset, write_old_byte_len, write_data, remain_buf))) {
      LOG_WARN("byte_write fail", K(ret), K(coll_type_), K(write_byte_offset), K(write_old_byte_len), K(write_data));
    }
  } else if (use_buffer()) {
    if (OB_FAIL(get_byte_range(write_char_offset, write_char_len, write_byte_offset, write_old_byte_len))) {
      LOG_WARN("get_byte_range fail", K(ret), K(write_char_offset), K(write_char_len), K(coll_type_), K(inner_buffer_));
    } else if (OB_FAIL(byte_write(write_byte_offset, write_old_byte_len, write_data, remain_buf))) {
      LOG_WARN("byte_write fail", K(ret), K(coll_type_), K(write_byte_offset), K(write_old_byte_len));
    }
  } else if (write_char_offset > 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write not at start pos", K(ret), K(write_char_offset), K(write_char_len), K(write_data));
  } else if (OB_FAIL(set_data(write_data.ptr(), write_data.length()))) {
    LOG_WARN("set_data fail", K(ret));
  }
  return ret;
}

int ObLobWriteBuffer::fill_zero_data(
    const int64_t byte_offset,
    const int64_t byte_len,
    const int64_t char_len,
    ObString &space)
{
  int ret = OB_SUCCESS;
  if (char_len * space.length() != byte_len) {
    LOG_WARN("fill zero length invalid", K(ret), K(space.length()), K(char_len), K(byte_len));
  } else if (space.length() == 1) {
    MEMSET(buffer_ptr() + byte_offset, space.ptr()[0], byte_len);
  } else {
    // used for utf16 currently
    int64_t space_len = space.length();
    for (int i = 0; i < char_len; i++) {
      MEMCPY(buffer_ptr() + byte_offset + i * space_len, space.ptr(), space_len);
    }
  }
  return ret;
}

ObString ObLobWriteBuffer::get_const_str() const
{
  ObString space;
  if (is_char()) {
    space = ObCharsetUtils::get_const_str(coll_type_, ' ');
  } else {
    space = ObCharsetUtils::get_const_str(coll_type_, '\x00');
  }
  return space;
}

int ObLobWriteBuffer::byte_fill_zero(
    int64_t fill_byte_offset,
    int64_t fill_old_byte_len,
    int64_t fill_char_len)
{
  int ret = OB_SUCCESS;
  ObString space = get_const_str();
  int64_t fill_new_byte_len = space.length() * fill_char_len;
  if (! use_buffer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be use_buffer mode", K(ret));
  } else if (OB_FAIL(move_data_for_write(fill_byte_offset, fill_old_byte_len, fill_new_byte_len))) {
    LOG_WARN("move_data_for_write fail", K(ret));
  } else if (OB_FAIL(fill_zero_data(fill_byte_offset, fill_new_byte_len, fill_char_len, space))){
    LOG_WARN("fill_zero_data fail", K(ret));
  }
  return ret;
}

int ObLobWriteBuffer::char_fill_zero(
    int64_t fill_char_offset,
    int64_t fill_char_len)
{
  int ret = OB_SUCCESS;
  int64_t fill_byte_offset = fill_char_offset;
  int64_t fill_old_byte_len = fill_char_len;
  if (! is_char()) {
    if (OB_FAIL(byte_fill_zero(fill_byte_offset, fill_old_byte_len, fill_char_len))) {
      LOG_WARN("byte_fill_zero fail", K(ret), K(coll_type_), K(fill_char_offset), K(fill_char_len), K(fill_byte_offset), K(fill_old_byte_len));
    }
  } else if (OB_FAIL(get_byte_range(fill_char_offset, fill_char_len, fill_byte_offset, fill_old_byte_len))) {
    LOG_WARN("get_byte_range fail", K(ret), K(fill_char_offset), K(fill_char_len), K(coll_type_), K(inner_buffer_));
  } else if (OB_FAIL(byte_fill_zero(fill_byte_offset, fill_old_byte_len, fill_char_len))) {
    LOG_WARN("byte_fill_zero fail", K(ret));
  }
  return ret;
}

int ObLobWriteBuffer::padding(int64_t char_len, int64_t &real_write_byte_len)
{
  int ret = OB_SUCCESS;
  ObString space = ObCharsetUtils::get_const_str(coll_type_, ' ');
  int64_t space_len = space.length();
  int64_t byte_len = OB_MIN(space_len * char_len, remain());
  int64_t inner_buffer_length = inner_buffer_.length();
  if (! use_buffer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should be use_buffer mode when padding", K(ret));
  } else if (is_char()) {
    if (space.length() == 1) {
      MEMSET(inner_buffer_.ptr() + inner_buffer_length, space.ptr()[0], byte_len);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support", K(ret), K(coll_type_), K(space));
    }
  } else {
    MEMSET(inner_buffer_.ptr() + inner_buffer_length, 0x00, byte_len);
  }

  if (OB_FAIL(ret)) {
  } else if (inner_buffer_.set_length(inner_buffer_length + byte_len) != inner_buffer_length + byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set_length fail", K(ret), K(inner_buffer_.size()), K(inner_buffer_.length()), K(inner_buffer_length), K(byte_len));
  } else {
    real_write_byte_len = byte_len;
    is_full_ = (is_char() && inner_buffer_.remain() < max_bytes_in_char_);
  }
  return ret;
}

int ObLobWriteBuffer::append(char *data_ptr, int64_t data_byte_len, int64_t &real_write_byte_len)
{
  int ret = OB_SUCCESS;
  int64_t write_byte_len = 0;
  int64_t write_char_len = 0;

  if (! use_buffer()) {
    if (OB_FAIL(set_data(data_ptr, data_byte_len, real_write_byte_len))) {
      LOG_WARN("set_data fail", K(ret), K(data_byte_len), K(real_write_byte_len));
    }
  } else if (OB_FAIL(align_write_postion(data_ptr, data_byte_len, inner_buffer_.remain(), write_byte_len, write_char_len))) {
    LOG_WARN("align_write_postion fail", K(ret), K(data_byte_len), K(inner_buffer_));
  } else if (inner_buffer_.write(data_ptr, write_byte_len) != write_byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write data to inner buffer fail", K(ret), K(data_byte_len), K(write_byte_len), K(inner_buffer_));
  } else {
    real_write_byte_len = write_byte_len;
    is_full_ = (is_char() && inner_buffer_.remain() < max_bytes_in_char_);
  }
  return ret;
}

int ObLobWriteBuffer::append(char *data_ptr, int64_t data_byte_len)
{
  int ret = OB_SUCCESS;
  int64_t real_write_byte_len = 0;
  if (OB_FAIL(append(data_ptr, data_byte_len, real_write_byte_len))) {
    LOG_WARN("append fail", K(ret), K(data_byte_len), KP(data_ptr));
  } else if (real_write_byte_len != data_byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write fail", K(ret), K(real_write_byte_len), K(data_byte_len), KP(data_ptr));
  }
  return ret;
}

int ObLobWriteBuffer::char_append(ObString &data, int64_t write_char_offset, int64_t write_char_len)
{
  int ret = OB_SUCCESS;
  int64_t write_byte_offset = 0;
  int64_t write_byte_len = 0;
  if (OB_FAIL(get_byte_range(data.ptr(), data.length(), write_char_offset, write_char_len, write_byte_offset, write_byte_len))) {
    LOG_WARN("get_byte_range fail", K(ret), K(write_char_offset), K(write_char_len), K(coll_type_), K(inner_buffer_));
  } else if (OB_FAIL(append(data.ptr() + write_byte_offset, write_byte_len))) {
    LOG_WARN("append fail", K(ret), K(write_byte_offset), K(write_byte_len), K(write_char_offset), K(write_char_len));
  }
  return ret;
}

int ObLobWriteBuffer::set_data(char *data_ptr, int64_t data_byte_len, int64_t &real_set_byte_len)
{
  int ret = OB_SUCCESS;
  int64_t char_len = 0;
  int64_t byte_len = 0;
  if (OB_FAIL(align_write_postion(data_ptr, data_byte_len, max_byte_len_, byte_len, char_len))) {
    LOG_WARN("align_write_postion fail", K(ret), K(data_byte_len), KP(data_ptr));
  } else if (char_len != -1 && OB_FAIL(set_char_len(char_len))) {
    LOG_WARN("set_char_len fail", K(ret), K(char_len), K(data_byte_len), K(byte_len));
  } else {
    inner_buffer_.assign_ptr(data_ptr, byte_len);
    use_buffer_ = false;
    is_full_ = true;
    real_set_byte_len = byte_len;
  }
  return ret;
}

int ObLobWriteBuffer::set_data(char *data_ptr, int64_t data_byte_len)
{
  int ret = OB_SUCCESS;
  int64_t real_set_byte_len = 0;
  if (OB_FAIL(set_data(data_ptr, data_byte_len, real_set_byte_len))) {
    LOG_WARN("set_data fail", K(ret), K(data_ptr), KP(data_ptr));
  } else if (real_set_byte_len != data_byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("write fail", K(ret), K(real_set_byte_len), K(data_byte_len), KP(data_ptr));
  }
  return ret;
}

int ObLobWriteBuffer::set_buffer(char *buf_ptr, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (buf_len != max_byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buf_len should equal to max_byte_len", K(ret), K(buf_len), K(max_byte_len_));
  } else {
    inner_buffer_.assign_buffer(buf_ptr, buf_len);
    use_buffer_ = true;
    is_full_ = false;
    is_char_len_valid_ = false;
  }
  return ret;
}

int ObLobWriteBuffer::to_lob_meta_info(ObLobMetaInfo &meta_info) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_char_len(meta_info.char_len_))) {
    LOG_WARN("get_char_len fail", K(ret), KPC(this));
  } else {
    meta_info.byte_len_ = inner_buffer_.length();
    meta_info.lob_data_ = inner_buffer_;
  }
  return ret;
}

int ObLobWriteBuffer::set_char_len(const int64_t char_len)
{
  int ret = OB_SUCCESS;
  if (is_char_len_valid_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not set because char_len is valid", K(ret), K(is_char_len_valid_), K(char_len_), K(char_len));
  } else {
    char_len_ = char_len;
    is_char_len_valid_ = true;
  }
  return ret;
}

int ObLobWriteBuffer::get_char_len(uint32_t &char_len) const
{
  int ret = OB_SUCCESS;
  if (is_store_char_len_) {
    if (is_char_len_valid_) {
      char_len = char_len_;
    } else {
      char_len = ObCharset::strlen_char(coll_type_, inner_buffer_.ptr(), inner_buffer_.length());
    }
  } else {
    if (is_char_len_valid_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unkown situtation", K(ret), KPC(this));
    } else {
      char_len = UINT32_MAX;
    }
  }
  return ret;
}

}
}
