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
 * This file contains interface support for the json base abstraction.
 */

#ifndef OCEANBASE_SQL_OB_LOB_BASE
#define OCEANBASE_SQL_OB_LOB_BASE

#include "lib/string/ob_string_buffer.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase {
namespace common {

class ObILobCursor
{
public:
  ObILobCursor() {}
  virtual ~ObILobCursor(){}
  virtual int get_data(ObString &data) const = 0;
  virtual int set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove=false) = 0;
  int set(int64_t offset, const ObString &data) { return set(offset, data.ptr(), data.length()); }

  virtual int get(int64_t offset, int64_t len, ObString &data) const = 0;
  int get_for_write(int64_t offset, int64_t len, ObString &data);
  virtual int64_t get_length() const = 0;

  virtual bool is_full_mode() const = 0;
  virtual int append(const char* buf, int64_t buf_len) = 0;
  virtual int reset_data(const ObString &data) = 0;
  virtual int append(const ObString& data) = 0;

  virtual int reset() = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "base");
    return pos;
  }

public:
  int check_and_get(int64_t offset, int64_t len, const char *&ptr, int64_t &avail_len) const;
  int read_bool(int64_t offset, bool *val) const;
  int read_i8(int64_t offset, int8_t *val) const;
  int read_i16(int64_t offset, int16_t *val) const;
  int read_i32(int64_t offset, int32_t *val) const;
  int read_i64(int64_t offset, int64_t *val) const;
  int read_float(int64_t offset, float *val) const;
  int read_double(int64_t offset, double *val) const;

  int decode_i16(int64_t &offset, int16_t *val) const;
  int decode_vi64(int64_t &offset, int64_t *val) const;
  int deserialize(int64_t &offset, number::ObNumber *number) const;

  int write_i8(int64_t offset, int8_t val);
  int write_i16(int64_t offset, int16_t val);
  int write_i32(int64_t offset, int32_t val);
  int write_i64(int64_t offset, int64_t val);
  int move_data(int64_t dst_offset, int64_t src_offset, int64_t move_len);

protected:
  virtual int get_ptr(int64_t offset, int64_t len, const char *&ptr) const = 0;
  virtual int get_ptr_for_write(int64_t offset, int64_t len, char *&ptr) = 0;
};

class ObLobInRowCursor : public ObILobCursor
{
public:
  ObLobInRowCursor():
    data_()
  {}
  ObLobInRowCursor(ObString& data):
    data_(data)
  {}
  ObLobInRowCursor(const char *data, const int64_t length):
    data_(length, data)
  {}
  int init(const ObString &data) { data_ = data; return  OB_SUCCESS; }
  virtual int get_data(ObString &data) const { data = data_; return OB_SUCCESS; }
  virtual int get(int64_t offset, int64_t len, ObString &data) const { data.assign_ptr(data_.ptr() + offset, len); return OB_SUCCESS; }
  virtual int64_t get_length() const { return data_.length(); }
  virtual bool is_full_mode() const { return true; }
  virtual int set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove) { return OB_NOT_SUPPORTED; }
  virtual int append(const char* buf, int64_t buf_len) { return OB_NOT_SUPPORTED; }
  virtual int append(const ObString& data) { return OB_NOT_SUPPORTED; }
  virtual int reset_data(const ObString &data) { return init(data); }
  virtual int reset() { data_.reset(); return OB_SUCCESS; }
  ObString& data() { return data_; }
  const ObString& data() const { return data_; }
  TO_STRING_KV(K(data_));

protected:
  virtual int get_ptr(int64_t offset, int64_t len, const char *&ptr) const { ptr = data_.ptr() + offset; return OB_SUCCESS; }
  virtual int get_ptr_for_write(int64_t offset, int64_t len, char *&ptr) { ptr = data_.ptr() + offset; return OB_SUCCESS; }
private:
  ObString data_;
};

class ObLobInRowUpdateCursor : public ObILobCursor
{
public:
  ObLobInRowUpdateCursor(ObIAllocator *allocator):
    data_(allocator)
  {}

  ObLobInRowUpdateCursor():
    data_()
  {}
  virtual ~ObLobInRowUpdateCursor(){}

  int init(const ObILobCursor *data);
  virtual int get_data(ObString &data) const { data = data_.string(); return OB_SUCCESS; }

  virtual int set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove=false);
  virtual int get(int64_t offset, int64_t len, ObString &data) const { data.assign_ptr(data_.ptr() + offset, len); return OB_SUCCESS; }
  virtual int64_t get_length() const { return data_.length(); }

  virtual bool is_full_mode() const { return true; }
  virtual int append(const char* buf, int64_t buf_len) { return data_.append(buf, buf_len); }
  virtual int append(const ObString& data) { return append(data.ptr(), data.length()); }
  virtual int reset_data(const ObString &data) { data_.reuse(); return data_.append(data); }
  virtual int reset() { data_.reset(); return OB_SUCCESS; }
  TO_STRING_KV(K(data_));

protected:
  virtual int get_ptr(int64_t offset, int64_t len, const char *&ptr) const { ptr = data_.ptr() + offset; return OB_SUCCESS; }
  virtual int get_ptr_for_write(int64_t offset, int64_t len, char *&ptr) { ptr = data_.ptr() + offset; return OB_SUCCESS; }

private:
  ObStringBuffer data_;
};


} // common
} // oceanbase
#endif  // OCEANBASE_SQL_OB_LOB_BASE