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

#ifndef OCEANBASE_ENCODING_OB_INTEGER_ARRAY_H_
#define OCEANBASE_ENCODING_OB_INTEGER_ARRAY_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace blocksstable
{

// bare array, caller's responsibility to make sure no overflow
class ObIIntegerArray
{
public:
  virtual int64_t at(const int64_t idx) const = 0;
  virtual void set(const int64_t idx, int64_t value) = 0;
  virtual int64_t lower_bound(const int64_t begin_idx,
      const int64_t end_idx, const int64_t key) const = 0;
  virtual int64_t upper_bound(const int64_t begin_idx,
      const int64_t end_idx, const int64_t key) const = 0;

  virtual void update_pointer(int64_t offset) = 0;
};

template <typename T>
class ObIntegerArray final : public ObIIntegerArray
{
public:
  ObIntegerArray(void *data) : data_(static_cast<T *>(data)) {}
  virtual int64_t at(const int64_t idx) const { return static_cast<int64_t>(data_[idx]); }
  virtual void set(const int64_t idx, int64_t value) { data_[idx] = static_cast<T>(value); }
  virtual int64_t lower_bound(const int64_t begin_idx,
      const int64_t end_idx, const int64_t key) const
  {
    T *pos = std::lower_bound(&data_[begin_idx], &data_[end_idx], key);
    return static_cast<int64_t>(pos - data_);
  }
  virtual int64_t upper_bound(const int64_t begin_idx,
      const int64_t end_idx, const int64_t key) const
  {
    T *pos = std::upper_bound(&data_[begin_idx], &data_[end_idx], key);
    return static_cast<int64_t>(pos - data_);
  }
  virtual void update_pointer(int64_t offset) override
  {
    if (NULL != data_) {
      data_ = reinterpret_cast<T *>(reinterpret_cast<intptr_t>(data_) + offset);
    }
  }
  virtual const char *get_data() const { return reinterpret_cast<const char *>(data_); }
private:
  T *data_;
};

class ObIntegerArrayGenerator final
{
public:
  ObIntegerArrayGenerator() {}

  OB_INLINE int init(char *data, const int64_t byte);
  OB_INLINE int init(const char *data, const int64_t byte)
  { return init(const_cast<char *>(data), byte); }
  ObIIntegerArray &get_array() { return *reinterpret_cast<ObIIntegerArray *>(data_); }
  ObIIntegerArray &get_array() const
  {
    return *reinterpret_cast<ObIIntegerArray *>(const_cast<char *>(data_));
  }
  const char *get_data() const { return reinterpret_cast<const char *>(&data_); }
private:
  char data_[sizeof(ObIntegerArray<uint64_t>)];
};

OB_INLINE int ObIntegerArrayGenerator::init(char *data, const int64_t byte)
{
  int ret = common::OB_SUCCESS;
  if (NULL == data || byte <= 0 || byte > sizeof(int64_t)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(data), K(byte));
  } else {
    switch (byte) {
      case 1: {
        new (data_) ObIntegerArray<uint8_t>(data);
        break;
      }
      case 2: {
        new (data_) ObIntegerArray<uint16_t>(data);
        break;
      }
      case 4: {
        new (data_) ObIntegerArray<uint32_t>(data);
        break;
      }
      case 8: {
        new (data_) ObIntegerArray<uint64_t>(data);
        break;
      }
      default:
        ret = common::OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "invalid integer byte", K(ret), K(byte));
    }
  }
  return ret;
}

typedef int64_t (*INT_ARRAY_AT)(const void *array, const int64_t idx);
typedef void (*INT_ARRAY_SET)(void *array, const int64_t idx, const int64_t value);
typedef int64_t (*INT_ARRAY_LOWER_BOUND)(const void *array,
    const int64_t begin, const int64_t end, const int64_t val);
typedef int64_t (*INT_ARRAY_UPPER_BOUND)(const void *array,
    const int64_t begin, const int64_t end, const int64_t val);

struct ObIntArrayFuncTable
{
  INT_ARRAY_AT at_;
  INT_ARRAY_SET set_;
  INT_ARRAY_LOWER_BOUND lower_bound_;
  INT_ARRAY_UPPER_BOUND upper_bound_;

  template <typename T>
  static int64_t at(const void *array, const int64_t idx)
  {
    return static_cast<int64_t>(static_cast<const T *>(array)[idx]);
  }

  template <typename T>
  static void set(void *array, const int64_t idx, const int64_t value)
  {
    static_cast<T *>(array)[idx] = static_cast<T>(value);
  }

  template <typename T>
  static int64_t lower_bound(const void *array, const int64_t begin,
      const int64_t end, const int64_t val)
  {
    const T *data = static_cast<const T *>(array);
    const T *pos = std::lower_bound(&data[begin], &data[end], val);
    return static_cast<int64_t>(pos - data);
  }

  template <typename T>
  static int64_t upper_bound(const void *array, const int64_t begin,
      const int64_t end, const int64_t val)
  {
    const T *data = static_cast<const T *>(array);
    const T *pos = std::upper_bound(&data[begin], &data[end], val);
    return static_cast<int64_t>(pos - data);
  }

  OB_INLINE static const ObIntArrayFuncTable *instance()
  {
    const static ObIntArrayFuncTable tables[sizeof(uint64_t) + 1] = {
      { NULL, NULL, NULL, NULL },
      { &at<uint8_t>, &set<uint8_t>, &lower_bound<uint8_t>, &upper_bound<uint8_t> },
      { &at<uint16_t>, &set<uint16_t>, &lower_bound<uint16_t>, &upper_bound<uint16_t> },
      { NULL, NULL, NULL, NULL },
      { &at<uint32_t>, &set<uint32_t>, &lower_bound<uint32_t>, &upper_bound<uint32_t> },
      { NULL, NULL, NULL, NULL },
      { NULL, NULL, NULL, NULL },
      { NULL, NULL, NULL, NULL },
      { &at<uint64_t>, &set<uint64_t>, &lower_bound<uint64_t>, &upper_bound<uint64_t> },
    };
    return tables;
  }

  OB_INLINE static const ObIntArrayFuncTable &instance(int64_t idx)
  {
    return instance()[idx];
  }
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_INTEGER_ARRAY_H_
