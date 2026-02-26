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

#ifndef OCEANBASE_ENCODING_OB_ROW_INDEX_H_
#define OCEANBASE_ENCODING_OB_ROW_INDEX_H_

#include "ob_integer_array.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

class ObIRowIndex
{
public:
  virtual int get(const int64_t row_id, const char *&data, int64_t &len) const = 0;
  // Batch read row data in row_datas, row_len in datums.len_ for batch decode
  virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      ObDatum *datums) const = 0;
  virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      uint32_t *row_lens) const = 0;
};

class ObVarRowIndex : public ObIRowIndex
{
public:
  ObVarRowIndex() : data_(NULL), row_cnt_(0) {};
  virtual ~ObVarRowIndex() {}
  // can init twice
  inline int init(const char *data, const int64_t len,
      const int64_t row_cnt, const int64_t index_byte);
  inline virtual int get(const int64_t row_id, const char *&data, int64_t &len) const;
  inline virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      ObDatum *datums) const;
  inline virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      uint32_t *row_lens) const override;
  inline const char *get_data() const { return data_; }
  inline const char *get_index_data() const { return index_.get_data(); }
private:
  const char *data_;
  int64_t row_cnt_;
  ObIntegerArrayGenerator index_;
};

class ObFixRowIndex : public ObIRowIndex
{
public:
  ObFixRowIndex() : data_(NULL), row_size_(0), row_cnt_(0) {}
  virtual ~ObFixRowIndex() {}
  // can init twice
  inline int init(const char *data, const int64_t len, const int64_t row_cnt);
  inline virtual int get(const int64_t row_id, const char *&data, int64_t &len) const;
  inline virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      ObDatum *datums) const;
  inline virtual int batch_get(
      const int32_t *row_ids,
      const int64_t row_cap,
      const bool has_ext,
      const char **row_datas,
      uint32_t *row_lens) const override;
private:
  const char *data_;
  int64_t row_size_;
  int64_t row_cnt_;
};

inline int ObVarRowIndex::init(const char *data, const int64_t len,
    const int64_t row_cnt, const int64_t index_byte )
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == data || row_cnt <= 0
      || index_byte < sizeof(char) || index_byte >= sizeof(int64_t)
      || len < (row_cnt + 1) * index_byte)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(data), K(len), K(row_cnt), K(index_byte));
  } else if (OB_FAIL(index_.init(data + len - index_byte * (row_cnt + 1), index_byte))) {
    STORAGE_LOG(WARN, "init index array failed", K(ret));
  } else {
    data_ = data;
    row_cnt_ = row_cnt;
  }
  return ret;
}

inline int ObVarRowIndex::get(const int64_t row_id, const char *&data, int64_t &len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), KP_(data));
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid row id", K(ret), K(row_id), K_(row_cnt));
  } else {
    uint64_t offset = index_.get_array().at(row_id);
    data = data_ + offset;
    len = index_.get_array().at(row_id + 1) - offset;
  }
  return ret;
}

inline int ObVarRowIndex::batch_get(
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool has_ext,
    const char **row_datas,
    ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret), KP_(data));
  } else if (OB_ISNULL(row_ids) || OB_ISNULL(datums) || OB_ISNULL(row_datas)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(row_ids), KP(row_datas), KP(datums));
  }
  // Not check if row_id is valid for performance
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    if (has_ext && datums[i].is_null()) {
      // skip
    } else {
      uint64_t offset = index_.get_array().at(row_ids[i]);
      row_datas[i] = data_ + offset;
      datums[i].pack_ = static_cast<int32_t>(index_.get_array().at(row_ids[i] + 1) - offset);
    }
  }
  return ret;
}

inline int ObVarRowIndex::batch_get(
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool has_ext,
    const char **row_datas,
    uint32_t *row_lens) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret), KP_(data));
  } else if (OB_ISNULL(row_ids) || OB_ISNULL(row_lens) || OB_ISNULL(row_datas)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(row_ids), KP(row_datas), KP(row_lens));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    uint64_t offset = index_.get_array().at(row_ids[i]);
    row_datas[i] = data_ + offset;
    row_lens[i] = static_cast<int32_t>(index_.get_array().at(row_ids[i] + 1) - offset);
  }
  return ret;
}

inline int ObFixRowIndex::init(const char *data, const int64_t len, const int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  // len might be 0, when no var column and no row header
  if (OB_UNLIKELY(NULL == data || len < 0 || row_cnt <= 0 || 0 != len % row_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(data), K(len), K(row_cnt));
  } else {
    data_ = data;
    row_size_ = len / row_cnt;
    row_cnt_ = row_cnt;
  }
  return ret;
}

inline int ObFixRowIndex::get(const int64_t row_id, const char *&data, int64_t &len) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret), KP_(data));
  } else if (OB_UNLIKELY(row_id < 0 || row_id >= row_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(row_id), K_(row_cnt));
  } else {
    len = row_size_;
    data = data_ + row_id * row_size_;
  }
  return ret;
}

inline int ObFixRowIndex::batch_get(
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool has_ext,
    const char **row_datas,
    ObDatum *datums) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret), KP_(data));
  } else if (OB_ISNULL(row_ids) || OB_ISNULL(datums) || OB_ISNULL(row_datas)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(row_ids), KP(row_datas), KP(datums));
  }
  // Not check row_id valid for performance
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    if (has_ext && datums[i].is_null()) {
      // skip
    } else {
      datums[i].pack_ = static_cast<int32_t>(row_size_);
      row_datas[i] = data_ + row_ids[i] * row_size_;
    }
  }
  return ret;
}

inline int ObFixRowIndex::batch_get(
    const int32_t *row_ids,
    const int64_t row_cap,
    const bool has_ext,
    const char **row_datas,
    uint32_t *row_lens) const
{
   int ret = OB_SUCCESS;
  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "Not inited", K(ret), KP_(data));
  } else if (OB_ISNULL(row_ids) || OB_ISNULL(row_lens) || OB_ISNULL(row_datas)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument", K(ret), KP(row_ids), KP(row_datas), KP(row_lens));
  }
  // Not check row_id valid for performance
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_lens[i] = static_cast<int32_t>(row_size_);
    row_datas[i] = data_ + row_ids[i] * row_size_;
  }
  return ret;
}

}

}



#endif // OCEANBASE_ENCODING_OB_ROW_INDEX_H_
