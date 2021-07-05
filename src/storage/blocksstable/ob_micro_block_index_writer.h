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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_MICRO_BLOCK_INDEX_WRITER_H_
#include "lib/string/ob_string.h"
#include "ob_data_buffer.h"
#include "ob_row_writer.h"

namespace oceanbase {
namespace common {
class ObStoreRowkey;
class ObString;
}  // namespace common
namespace blocksstable {

template <int64_t BUFFER_COUNT>
class ObCommonMicroBlockIndexWriter {
public:
  ObCommonMicroBlockIndexWriter();
  virtual ~ObCommonMicroBlockIndexWriter();
  virtual int init(const int64_t max_buffer_size);
  void reset();
  void reuse();
  template <typename T>
  int write(const int64_t buffer_idx, const T& value);
  int write(const int64_t buffer_idx, const char* buf, const int64_t buf_len);

protected:
  DISALLOW_COPY_AND_ASSIGN(ObCommonMicroBlockIndexWriter);
  bool is_inited_;
  ObSelfBufferWriter buffer_[BUFFER_COUNT];
  int64_t micro_block_cnt_;
};

template <int64_t BUFFER_COUNT>
ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::ObCommonMicroBlockIndexWriter()
    : is_inited_(false), buffer_(), micro_block_cnt_(0)
{
  for (int64_t i = 0; i < BUFFER_COUNT; ++i) {
    buffer_[i].set_prop("MicrBlocIndex", false /*aligned*/);
  }
}

template <int64_t BUFFER_COUNT>
ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::~ObCommonMicroBlockIndexWriter()
{}

template <int64_t BUFFER_COUNT>
int ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::init(const int64_t max_buffer_size)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(max_buffer_size <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(max_buffer_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < BUFFER_COUNT; ++i) {
      if (OB_FAIL(buffer_[i].ensure_space(max_buffer_size))) {
        STORAGE_LOG(WARN, "fail to ensure space for buffer", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

template <int64_t BUFFER_COUNT>
void ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::reuse()
{
  for (int64_t i = 0; i < BUFFER_COUNT; ++i) {
    buffer_[i].reuse();
  }
  micro_block_cnt_ = 0;
}

template <int64_t BUFFER_COUNT>
void ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::reset()
{
  for (int64_t i = 0; i < BUFFER_COUNT; ++i) {
    buffer_[i].reset();
  }
  micro_block_cnt_ = 0;
  is_inited_ = false;
}

template <int64_t BUFFER_COUNT>
template <typename T>
int ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::write(const int64_t buffer_idx, const T& value)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCommonMicroBlockIndexWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(buffer_idx < 0 || buffer_idx >= BUFFER_COUNT)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(buffer_idx));
  } else if (OB_FAIL(buffer_[buffer_idx].write(value))) {
    STORAGE_LOG(WARN, "fail to write to buffer", K(ret), K(buffer_idx));
  }
  return ret;
}

template <int64_t BUFFER_COUNT>
int ObCommonMicroBlockIndexWriter<BUFFER_COUNT>::write(const int64_t buffer_idx, const char* buf, const int64_t buf_len)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObCommonMicroBlockIndexWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(buffer_idx < 0 || buffer_idx >= BUFFER_COUNT || NULL == buf || buf_len <= 0)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(buffer_idx), KP(buf), K(buf_len));
  } else if (OB_FAIL(buffer_[buffer_idx].write(buf, buf_len))) {
    STORAGE_LOG(WARN, "fail to write buffer", K(ret), K(buffer_idx));
  }
  return ret;
}

class ObMicroBlockIndexWriter : public ObCommonMicroBlockIndexWriter<4L> {
public:
  static const int64_t INDEX_ENTRY_SIZE = sizeof(int32_t) * 2;
  static const int64_t MARK_DELETION_ENRTRY_SIZE = sizeof(uint8_t);
  static const int64_t DELTA_ENTRY_SIZE = sizeof(int32_t);
  static const int64_t DEFAULT_ENDKEY_BUFFER_SIZE = 32 * 1024;
  static const int64_t DEFAULT_INDEX_BUFFFER_SIZE = 8 * 1024;
  static const int64_t DEFAULT_MARKDELETION_BUFFER_SIZE = 256;
  static const int64_t DEFAULT_DELTA_BUFFER_SIZE = 2 * 1024;
  ObMicroBlockIndexWriter();
  virtual ~ObMicroBlockIndexWriter()
  {}

  void reset();
  int init(const int64_t max_buffer_size, bool is_multi_version_minor_merge);
  int add_entry(const common::ObString& rowkey, const int64_t data_offset, bool can_mark_deletion, const int32_t delta);
  int get_last_rowkey(common::ObString& rowkey);
  int add_last_entry(const int64_t data_offset);
  int merge(const int64_t data_end_offset, const ObMicroBlockIndexWriter& writer);

  inline int64_t get_block_count() const
  {
    return micro_block_cnt_;
  }
  inline const ObSelfBufferWriter& get_index() const
  {
    return buffer_[INDEX_BUFFER_IDX];
  }
  inline const ObSelfBufferWriter& get_data() const
  {
    return buffer_[ENDKEY_BUFFER_IDX];
  }
  inline const ObSelfBufferWriter& get_mark_deletion() const
  {
    return buffer_[MARK_DELETE_BUFFER_IDX];
  }
  inline const ObSelfBufferWriter& get_delta() const
  {
    return buffer_[DELTA_BUFFER_IDX];
  }
  inline int64_t get_block_size() const;
  static int64_t get_entry_size(bool is_multi_version_minor_merge);

protected:
  bool is_multi_version_minor_merge_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObMicroBlockIndexWriter);
  typedef ObCommonMicroBlockIndexWriter<4L> BaseWriter;
  static const int64_t ENDKEY_BUFFER_IDX = 0;
  static const int64_t INDEX_BUFFER_IDX = 1;
  static const int64_t MARK_DELETE_BUFFER_IDX = 2;
  static const int64_t DELTA_BUFFER_IDX = 3;
};

inline int64_t ObMicroBlockIndexWriter::get_block_size() const
{
  return get_data().length() + get_index().length() + get_mark_deletion().length() + get_delta().length() +
         INDEX_ENTRY_SIZE;
}
inline int64_t ObMicroBlockIndexWriter::get_entry_size(bool is_multi_version_minor_merge)
{
  int64_t size = INDEX_ENTRY_SIZE;
  if (is_multi_version_minor_merge) {
    size += MARK_DELETION_ENRTRY_SIZE + DELTA_ENTRY_SIZE;
  }
  return size;
}

class ObLobMicroBlockIndexWriter : public ObCommonMicroBlockIndexWriter<2L> {
public:
  static const int64_t INDEX_ENTRY_SIZE = 3 * sizeof(int32_t) + sizeof(int64_t) * 2;
  ObLobMicroBlockIndexWriter();
  virtual ~ObLobMicroBlockIndexWriter() = default;
  void reset();
  int add_entry(const int64_t data_offset, const int64_t byte_size, const int64_t char_size);
  int add_last_entry(const int64_t data_offset);
  ObSelfBufferWriter& get_index()
  {
    return buffer_[INDEX_BUFFER_IDX];
  }
  ObSelfBufferWriter& get_size_array()
  {
    return buffer_[SIZE_BUFFER_IDX];
  }
  int64_t get_block_size() const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobMicroBlockIndexWriter);
  typedef ObCommonMicroBlockIndexWriter<2L> BaseWriter;
  static const int64_t INDEX_BUFFER_IDX = 0;
  static const int64_t SIZE_BUFFER_IDX = 1;
};

}  // end namespace blocksstable
}  // end namespace oceanbase
#endif
