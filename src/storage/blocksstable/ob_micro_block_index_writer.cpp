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

#include "ob_micro_block_index_writer.h"
#include "ob_block_sstable_struct.h"
#include "common/row/ob_row.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {
ObMicroBlockIndexWriter::ObMicroBlockIndexWriter()
    : ObCommonMicroBlockIndexWriter<4L>(), is_multi_version_minor_merge_(false)
{}

void ObMicroBlockIndexWriter::reset()
{
  BaseWriter::reset();
  is_multi_version_minor_merge_ = false;
}

int ObMicroBlockIndexWriter::init(int64_t max_buffer_size, bool is_multi_version_minor_merge)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMicroBlockIndexWriter has been inited twice.", K(ret));
  } else if (OB_FAIL(BaseWriter::init(max_buffer_size))) {
    STORAGE_LOG(WARN, "Failed to init ObMicroBlockIndexWriter.", K(ret));
  } else {
    is_multi_version_minor_merge_ = is_multi_version_minor_merge;
  }

  return ret;
}

int ObMicroBlockIndexWriter::add_entry(
    const ObString& rowkey, const int64_t data_offset, bool can_mark_deletion, const int32_t delta)
{
  int ret = OB_SUCCESS;
  int32_t endkey_offset = static_cast<int32_t>(buffer_[ENDKEY_BUFFER_IDX].length());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMicroBlockIndexWriter has not been inited.", K(ret));
  } else if (rowkey.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "rowkey was empty", K(rowkey), K(ret));
  } else if (data_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "input data_offset is invalid", K(static_cast<int32_t>(data_offset)), K(ret));
  } else if (OB_FAIL(buffer_[INDEX_BUFFER_IDX].write(static_cast<int32_t>(data_offset)))) {
    STORAGE_LOG(WARN, "index buffer fail to write data_offset.", K(ret), K(data_offset));
  } else if (OB_FAIL(buffer_[INDEX_BUFFER_IDX].write(endkey_offset))) {
    STORAGE_LOG(WARN, "index buffer fail to write endkey_offset.", K(ret), K(endkey_offset));
  } else if (OB_FAIL(buffer_[ENDKEY_BUFFER_IDX].write(rowkey.ptr(), rowkey.length()))) {
    STORAGE_LOG(WARN, "data buffer fail to writer rowkey.", K(ret), K(rowkey));
  } else if (is_multi_version_minor_merge_ &&
             OB_FAIL(buffer_[MARK_DELETE_BUFFER_IDX].write(static_cast<uint8_t>(can_mark_deletion)))) {
    STORAGE_LOG(WARN, "fail to write mark deletion", K(ret), K(can_mark_deletion));
  } else if (is_multi_version_minor_merge_ && OB_FAIL(buffer_[DELTA_BUFFER_IDX].write(delta))) {
    STORAGE_LOG(WARN, "failed to write delta", K(ret));
  } else {
    ++micro_block_cnt_;
  }
  return ret;
}

int ObMicroBlockIndexWriter::get_last_rowkey(ObString& rowkey)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter& index_buffer = buffer_[INDEX_BUFFER_IDX];
  ObSelfBufferWriter& endkey_buffer = buffer_[ENDKEY_BUFFER_IDX];
  char* last_endkey_ptr = index_buffer.current() - sizeof(int32_t);
  int32_t last_endkey_offset = *reinterpret_cast<int32_t*>(last_endkey_ptr);
  int32_t last_endkey_size = static_cast<int32_t>(endkey_buffer.length() - last_endkey_offset);
  rowkey.assign(endkey_buffer.data() + last_endkey_offset, last_endkey_size);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMicroBlockIndexWriter has not been inited.", K(ret));
  } else if (rowkey.empty()) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR,
        "rowkey length must not 0",
        K(last_endkey_size),
        K(endkey_buffer.length()),
        K(last_endkey_offset),
        K(OB_P(index_buffer.current())),
        K(OB_P(index_buffer.data())),
        K(index_buffer.pos()),
        K(index_buffer.capacity()),
        K(rowkey),
        K(ret));
  }
  return ret;
}

int ObMicroBlockIndexWriter::add_last_entry(const int64_t data_offset)
{
  int ret = OB_SUCCESS;
  ObSelfBufferWriter& index_buffer = buffer_[INDEX_BUFFER_IDX];
  ObSelfBufferWriter& endkey_buffer = buffer_[ENDKEY_BUFFER_IDX];
  int32_t endkey_offset = static_cast<int32_t>(endkey_buffer.length());

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMicroBlockIndexWriter has not been inited.", K(ret));
  } else if (data_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "data offset is invalid", K(ret), K(data_offset));
  } else if (OB_FAIL(index_buffer.write(static_cast<int32_t>(data_offset)))) {
    STORAGE_LOG(WARN, "index buffer writer fail to write data_offset.", K(ret), K(data_offset));
  } else if (OB_FAIL(index_buffer.write(endkey_offset))) {
    STORAGE_LOG(WARN, "index buffer fail to write endkey_offset.", K(ret), K(endkey_offset));
  }
  return ret;
}

int ObMicroBlockIndexWriter::merge(const int64_t data_end_offset, const ObMicroBlockIndexWriter& writer)
{
  int ret = OB_SUCCESS;
  int32_t data_offset = 0;
  int32_t endkey_offset = 0;
  const char* data_ptr = NULL;
  const char* endkey_ptr = NULL;
  ObSelfBufferWriter& index_buffer = buffer_[INDEX_BUFFER_IDX];
  ObSelfBufferWriter& endkey_buffer = buffer_[ENDKEY_BUFFER_IDX];
  const ObSelfBufferWriter& other_index_buffer = writer.buffer_[INDEX_BUFFER_IDX];
  const ObSelfBufferWriter& other_endkey_buffer = writer.buffer_[ENDKEY_BUFFER_IDX];
  const ObSelfBufferWriter& other_mark_buffer = writer.buffer_[MARK_DELETE_BUFFER_IDX];
  const ObSelfBufferWriter& other_delta_buffer = writer.buffer_[DELTA_BUFFER_IDX];

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMicroBlockIndexWriter has not been inited.", K(ret));
  } else if (0 == other_endkey_buffer.length()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "writer.endkey_buffer_.length must not 0", K(ret));
  } else if (data_end_offset < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "data_end_offset is invalid", K(ret), K(data_end_offset));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < writer.get_block_count(); ++i) {
    data_ptr = other_index_buffer.data() + i * INDEX_ENTRY_SIZE;
    endkey_ptr = other_index_buffer.data() + i * INDEX_ENTRY_SIZE + sizeof(int32_t);
    data_offset = *reinterpret_cast<const int32_t*>(data_ptr) + static_cast<int32_t>(data_end_offset);
    endkey_offset = *reinterpret_cast<const int32_t*>(endkey_ptr) + static_cast<int32_t>(endkey_buffer.length());
    if (OB_FAIL(index_buffer.write(data_offset))) {
      STORAGE_LOG(WARN, "index buffer fail to write data offset.", K(ret), K(data_offset));
    } else if (OB_FAIL(index_buffer.write(endkey_offset))) {
      STORAGE_LOG(WARN, "index buffer fail to write endkey offset.", K(ret), K(endkey_offset));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(endkey_buffer.write(other_endkey_buffer.data(), other_endkey_buffer.length()))) {
      STORAGE_LOG(WARN, "endkey buffer fail to write other endkey buffer.", K(ret), K(other_endkey_buffer.length()));
    } else if (OB_FAIL(buffer_[MARK_DELETE_BUFFER_IDX].write(other_mark_buffer.data(), other_mark_buffer.length()))) {
      STORAGE_LOG(WARN,
          "mark deletion buffer fail to write other mark deletion buffer.",
          K(ret),
          K(other_mark_buffer.length()));
    } else if (OB_FAIL(buffer_[DELTA_BUFFER_IDX].write(other_delta_buffer.data(), other_delta_buffer.length()))) {
      STORAGE_LOG(WARN, "failed to write delta buffer", K(ret), K(other_delta_buffer.length()));
    }
  }
  return ret;
}

ObLobMicroBlockIndexWriter::ObLobMicroBlockIndexWriter() : ObCommonMicroBlockIndexWriter<2L>()
{}

void ObLobMicroBlockIndexWriter::reset()
{
  BaseWriter::reset();
}

int ObLobMicroBlockIndexWriter::add_entry(const int64_t data_offset, const int64_t byte_size, const int64_t char_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobMicroBlockIndexWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(data_offset < 0 || byte_size <= 0 || char_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_offset), K(byte_size), K(char_size));
  } else if (OB_FAIL(buffer_[INDEX_BUFFER_IDX].write(static_cast<int32_t>(data_offset)))) {
    STORAGE_LOG(WARN, "fail to write data offset", K(ret));
  } else if (OB_FAIL(buffer_[SIZE_BUFFER_IDX].write(byte_size))) {
    STORAGE_LOG(WARN, "fail to write byte size", K(ret));
  } else if (OB_FAIL(buffer_[SIZE_BUFFER_IDX].write(char_size))) {
    STORAGE_LOG(WARN, "fail to write char len", K(ret));
  }
  return ret;
}

int ObLobMicroBlockIndexWriter::add_last_entry(const int64_t data_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobMicroBlockIndexWriter has not been inited", K(ret));
  } else if (OB_UNLIKELY(data_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(data_offset));
  } else if (OB_FAIL(buffer_[INDEX_BUFFER_IDX].write(data_offset))) {
    STORAGE_LOG(WARN, "fail to write data offset", K(ret));
  }
  return ret;
}

int64_t ObLobMicroBlockIndexWriter::get_block_size() const
{
  return buffer_[INDEX_BUFFER_IDX].length() + buffer_[SIZE_BUFFER_IDX].length() + 2 * sizeof(int32_t);
}

}  // end namespace blocksstable
}  // end namespace oceanbase
