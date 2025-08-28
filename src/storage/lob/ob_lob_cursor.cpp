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
#include "ob_lob_cursor.h"
#include "ob_lob_manager.h"

namespace oceanbase
{
namespace storage
{

/********** ObLobCursor ****************/
ObLobCursor::~ObLobCursor()
{
  // meta_cache_.destroy();
  // modified_metas_.destroy(); 
  if (nullptr != param_) {
    param_->~ObLobAccessParam();
    param_ = nullptr;
  }
  if (nullptr != partial_data_) {
    partial_data_->~ObLobPartialData();
    partial_data_ = nullptr;
  }
}
int ObLobCursor::init(ObIAllocator *allocator, ObLobAccessParam* param, ObLobPartialData *partial_data, ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  param_ = param;
  allocator_ = allocator;
  partial_data_ = partial_data;
  update_buffer_.set_allocator(allocator_);
  if (OB_ISNULL(partial_data)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data is null", KR(ret));
  } else if (OB_FAIL(partial_data->get_ori_data_length(ori_data_length_))) {
    LOG_WARN("get_ori_data_length fail", KR(ret));
  } else if (partial_data->is_full_mode()) {
    if (OB_FAIL(init_full(allocator, partial_data))){
      LOG_WARN("init_full fail", KR(ret));
    }
  } else if (OB_FAIL(lob_meta_mngr->open(*param_, &getter_))) {
    LOG_WARN("ObLobMetaSingleGetter open fail", K(ret));
  }
  return ret;
}

int ObLobCursor::init_full(ObIAllocator *allocator, ObLobPartialData *partial_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_buffer_.append(partial_data->data_[0].data_))) {
    LOG_WARN("append data to update buffer fail", KR(ret), K(partial_data->data_.count()));
  } else {
    partial_data_->data_[0].data_ = update_buffer_.string();
    is_full_mode_ = true;
  }
  return ret;
}

int ObLobCursor::get_ptr(int64_t offset, int64_t len, const char *&ptr)
{
  INIT_SUCC(ret);
  ObString data;
  int64_t start_offset = offset;
  int64_t end_offset = offset + len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  if (start_chunk_pos != end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else if (OB_FAIL(get_chunk_data(start_chunk_pos, data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else if (data.empty() || data.length() < start_offset - get_chunk_offset(start_chunk_pos) + len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data not enough", KR(ret), K(offset), K(len), K(start_offset), "data_len", data.length());
  } else {
    ptr = data.ptr() + offset - get_chunk_offset(start_chunk_pos);
  }
  return ret;
}

int ObLobCursor::get_ptr_for_write(int64_t offset, int64_t len, char *&ptr)
{
  INIT_SUCC(ret);
  ObString data;
  int64_t start_offset = offset;
  int64_t end_offset = offset + len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  if (start_chunk_pos != end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
  } else {
    for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      int chunk_idx = -1;
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i), K(start_chunk_pos), K(end_chunk_pos));
      } else if (OB_FAIL(record_chunk_old_data(chunk_idx))) {
        LOG_WARN("record_chunk_old_data fail", KR(ret), K(i));
      }
    }
    if(OB_FAIL(ret)) {
    } else if (OB_FAIL(get_chunk_data(start_chunk_pos, data))) {
      LOG_WARN("get_chunk_data fail", KR(ret), K(start_chunk_pos), K(end_chunk_pos), K(offset), K(len));
    } else if (data.empty() || data.length() < start_offset - get_chunk_offset(start_chunk_pos) + len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data not enough", KR(ret), K(offset), K(len), K(start_offset), "data_len", data.length());
    } else {
      ptr = data.ptr() + offset - get_chunk_offset(start_chunk_pos);
    } 
  }
  return ret;
}

int ObLobCursor::get_chunk_data_start_pos(const int cur_chunk_pos, int &start_pos)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  if (OB_FAIL(get_chunk_data(cur_chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(cur_chunk_pos));
  } else {
    start_pos = get_chunk_pos(chunk_index->offset_ - chunk_index->pos_);
  }
  return ret;
}

int ObLobCursor::get_chunk_data_end_pos(const int cur_chunk_pos, int &end_pos)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  if (OB_FAIL(get_chunk_data(cur_chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(cur_chunk_pos));
  } else {
    end_pos = get_chunk_pos(chunk_index->offset_ - chunk_index->pos_ + chunk_data->data_.length() - 1);
  }
  return ret;
}

int ObLobCursor::merge_chunk_data(int start_chunk_pos, int end_chunk_pos)
{
  INIT_SUCC(ret);
  bool need_merge = false;
  ObSEArray<int, 10> chunk_idx_array;
  // get the fisrt and last chunk pos of chunk data that start_chunk_pos use
  if (OB_FAIL(get_chunk_data_start_pos(start_chunk_pos, start_chunk_pos))) {
    LOG_WARN("get_chunk_data_start_pos fail", KR(ret), K(start_chunk_pos));
  } else if (OB_FAIL(get_chunk_data_end_pos(end_chunk_pos, end_chunk_pos))) {
    LOG_WARN("get_chunk_data_start_pos fail", KR(ret), K(end_chunk_pos));
  }
  // get chunk_index data array index
  for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
    const ObLobChunkIndex *chunk_index = nullptr;
    const ObLobChunkData *chunk_data = nullptr;
    if (OB_FAIL(get_chunk_data(i, chunk_index, chunk_data))) {
      LOG_WARN("get_chunk_data fail", KR(ret), K(i), K(start_chunk_pos), K(end_chunk_pos));
    // some chunk share same data area, so no need push again
    // and it will only be shared with adjacent chunks, so only need to check the last
    } else if (! chunk_idx_array.empty() && chunk_idx_array[chunk_idx_array.count() - 1] == chunk_index->data_idx_) {// skip
    } else if (OB_FAIL(chunk_idx_array.push_back(chunk_index->data_idx_))) {
      LOG_WARN("push_back idx fail", KR(ret), K(i));
    }
  }
  int64_t merge_len = 0;
  bool use_update_buffer = false;
  if (OB_SUCC(ret)) {
    // should merge if has multi data area
    need_merge = chunk_idx_array.count() != 1;
    for (int i = 0; OB_SUCC(ret) && need_merge && i < chunk_idx_array.count(); ++i) {
      const ObLobChunkData &chunk_data = partial_data_->data_[chunk_idx_array[i]];
      merge_len += chunk_data.data_.length();

      if (! update_buffer_.empty() && update_buffer_.ptr() == chunk_data.data_.ptr()) {
        // if update_buffer_ is uesed, last chunk should have some pointer with update_buffer_
        if (i != chunk_idx_array.count() - 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid chunk data", KR(ret), K(i), K(chunk_idx_array), K(start_chunk_pos), K(end_chunk_pos));
        } else {
          use_update_buffer = true;
          LOG_DEBUG("set use update buffer", K(i), K(chunk_idx_array.count()));
        }
      }
    }
  }

  // get merge buffer ptr
  char *buf = nullptr;
  if (OB_FAIL(ret) || ! need_merge) {
  } else if (use_update_buffer) {
    // old data also record in chunk data, so there just reset update_buffer_ but not free
    // and reserve new buffer for merge
    ObString old_data;
    if (OB_FAIL(update_buffer_.get_result_string(old_data))) {
      LOG_WARN("alloc fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array), K(update_buffer_));
    } else if (OB_FAIL(update_buffer_.reserve(merge_len))) {
      LOG_WARN("reserve buffer fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array), K(update_buffer_));
    } else {
      buf = update_buffer_.ptr();
      update_buffer_.set_length(merge_len);
    }
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_->alloc(merge_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", KR(ret), K(merge_len), K(start_chunk_pos), K(end_chunk_pos), K(chunk_idx_array));
  }
  
  // do merge if need
  if (OB_FAIL(ret) || ! need_merge) {
  } else {
    int new_chunk_data_idx = chunk_idx_array[0];
    // copy data from old area to merge area
    int64_t pos = 0;
    for (int i = 0; OB_SUCC(ret) && i < chunk_idx_array.count(); ++i) {
      ObLobChunkData &chunk_data = partial_data_->data_[chunk_idx_array[i]];
      MEMCPY(buf + pos, chunk_data.data_.ptr(), chunk_data.data_.length());
      pos += chunk_data.data_.length();
      allocator_->free(chunk_data.data_.ptr());
      chunk_data.data_.reset();
    }
    if (OB_SUCC(ret) && pos != merge_len) { 
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data merge len incorrect", KR(ret), K(pos), K(merge_len));
    }
    // update chunk index offset info
    pos = 0;
    bool append_chunk_set = false;
    for (int i = start_chunk_pos; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      int chunk_idx = -1;
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i));
      } else if (append_chunk_set && chunk_index(chunk_idx).is_add_) {
      } else {
        chunk_index(chunk_idx).pos_ = pos;
        chunk_index(chunk_idx).data_idx_ = new_chunk_data_idx;
        pos += chunk_index(chunk_idx).byte_len_;
        if (chunk_index(chunk_idx).is_add_) append_chunk_set = true;
      }
    }
    // update chunk data pointer
    if (OB_SUCC(ret)) {
      partial_data_->data_[new_chunk_data_idx].data_.assign_ptr(buf, merge_len);
    }
    // defensive check 
    if (OB_SUCC(ret) && OB_FAIL(check_data_length())) {
      LOG_WARN("check len fail", KR(ret));
    }
  }
  return ret;
}

int ObLobCursor::check_data_length()
{
  INIT_SUCC(ret);
  int64_t check_data_len = 0;
  for (int i = 0; i < partial_data_->data_.count(); ++i) {
    check_data_len += partial_data_->data_[i].data_.length();
  }
  int64_t check_index_len = 0;
  for (int i = 0; i < partial_data_->index_.count(); ++i) {
    check_index_len += partial_data_->index_[i].byte_len_;
  }
  if (check_data_len != check_index_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check len fail", KR(ret), K(check_data_len), K(check_index_len));
  }
  return ret;
}

int ObLobCursor::get_chunk_data(int chunk_pos, ObString &data)
{
  INIT_SUCC(ret);
  const ObLobChunkIndex *chunk_index = nullptr;
  const ObLobChunkData *chunk_data = nullptr;
  int64_t pos = 0;
  if (OB_FAIL(get_chunk_data(chunk_pos, chunk_index, chunk_data))) {
    LOG_WARN("get_chunk_data fail", KR(ret), K(chunk_pos));
  // all append chunk will share same chunk index. so the real data pos need subtract append chunk offset
  // for normal exist chunk, get_chunk_offset(chunk_pos) is equal to chunk_index->offset_
  } else if (0 > (pos = chunk_index->pos_ + get_chunk_offset(chunk_pos) - chunk_index->offset_)) {
    ret  = OB_ERR_UNEXPECTED;
    LOG_WARN("pos is invalid", KR(ret), K(pos), K(chunk_index->pos_), K(get_chunk_offset(chunk_pos)), K(chunk_pos), K(chunk_index->offset_));
  } else {
    data.assign_ptr(chunk_data->data_.ptr() + pos, chunk_data->data_.length() - pos);
  }
  return ret;
}

int ObLobCursor::get_last_chunk_data_idx(int &chunk_idx)
{
  INIT_SUCC(ret);
  int chunk_pos = get_chunk_pos(partial_data_->data_length_ - 1);
  if (OB_FAIL(get_chunk_idx(chunk_pos, chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(partial_data_->data_length_), K(chunk_pos));
  }
  return ret;
}

bool ObLobCursor::is_append_chunk(int chunk_pos) const
{
  int64_t chunk_offset = get_chunk_offset(chunk_pos);
  return chunk_offset >= ori_data_length_;
}

int ObLobCursor::get_chunk_idx(int chunk_pos, int &chunk_idx)
{
  INIT_SUCC(ret);
  ObLobMetaInfo meta_info;
  ObLobChunkIndex new_chunk_index;
  ObLobChunkData chunk_data;
  int real_idx = -1;
  if (is_append_chunk(chunk_pos)) {
    int append_chunk_pos = get_chunk_pos(ori_data_length_ + partial_data_->chunk_size_ - 1);
    if (OB_FAIL(partial_data_->search_map_.get_refactored(append_chunk_pos, real_idx))) {
      LOG_WARN("get append chunk fail", KR(ret), K(chunk_pos), K(append_chunk_pos), K(ori_data_length_));
    } else {
      chunk_idx = real_idx;
    }
  } else if (OB_SUCC(partial_data_->search_map_.get_refactored(chunk_pos, real_idx))) {
    chunk_idx = real_idx;
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("get from search map fail", K(ret), K(chunk_pos));
  } else if (OB_FAIL(fetch_meta(chunk_pos, meta_info))) {
    LOG_WARN("fetch_meta fail", KR(ret), K(chunk_pos));
  } else if (meta_info.lob_data_.length() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta info is invalid", K(ret), K(chunk_pos), K(meta_info));
  // data return by storage points origin data memory
  // should copy if the data may be modified, or old data may be corrupted
  } else if (OB_FAIL(ob_write_string(*allocator_, meta_info.lob_data_, chunk_data.data_))) {
    LOG_WARN("copy data fail", KR(ret), K(chunk_pos), K(meta_info));
  } else if (OB_FAIL(ob_write_string(*allocator_, meta_info.seq_id_, new_chunk_index.seq_id_))) {
    LOG_WARN("copy seq_id data fail", KR(ret), K(chunk_pos), K(meta_info));
  } else if (OB_FAIL(partial_data_->data_.push_back(chunk_data))) {
    LOG_WARN("push_back data fail", KR(ret), K(chunk_pos), K(chunk_data));
  } else {
    new_chunk_index.offset_ = chunk_pos * partial_data_->chunk_size_;
    new_chunk_index.byte_len_ = meta_info.byte_len_;
    new_chunk_index.data_idx_ = partial_data_->data_.count() - 1;
    if (OB_FAIL(partial_data_->push_chunk_index(new_chunk_index))) {
      LOG_WARN("push_back index fail", KR(ret), K(chunk_pos), K(new_chunk_index));
    } else {
      chunk_idx = partial_data_->index_.count() - 1;
    }
  }
  return ret;
}

int ObLobCursor::get_chunk_data(int chunk_pos, const ObLobChunkIndex *&chunk_index, const ObLobChunkData *&chunk_data)
{
  INIT_SUCC(ret);
  int chunk_idx = -1;
  if (OB_FAIL(get_chunk_idx(chunk_pos, chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(chunk_pos));
  } else {
    chunk_index = &partial_data_->index_[chunk_idx];
    chunk_data = &partial_data_->data_[chunk_index->data_idx_];
  }
  return ret;
}


int ObLobCursor::get_chunk_pos(int64_t offset) const
{
  return offset / partial_data_->chunk_size_;
}

int64_t ObLobCursor::get_chunk_offset(int pos) const
{
  return pos * partial_data_->chunk_size_;
}

int ObLobCursor::fetch_meta(int idx, ObLobMetaInfo &meta_info)
{
  INIT_SUCC(ret);
  if (OB_FAIL(getter_.get_next_row(idx, meta_info))) {
    LOG_WARN("get_next_row fail", K(ret), K(idx));
  }
  return ret;
}

int ObLobCursor::append(const char* buf, int64_t buf_len)
{
  return set(partial_data_->data_length_, buf, buf_len);
}

int ObLobCursor::get_data(ObString &data) const
{
  INIT_SUCC(ret);
  if (is_full_mode()) {
    data = update_buffer_.string();
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLobCursor::reset_data(const ObString &data)
{
  INIT_SUCC(ret);
  if (is_full_mode()) {
    update_buffer_.reuse();
    ret = update_buffer_.append(data);
    partial_data_->data_[0].data_ = update_buffer_.string();
    partial_data_->data_length_ = update_buffer_.length();
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

int ObLobCursor::move_data_to_update_buffer(ObLobChunkData *chunk_data)
{
  INIT_SUCC(ret);
  if (update_buffer_.length() == 0) {
    if (OB_FAIL(update_buffer_.append(chunk_data->data_))) {
      LOG_WARN("update buffer reserve fail", KR(ret), KPC(chunk_data), K(update_buffer_));
    } else {
      chunk_data->data_ = update_buffer_.string();
    }
  } else if (update_buffer_.ptr() != chunk_data->data_.ptr() || update_buffer_.length() != chunk_data->data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update buffer state incorrect", KR(ret), K(update_buffer_), KPC(chunk_data));
  }
  return ret;
}

int ObLobCursor::push_append_chunk(int64_t append_len)
{
  INIT_SUCC(ret);
  int last_chunk_idx = -1;
  if (OB_FAIL(get_last_chunk_data_idx(last_chunk_idx))) {
    LOG_WARN("get_last_chunk_data fail", KR(ret));
  } else if (! chunk_index(last_chunk_idx).is_add_ && chunk_index(last_chunk_idx).byte_len_ + append_len > partial_data_->chunk_size_) {
    ObLobChunkIndex new_chunk_index;
    new_chunk_index.offset_ = chunk_index(last_chunk_idx).offset_ + partial_data_->chunk_size_;
    new_chunk_index.pos_ = chunk_index(last_chunk_idx).pos_ + partial_data_->chunk_size_;
    new_chunk_index.byte_len_ = 0;
    new_chunk_index.is_add_ = 1;
    new_chunk_index.data_idx_ = chunk_index(last_chunk_idx).data_idx_;
    if (OB_FAIL(record_chunk_old_data(last_chunk_idx))) {
      LOG_WARN("record_chunk_old_data fail", KR(ret), K(last_chunk_idx));
    } else if (OB_FAIL(partial_data_->push_chunk_index(new_chunk_index))) {
      LOG_WARN("push_back index fail", KR(ret), K(new_chunk_index));
    } else {
      // should be careful. this may cause check_data_length fail
      chunk_index(last_chunk_idx).byte_len_ = partial_data_->chunk_size_;
    }
  }
  return ret;
}

int ObLobCursor::set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove)
{
  INIT_SUCC(ret);
  int64_t start_offset = offset;
  int64_t end_offset = offset + buf_len;
  int start_chunk_pos = get_chunk_pos(start_offset);
  int old_end_chunk_pos = get_chunk_pos(partial_data_->data_length_ - 1);
  int end_chunk_pos = get_chunk_pos(end_offset - 1);
  int64_t append_len = end_offset > partial_data_->data_length_ ? end_offset - partial_data_->data_length_ : 0;
  int start_chunk_idx = -1;
  if (start_chunk_pos < old_end_chunk_pos && OB_FAIL(merge_chunk_data(start_chunk_pos, old_end_chunk_pos))) {
    LOG_WARN("merge_chunk_data fail", KR(ret), K(start_chunk_pos), K(old_end_chunk_pos), K(offset), K(buf_len), K(end_chunk_pos));
  } else if (append_len > 0 && OB_FAIL(push_append_chunk(append_len))) {
    LOG_WARN("push_append_chunk fail", KR(ret), K(append_len));
  } else if (OB_FAIL(get_chunk_idx(start_chunk_pos, start_chunk_idx))) {
    LOG_WARN("get_chunk_idx fail", KR(ret), K(start_chunk_pos));
  } else if (append_len > 0 && OB_FAIL(move_data_to_update_buffer(&chunk_data(start_chunk_idx)))) {
    LOG_WARN("move_data_to_update_buffer fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FAIL(update_buffer_.reserve(append_len))) {
    LOG_WARN("reserve fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FAIL(update_buffer_.set_length(update_buffer_.length() + append_len))) {
    LOG_WARN("set_length fail", KR(ret), K(start_chunk_pos), K(append_len), K(start_chunk_idx));
  } else if (append_len > 0 && OB_FALSE_IT(chunk_data(start_chunk_idx).data_ = update_buffer_.string())) {
  } else {
    for (int i = start_chunk_pos, chunk_idx = -1; OB_SUCC(ret) && i <= end_chunk_pos; ++i) {
      if (OB_FAIL(get_chunk_idx(i, chunk_idx))) {
        LOG_WARN("get_chunk_idx fail", KR(ret), K(i));
      } else if (OB_FAIL(record_chunk_old_data(chunk_idx))) {
        LOG_WARN("record_chunk_old_data fail", KR(ret), K(i));
      } else if (i == end_chunk_pos && append_len > 0) {
        chunk_index(chunk_idx).byte_len_ = (end_offset - chunk_index(chunk_idx).offset_);
      }
    }
    if (OB_SUCC(ret)) {
      if (use_memmove) {
        MEMMOVE(chunk_data(start_chunk_idx).data_.ptr() + chunk_index(start_chunk_idx).pos_ + (start_offset - chunk_index(start_chunk_idx).offset_), buf, buf_len);
      } else {
        MEMCPY(chunk_data(start_chunk_idx).data_.ptr() + chunk_index(start_chunk_idx).pos_ + (start_offset - chunk_index(start_chunk_idx).offset_), buf, buf_len);
      }
      partial_data_->data_length_ += append_len;
    }
    // defensive check 
    if (OB_SUCC(ret) && OB_FAIL(check_data_length())) {
      LOG_WARN("check len fail", KR(ret));
    }
  }
  return ret;
}

int ObLobCursor::record_chunk_old_data(int chunk_idx)
{
  INIT_SUCC(ret);
  ObLobChunkIndex &chunk_index = partial_data_->index_[chunk_idx];
  if (OB_FAIL(set_old_data(chunk_index))) {
    LOG_WARN("record_chunk_old_data fail", KR(ret), K(chunk_index));
  }
  return ret;  
}

int ObLobCursor::set_old_data(ObLobChunkIndex &chunk_index)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (chunk_index.old_data_idx_ >= 0) { // has set old
  } else if (chunk_index.is_add_) { // add no old
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator_->alloc(chunk_index.byte_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", KR(ret), K(chunk_index));
  } else {
    const ObLobChunkData &chunk_data = partial_data_->data_[chunk_index.data_idx_];
    MEMCPY(buf, chunk_data.data_.ptr() + chunk_index.pos_, chunk_index.byte_len_);
    if (OB_FAIL(partial_data_->old_data_.push_back(ObLobChunkData(ObString(chunk_index.byte_len_, buf))))) {
      LOG_WARN("push_back fail", KR(ret), K(chunk_index));
    } else {
      chunk_index.old_data_idx_ = partial_data_->old_data_.count() - 1;
      chunk_index.is_modified_ = 1;
    }
  }
  return ret;
}

int ObLobCursor::get(int64_t offset, int64_t len, ObString &data) const
{
  INIT_SUCC(ret);
  const char *ptr = nullptr;;
  if (OB_FAIL(get_ptr(offset, len, ptr))) {
    LOG_WARN("get_ptr fail", KR(ret), K(offset), K(len), K(data.length()));
  } else if (OB_ISNULL(ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get_ptr fail", KR(ret), K(offset), K(len), K(data.length()));
  } else {
    data.assign_ptr(ptr, len);
  }
  return ret;
}

// if lob has only one chunk and contains all data, will return true
bool ObLobCursor::has_one_chunk_with_all_data()
{
  bool res = false;
  if (OB_ISNULL(partial_data_)) {
  } else if (1 != partial_data_->index_.count()) {
  } else {
    res = (ori_data_length_ == chunk_data(0).data_.length());
  }
  return res;
}

int ObLobCursor::get_one_chunk_with_all_data(ObString &data)
{
  INIT_SUCC(ret);
  if (OB_ISNULL(partial_data_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ is null", KR(ret), KPC(this));
  } else if (1 != partial_data_->index_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ has not only one chunk", KR(ret), K(partial_data_->index_.count()));
  } else if (ori_data_length_ != chunk_data(0).data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partial_data_ data length incorrect", KR(ret), K(ori_data_length_), K(chunk_data(0)));
  } else {
    data = chunk_data(0).data_;
    LOG_DEBUG("get chunk data success", K(chunk_data(0)), K(data));
  }
  return ret;
}

/********** ObLobCursor ****************/

} // storage
} // oceanbase