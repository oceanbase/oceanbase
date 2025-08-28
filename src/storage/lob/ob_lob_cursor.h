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

#ifndef OCEANBASE_STORAGE_OB_LOB_CURSOR_H_
#define OCEANBASE_STORAGE_OB_LOB_CURSOR_H_

#include "lib/lob/ob_lob_base.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/lob/ob_lob_persistent_iterator.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaManager;

class ObLobCursor : public ObILobCursor
{
public:
  ObLobCursor():
    param_(nullptr),
    allocator_(nullptr),
    is_full_mode_(false),
    ori_data_length_(0),
    partial_data_(nullptr),
    getter_()
  {}

  ~ObLobCursor();
  int init(ObIAllocator *allocator, ObLobAccessParam* param, ObLobPartialData *partial_data, ObLobMetaManager *lob_meta_mngr);
  int get_data(ObString &data) const;
  int64_t get_length() const { return partial_data_->data_length_; }
  int reset() { return OB_SUCCESS; }
  bool is_full_mode() const { return is_full_mode_; }
  bool is_append_chunk(int chunk_pos) const;
  int append(const char* buf, int64_t buf_len);
  int append(const ObString& data) { return append(data.ptr(), data.length()); }

  int reset_data(const ObString &data);
  int set(int64_t offset, const char *buf, int64_t buf_len, bool use_memmove=false);
  int get(int64_t offset, int64_t len, ObString &data) const;
  // if lob has only one chunk and contains all data, will return true
  bool has_one_chunk_with_all_data();
  int get_one_chunk_with_all_data(ObString &data);

  TO_STRING_KV(K(is_full_mode_), K(ori_data_length_));

protected:
  int get_ptr(int64_t offset, int64_t len, const char *&ptr);
  int get_ptr(int64_t offset, int64_t len, const char *&ptr) const{ return const_cast<ObLobCursor*>(this)->get_ptr(offset, len, ptr); }
  int get_ptr_for_write(int64_t offset, int64_t len, char *&ptr);

private:
  int init_full(ObIAllocator *allocator, ObLobPartialData *partial_data);
  int get_chunk_pos(int64_t offset) const;
  int64_t get_chunk_offset(int pos) const;
  int fetch_meta(int idx, ObLobMetaInfo &meta_info);
  int merge_chunk_data(int start_meta_idx, int end_meta_idx);
  int get_chunk_data(int chunk_pos, ObString &data);
  int get_chunk_idx(int chunk_pos, int &chunk_idx);
  int get_chunk_data(int chunk_pos, const ObLobChunkIndex *&chunk_index, const ObLobChunkData *&chunk_data);
  int get_last_chunk_data_idx(int &chunk_idx);

  int push_append_chunk(int64_t append_len);
  int move_data_to_update_buffer(ObLobChunkData *chunk_data);
  int set_old_data(ObLobChunkIndex &chunk_index);
  int record_chunk_old_data(int chunk_idx);
  int record_chunk_old_data(ObLobChunkIndex *chunk_pos);
  int get_chunk_data_start_pos(const int cur_chunk_pos, int &start_pos);
  int get_chunk_data_end_pos(const int cur_chunk_pos, int &end_pos);

  ObLobChunkIndex& chunk_index(int chunk_idx) { return  partial_data_->index_[chunk_idx]; }
  const ObLobChunkIndex& chunk_index(int chunk_idx) const { return  partial_data_->index_[chunk_idx]; }
  ObLobChunkData& chunk_data(int chunk_idx) { return  partial_data_->data_[partial_data_->index_[chunk_idx].data_idx_]; }
  const ObLobChunkData& chunk_data(int chunk_idx) const { return  partial_data_->data_[partial_data_->index_[chunk_idx].data_idx_]; }

  int check_data_length();
public:
  ObLobAccessParam *param_;
  ObIAllocator *allocator_;
  ObStringBuffer update_buffer_;
  bool is_full_mode_;
  int64_t ori_data_length_;
  ObLobPartialData *partial_data_;
  ObLobMetaSingleGetter getter_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_CURSOR_H_

