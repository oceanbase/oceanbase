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

#ifndef OB_DTL_VECTORS_BUFFER_H
#define OB_DTL_VECTORS_BUFFER_H

#include <stdint.h>
#include <functional>
#include "sql/engine/basic/ob_compact_row.h"
#include "src/share/vector/ob_i_vector.h"

namespace oceanbase {


namespace sql {
namespace dtl {


struct ObVectorSegment
{
  ObVectorSegment(int32_t capacity) : cap_(capacity),
                                      data_pos_(0),
                                      offset_pos_(capacity),
                                      fixed_len_(0),
                                      seg_rows_(0),
                                      format_(VEC_INVALID),
                                      next_(nullptr),
                                      nulls_(nullptr) {}
  static const int32_t MAX_ROW_CNT = 512;
  static int64_t get_bit_vector_size()
  {
    return ObBitVector::memory_size(MAX_ROW_CNT);
  }
  int append_col_in_one_row(const ObExpr &expr,
                            const bool is_fixed_len,
                            const int32_t batch_idx,
                            ObEvalCtx &ctx);
  void init(const VectorFormat format, const int32_t len);
  inline void set_offset(int32_t pos)
  {
    offset_pos_ -= sizeof(uint32_t);
    *reinterpret_cast<uint32_t *> (payload_ + offset_pos_) = pos;
  }
  inline uint32_t get_offset(int32_t idx) const {
    return *reinterpret_cast<const uint32_t *> (payload_ + cap_ - sizeof(uint32_t) * (idx + 1));
  }
  inline int32_t get_length(int32_t idx) { return *reinterpret_cast<uint32_t *> (payload_ + cap_ - sizeof(uint32_t) * (idx + 2))
                                                - *reinterpret_cast<uint32_t *> (payload_ + cap_ - sizeof(uint32_t) * (idx + 1)); }
  bool can_append_col(int32_t real_data_size, int32_t fixed_len) const;
  static int32_t calc_alloc_size(int32_t data_size, int32_t fixed_len);
  static int32_t get_real_data_size(ObIVector &vec, int32_t batch_idx)
  {
    return vec.is_null(batch_idx) ? 0 : vec.get_length(batch_idx);
  }
  static int32_t get_fixed_len(const ObExpr &expr, int32_t batch_idx)
  {
    return expr.is_fixed_length_data_
              ? expr.res_buf_len_ : sizeof(uint32_t)/*for offsets*/;
  }

  char *head() { return payload_ + data_start_pos_; }
  TO_STRING_KV(K_(cap), K_(data_start_pos), K_(data_pos),
              K_(offset_pos), K_(fixed_len), K_(seg_rows), K_(format));
  int32_t cap_;
  int32_t data_start_pos_;
  int32_t data_pos_;
  int32_t offset_pos_;
  int32_t fixed_len_;
  int32_t seg_rows_; /*how many rows have been appended successfully*/
  VectorFormat format_;
  ObVectorSegment *next_;
  ObBitVector *nulls_;
  char payload_[0];
} __attribute__((packed));

class ObDtlVectorsBuffer;
struct ObDtlVectorsBlock
{
  ObDtlVectorsBlock() : blk_size_(0), rows_(0) {}

  inline void set_block_size(int32 blk_size) { blk_size_ = blk_size; }
  ObDtlVectorsBuffer *get_buffer();
  inline int32_t data_size();
  inline int32_t rows() { return rows_; }
  inline int32_t remain();
  friend class ObDtlVectorsBuffer;
  TO_STRING_KV(K_(blk_size), K_(rows));
  int32_t blk_size_;
  int32_t rows_;
  char payload_[0];
} __attribute__((packed));

class ObDtlVectorsBuffer
{
public:
  static const int32_t MAX_COL_CNT = 16;
  static const int32_t DEFAULT_BLOCK_SIZE = 4 * 1024;
  static const int32_t MAGIC = 0xe02d8536;
  static int init_vector_buffer(void* mem, const int32_t size, ObDtlVectorsBlock *&block);
  ObDtlVectorsBuffer() : cur_pos_(0), cap_(0), data_(nullptr), meta_() {}
  ~ObDtlVectorsBuffer() { meta_.reset(); }
  int init(char *buf, const int32_t buf_size);
  int init_row_meta(const ObExprPtrIArray &exprs);
  inline int32_t data_size() const { return cur_pos_; }
  inline int32_t remain() const { return cap_ - cur_pos_; }
  inline void fast_advance(int32_t size) { cur_pos_ += size; }
  inline int32_t capacity() const { return cap_; }
  int append_row(const common::ObIArray<ObExpr*> &exprs, const int32_t batch_idx, ObEvalCtx &ctx);
  int alloc_segmant(int32_t col_idx, int32_t data_size,
                    int32_t fixed_len, ObVectorSegment *pre_seg);
  bool can_append_row(const common::ObIArray<ObExpr*> &exprs,
                      const int32_t batch_idx,
                      ObEvalCtx &ctx,
                      int64_t &new_block_size) const;
  static void calc_new_buffer_size(const common::ObIArray<ObExpr*> *exprs,
                                   const int32_t batch_idx,
                                   ObEvalCtx &ctx,
                                   int64_t &new_block_size);
  static int64_t inline min_buf_size()
  {
    return sizeof(ObDtlVectorsBlock) + sizeof(ObDtlVectorsBuffer);
  }
  int32_t get_col_cnt() const { return meta_.col_cnt_; }
  ObVectorSegment *get_start_seg(int32_t col_idx)
  { return reinterpret_cast<ObVectorSegment *> (data_ + cols_seg_start_pos_[col_idx]); }
//private:
  int32_t cols_seg_start_pos_[MAX_COL_CNT];
  int32_t cols_seg_pos_[MAX_COL_CNT];
  int32_t cur_pos_;
  int32_t cap_;
  union {
    char *data_;
    ObDtlVectorsBlock *block_;
  };
  RowMeta meta_;
};

struct VectorInfo {
  VectorFormat format_;
  int32_t nulls_offset_;
  int32_t fixed_len_;
  int32_t offsets_offset_;
  int32_t data_offset_;
  TO_STRING_KV(K(format_), K_(nulls_offset), K_(fixed_len), K_(offsets_offset), K_(data_offset));
};


/*magic_num : 4
col_cnt : 4
row_cnt : 4
(format : 4 + nulls offset : 4 + fixed len : 4 + offsets offset : 4 + data offset : 4) * col_cnt
(nulls : to_bit_vector(row_cnt) + offsets : 4 * (row_cnt + 1) + data)  * col_cnt
*/
class ObDtlVectors {
public:
  static const int64_t HEAD_SIZE = sizeof(int32_t) * 3;
  static const int64_t ROW_CNT_OFFSET = sizeof(int32_t) * 2;
  static int64_t min_buf_size() { return HEAD_SIZE; }
  static int32_t decode_row_cnt(char *buf) { return *reinterpret_cast<int32_t *> (buf + ROW_CNT_OFFSET); }
  ObDtlVectors() : buf_(nullptr), magic_(nullptr), col_cnt_(nullptr), row_cnt_(nullptr), infos_(nullptr), mem_limit_(0), row_limit_(0), read_rows_(0), row_size_(0), inited_(false) {}
  void set_buf(char *buf, int32_t mem_limit) { inited_ = false; /*set false to reinit vector*/ buf_ = buf; mem_limit_ = mem_limit; }
  char *get_buf() { return buf_; }
  int init(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx); // only for fixed vector
  int init();
  bool is_inited() const { return inited_; }
  int decode(); // only for variable length vector
  int append_row(const common::ObIArray<ObExpr*> &exprs, const int32_t batch_idx, ObEvalCtx &ctx);
  int append_batch(const common::ObIArray<ObExpr*> &exprs,
                   const common::ObIArray<ObIVector *> &vectors,
                   const uint16_t selector[],
                   const int64_t size,
                   ObEvalCtx &ctx);
  void reset() { new (this) ObDtlVectors(); }
  int32_t get_magic() const { return *magic_; }
  int32_t get_col_cnt() const { return *col_cnt_; }
  int32_t get_row_cnt() const { return *row_cnt_; }
  int32_t get_row_limit() const { return row_limit_; }
  int32_t get_mem_used() const { return mem_limit_; } //TODO
  int32_t get_mem_limit() const { return mem_limit_; }
  const VectorInfo &get_info(int32_t col_idx) const { return infos_[col_idx]; }
  VectorFormat get_format(int32_t col_idx) const { return get_info(col_idx).format_; }
  ObBitVector *get_nulls(int32_t col_idx) { return reinterpret_cast<ObBitVector *> (buf_ + get_info(col_idx).nulls_offset_); }
  int32_t get_fixed_length(int32_t col_idx) const { return get_info(col_idx).fixed_len_; }
  uint32_t *get_offsets(int32_t col_idx) { return reinterpret_cast<uint32_t *>(buf_ + get_info(col_idx).offsets_offset_); }
  char *get_data(int32_t col_idx) { return buf_ + get_info(col_idx).data_offset_; }
  int32_t get_read_rows() const { return read_rows_; }
  int32_t get_remain_rows() const { return get_row_cnt() - read_rows_; }
  void inc_read_rows(int32_t read_rows) { read_rows_ += read_rows; }
private:
  char *buf_;
  int32_t *magic_;
  int32_t *col_cnt_;
  int32_t *row_cnt_;
  VectorInfo *infos_;
  int32_t mem_limit_;
  int32_t row_limit_;
  int32_t read_rows_;
  int32_t row_size_; // to be remove
  bool inited_;
};


}  // dtl
}  // sql
}  // oceanbase

#endif /* OB_DTL_VECTORS_BUFFER_H */
