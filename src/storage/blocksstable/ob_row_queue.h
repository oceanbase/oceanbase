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

#ifndef OB_ROW_QUEUE_H_
#define OB_ROW_QUEUE_H_

#include "lib/container/ob_raw_se_array.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{

class ObRowQueue
{
public:
  ObRowQueue() : col_cnt_(0), cur_pos_(0), is_inited_(false) {}
  int init(const int64_t col_cnt);
  int add_empty_row(common::ObIAllocator &allocator);
  int add_row(const ObDatumRow &row, common::ObIAllocator &allocator);
  int get_next_row(const ObDatumRow *&row);
  OB_INLINE bool is_empty() { return 0 == rows_.count(); }
  OB_INLINE bool has_next() const { return cur_pos_ < rows_.count(); }
  OB_INLINE ObDatumRow *get_first()
  {
    ObDatumRow *row = NULL;
    if (!is_empty()) {
      row = rows_.at(0);
    }
    return row;
  }
  OB_INLINE ObDatumRow *get_last()
  {
    ObDatumRow *row = NULL;
    if (!is_empty()) {
      row = rows_.at(rows_.count() - 1);
    }
    return row;
  }
  OB_INLINE int64_t count() const { return rows_.count(); }
  void reset()
  {
    col_cnt_ = 0;
    cur_pos_ = 0;
    rows_.reset();
    is_inited_ = false;
  }

  void reuse()
  {
    cur_pos_ = 0;
    rows_.reuse();
  }
  void print_rows()
  {
    for (int64_t i = 0; i < rows_.count(); i++) {
      STORAGE_LOG_RET(WARN, common::OB_SUCCESS, "print rows of row queue", K(i), KPC(rows_.at(i)));
    }
  }
  TO_STRING_KV(K_(col_cnt),
               K_(cur_pos),
               "count", rows_.count());
private:
  int alloc_row(ObDatumRow *&row, common::ObIAllocator &allocator);
private:
  static const int64_t DEFAULT_MULTIVERSION_ROW_COUNT = 64;
  int64_t col_cnt_;
  int64_t cur_pos_;
  common::ObRawSEArray<ObDatumRow *, DEFAULT_MULTIVERSION_ROW_COUNT> rows_;
  bool is_inited_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif
