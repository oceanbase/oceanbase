/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "storage/access/ob_store_row_iterator.h"

namespace oceanbase
{
namespace share
{
class ObTabletCacheInterval;
} // namespace share
namespace storage
{

struct ObDirectLoadRowFlag
{
public:
  ObDirectLoadRowFlag() : flag_(0) {}
  void reset() { flag_ = 0; }
  OB_INLINE int64_t get_column_count(const int64_t column_count) const
  {
    return uncontain_hidden_pk_ ? column_count + 1 : column_count;
  }
  TO_STRING_KV(K_(uncontain_hidden_pk), K_(has_multi_version_cols), K_(has_delete_row));
public:
  union
  {
    struct
    {
      bool uncontain_hidden_pk_ : 1; // used for heap table
      bool has_multi_version_cols_ : 1;
      bool has_delete_row_ : 1; // TODO: 后续维护这个flag
      int64_t reserved_ : 61;
    };
    int64_t flag_;
  };
};

class ObDirectLoadIStoreRowIterator : public ObIStoreRowIterator
{
public:
  ObDirectLoadIStoreRowIterator() : row_flag_(), column_count_(0), is_batch_result_(false) {}
  virtual ~ObDirectLoadIStoreRowIterator() = default;
  virtual int get_next_batch(const IVectorPtrs *&vectors, int64_t &batch_size)
  {
    return OB_NOT_SUPPORTED;
  }
  ObDirectLoadRowFlag &get_row_flag() { return row_flag_; }
  const ObDirectLoadRowFlag &get_row_flag() const { return row_flag_; }
  int64_t get_column_count() const { return column_count_; }
  bool is_batch_result() const { return is_batch_result_; }
  virtual share::ObTabletCacheInterval *get_hide_pk_interval() const { return nullptr; }
  virtual bool is_valid() const
  {
    return (!row_flag_.uncontain_hidden_pk_ || nullptr != get_hide_pk_interval()) &&
           column_count_ > 0;
  }
  VIRTUAL_TO_STRING_KV(K_(row_flag),
                       K_(column_count),
                       K_(is_batch_result));
protected:
  ObDirectLoadRowFlag row_flag_;
  int64_t column_count_;
  bool is_batch_result_;
};

class ObDirectLoadStoreRowIteratorWrap final : public ObDirectLoadIStoreRowIterator
{
public:
  ObDirectLoadStoreRowIteratorWrap() : iter_(nullptr), is_inited_(false) {}
  virtual ~ObDirectLoadStoreRowIteratorWrap() = default;
  int init(ObIStoreRowIterator *iter,
           const ObDirectLoadRowFlag &row_flag,
           const int64_t column_count);
  int get_next_row(const blocksstable::ObDatumRow *&row) override;
  INHERIT_TO_STRING_KV("IStoreRowIterator", ObDirectLoadIStoreRowIterator,
                       KP_(iter),
                       K_(is_inited));
private:
  ObIStoreRowIterator *iter_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
