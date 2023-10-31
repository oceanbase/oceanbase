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

#ifndef OCEANBASE_ENCODING_OB_COLUMN_DATUM_ITER_H_
#define OCEANBASE_ENCODING_OB_COLUMN_DATUM_ITER_H_

#include "ob_column_encoding_struct.h"
#include "storage/blocksstable/encoding/ob_encoding_util.h"
#include "storage/blocksstable/encoding/ob_encoding_hash_util.h"

namespace oceanbase
{
namespace blocksstable
{
class ObIDatumIter
{
public:
  ObIDatumIter() {}
  virtual ~ObIDatumIter() {}
  virtual int get_next(const ObDatum *&datum) = 0;
  virtual int64_t size() const = 0;
  bool empty() const { return 0 == size(); }
  virtual void reset() = 0;
};

class ObColumnDatumIter : public ObIDatumIter
{
public:
  explicit ObColumnDatumIter(const ObColDatums &col_datums)
    : col_datums_(col_datums), idx_(0) { }
  ~ObColumnDatumIter() {}
  ObColumnDatumIter(const ObColumnDatumIter&) = delete;
  ObColumnDatumIter &operator=(const ObColumnDatumIter&) = delete;
  int get_next(const ObDatum *&datum) override;
  int64_t size() const override { return col_datums_.count(); }
  virtual void reset() override { idx_ = 0; }

private:
  const ObColDatums &col_datums_;
  int64_t idx_;
};

class ObDictDatumIter : public ObIDatumIter
{
public:
  explicit ObDictDatumIter(const ObEncodingHashTable &ht)
    : ht_(ht), iter_(ht_.begin()) { }
  ~ObDictDatumIter() {}
  ObDictDatumIter(const ObDictDatumIter&) = delete;
  ObDictDatumIter &operator=(const ObDictDatumIter&) = delete;

  int get_next(const ObDatum *&datum) override;
  int64_t size() const override { return ht_.size(); }
  virtual void reset() override {  iter_ = ht_.begin(); }

private:
  const ObEncodingHashTable &ht_;
  ObEncodingHashTable::ConstIterator iter_;
};


}  // namespace blocksstable
}  // namespace oceanbase

#endif  // OCEANBASE_ENCODING_OB_COLUMN_DATUM_ITER_H_
