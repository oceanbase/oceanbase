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
#pragma once

#include "lib/container/ob_heap.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/direct_load/ob_direct_load_multiple_datum_rowkey.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageDatumUtils;
class ObSSTableSecMetaIterator;
} // namespace blocksstable
namespace storage
{

template <class Rowkey>
class ObIDirectLoadRowkeyIterator
{
public:
  virtual ~ObIDirectLoadRowkeyIterator() = default;
  virtual int get_next_rowkey(const Rowkey *&rowkey) = 0;
  TO_STRING_EMPTY();
};

typedef ObIDirectLoadRowkeyIterator<blocksstable::ObDatumRowkey> ObIDirectLoadDatumRowkeyIterator;
typedef ObIDirectLoadRowkeyIterator<ObDirectLoadMultipleDatumRowkey>
  ObIDirectLoadMultipleDatumRowkeyIterator;

class ObDirectLoadDatumRowkeyArrayIterator : public ObIDirectLoadDatumRowkeyIterator
{
public:
  ObDirectLoadDatumRowkeyArrayIterator();
  virtual ~ObDirectLoadDatumRowkeyArrayIterator();
  int init(const common::ObIArray<blocksstable::ObDatumRowkey> &rowkey_array);
  int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
private:
  const common::ObIArray<blocksstable::ObDatumRowkey> *rowkey_array_;
  int64_t pos_;
  bool is_inited_;
};

class ObDirectLoadMacroBlockEndKeyIterator : public ObIDirectLoadDatumRowkeyIterator
{
public:
  ObDirectLoadMacroBlockEndKeyIterator();
  virtual ~ObDirectLoadMacroBlockEndKeyIterator();
  int init(blocksstable::ObSSTableSecMetaIterator *macro_meta_iter);
  int get_next_rowkey(const blocksstable::ObDatumRowkey *&rowkey) override;
private:
  blocksstable::ObSSTableSecMetaIterator *macro_meta_iter_;
  blocksstable::ObDatumRowkey rowkey_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
