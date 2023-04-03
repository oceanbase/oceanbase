// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
