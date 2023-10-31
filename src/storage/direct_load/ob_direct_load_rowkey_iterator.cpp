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

#include "storage/direct_load/ob_direct_load_rowkey_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

/**
 * ObDirectLoadDatumRowkeyArrayIterator
 */

ObDirectLoadDatumRowkeyArrayIterator::ObDirectLoadDatumRowkeyArrayIterator()
  : rowkey_array_(nullptr), pos_(0), is_inited_(false)
{
}

ObDirectLoadDatumRowkeyArrayIterator::~ObDirectLoadDatumRowkeyArrayIterator()
{
}

int ObDirectLoadDatumRowkeyArrayIterator::init(const ObIArray<ObDatumRowkey> &rowkey_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadDatumRowkeyArrayIterator init twice", KR(ret), KP(this));
  } else {
    rowkey_array_ = &rowkey_array;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadDatumRowkeyArrayIterator::get_next_rowkey(const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowkeyArrayIterator not init", KR(ret), KP(this));
  } else if (pos_ >= rowkey_array_->count()) {
    ret = OB_ITER_END;
  } else {
    rowkey = &rowkey_array_->at(pos_++);
  }
  return ret;
}

/**
 * ObDirectLoadMacroBlockEndKeyIterator
 */

ObDirectLoadMacroBlockEndKeyIterator::ObDirectLoadMacroBlockEndKeyIterator()
  : macro_meta_iter_(nullptr), is_inited_(false)
{
}

ObDirectLoadMacroBlockEndKeyIterator::~ObDirectLoadMacroBlockEndKeyIterator()
{
  if (nullptr != macro_meta_iter_) {
    macro_meta_iter_->~ObSSTableSecMetaIterator();
    macro_meta_iter_ = nullptr;
  }
}

int ObDirectLoadMacroBlockEndKeyIterator::init(ObSSTableSecMetaIterator *macro_meta_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDirectLoadMacroBlockEndKeyIterator init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(macro_meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(macro_meta_iter));
  } else {
    macro_meta_iter_ = macro_meta_iter;
    is_inited_ = true;
  }
  return ret;
}

int ObDirectLoadMacroBlockEndKeyIterator::get_next_rowkey(const ObDatumRowkey *&rowkey)
{
  int ret = OB_SUCCESS;
  rowkey = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadMacroBlockEndKeyIterator not init", KR(ret), KP(this));
  } else {
    ObDataMacroBlockMeta macro_meta;
    if (OB_FAIL(macro_meta_iter_->get_next(macro_meta))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next macro block meta", KR(ret));
      }
    } else if (OB_FAIL(rowkey_.assign(macro_meta.end_key_.datums_,
                                      macro_meta.val_.rowkey_count_ -
                                        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt()))) {
      LOG_WARN("fail to get datum rowkey", KR(ret));
    } else {
      rowkey = &rowkey_;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
