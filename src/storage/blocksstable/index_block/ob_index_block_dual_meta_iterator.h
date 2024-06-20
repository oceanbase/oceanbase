/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_INDEX_BLOCK_DUAL_META_ITERATOR_H_
#define OB_INDEX_BLOCK_DUAL_META_ITERATOR_H_

#include "ob_index_block_tree_cursor.h"
#include "ob_index_block_macro_iterator.h"
#include "ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_datum_rowkey.h"

namespace oceanbase {
namespace blocksstable {

// Wrap-up of iterate both index block tree and secondary meta in sstable
class ObDualMacroMetaIterator final : public ObIMacroBlockIterator
{
public:
  ObDualMacroMetaIterator();
  virtual ~ObDualMacroMetaIterator() {}

  void reset() override;
  virtual int open(
      ObSSTable &sstable,
      const ObDatumRange &query_range,
      const ObITableReadInfo &rowkey_read_info,
      ObIAllocator &allocator,
      const bool is_reverse_scan = false,
      const bool need_record_micro_info = false) override;
  virtual int get_next_macro_block(blocksstable::ObMacroBlockDesc &block_desc) override;

  virtual const ObIArray<blocksstable::ObMicroIndexInfo> &get_micro_index_infos() const
  {
    return macro_iter_.get_micro_index_infos();
  }
  virtual const ObIArray<ObDatumRowkey> &get_micro_endkeys() const
  {
    return macro_iter_.get_micro_endkeys();
  }
  TO_STRING_KV(K_(iter_end), K_(is_inited), K_(macro_iter), K_(sec_meta_iter));
private:
  ObIAllocator *allocator_; // allocator for member struct and macro endkeys
  ObIndexBlockMacroIterator macro_iter_;
  ObSSTableSecMetaIterator sec_meta_iter_;
  bool iter_end_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDualMacroMetaIterator);
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OB_INDEX_BLOCK_DUAL_META_ITERATOR_H_
