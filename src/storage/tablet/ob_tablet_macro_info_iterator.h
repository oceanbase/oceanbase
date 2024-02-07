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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MACRO_INFO_ITERATOR
#define OCEANBASE_STORAGE_OB_TABLET_MACRO_INFO_ITERATOR

#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/slog_ckpt/ob_linked_macro_block_reader.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
struct ObTabletBlockInfo final
{
public:
  ObTabletBlockInfo();
  ObTabletBlockInfo(
      const blocksstable::MacroBlockId &macro_id,
      const ObTabletMacroType block_type,
      const int64_t occupy_size);
  ~ObTabletBlockInfo();
  void reset();
  TO_STRING_KV(K_(macro_id), K_(block_type), K_(occupy_size));
public:
  blocksstable::MacroBlockId macro_id_;
  ObTabletMacroType block_type_;
  int64_t occupy_size_;
};

class ObMacroInfoIterator final
{
public:
  ObMacroInfoIterator();
  ~ObMacroInfoIterator();
  ObMacroInfoIterator(const ObMacroInfoIterator &) = delete;
  ObMacroInfoIterator &operator=(const ObMacroInfoIterator &) = delete;
  void destroy();
  int reuse();
  // ObTabletMacroType::MAX means iterate all kinds of ids
  int init(const ObTabletMacroType target_type, const ObTabletMacroInfo &macro_info);
  int get_next(ObTabletBlockInfo &block_info);
  TO_STRING_KV(KPC_(macro_info), K_(cur_type), K_(target_type), K_(is_linked));
private:
  int read_from_disk();
  int read_from_memory();
  int reuse_info_arr(const int64_t cnt);
  int convert_to_block_info(const ObTabletMacroInfo::ObBlockInfoArray<ObSharedBlockInfo> &tmp_arr);
  int convert_to_block_info(const ObTabletMacroInfo::ObBlockInfoArray<blocksstable::MacroBlockId> &tmp_arr);
private:
  const ObTabletMacroInfo *macro_info_;
  ObLinkedMacroBlockItemReader block_reader_;
  int64_t cur_pos_;
  int64_t cur_size_;
  ObTabletMacroType cur_type_;
  ObTabletMacroType target_type_;
  ObTabletMacroInfo::ObBlockInfoArray<ObTabletBlockInfo> block_info_arr_;
  common::ObArenaAllocator allocator_;
  bool is_linked_;
  bool is_inited_;
};
}
}

#endif