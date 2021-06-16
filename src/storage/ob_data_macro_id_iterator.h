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

#ifndef OB_DATA_MACRO_ID_ITERATOR_H_
#define OB_DATA_MACRO_ID_ITERATOR_H_

#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_table.h"
#include "storage/ob_sstable.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPGPartitionIterator;

class ObDataMacroIdIterator {
public:
  ObDataMacroIdIterator();
  virtual ~ObDataMacroIdIterator();
  int init(ObPartitionService& partition_service, ObIPartitionGroup* pg = nullptr);
  int locate(const int64_t last_partition_idx, const int64_t last_sstable_idx, const int64_t last_macro_idx);
  int get_last_idx(int64_t& last_partition_idx, int64_t& last_sstable_idx, int64_t& last_macro_idx);
  int get_next_macro_id(
      blocksstable::MacroBlockId& block_id, blocksstable::ObMacroBlockCommonHeader::MacroBlockType& block_type);
  int get_next_macro_info(blocksstable::ObMacroBlockInfoPair& pair, ObTenantFileKey& file_key);

private:
  bool is_inited_;
  ObPartitionService* partition_service_;
  int64_t cur_store_idx_;
  int64_t cur_sstable_idx_;
  int64_t cur_macro_idx_;
  int64_t cur_meta_macro_idx_;
  int64_t cur_partition_idx_;
  ObIPGPartitionIterator* partition_iter_;
  ObTablesHandle stores_handle_;
  common::ObArray<ObSSTable*> sstables_;
  const ObSSTable::ObSSTableGroupMacroBlocks* macro_blocks_;
  int64_t normal_data_block_count_;
  blocksstable::MacroBlockId bf_macro_block_id_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_DATA_MACRO_ID_ITERATOR_H_ */
