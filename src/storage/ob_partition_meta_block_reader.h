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

#ifndef OB_PARTITION_META_BLOCK_READER_H_
#define OB_PARTITION_META_BLOCK_READER_H_
#include "blocksstable/ob_meta_block_reader.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPartitionMetaBlockReader : public blocksstable::ObMetaBlockReader {
public:
  ObPartitionMetaBlockReader(ObPartitionService& partition_service);
  virtual ~ObPartitionMetaBlockReader();

protected:
  virtual int parse(const blocksstable::ObMacroBlockCommonHeader& common_header,
      const blocksstable::ObLinkedMacroBlockHeader& linked_header, const char* buf, const int64_t buf_len);

private:
  ObPartitionService& partition_service_;
  common::ObArenaAllocator allocator_;
  char* read_buf_;
  int64_t offset_;
  int64_t buf_size_;
};

}  // namespace storage
}  // namespace oceanbase

#endif /* OB_PARTITION_META_BLOCK_READER_H_ */
