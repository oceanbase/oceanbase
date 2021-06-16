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

#ifndef OB_SERVER_CHECKPOINT_LOG_READER_OLD_H_
#define OB_SERVER_CHECKPOINT_LOG_READER_OLD_H_

#include "blocksstable/ob_block_sstable_struct.h"

namespace oceanbase {
namespace common {
class ObLogCursor;
}

namespace storage {

class ObServerCheckpointLogReaderV1 final {
public:
  ObServerCheckpointLogReaderV1() = default;
  ~ObServerCheckpointLogReaderV1() = default;
  int read_checkpoint_and_replay_log(
      blocksstable::ObSuperBlockV2& super_block, common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);

private:
  int read_checkpoint(blocksstable::ObSuperBlockV2& super_block, common::ObLogCursor& cursor,
      common::ObIArray<blocksstable::MacroBlockId>& meta_block_list);
  int set_replay_log_seq_num(blocksstable::ObSuperBlockV2& super_block);
  int replay_slog(common::ObLogCursor& replay_start_cursor);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_SERVER_CHECKPOINT_LOG_READER_OLD_H_
