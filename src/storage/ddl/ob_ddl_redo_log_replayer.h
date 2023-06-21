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

#ifndef OCEANBASE_STORAGE_OB_DDL_REDO_LOG_REPLAYER_H
#define OCEANBASE_STORAGE_OB_DDL_REDO_LOG_REPLAYER_H

#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTabletHandle;

class ObDDLRedoLogReplayer final
{
public:
  ObDDLRedoLogReplayer();
  ~ObDDLRedoLogReplayer();
  int init(ObLS *ls);
  void reset() { destroy(); }
  int replay_start(const ObDDLStartLog &log, const share::SCN &scn);
  int replay_redo(const ObDDLRedoLog &log, const share::SCN &scn);
  int replay_commit(const ObDDLCommitLog &log, const share::SCN &scn);
private:
  void destroy();

private:
  static const int64_t TOTAL_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t HOLD_LIMIT = 10 * 1024 * 1024 * 1024L;
  static const int64_t DEFAULT_HASH_BUCKET_COUNT = 100;
  static const int64_t DEFAULT_ID_MAP_HASH_BUCKET_COUNT = 1543;
  static const int64_t RETRY_INTERVAL = 100 * 1000L; // 100ms
  bool is_inited_;
  ObLS *ls_;
  common::ObConcurrentFIFOAllocator allocator_;
  common::ObBucketLock bucket_lock_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_DDL_REDO_LOG_REPLAYER_H

