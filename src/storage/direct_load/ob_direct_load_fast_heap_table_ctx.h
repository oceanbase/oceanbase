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

#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/table/ob_table_load_define.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadFastHeapTableTabletContext;

class ObDirectLoadFastHeapTableContext
{
public:
  ObDirectLoadFastHeapTableContext();
  ~ObDirectLoadFastHeapTableContext();
  int init(uint64_t tenant_id,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
           int64_t reserved_parallel);
  int get_tablet_context(const common::ObTabletID &tablet_id,
                         ObDirectLoadFastHeapTableTabletContext *&tablet_ctx) const;
private:
  int create_all_tablet_contexts(uint64_t tenant_id,
                                 const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
                                 const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids,
                                 int64_t reserved_parallel);
  typedef common::hash::ObHashMap<common::ObTabletID, ObDirectLoadFastHeapTableTabletContext *>
    TABLET_CTX_MAP;
  common::ObArenaAllocator allocator_;
  TABLET_CTX_MAP tablet_ctx_map_;
  bool is_inited_;
};

struct ObDirectLoadFastHeapTableTabletWriteCtx
{
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_interval_;
  TO_STRING_KV(K_(start_seq), K_(pk_interval));
};

class ObDirectLoadFastHeapTableTabletContext
{
  static const int64_t PK_CACHE_SIZE = 1000000;
  static const int64_t WRITE_BATCH_SIZE = 100000;
public:
  ObDirectLoadFastHeapTableTabletContext();
  int init(uint64_t tenant_id,
           const common::ObTabletID &tablet_id,
           const common::ObTabletID &target_tablet_id,
           int64_t reserved_parallel);
  int get_write_ctx(ObDirectLoadFastHeapTableTabletWriteCtx &write_ctx);
  const common::ObTabletID &get_target_tablet_id()
  {
    return target_tablet_id_;
  }
private:
  int refresh_pk_cache();
private:
  uint64_t tenant_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID target_tablet_id_;
  lib::ObMutex mutex_;
  blocksstable::ObMacroDataSeq start_seq_;
  share::ObTabletCacheInterval pk_cache_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
