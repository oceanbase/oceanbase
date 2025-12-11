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
#include "observer/table_load/ob_table_load_bucket.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadParam;
class ObTableLoadStoreCtx;
class ObTableLoadTransCtx;
class ObTableLoadCoordinatorCtx;

class ObTableLoadTransBucketWriter
{
public:
  ObTableLoadTransBucketWriter(ObTableLoadTransCtx *trans_ctx);
  ~ObTableLoadTransBucketWriter();
  int init();
  int advance_sequence_no(int32_t session_id, uint64_t sequence_no, ObTableLoadMutexGuard &guard);
  // 只在对应工作线程中调用, 串行执行
  int write(int32_t session_id, table::ObTableLoadObjRowArray &obj_rows);
  int flush(int32_t session_id);
private:
  class SessionContext;
  int init_session_ctx_array();
  int handle_partition_with_autoinc_identity(SessionContext &session_ctx,
                                             table::ObTableLoadObjRowArray &obj_rows,
                                             const uint64_t &sql_mode, int32_t session_id);
  int handle_autoinc_column(const share::schema::ObColumnSchemaV2 *column_schema,
                            const common::ObObj &obj,
                            common::ObObj &out_obj,
                            int32_t session_id,
                            const uint64_t &sql_mode);
  int handle_identity_column(const share::schema::ObColumnSchemaV2 *column_schema,
                             const common::ObObj &obj,
                             common::ObObj &out_obj,
                             common::ObArenaAllocator &cast_allocator);
  // 非分区表
  int write_for_non_partitioned(SessionContext &session_ctx,
                                const table::ObTableLoadObjRowArray &obj_rows);
  // 分区表
  int write_for_partitioned(SessionContext &session_ctx,
                            const table::ObTableLoadObjRowArray &obj_rows);
  int get_load_bucket(SessionContext &session_ctx, const table::ObTableLoadPartitionId &partition_id,
                      ObTableLoadBucket *&load_bucket);
  int write_load_bucket(SessionContext &session_ctx, ObTableLoadBucket *load_bucket);
private:
  static const int64_t WRITE_ROW_SIZE = 2LL * 1024 * 1024;
  ObTableLoadTransCtx *const trans_ctx_;
  ObTableLoadCoordinatorCtx *const coordinator_ctx_;
  const ObTableLoadParam &param_;
  common::ObArenaAllocator allocator_;
  bool is_partitioned_;
  int64_t column_count_;
  common::ObCastMode cast_mode_;
  struct SessionContext
  {
    SessionContext();
    ~SessionContext();
    void release();
    int32_t session_id_;
    // 以下参数只在对应工作线程中访问
    common::ObArenaAllocator allocator_;
    // for non-partitioned table
    table::ObTableLoadPartitionId partition_id_;
    ObTableLoadBucket load_bucket_;
    // for partitioned table
    common::hash::ObHashMap<common::ObAddr, ObTableLoadBucket *> load_bucket_map_;
    common::ObArray<ObTableLoadBucket *> load_bucket_array_;
    // 以下参数加锁访问
    lib::ObMutex mutex_;
    uint64_t last_receive_sequence_no_;
    bool is_flush_;
  };
  SessionContext *session_ctx_array_;
  bool is_flush_;
  bool is_inited_;
};

}  // namespace observer
}  // namespace oceanbase
