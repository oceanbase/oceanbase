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

#include "common/object/ob_object.h"
#include "lib/allocator/page_arena.h"
#include "observer/table_load/ob_table_load_obj_cast.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/ob_table_load_time_convert.h"
#include "share/ob_autoincrement_param.h"
#include "share/object/ob_obj_cast.h"
#include "share/schema/ob_column_schema.h"
#include "share/table/ob_table_load_row_array.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/direct_load/ob_direct_load_table_store.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
} // namespace storage
namespace observer
{
class ObTableLoadParam;
class ObTableLoadTransCtx;
class ObTableLoadStoreCtx;

class ObTableLoadTransStore
{
public:
  ObTableLoadTransStore(ObTableLoadTransCtx *trans_ctx) : trans_ctx_(trans_ctx)
  {
    session_store_array_.set_tenant_id(MTL_ID());
  }
  ~ObTableLoadTransStore() { reset(); }
  int init();
  void reset();
  TO_STRING_KV(KP_(trans_ctx), K_(session_store_array));
  struct SessionStore
  {
    SessionStore() : session_id_(0), allocator_("TLD_SessStore")
    {
      allocator_.set_tenant_id(MTL_ID());
      partition_table_array_.set_block_allocator(ModulePageAllocator(allocator_));
    }
    int32_t session_id_;
    common::ObArenaAllocator allocator_;
    common::ObArray<storage::ObIDirectLoadPartitionTable *> partition_table_array_;
    TO_STRING_KV(K_(session_id), K_(partition_table_array));
  };
  ObTableLoadTransCtx *const trans_ctx_;
  common::ObArray<SessionStore *> session_store_array_;
};

class ObTableLoadTransStoreWriter
{
public:
  ObTableLoadTransStoreWriter(ObTableLoadTransStore *trans_store);
  ~ObTableLoadTransStoreWriter();
  int init();
  int advance_sequence_no(int32_t session_id, uint64_t sequence_no, ObTableLoadMutexGuard &guard);
  TO_STRING_KV(KP_(trans_ctx));
public:
  // 只在对应工作线程中调用, 串行执行
  int write(int32_t session_id, const table::ObTableLoadTabletObjRowArray &row_array);
  int px_write(const ObTabletID &tablet_id, const common::ObNewRow &row);
  int flush(int32_t session_id);
  int clean_up(int32_t session_id);
public:
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
private:
  class SessionContext;
  int init_session_ctx_array();
  int init_column_schemas_and_lob_info();
  int cast_row(common::ObArenaAllocator &cast_allocator, ObDataTypeCastParams cast_params,
               const common::ObNewRow &row, blocksstable::ObDatumRow &datum_row,
               int32_t session_id);
  int cast_column(common::ObArenaAllocator &cast_allocator,
                  ObDataTypeCastParams cast_params,
                  const share::schema::ObColumnSchemaV2 *column_schema,
                  const common::ObObj &obj,
                  blocksstable::ObStorageDatum &datum,
                  int32_t session_id);
  int handle_autoinc_column(const share::schema::ObColumnSchemaV2 *column_schema,
                            blocksstable::ObStorageDatum &datum,
                            const ObObjTypeClass &tc,
                            int32_t session_id);
  int handle_identity_column(const share::schema::ObColumnSchemaV2 *column_schema,
                             blocksstable::ObStorageDatum &datum,
                             common::ObArenaAllocator &cast_allocator);
  int write_row_to_table_store(storage::ObDirectLoadTableStore &table_store,
                               const common::ObTabletID &tablet_id,
                               const table::ObTableLoadSequenceNo &seq_no,
                               const blocksstable::ObDatumRow &datum_row);
private:
  ObTableLoadTransStore *const trans_store_;
  ObTableLoadTransCtx *const trans_ctx_;
  ObTableLoadStoreCtx *const store_ctx_;
  const ObTableLoadParam &param_;
  common::ObArenaAllocator allocator_;
  storage::ObDirectLoadTableDataDesc *table_data_desc_;
  common::ObCollationType collation_type_;
  common::ObCastMode cast_mode_;
  ObTableLoadTimeConverter time_cvrt_;
  // does not contain hidden primary key columns
  // and does not contain virtual generated columns
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  struct SessionContext
  {
    SessionContext(int32_t session_id, uint64_t tenant_id, ObDataTypeCastParams cast_params);
    ~SessionContext();
    const int32_t session_id_;
    blocksstable::ObDatumRow datum_row_;
    common::ObArenaAllocator cast_allocator_;
    ObDataTypeCastParams cast_params_;
    storage::ObDirectLoadTableStore table_store_;
    uint64_t last_receive_sequence_no_;
    char *extra_buf_;
    int64_t extra_buf_size_;
  };
  SessionContext *session_ctx_array_;
  int64_t lob_inrow_threshold_; // for incremental direct load
  int64_t ref_count_ CACHE_ALIGNED;
  bool is_inited_;
  ObSchemaGetterGuard schema_guard_;
};

} // namespace observer
} // namespace oceanbase
