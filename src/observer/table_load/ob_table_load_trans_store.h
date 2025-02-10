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
#include "storage/direct_load/ob_direct_load_datum_row.h"
#include "storage/direct_load/ob_direct_load_i_table.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTableDataDesc;
class ObDirectLoadITable;
class ObIDirectLoadPartitionTableBuilder;
class ObDirectLoadInsertTableBatchRowDirectWriter;
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
    SessionStore() : session_id_(0)
    {
    }
    int32_t session_id_;
    ObDirectLoadTableHandleArray tables_handle_;
    TO_STRING_KV(K_(session_id), K_(tables_handle));
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
  int px_write(common::ObIVector *tablet_id_vector,
               const ObIArray<common::ObIVector *> &vectors,
               const sql::ObBatchRows &batch_rows,
               int64_t &affected_rows);
  int cast_row(int32_t session_id,
               const table::ObTableLoadObjRow &obj_row,
               const ObDirectLoadDatumRow *&datum_row);
  int flush(int32_t session_id);
  int clean_up(int32_t session_id);
public:
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
private:
  int init_session_ctx_array();
  int init_column_schemas_and_lob_info();
  int cast_column(common::ObArenaAllocator &cast_allocator,
                  ObDataTypeCastParams cast_params,
                  const share::schema::ObColumnSchemaV2 *column_schema,
                  const common::ObObj &obj,
                  blocksstable::ObStorageDatum &datum,
                  int32_t session_id);
  int cast_row(common::ObArenaAllocator &cast_allocator,
               ObDataTypeCastParams cast_params,
               const table::ObTableLoadObjRow &obj_row,
               ObDirectLoadDatumRow &datum_row,
               int32_t session_id);
  int handle_autoinc_column(const share::schema::ObColumnSchemaV2 *column_schema,
                            const common::ObObj &obj,
                            blocksstable::ObStorageDatum &datum,
                            int32_t session_id);
  int handle_identity_column(const share::schema::ObColumnSchemaV2 *column_schema,
                             const common::ObObj &obj,
                             common::ObObj &out_obj,
                             common::ObArenaAllocator &cast_allocator);

private:
  class IWriter
  {
  public:
    IWriter() = default;
    virtual ~IWriter() = default;
    virtual void reset() = 0;
    virtual int append_row(const common::ObTabletID &tablet_id,
                           const ObDirectLoadDatumRow &datum_row) = 0;
    virtual int append_batch(common::ObIVector *tablet_id_vector,
                             const ObIArray<common::ObIVector *> &vectors,
                             const sql::ObBatchRows &batch_rows,
                             int64_t &affected_rows) = 0;
    virtual int close() = 0;
  };

  class StoreWriter : public IWriter
  {
  public:
    StoreWriter();
    virtual ~StoreWriter();
    void reset() override;
    int init(ObTableLoadStoreCtx *store_ctx,
             ObTableLoadTransStore *trans_store,
             int32_t session_id);
    int append_row(const common::ObTabletID &tablet_id,
                   const ObDirectLoadDatumRow &datum_row) override;
    int append_batch(common::ObIVector *tablet_id_vector,
                     const ObIArray<common::ObIVector *> &vectors,
                     const sql::ObBatchRows &batch_rows,
                     int64_t &affected_rows) override;
    int close() override;
  private:
    int new_table_builder(const common::ObTabletID &tablet_id,
                          ObIDirectLoadPartitionTableBuilder *&table_builder);
    int get_table_builder(const common::ObTabletID &tablet_id,
                          ObIDirectLoadPartitionTableBuilder *&table_builder);
    int inner_append_row(const common::ObTabletID &tablet_id,
                         const ObDirectLoadDatumRow &datum_row);
  private:
    typedef common::hash::ObHashMap<common::ObTabletID, ObIDirectLoadPartitionTableBuilder *>
      TableBuilderMap;
    ObTableLoadStoreCtx *store_ctx_;
    ObTableLoadTransStore *trans_store_;
    int32_t session_id_;
    ObArenaAllocator allocator_;
    TableBuilderMap table_builder_map_;
    ObSEArray<ObIDirectLoadPartitionTableBuilder *, 1> table_builders_;
    ObDirectLoadDatumRow datum_row_;
    bool is_single_part_;
    bool is_closed_;
    bool is_inited_;
  };

  class DirectWriter : public IWriter
  {
  public:
    DirectWriter();
    virtual ~DirectWriter();
    void reset() override;
    int init(ObTableLoadStoreCtx *store_ctx);
    int append_row(const common::ObTabletID &tablet_id,
                   const ObDirectLoadDatumRow &datum_row) override;
    int append_batch(common::ObIVector *tablet_id_vector,
                     const ObIArray<common::ObIVector *> &vectors,
                     const sql::ObBatchRows &batch_rows,
                     int64_t &affected_rows) override;
    int close() override;
  private:
    int new_batch_writer(const common::ObTabletID &tablet_id,
                          ObDirectLoadInsertTableBatchRowDirectWriter *&batch_writer);
    int get_batch_writer(const common::ObTabletID &tablet_id,
                          ObDirectLoadInsertTableBatchRowDirectWriter *&batch_writer);
  private:
    typedef common::hash::ObHashMap<common::ObTabletID, ObDirectLoadInsertTableBatchRowDirectWriter *>
      BatchWriterMap;
    ObTableLoadStoreCtx *store_ctx_;
    ObArenaAllocator allocator_;
    ObArenaAllocator lob_allocator_;
    BatchWriterMap batch_writer_map_;
    ObSEArray<ObDirectLoadInsertTableBatchRowDirectWriter *, 1> batch_writers_;
    int64_t max_batch_size_;
    bool is_single_part_;
    bool is_closed_;
    bool is_inited_;
  };

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
    ObDirectLoadDatumRow datum_row_;
    common::ObArenaAllocator cast_allocator_;
    ObDataTypeCastParams cast_params_;
    IWriter *writer_;
    uint64_t last_receive_sequence_no_;
  };
  SessionContext *session_ctx_array_;
  int64_t lob_inrow_threshold_; // for incremental direct load
  int64_t ref_count_ CACHE_ALIGNED;
  bool is_inited_;
  ObSchemaGetterGuard schema_guard_;
};

} // namespace observer
} // namespace oceanbase
