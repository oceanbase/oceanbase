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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_autoinc_nextval.h"
#include "observer/table_load/ob_table_load_trans_bucket_writer.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_error_row_handler.h"
#include "observer/table_load/ob_table_load_obj_cast.h"
#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_ctx.h"
#include "share/ob_autoincrement_service.h"
#include "share/sequence/ob_sequence_cache.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace common::hash;
using namespace share::schema;
using namespace sql;
using namespace table;

ObTableLoadTransBucketWriter::SessionContext::SessionContext()
  : session_id_(0), allocator_("TLD_TB_SessCtx"), last_receive_sequence_no_(0)
{
  allocator_.set_tenant_id(MTL_ID());
  load_bucket_array_.set_tenant_id(MTL_ID());
}

ObTableLoadTransBucketWriter::SessionContext::~SessionContext()
{
  reset();
}

void ObTableLoadTransBucketWriter::SessionContext::reset()
{
  for (int64_t i = 0; i < load_bucket_array_.count(); ++i) {
    ObTableLoadBucket *load_bucket = load_bucket_array_.at(i);
    load_bucket->~ObTableLoadBucket();
    allocator_.free(load_bucket);
  }
  load_bucket_array_.reset();
  load_bucket_map_.reuse();
}

ObTableLoadTransBucketWriter::ObTableLoadTransBucketWriter(ObTableLoadTransCtx *trans_ctx)
  : trans_ctx_(trans_ctx),
    coordinator_ctx_(trans_ctx_->ctx_->coordinator_ctx_),
    param_(trans_ctx_->ctx_->param_),
    allocator_("TLD_TBWriter"),
    is_partitioned_(false),
    cast_mode_(CM_NONE),
    session_ctx_array_(nullptr),
    ref_count_(0),
    is_flush_(false),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadTransBucketWriter::~ObTableLoadTransBucketWriter()
{
  if (nullptr != session_ctx_array_) {
    for (int64_t i = 0; i < param_.write_session_count_; i++) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->~SessionContext();
    }
    allocator_.free(session_ctx_array_);
    session_ctx_array_ = nullptr;
  }
}

int ObTableLoadTransBucketWriter::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadTransBucketWriter init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(coordinator_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null coordinator ctx", KR(ret));
  } else {
    is_partitioned_ = coordinator_ctx_->ctx_->schema_.is_partitioned_table_;
    if (OB_FAIL(ObSQLUtils::get_default_cast_mode(coordinator_ctx_->ctx_->session_info_, cast_mode_))) {
      LOG_WARN("fail to get_default_cast_mode", KR(ret));
    } else if (OB_FAIL(init_session_ctx_array())) {
      LOG_WARN("fail to init session ctx array", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::init_session_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(SessionContext) * param_.write_session_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    session_ctx_array_ = new (buf) SessionContext[param_.write_session_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.write_session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->session_id_ = i + 1;
      if (!is_partitioned_) {
        ObTableLoadPartitionLocation::PartitionLocationInfo info;
        if (OB_UNLIKELY(1 != coordinator_ctx_->ctx_->schema_.partition_ids_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected partition id num in non partitioned table", KR(ret), "count",
                   coordinator_ctx_->ctx_->schema_.partition_ids_.count());
        } else if (FALSE_IT(session_ctx->partition_id_ =
                              coordinator_ctx_->ctx_->schema_.partition_ids_[0])) {
        } else if (OB_FAIL(coordinator_ctx_->partition_location_.get_leader(
                     session_ctx->partition_id_.tablet_id_, info))) {
          LOG_WARN("failed to get leader addr", K(ret));
        } else if (OB_FAIL(session_ctx->load_bucket_.init(info.leader_addr_))) {
          LOG_WARN("fail to init bucket", KR(ret));
        }
      } else {
        if (OB_FAIL(session_ctx->load_bucket_map_.create(1024, "TLD_BucketMap", "TLD_BucketMap",
                                                         param_.tenant_id_))) {
          LOG_WARN("fail to init partition bucket map", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::advance_sequence_no(int32_t session_id, uint64_t sequence_no,
                                                      ObTableLoadMutexGuard &guard)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransBucketWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.write_session_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(sequence_no));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (OB_FAIL(guard.init(session_ctx.mutex_))) {
      LOG_WARN("fail to init mutex guard", KR(ret));
    } else {
      // if (OB_UNLIKELY(sequence_no != session_ctx.last_receive_sequence_no_ + 1)) {
      //  if (OB_UNLIKELY(sequence_no != session_ctx.last_receive_sequence_no_)) {
      //    ret = OB_INVALID_ARGUMENT;
      //    LOG_WARN("invalid sequence no", KR(ret), K(sequence_no),
      //             K(session_ctx.last_receive_sequence_no_));
      //  } else {
      //    ret = OB_ENTRY_EXIST;
      //  }
      //} else {
      //  session_ctx.last_receive_sequence_no_ = sequence_no;
      //}
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::write(int32_t session_id, ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransBucketWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.write_session_count_ || obj_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(obj_rows.count()));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (!is_partitioned_) {
      if (OB_FAIL(write_for_non_partitioned(session_ctx, obj_rows))) {
        LOG_WARN("fail to write for non partitioned", KR(ret));
      }
    } else {
      if (coordinator_ctx_->partition_calc_.is_partition_with_autoinc_ &&
          OB_FAIL(handle_partition_with_autoinc_identity(
            session_ctx, obj_rows, coordinator_ctx_->ctx_->session_info_->get_sql_mode(),
            session_id))) {
        LOG_WARN("fail to handle partition column with autoincrement or identity", KR(ret));
      } else if (OB_FAIL(write_for_partitioned(session_ctx, obj_rows))) {
        LOG_WARN("fail to write for partitioned", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t row_cnt = obj_rows.count();
      ATOMIC_AAF(&trans_ctx_->ctx_->job_stat_->coordinator_.received_rows_, row_cnt);
      ATOMIC_AAF(&trans_ctx_->ctx_->coordinator_ctx_->result_info_.records_, row_cnt);
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::handle_partition_with_autoinc_identity(
  SessionContext &session_ctx, table::ObTableLoadObjRowArray &obj_rows, const uint64_t &sql_mode,
  int32_t session_id)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = obj_rows.count();
  ObArenaAllocator autoinc_allocator("TLD_Autoinc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObDataTypeCastParams cast_params(coordinator_ctx_->partition_calc_.session_info_->get_timezone_info());
  ObCastCtx cast_ctx(&autoinc_allocator, &cast_params, cast_mode_,
                      ObCharset::get_system_collation());
  ObTableLoadCastObjCtx cast_obj_ctx(param_, &(coordinator_ctx_->partition_calc_.time_cvrt_), &cast_ctx,
                                      true);
  ObObj out_obj;
  for (int64_t j = 0; OB_SUCC(ret) && j < row_count; ++j) {
    ObStorageDatum storage_datum;
    ObTableLoadObjRow &obj_row = obj_rows.at(j);
    out_obj.set_null();
    const ObTableLoadPartitionCalc::IndexAndType &index_and_type =
      coordinator_ctx_->partition_calc_.part_key_obj_index_.at(
        coordinator_ctx_->partition_calc_.partition_with_autoinc_idx_);
    const ObColumnSchemaV2 *column_schema = index_and_type.column_schema_;
    const int64_t obj_index = index_and_type.index_;
    if (OB_UNLIKELY(obj_index >= param_.column_count_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid length", KR(ret), K(obj_index), K(param_.column_count_));
    } else if (!obj_row.cells_[obj_index].is_null() &&
        OB_FAIL(ObTableLoadObjCaster::cast_obj(cast_obj_ctx, index_and_type.column_schema_,
                                                obj_row.cells_[obj_index], out_obj))) {
      LOG_WARN("fail to cast obj", KR(ret));
    } else if (OB_FAIL(storage_datum.from_obj_enhance(out_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(out_obj));
    } else if (column_schema->is_autoincrement() &&
                OB_FAIL(handle_autoinc_column(storage_datum,
                                              column_schema->get_meta_type().get_type_class(),
                                              session_id, sql_mode))) {
      LOG_WARN("fail to handle autoinc column", KR(ret), K(storage_datum));
    } else if (column_schema->is_identity_column() &&
                OB_FAIL(handle_identity_column(column_schema, storage_datum,
                                              autoinc_allocator))) {
      LOG_WARN("fail to handle identity column", KR(ret), K(storage_datum));
    } else if (OB_FAIL(storage_datum.to_obj_enhance(obj_row.cells_[obj_index],
                                                    column_schema->get_meta_type()))) {
      LOG_WARN("fail to obj enhance", KR(ret), K(obj_row.cells_[obj_index]));
    } else if (OB_FAIL(ob_write_obj(*(obj_row.get_allocator_handler()), obj_row.cells_[obj_index],
                                    obj_row.cells_[obj_index]))) {
      LOG_WARN("fail to deep copy obj", KR(ret), K(obj_row.cells_[obj_index]));
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::handle_autoinc_column(ObStorageDatum &datum,
                                                        const ObObjTypeClass &tc,
                                                        int32_t session_id,
                                                        const uint64_t &sql_mode)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadAutoincNextval::eval_nextval(
        &(coordinator_ctx_->session_ctx_array_[session_id - 1].autoinc_param_), datum, tc,
        sql_mode))) {
    LOG_WARN("fail to get auto increment next value", KR(ret));
  }
  return ret;
}

int ObTableLoadTransBucketWriter::handle_identity_column(const ObColumnSchemaV2 *column_schema,
                                                         ObStorageDatum &datum,
                                                         ObArenaAllocator &cast_allocator)
{
  int ret = OB_SUCCESS;
  if (column_schema->is_always_identity_column()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct-load does not support always identity column", KR(ret));
    FORWARD_USER_ERROR_MSG(ret, "direct-load does not support always identity column");
  } else if (column_schema->is_default_identity_column() && datum.is_null()) {
    ret = OB_ERR_INVALID_NOT_NULL_CONSTRAINT_ON_IDENTITY_COLUMN;
    LOG_WARN("default identity column has null value", KR(ret));
  } else if (column_schema->is_default_on_null_identity_column()) {
    ObSequenceValue seq_value;
    if (OB_FAIL(share::ObSequenceCache::get_instance().nextval(coordinator_ctx_->sequence_schema_,
                                                               cast_allocator, seq_value))) {
      LOG_WARN("fail get nextval for seq", KR(ret));
    } else if (datum.is_null()) {
      datum.set_number(seq_value.val());
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::write_for_non_partitioned(SessionContext &session_ctx,
                                                            const ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = obj_rows.count();
  ObTableLoadBucket *load_bucket = &session_ctx.load_bucket_;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
    const ObTableLoadObjRow &row = obj_rows.at(i);
    bool need_write = false;
    if (OB_UNLIKELY(row.count_ != param_.column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count not match", KR(ret), K(row.count_), K(param_.column_count_));
    } else if (OB_FAIL(load_bucket->add_row(session_ctx.partition_id_.tablet_id_,
                                            row,
                                            param_.batch_size_,
                                            need_write))) {
      LOG_WARN("fail to add row", KR(ret));
    } else if (need_write && OB_FAIL(write_load_bucket(session_ctx, load_bucket))) {
      LOG_WARN("fail to write partition bucket", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::write_for_partitioned(SessionContext &session_ctx,
                                                        const ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("TLD_Misc");
  const int64_t part_key_obj_count = coordinator_ctx_->partition_calc_.get_part_key_obj_count();
  ObArray<ObTableLoadPartitionId> partition_ids;
  ObArray<ObNewRow> part_keys;
  ObArray<int64_t> row_idxs;
  ObTableLoadErrorRowHandler *error_row_handler =
        coordinator_ctx_->error_row_handler_;
  allocator.set_tenant_id(MTL_ID());
  partition_ids.set_block_allocator(common::ModulePageAllocator(allocator));
  part_keys.set_block_allocator(common::ModulePageAllocator(allocator));
  row_idxs.set_block_allocator(common::ModulePageAllocator(allocator));
  for (int64_t i = 0; OB_SUCC(ret) && i < obj_rows.count(); ++i) {
    const ObTableLoadObjRow &row = obj_rows.at(i);
    ObNewRow part_key;
    part_key.count_ = part_key_obj_count;
    part_key.cells_ = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * part_key_obj_count));
    if (OB_UNLIKELY(row.count_ != param_.column_count_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count not match", KR(ret), K(row.count_), K(param_.column_count_));
    } else if (OB_ISNULL(part_key.cells_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->partition_calc_.get_part_key(obj_rows.at(i), part_key))) {
      LOG_WARN("fail to get part key", KR(ret));
    } else if (OB_FAIL(coordinator_ctx_->partition_calc_.cast_part_key(part_key, allocator))) {
      if (OB_FAIL(error_row_handler->handle_error_row(ret, part_key))) {
        LOG_WARN("failed to handle error row", K(ret), K(part_key));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(part_keys.push_back(part_key))) {
      LOG_WARN("fail to push back part key", KR(ret));
    } else if (OB_FAIL(row_idxs.push_back(i))) {
      LOG_WARN("fail to push back row idx", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(coordinator_ctx_->partition_calc_.get_partition_by_row(part_keys, partition_ids))) {
      LOG_WARN("fail to calc partition", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_idxs.count(); ++i) {
    const ObTableLoadPartitionId &partition_id = partition_ids.at(i);
    const ObTableLoadObjRow &row = obj_rows.at(row_idxs.at(i));
    ObTableLoadBucket *load_bucket = nullptr;
    bool need_write = false;
    if (OB_UNLIKELY(!partition_id.is_valid())) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      if (OB_FAIL(error_row_handler->handle_error_row(ret, part_keys.at(i)))) {
        LOG_WARN("failed to handle error row", K(ret), K(part_keys.at(i)));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(get_load_bucket(session_ctx, partition_id, load_bucket))) {
      LOG_WARN("fail to get partition bucket", KR(ret), K(session_ctx.session_id_),
               K(partition_id));
    } else if (OB_FAIL(load_bucket->add_row(partition_id.tablet_id_,
                                            row,
                                            param_.batch_size_,
                                            need_write))) {
      LOG_WARN("fail to add row", KR(ret));
    } else if (need_write && OB_FAIL(write_load_bucket(session_ctx, load_bucket))) {
      LOG_WARN("fail to write partition bucket", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::flush(int32_t session_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransBucketWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.write_session_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (!is_partitioned_) {
      ObTableLoadBucket *load_bucket = &session_ctx.load_bucket_;
      if (!(load_bucket->row_array_.empty())) {
        if (OB_FAIL(write_load_bucket(session_ctx, load_bucket))) {
          LOG_WARN("fail to write partition bucket", KR(ret), KPC(load_bucket));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < session_ctx.load_bucket_array_.count(); ++i) {
        ObTableLoadBucket *load_bucket = session_ctx.load_bucket_array_.at(i);
        if (!(load_bucket->row_array_.empty())) {
          if (OB_FAIL(write_load_bucket(session_ctx, load_bucket))) {
            LOG_WARN("fail to write partition bucket", KR(ret), KPC(load_bucket));
          }
        }
      }
    }
    // release memory
    session_ctx.reset();
  }
  return ret;
}

int ObTableLoadTransBucketWriter::get_load_bucket(SessionContext &session_ctx,
                                                  const ObTableLoadPartitionId &partition_id,
                                                  ObTableLoadBucket *&load_bucket)
{
  OB_TABLE_LOAD_STATISTICS_TIME_COST(DEBUG, get_part_bucket_time_us);
  int ret = OB_SUCCESS;
  load_bucket = nullptr;
  if (OB_UNLIKELY(!is_partitioned_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non partitioned table", KR(ret));
  } else {
    ObTableLoadPartitionLocation::PartitionLocationInfo info;
    if (OB_FAIL(coordinator_ctx_->partition_location_.get_leader(partition_id.tablet_id_, info))) {
      LOG_WARN("failed to get leader addr", K(ret));
    }
    if (OB_SUCC(ret)) {
      ret = session_ctx.load_bucket_map_.get_refactored(info.leader_addr_, load_bucket);
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_ISNULL(load_bucket = OB_NEWx(ObTableLoadBucket, (&session_ctx.allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to new partition bucket", KR(ret));
        } else if (OB_FAIL(load_bucket->init(info.leader_addr_))) {
          LOG_WARN("fail to init", KR(ret));
        } else if (OB_FAIL(session_ctx.load_bucket_map_.set_refactored(info.leader_addr_, load_bucket))) {
          LOG_WARN("fail to put bucket", KR(ret));
        } else if (OB_FAIL(session_ctx.load_bucket_array_.push_back(load_bucket))) {
          LOG_WARN("fail to push back bucket", KR(ret));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != load_bucket) {
            load_bucket->~ObTableLoadBucket();
            session_ctx.allocator_.free(load_bucket);
            load_bucket = nullptr;
          }
        }
      } else if (OB_FAIL(ret)) {
        LOG_WARN("fail to get bucket", KR(ret), K(partition_id));
      }
    }
  }
  return ret;
}

int ObTableLoadTransBucketWriter::write_load_bucket(SessionContext &session_ctx,
                                                    ObTableLoadBucket *load_bucket)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(load_bucket)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(load_bucket));
  } else {
    ObTableLoadCoordinator coordinator(coordinator_ctx_->ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (OB_FAIL(coordinator.write_peer_leader(
                 trans_ctx_->trans_id_, session_ctx.session_id_, ++load_bucket->sequence_no_,
                 load_bucket->row_array_, load_bucket->leader_addr_))) {
      LOG_WARN("fail to coordinator write peer leader", KR(ret), K(session_ctx.session_id_),
               KPC(load_bucket));
    }
  }
  if (OB_SUCC(ret)) {
    load_bucket->clear_data();
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
