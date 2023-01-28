// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_trans_bucket_writer.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_partition_calc.h"
#include "observer/table_load/ob_table_load_stat.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_trans_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace common::hash;
using namespace share::schema;
using namespace sql;
using namespace table;

ObTableLoadTransBucketWriter::SessionContext::SessionContext()
  : session_id_(0), allocator_("TLD_TB_SessCtx"), last_receive_sequence_no_(0)
{
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
    allocator_("TLD_TBWriter", OB_MALLOC_NORMAL_BLOCK_SIZE, param_.tenant_id_),
    is_partitioned_(false),
    session_ctx_array_(nullptr),
    ref_count_(0),
    is_flush_(false),
    is_inited_(false)
{
}

ObTableLoadTransBucketWriter::~ObTableLoadTransBucketWriter()
{
  if (nullptr != session_ctx_array_) {
    for (int64_t i = 0; i < param_.session_count_; i++) {
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
    if (OB_FAIL(init_session_ctx_array())) {
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
  if (OB_ISNULL(buf = allocator_.alloc(sizeof(SessionContext) * param_.session_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    session_ctx_array_ = new (buf) SessionContext[param_.session_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < param_.session_count_; ++i) {
      SessionContext *session_ctx = session_ctx_array_ + i;
      session_ctx->session_id_ = i + 1;
      session_ctx->allocator_.set_tenant_id(param_.tenant_id_);
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
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.session_count_)) {
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

int ObTableLoadTransBucketWriter::write(int32_t session_id, const ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadTransBucketWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.session_count_ || obj_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(obj_rows.count()));
  } else {
    SessionContext &session_ctx = session_ctx_array_[session_id - 1];
    if (!is_partitioned_) {
      if (OB_FAIL(write_for_non_partitioned(session_ctx, obj_rows))) {
        LOG_WARN("fail to write for non partitioned", KR(ret));
      }
    } else {
      if (OB_FAIL(write_for_partitioned(session_ctx, obj_rows))) {
        LOG_WARN("fail to write for partitioned", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t row_cnt = obj_rows.count();
      ATOMIC_AAF(&trans_ctx_->ctx_->job_stat_->coordinator.received_rows_, row_cnt);
      ATOMIC_AAF(&trans_ctx_->ctx_->coordinator_ctx_->result_info_.records_, row_cnt);
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
    bool need_write = false;
    if (OB_FAIL(load_bucket->add_row(session_ctx.partition_id_.tablet_id_,
                                     obj_rows.at(i), param_.column_count_,
                                     param_.batch_size_, need_write))) {
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
  ObArenaAllocator allocator("TLD_Misc", OB_MALLOC_NORMAL_BLOCK_SIZE, param_.tenant_id_);
  ObTableLoadPartitionCalcContext calc_ctx(obj_rows, param_.column_count_, allocator);
  if (OB_FAIL(coordinator_ctx_->partition_calc_.calc(calc_ctx))) {
    LOG_WARN("fail to calc partition", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < calc_ctx.partition_ids_.count(); ++i) {
    const ObTableLoadPartitionId &partition_id = calc_ctx.partition_ids_.at(i);
    ObTableLoadBucket *load_bucket = nullptr;
    bool need_write = false;
    if (OB_FAIL(get_load_bucket(session_ctx, partition_id, load_bucket))) {
      LOG_WARN("fail to get partition bucket", KR(ret), K(session_ctx.session_id_),
               K(partition_id));
    } else if (OB_FAIL(load_bucket->add_row(
                 partition_id.tablet_id_, obj_rows.at(i),
                 param_.column_count_, param_.batch_size_, need_write))) {
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
  } else if (OB_UNLIKELY(session_id < 1 || session_id > param_.session_count_)) {
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
  OB_TABLE_LOAD_STATISTICS_TIME_COST(get_part_bucket_time_us);
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
