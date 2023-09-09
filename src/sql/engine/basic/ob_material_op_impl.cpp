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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_material_op_impl.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
ObMaterialOpImpl::ObMaterialOpImpl(ObMonitorNode &op_monitor_info, ObSqlWorkAreaProfile &profile)
: inited_(false),
  got_first_row_(false),
  tenant_id_(OB_INVALID_ID),
  exec_ctx_(nullptr),
  mem_context_(nullptr),
  datum_store_(ObModIds::OB_HASH_NODE_GROUP_ROWS),
  datum_store_it_(),
  eval_ctx_(nullptr),
  profile_(profile),
  sql_mem_processor_(profile, op_monitor_info),
  input_rows_(OB_INVALID_ID),
  input_width_(OB_INVALID_ID),
  op_type_(PHY_INVALID),
  op_id_(UINT64_MAX)
{}

ObMaterialOpImpl::~ObMaterialOpImpl()
{
  reset();
  if (nullptr != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = nullptr;
  }
}

void ObMaterialOpImpl::reset()
{
  sql_mem_processor_.unregister_profile();
  io_event_observer_ = nullptr;
  datum_store_.reset();
  datum_store_it_.reset();
  got_first_row_ = false;
  inited_ = false;
  // can not destroy mem_entify here, the memory may hold by %iter_ or %datum_store_
}

int ObMaterialOpImpl::init(const uint64_t tenant_id,
                          ObEvalCtx *eval_ctx,
                          ObExecContext *exec_ctx,
                          ObIOEventObserver *observer,
                          const int64_t default_block_size)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice");
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id));
  } else if (OB_ISNULL(eval_ctx) || OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get null argument", K(eval_ctx), K(exec_ctx));
  } else {
    tenant_id_ = tenant_id;
    eval_ctx_ = eval_ctx;
    exec_ctx_ = exec_ctx;
    io_event_observer_ = observer;
    if (OB_ISNULL(mem_context_)) {
      lib::ContextParam param;
      param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("create entity failed");
      } else if (OB_ISNULL(mem_context_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null memory entity returned");
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(datum_store_.init(UINT64_MAX, tenant_id_, ObCtxIds::WORK_AREA))) {
      LOG_WARN("init row store failed");
    } else {
      datum_store_.set_allocator(mem_context_->get_malloc_allocator());
      datum_store_.set_callback(&sql_mem_processor_);
      datum_store_.set_io_event_observer(io_event_observer_);
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int ObMaterialOpImpl::add_row(const common::ObIArray<ObExpr*> &exprs,
                              const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed");
  } else if (OB_FAIL(datum_store_.add_row(exprs, eval_ctx_, &sr))) {
    LOG_WARN("add store row failed", K(mem_context_->used()), K(get_memory_limit()));
  } else {
    store_row = sr;
  }
  return ret;
}

int ObMaterialOpImpl::add_row(const ObChunkDatumStore::StoredRow &src_sr,
                              const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed");
  } else if (OB_FAIL(datum_store_.add_row(src_sr, &sr))) {
    LOG_WARN("add store row failed", K(mem_context_->used()), K(get_memory_limit()));
  } else {
    store_row = sr;
  }
  return ret;
}

int ObMaterialOpImpl::add_row(const ObDatum *src_datums,
                              const int64_t datum_cnt,
                              const int64_t extra_size,
                              const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *sr = NULL;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed");
  } else if (OB_FAIL(datum_store_.add_row(src_datums, datum_cnt, extra_size, &sr))) {
    LOG_WARN("add store row failed", K(mem_context_->used()), K(get_memory_limit()));
  } else {
    store_row = sr;
  }
  return ret;
}

int ObMaterialOpImpl::add_batch(const common::ObIArray<ObExpr *> &exprs,
                                const ObBitVector &skip,
                                const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = -1;
  if (OB_FAIL(before_add_row())) {
    LOG_WARN("before add row process failed");
  } else if (OB_FAIL(datum_store_.add_batch(exprs, *eval_ctx_, skip, batch_size, read_rows))) {
    LOG_WARN("add store row failed", K(mem_context_->used()), K(get_memory_limit()));
  }
  return ret;
}

int ObMaterialOpImpl::finish_add_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(datum_store_.finish_add_row(false))) {
    LOG_WARN("failed to finish add row to row store");
  } else if (OB_FAIL(datum_store_.begin(datum_store_it_))) {
    LOG_WARN("failed to begin iterator for chunk row store");
  }
  return ret;
}


int ObMaterialOpImpl::before_add_row()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init");
  } else if (OB_UNLIKELY(!got_first_row_)) {
    int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
    if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                        tenant_id_, size, op_type_,
                                        op_id_, exec_ctx_))) {
      LOG_WARN("failed to init sql memory manager processor", K(ret));
    } else {
      got_first_row_ = true;
      datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
      LOG_TRACE("trace init sql mem mgr for material", K(size), K(input_width_),
        K(profile_.get_cache_size()), K(profile_.get_expect_size()));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(process_dump())) {
      LOG_WARN("failed to process dump", K(ret));
    }
  }
  return ret;
}

int ObMaterialOpImpl::process_dump()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  bool dumped = false;
  UNUSED(updated);
  // 对于material由于需要保序，所以dump实现方式是，会dump掉所有的，同时可以保留最后的内存数据。目前选择一定是从前往后dump
  //      还一种方式实现是，dump剩下到最大内存量的数据，以后写入数据则必须dump，但这种方式必须对内存进行伸缩处理
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
      &mem_context_->get_malloc_allocator(),
      [&](int64_t cur_cnt){ return datum_store_.get_row_cnt_in_memory() > cur_cnt; },
      updated))) {
    LOG_WARN("failed to update max available memory size periodically", K(ret));
  } else if (need_dump() && GCONF.is_sql_operator_dump_enabled()
          && OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &mem_context_->get_malloc_allocator(),
            [&](int64_t max_memory_size) {
              return sql_mem_processor_.get_data_size() > max_memory_size;
            },
            dumped, sql_mem_processor_.get_data_size()))) {
    LOG_WARN("failed to extend max memory size", K(ret));
  } else if (dumped) {
    if (OB_FAIL(datum_store_.dump(false, true))) {
      LOG_WARN("failed to dump row store", K(ret));
    } else {
      sql_mem_processor_.reset();
      sql_mem_processor_.set_number_pass(1);
      LOG_TRACE("trace material dump",
        K(sql_mem_processor_.get_data_size()),
        K(datum_store_.get_row_cnt_in_memory()),
        K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObMaterialOpImpl::get_next_row(const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(datum_store_it_.get_next_row(exprs, *eval_ctx_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from row store failed");
    }
  }
  return ret;
}

int ObMaterialOpImpl::get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(datum_store_it_.get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get row from row store failed");
    }
  }
  return ret;
}

int ObMaterialOpImpl::get_next_batch(const common::ObIArray<ObExpr*> &exprs,
                                     const int64_t max_rows,
                                     int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(datum_store_it_.get_next_batch(exprs, *eval_ctx_, max_rows, read_rows))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next batch from datum store");
    }
  }
  return ret;
}

int ObMaterialOpImpl::rescan()
{
  int ret = OB_SUCCESS;
  got_first_row_ = false;
  datum_store_it_.reset();
  datum_store_.reset();
  inited_ = false;
  return ret;
}

int ObMaterialOpImpl::reuse()
{
  int ret = OB_SUCCESS;
  got_first_row_ = false;
  datum_store_it_.reset();
  datum_store_.reset();
  return ret;
}

int ObMaterialOpImpl::rewind()
{
  int ret = OB_SUCCESS;
  datum_store_it_.reset();
  return ret;
}

}
}