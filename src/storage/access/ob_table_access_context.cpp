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

#define USING_LOG_PREFIX STORAGE
#include "ob_table_access_context.h"
#include "ob_dml_param.h"
#include "share/ob_lob_access_utils.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{
int ObTableAccessContext::init_column_scale_info(ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  if (!lib::is_oracle_mode()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode", K(ret));
  } else if (OB_ISNULL(scan_param.table_param_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected table parameter to init column scale info", K(ret), KPC(scan_param.table_param_));
  } else {
    const ObIArray<share::schema::ObColumnParam *> *out_col_param = scan_param.table_param_->get_read_info().get_columns();
    const ObIArray<int32_t> *out_col_project = &scan_param.table_param_->get_output_projector();
    for (int64_t i = 0; OB_SUCC(ret) && i < out_col_project->count(); ++i) {
      share::schema::ObColumnParam *col_param = NULL;
      int32_t idx = out_col_project->at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= out_col_param->count())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid project idx", K(ret), K(idx), K(out_col_param->count()));
      } else if (OB_ISNULL(col_param = out_col_param->at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "column param is null", K(ret), K(idx));
      }
    }
    STORAGE_LOG(DEBUG, "check need fill scale", K(ret), KPC(out_col_project), KPC(out_col_param));
  }
  return ret;
}


ObTableAccessContext::ObTableAccessContext()
  : is_inited_(false),
    use_fuse_row_cache_(false),
    need_scn_(false),
    timeout_(0),
    query_flag_(),
    sql_mode_(0),
    store_ctx_(NULL),
    limit_param_(NULL),
    stmt_allocator_(NULL),
    allocator_(NULL),
    range_allocator_(nullptr),
    scan_mem_(nullptr),
    table_scan_stat_(NULL),
    table_store_stat_(),
    out_cnt_(0),
    trans_version_range_(),
    range_array_pos_(nullptr),
    merge_scn_(),
    lob_allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    lob_locator_helper_(nullptr),
    iter_pool_(nullptr),
    block_row_store_(nullptr),
    io_callback_(nullptr),
    trans_state_mgr_(nullptr)
{
  merge_scn_.set_max();
}

ObTableAccessContext::~ObTableAccessContext()
{
  reset_lob_locator_helper();
}

int ObTableAccessContext::build_lob_locator_helper(ObTableScanParam &scan_param,
                                                   const ObStoreCtx &ctx,
                                                   const ObVersionRange &trans_version_range)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // reset lob allocator for mem used;
  reset_lob_locator_helper();
  // locator is used for all types of lobs
  if (OB_UNLIKELY(nullptr == scan_param.table_param_ || nullptr == stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to build lob locator helper", K(ret), K(scan_param), KP_(stmt_allocator));
  } else if (!scan_param.table_param_->use_lob_locator()) {
    lob_locator_helper_ = nullptr;
  } else if (!scan_param.table_param_->enable_lob_locator_v2() && !lib::is_oracle_mode()) {
    // if lob locator v2 is enabled, locator will be used for all types of lobs, including mysql mode
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected tenant mode", K(ret), K(lib::is_oracle_mode()));
  } else if (OB_ISNULL(buf = lob_allocator_.alloc(sizeof(ObLobLocatorHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObLobLocatorHelper", K(ret));
  } else if (FALSE_IT(lob_locator_helper_ = new (buf) ObLobLocatorHelper())) {
  } else if (OB_FAIL(lob_locator_helper_->init(scan_param,
                                               ctx,
                                               scan_param.ls_id_,
                                               trans_version_range.snapshot_version_))) {
    STORAGE_LOG(WARN, "Failed to init lob locator helper",
      K(ret), KPC(scan_param.table_param_), K(scan_param.ls_id_), K(trans_version_range));
    reset_lob_locator_helper();
  } else {
    STORAGE_LOG(DEBUG, "succ to init lob locator helper", KPC(lob_locator_helper_));
  }

  return ret;
}

int ObTableAccessContext::build_lob_locator_helper(const ObStoreCtx &ctx,
                                                   const ObVersionRange &trans_version_range)
{
  // lob locator for internal routine, no rowid
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  // reset lob allocator for mem used;
  reset_lob_locator_helper();
  // locator is used for all types of lobs
  if (!ob_enable_lob_locator_v2()) {
    // do nothing
  } else if (OB_ISNULL(buf = lob_allocator_.alloc(sizeof(ObLobLocatorHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "Failed to alloc memory for ObLobLocatorHelper", K(ret));
  } else if (FALSE_IT(lob_locator_helper_ = new (buf) ObLobLocatorHelper())) {
  } else if (OB_FAIL(lob_locator_helper_->init(table_store_stat_,
                                               ctx,
                                               ls_id_,
                                               trans_version_range.snapshot_version_))) {
    STORAGE_LOG(WARN, "Failed to init lob locator helper limit", K(ret), K(ls_id_), K(trans_version_range));
    reset_lob_locator_helper();
  } else {
    STORAGE_LOG(DEBUG, "succ to init lob locator helper", KPC(lob_locator_helper_));
  }

  return ret;
}

int ObTableAccessContext::init(ObTableScanParam &scan_param,
                               ObStoreCtx &ctx,
                               const ObVersionRange &trans_version_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_LIKELY(!scan_param.sample_info_.is_block_sample())) {
    allocator_ = scan_param.scan_allocator_;
  } else {
    if (scan_mem_ == nullptr) {
      lib::ContextParam param;
      param.set_mem_attr(scan_param.tenant_id_,
                         ObModIds::OB_TABLE_SCAN_ITER,
                         ObCtxIds::DEFAULT_CTX_ID)
        .set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(scan_mem_, param))) {
        LOG_WARN("fail to create entity", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      allocator_ = &scan_mem_->get_arena_allocator();
    }
  }
  if (OB_SUCC(ret)) {
    stmt_allocator_ = scan_param.allocator_;
    range_allocator_ = nullptr;
    ls_id_ = scan_param.ls_id_;
    tablet_id_ = scan_param.tablet_id_;
    query_flag_ = scan_param.scan_flag_;
    sql_mode_ = scan_param.sql_mode_;
    timeout_ = scan_param.timeout_;
    store_ctx_ = &ctx;
    table_scan_stat_ = &scan_param.main_table_scan_stat_;
    limit_param_ = scan_param.limit_param_.is_valid() ? &scan_param.limit_param_ : NULL;
    table_scan_stat_->reset();
    table_store_stat_.ls_id_ = scan_param.ls_id_;
    table_store_stat_.tablet_id_ = scan_param.tablet_id_;
    table_store_stat_.table_id_ = scan_param.index_id_;
    trans_version_range_ = trans_version_range;
    need_scn_ = scan_param.need_scn_;
    range_array_pos_ = &scan_param.range_array_pos_;
    use_fuse_row_cache_ = false;
    if(OB_FAIL(build_lob_locator_helper(scan_param, ctx, trans_version_range))) {
      STORAGE_LOG(WARN, "Failed to build lob locator helper", K(ret));
      // new static engine do not need fill scale
    } else if (lib::is_oracle_mode() && OB_ISNULL(scan_param.output_exprs_)
        && OB_FAIL(init_column_scale_info(scan_param))) {
      LOG_WARN("init column scale info failed", K(ret), K(scan_param));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableAccessContext::init(const common::ObQueryFlag &query_flag,
                               ObStoreCtx &ctx,
                               ObIAllocator &allocator,
                               ObIAllocator &stmt_allocator,
                               const ObVersionRange &trans_version_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    query_flag_ = query_flag;
    use_fuse_row_cache_ = false;
    store_ctx_ = &ctx;
    timeout_ = ctx.timeout_;
    allocator_ = &allocator;
    stmt_allocator_ = &stmt_allocator;
    range_allocator_ = nullptr;
    trans_version_range_ = trans_version_range;
    ls_id_ = ctx.ls_id_;
    tablet_id_ = ctx.tablet_id_;
    table_store_stat_.ls_id_ = ctx.ls_id_;
    table_store_stat_.tablet_id_ = ctx.tablet_id_;
    table_store_stat_.table_id_ = ctx.tablet_id_.id(); // TODO  (yuanzhe) remove table_id in virtual table
    // handle lob types without ObTableScanParam:
    // 1. use lob locator instead of full lob data
    // 2. without rowkey, since need not send result to dbmslob/client
    // 3. tablet id/ table id here maybe invalid, call update_lob_locator_ctx to fix
    // 4. only init lob locator helper when nessary?
    if (OB_FAIL(build_lob_locator_helper(ctx, trans_version_range))) {
      STORAGE_LOG(WARN, "Failed to build lob locator helper", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}
int ObTableAccessContext::init(const common::ObQueryFlag &query_flag,
                               ObStoreCtx &ctx,
                               common::ObIAllocator &allocator,
                               const common::ObVersionRange &trans_version_range)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else {
    query_flag_ = query_flag;
    use_fuse_row_cache_ = false;
    store_ctx_ = &ctx;
    timeout_ = ctx.timeout_;
    allocator_ = &allocator;
    stmt_allocator_ = &allocator;
    range_allocator_ = nullptr;
    trans_version_range_ = trans_version_range;
    ls_id_ = ctx.ls_id_;
    tablet_id_ = ctx.tablet_id_;
    table_store_stat_.ls_id_ = ctx.ls_id_;
    table_store_stat_.tablet_id_ = ctx.tablet_id_;
    table_store_stat_.table_id_ = ctx.tablet_id_.id(); // TODO  (yuanzhe) remove table_id in virtual table
    lob_locator_helper_ = nullptr;
    is_inited_ = true;
  }
  return ret;
}

void ObTableAccessContext::reset()
{
  is_inited_ = false;
  timeout_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  store_ctx_ = NULL;
  limit_param_ = NULL;
  reset_lob_locator_helper();
  stmt_allocator_ = NULL;
  if (NULL != scan_mem_) {
    DESTROY_CONTEXT(scan_mem_);
    scan_mem_ = NULL;
  }
  allocator_ = NULL;
  range_allocator_ = nullptr;
  table_scan_stat_ = NULL;
  table_store_stat_.reset();
  out_cnt_ = 0;
  trans_version_range_.reset();
  use_fuse_row_cache_ = false;
  range_array_pos_ = nullptr;
  iter_pool_ = nullptr;
  block_row_store_ = nullptr;
}

void ObTableAccessContext::reuse()
{
  is_inited_ = false;
  timeout_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  query_flag_.reset();
  sql_mode_ = 0;
  store_ctx_ = NULL;
  limit_param_ = NULL;
  reset_lob_locator_helper();
  stmt_allocator_ = NULL;
  if (NULL != scan_mem_) {
    scan_mem_->reuse_arena();
  }
  allocator_ = NULL;
  range_allocator_ = nullptr;
  table_scan_stat_ = NULL;
  out_cnt_ = 0;
  trans_version_range_.reset();
  use_fuse_row_cache_ = false;
  range_array_pos_ = nullptr;
  iter_pool_ = nullptr;
  block_row_store_ = nullptr;
}

} // namespace storage
} // namespace oceanbase
