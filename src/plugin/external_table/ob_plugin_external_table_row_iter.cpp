/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include <string>

#include "plugin/external_table/ob_plugin_external_table_row_iter.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "plugin/sys/ob_plugin_helper.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "lib/alloc/alloc_struct.h"


namespace oceanbase {
namespace plugin {

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::lib;
using namespace oceanbase::share;

using namespace std;
using namespace arrow;
using namespace oceanbase::plugin::external;

const ObLabel MEMORY_LABEL = ObLabel("PluginIter");

////////////////////////////////////////////////////////////////////////////////
// ObPluginExternalTableRowIterator
ObPluginExternalTableRowIterator::ObPluginExternalTableRowIterator()
    : arena_allocator_(ob_plugin_mem_attr())
{}

ObPluginExternalTableRowIterator::~ObPluginExternalTableRowIterator()
{
  destroy();
}

int ObPluginExternalTableRowIterator::init(const ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("plugin external table row iter init", KP(this), KPC(scan_param), KCSTRING(lbt()));
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->ext_file_column_exprs_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(scan_param), K(ret));
  } else if (OB_FAIL(sql::ObExternalTableRowIterator::init(scan_param))) {
    LOG_WARN("failed to init external table row iter", K(ret), KPC(scan_param));
  } else if (FALSE_IT(allocator_.set_attr(ObMemAttr(scan_param->tenant_id_, MEMORY_LABEL)))) {
  } else if (OB_FAIL(scan_param->external_file_format_.plugin_format_.create_engine(arena_allocator_, data_engine_))) {
    LOG_WARN("failed to create data engine", K(ret));
  }

  if (OB_SUCC(ret) && OB_FAIL(open_scanner(scan_param))) {
    LOG_WARN("failed to open scanner", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
    ObExpr *expr = scan_param->ext_file_column_exprs_->at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(i), K(ret));
    } else if (expr->type_ != T_PSEUDO_PARTITION_LIST_COL &&
               expr->type_ != T_PSEUDO_EXTERNAL_FILE_COL) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("got unsupported expr type", K(expr->type_));
    }
  }

  if (OB_SUCC(ret) && OB_ISNULL(bit_vector_cache_)) {
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }

  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObPluginExternalTableRowIterator::rescan(const ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  // reinit used by rescan
  if (OB_ISNULL(data_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    reset();

    if (OB_FAIL(open_scanner(scan_param))) {
      LOG_WARN("failed to init external scan param", K(ret));
    }
  }
  return ret;
}

void ObPluginExternalTableRowIterator::reset()
{
  if (OB_NOT_NULL(data_engine_) && OB_NOT_NULL(scanner_)) {
    OB_DELETEx(ObExternalScanner, &allocator_, scanner_);
    scanner_ = nullptr;
  }
  external_scan_param_.reset();
  iterator_state_.reuse();
}

void ObPluginExternalTableRowIterator::destroy()
{
  reset();

  if (OB_NOT_NULL(data_engine_)) {
    OB_DELETEx(ObExternalDataEngine, &arena_allocator_, data_engine_);
    data_engine_ = nullptr;
  }

  if (OB_NOT_NULL(bit_vector_cache_)) {
    allocator_.free(bit_vector_cache_);
    bit_vector_cache_ = nullptr;
  }

  for (int64_t i = 0; i < plugin_column_loaders_.count(); i++) {
    ObArrowDataLoader *loader = plugin_column_loaders_.at(i);
    if (OB_NOT_NULL(loader)) {
      loader->destroy();
      OB_DELETEx(ObArrowDataLoader, &arena_allocator_, loader);
    }
    plugin_column_loaders_[i] = nullptr;
  }
  plugin_column_index_map_.reset();
  plugin_column_loaders_.reset();
  external_scan_param_.reset();
  iterator_state_.reuse();
}

int ObPluginExternalTableRowIterator::open_scanner(const ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_external_scan_param(scan_param))) {
    LOG_WARN("failed to init external scan param", K(ret));
  } else if (FALSE_IT(scanner_ = nullptr)) {
  } else if (FALSE_IT(record_batch_ = nullptr)) {
  } else if (FALSE_IT(iterator_state_.reuse())) {
  } else if (OB_FAIL(data_engine_->open_scanner(allocator_, external_scan_param_, scanner_))) {
    // I don't use `arena_allocator_` because we may create many scanners in one query request.
    LOG_WARN("failed to open scanner", K(ret));
  }
  LOG_DEBUG("open scanner", KP(this), KP(scanner_));
  return ret;
}

int64_t time_elapse(struct timespec &begin, struct timespec &end)
{
  int64_t second = end.tv_sec - begin.tv_sec;
  int64_t nano_second = end.tv_nsec - begin.tv_nsec;
  return second * 1000 + (nano_second / 1000000);
}

int ObPluginExternalTableRowIterator::get_next_row()
{
  int64_t count = 0;
  return get_next_rows(count, 1);
}

int ObPluginExternalTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray *file_column_exprs = nullptr;
  ObEvalCtx *eval_ctx = nullptr;
  int64_t batch_size = 0;
  shared_ptr<RecordBatch> record_batch_slice;

  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->op_) || OB_ISNULL(data_engine_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KPC(scan_param_), KP(data_engine_), K(ret), KP(this));
  } else if (OB_ISNULL(eval_ctx = &scan_param_->op_->get_eval_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get eval context", KPC(scan_param_), K(ret));
  } else if (OB_ISNULL(scan_param_->ext_column_dependent_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("scan_param_->ext_column_dependent_exprs_ is null");
  } else if (scan_param_->ext_column_dependent_exprs_->count() != column_exprs_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dependent exprs count not equal to column expr count",
             K(scan_param_->ext_column_dependent_exprs_->count()), K(column_exprs_.count()));
  } else if (0 == external_scan_param_.task().compare(ObExternalTableUtils::dummy_file_name())) {
    ret = OB_ITER_END;
    LOG_TRACE("dummy task, ignore");
  } else if (FALSE_IT(file_column_exprs = scan_param_->ext_file_column_exprs_)) {
  } else if (FALSE_IT(batch_size = MAX(1, MIN(capacity, eval_ctx->max_batch_size_)))) {
  } else if (OB_ISNULL(record_batch_) || record_batch_offset_ >= record_batch_->num_rows()) {
    record_batch_ = nullptr;

    ret = get_next_record_batch();
  }

  if (OB_SUCC(ret)) {
    OB_TRY_BEGIN;
    if (!(record_batch_slice = record_batch_->Slice(record_batch_offset_, MIN(batch_size, record_batch_->num_rows())))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get record batch slice", K(ret), K(record_batch_->num_rows()));
    } else {
      record_batch_offset_ += record_batch_slice->num_rows();
    }
    OB_TRY_END;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_plugin_column_index_map_if_need(record_batch_))) {
    LOG_WARN("failed to init column index map", K(ret));
  } else {
    LOG_TRACE("got a batch of rows", K(record_batch_slice->num_rows()));

    for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs->count(); i++) {
      ObExpr *expr = file_column_exprs->at(i);
      Array *arrow_array = nullptr;
      if (OB_SUCC(ret) && expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        OZ (expr->init_vector_for_write(*eval_ctx, expr->get_default_res_format(), record_batch_slice->num_rows()));
        int column_index = plugin_column_index_map_.at(i);
        ObArrowDataLoader * loader = plugin_column_loaders_.at(i);
        if (column_index < 0 || OB_ISNULL(loader)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cannot get load functor.", K(i), KPC(expr), K(column_index), K(ret));
        } else if (OB_ISNULL(arrow_array = record_batch_slice->column(column_index).get())) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no column data in record batch", K(column_index), K(ret));
        } else if (OB_FAIL(loader->load(*arrow_array, *eval_ctx, expr))) {
          LOG_WARN("failed to load data from record_batch into expr", K(ret));
        } else {
          //expr->set_evaluated_flag(*eval_ctx);
          expr->set_evaluated_projected(*eval_ctx);
          LOG_DEBUG("set expr evaluated", KP(expr));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("expr type is not supported", K(expr->type_));
      }
    }
    const ObBitVector *skip = nullptr;

    // assign data from column_convert_expr to column_expr
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      ObExpr *column_expr = column_exprs_.at(i);
      ObExpr *column_convert_expr = scan_param_->ext_column_dependent_exprs_->at(i);
      if (OB_ISNULL(column_expr) || OB_ISNULL(column_convert_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", KP(column_expr), KP(column_convert_expr));
      } else if (!column_convert_expr->get_eval_info(*eval_ctx).evaluated_) {
        LOG_DEBUG("this column is not evaluated", K(i), K(column_exprs_.count()), KP(column_convert_expr));
        if (OB_FAIL(column_convert_expr->init_vector_default(*eval_ctx, record_batch_slice->num_rows()))) {
          LOG_WARN("failed to init column_convert_expr default vector", K(ret));
        } else if (OB_FAIL(column_convert_expr->eval_vector(
            *eval_ctx, *bit_vector_cache_, record_batch_slice->num_rows(), true/*all_rows_active*/))) {
          LOG_WARN("failed to do eval_vector of column_convert_expr", K(ret));
        } else {
          column_convert_expr->set_evaluated_projected(*eval_ctx);
        }
      }

      if (OB_SUCC(ret)) {
        VectorHeader &column_convert_vector_header = column_convert_expr->get_vector_header(*eval_ctx);
        LOG_DEBUG("column vector header", K(i), K(column_convert_vector_header.get_format()));
        if (VEC_UNIFORM_CONST == column_convert_vector_header.get_format()) {
          const ObDatum &convert_datum =
              static_cast<ObUniformFormat<true/*IS_CONST*/> *>(column_convert_expr->get_vector(*eval_ctx))->get_datum(0);
          if (OB_FAIL(column_expr->init_vector(*eval_ctx, VEC_UNIFORM, record_batch_slice->num_rows()))) {
            LOG_WARN("failed to init vector of column_expr", K(ret));
          } else if (OB_ISNULL(column_expr->get_vector(*eval_ctx))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("init vector success but got null");
          } else {
            ObUniformFormat<false/*IS_CONST*/> *column_vector =
                static_cast<ObUniformFormat<false/*IS_CONST*/> *>(column_expr->get_vector(*eval_ctx));
            for (int64_t row_index = 0; OB_SUCC(ret) && row_index < record_batch_slice->num_rows(); row_index++) {
              column_vector->get_datum(row_index) = convert_datum;
            }
          }
        } else if (VEC_UNIFORM == column_convert_vector_header.get_format()) {
          ObUniformBase *convert_vector = static_cast<ObUniformBase *>(column_convert_expr->get_vector(*eval_ctx));
          CK (OB_NOT_NULL(convert_vector));
          const ObDatum *convert_datums = convert_vector->get_datums();
          ObDatum *column_datums = column_expr->locate_batch_datums(*eval_ctx);
          CK (OB_NOT_NULL(convert_datums));
          CK (OB_NOT_NULL(column_datums));
          if (column_datums != convert_datums) {
            MEMCPY(column_datums, convert_datums, record_batch_slice->num_rows() * sizeof(ObDatum));
          }
          if (OB_FAIL(column_expr->init_vector(*eval_ctx, VEC_UNIFORM, record_batch_slice->num_rows()))) {
            LOG_WARN("failed to init vector of expr", K(ret));
          } else {
            LOG_DEBUG("convert columns", KP(eval_ctx), KP(column_expr->locate_batch_datums(*eval_ctx)));
          }
        } else if (OB_FAIL(column_expr->get_vector_header(*eval_ctx).assign(column_convert_vector_header))) {
          LOG_WARN("failed to assign vector header", K(ret));
        }
        column_expr->set_evaluated_projected(*eval_ctx);
        LOG_DEBUG("column expr set evaluated", K(i), KP(column_expr), KP(column_convert_expr));
      }
    }
    LOG_DEBUG("ext column convert rows dump");

    LOG_DEBUG("column rows dump");

    // calc the file id and line num
    OZ (calc_exprs_for_rowid(record_batch_slice->num_rows(), iterator_state_));

    if (OB_SUCC(ret)) {
      count = record_batch_slice->num_rows();
    }
  }

  LOG_DEBUG("got rows", K(ret), K(count), K(capacity));
  return ret;
}
int ObPluginExternalTableRowIterator::get_next_record_batch()
{
  int ret = OB_SUCCESS;
  ObEvalCtx *eval_ctx = nullptr;
  int64_t batch_size = 0;
  record_batch_ = nullptr;

  int64_t num_rows = 0;
  if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->op_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KPC(scan_param_), KP(data_engine_), K(ret), KP(this));
  } else if (OB_ISNULL(eval_ctx = &scan_param_->op_->get_eval_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cannot get eval context", KPC(scan_param_), K(ret));
  } else if (FALSE_IT(batch_size = eval_ctx->max_batch_size_)) {
  } else if (OB_FAIL(scanner_->next(batch_size, record_batch_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("failed to get next rows", K(ret));
    } else {
      LOG_DEBUG("touch the end", K(ret));
    }
  } else if (OB_ISNULL(record_batch_)) {
    ret = OB_PLUGIN_ERROR;
    LOG_WARN("got null record batch");
  } else if (FALSE_IT(num_rows = record_batch_->num_rows())) {
  } else if (0 == num_rows) {
    ret = OB_ITER_END;
    LOG_TRACE("got 0 num row, set ITER_END", K(ret));
  } else if (num_rows < 0) {
    ret = OB_PLUGIN_ERROR;
    LOG_WARN("got invalid record batch row num", K(record_batch_->num_rows()), K(batch_size));
  } else {
    record_batch_offset_ = 0;
  }
  LOG_DEBUG("got rows", K(ret), K(num_rows), K(batch_size));
  return ret;
}

int ObPluginExternalTableRowIterator::init_plugin_column_index_map_if_need(RecordBatch *record_batch)
{
  int ret = OB_SUCCESS;
  Schema *schema = nullptr;
  const sql::ExprFixedArray *extern_exprs = nullptr;
  if (plugin_column_index_map_.count() != 0) {
  } else if (OB_ISNULL(scan_param_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KP(scan_param_));
  } else if (OB_ISNULL(record_batch)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(record_batch));
  } else if (FALSE_IT(extern_exprs = scan_param_->ext_file_column_exprs_)) {
  } else if (extern_exprs->count() == 0) {
    LOG_INFO("got empty ext file column exprs, nothing to do");
  } else if (!record_batch->schema()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (FALSE_IT(schema = record_batch->schema().get())) {
  } else if (FALSE_IT(plugin_column_index_map_.set_tenant_id(MTL_ID()))) {
  } else if (FALSE_IT(plugin_column_index_map_.set_label(MEMORY_LABEL))) {
  } else if (OB_FAIL(plugin_column_index_map_.reserve(extern_exprs->count()))) {
    LOG_WARN("failed to reserve array", K(extern_exprs->count()), K(ret));
  } else if (FALSE_IT(plugin_column_loaders_.set_tenant_id(MTL_ID()))) {
  } else if (FALSE_IT(plugin_column_loaders_.set_label(MEMORY_LABEL))) {
  } else if (OB_FAIL(plugin_column_loaders_.reserve(extern_exprs->count()))) {
    LOG_WARN("failed to reserve load functor array", K(extern_exprs->count()), K(ret));
  } else {
    LOG_TRACE("schema information", KCSTRING(schema->ToString(true/*show_metadata*/).c_str()));

    ObDataAccessPathExtraInfo *data_access_info = nullptr;
    string field_name;
    int column_index = -1;
    DataType *data_type = nullptr;
    ObArrowDataLoaderFactory arrow_loader_factory;
    for (int64_t i = 0; OB_SUCC(ret) && i < extern_exprs->count(); i++) {
      ObExpr *expr = extern_exprs->at(i);
      ObArrowDataLoader *arrow_loader = nullptr;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got null expr", K(i));
      } else if (expr->type_ != T_PSEUDO_EXTERNAL_FILE_COL) {
        // skip
        if (OB_FAIL(plugin_column_index_map_.push_back(-1)) ||
            OB_FAIL(plugin_column_loaders_.push_back(nullptr))) {
          LOG_WARN("failed to push item into index map or load functor array", K(ret));
        }
      } else if (OB_ISNULL(data_access_info = static_cast<ObDataAccessPathExtraInfo *>(expr->extra_info_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got null extra info", KPC(expr));
      } else if (FALSE_IT(field_name.assign(data_access_info->data_access_path_.ptr(),
                                            data_access_info->data_access_path_.length()))) {
        LOG_DEBUG("use data access path as the field name", K(data_access_info->data_access_path_));
      } else if (-1 == (column_index = schema->GetFieldIndex(field_name))) {
        ret = OB_INVALID_EXTERNAL_FILE_COLUMN_PATH;
        LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE_COLUMN_PATH,
                       data_access_info->data_access_path_.length(),
                       data_access_info->data_access_path_.ptr());
        LOG_WARN("cannot find field",
                 K(data_access_info->data_access_path_),
                 K(i),
                 KCSTRING(schema->ToString(true/*show_metadata*/).c_str()));
      } else if (!schema->field(column_index) ||
                 OB_ISNULL(data_type = schema->field(column_index)->type().get())) {
        ret = OB_INVALID_EXTERNAL_FILE_COLUMN_PATH;
        LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE_COLUMN_PATH,
                       data_access_info->data_access_path_.length(),
                       data_access_info->data_access_path_.ptr());
        LOG_WARN("no such field",
                 K(data_access_info->data_access_path_),
                 KP(data_type),
                 K(i),
                 KCSTRING(schema->ToString(true/*show_metadata*/).c_str()));
      } else if (OB_FAIL(arrow_loader_factory.select_loader(
          arena_allocator_, *data_type, expr->datum_meta_, arrow_loader))) {
        LOG_WARN("convertion between expr and data type is not supported",
                 K(ret), K(expr->datum_meta_), K(data_type->ToString().c_str()));
      } else if (OB_ISNULL(arrow_loader)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("got a null arrow loader", KP(arrow_loader), K(ret));
      } else if (OB_FAIL(plugin_column_index_map_.push_back(column_index))) {
        LOG_WARN("failed to push column index into column index map",
                 K(plugin_column_index_map_), K(column_index), K(ret));
      } else if (OB_FAIL(plugin_column_loaders_.push_back(arrow_loader))) {
        LOG_WARN("failed to push load functor into array", K(ret));
      } else {
        LOG_DEBUG("create field map", K(column_index), K(plugin_column_index_map_));
      }
    }
  }
  return ret;
}

int ObPluginExternalTableRowIterator::init_external_scan_param(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(external_scan_param_.init())) {
    LOG_WARN("failed to init external scan param", K(ret));
  } else if (FALSE_IT(external_scan_param_.set_storage_param(scan_param))) {
  }

  if (OB_FAIL(ret)) {
  } else if (scan_param->scan_tasks_.count() > 0) {
    ObString task_info = scan_param->scan_tasks_.at(0)->file_url_;
    external_scan_param_.set_task(task_info);
    LOG_TRACE("get a task", K(task_info));
  } else {
    LOG_TRACE("no task");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
    ObExpr *expr = scan_param->ext_file_column_exprs_->at(i);
    CK (OB_NOT_NULL(expr));

    if (expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
      CK (OB_NOT_NULL(expr->extra_info_));
      ObDataAccessPathExtraInfo *data_access_info = static_cast<ObDataAccessPathExtraInfo *>(expr->extra_info_);
      if (OB_FAIL(external_scan_param_.append_column(data_access_info->data_access_path_))) {
        LOG_WARN("failed to append column", K(data_access_info->data_access_path_), K(ret));
      }
    } else {
      // ignore other types
    }
  }
  LOG_TRACE("init external scan param done",
           K(scan_param->ext_file_column_exprs_->count()),
           K(external_scan_param_.columns().count()),
           K(ret), KP(this));
  return ret;
}
#if 0
int ObPluginExternalTableRowIterator::load_data(ObExpr *expr, arrow::Array &arrow_array, LoadFunctor loader)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  if (OB_FAIL(expr->init_vector_for_write(eval_ctx, expr->get_default_res_format(), eval_ctx.max_batch_size_))) {
    LOG_WARN("failed to init expr vector", K(ret), K(eval_ctx.max_batch_size_));
  } else if (OB_FAIL(loader(eval_ctx, expr, arrow_array))) {
    LOG_WARN("failed to load array data into expr", K(ret));
  } else {
    expr->set_evaluated_flag(eval_ctx);
  }
  return ret;
}
#endif
} // namespace plugin
} // namespace oceanbase
