/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_ext_table_plugin_row_iter.h"

#include "common/object/ob_object.h"  // OB_APP_MIN_COLUMN_ID
#include "lib/string/ob_sql_string.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "share/catalog/ob_catalog_properties.h"  // lake_plugin_slot_of
#include "sql/optimizer/file_prune/ob_ext_predicate_json.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"  // ObPluginScanTask
#include "plugin/v2/external_table/ob_ext_plugin_util.h"
#include "plugin/v2/host/ob_ext_malloc_guard.h"
#include "share/rc/ob_tenant_base.h"
#include "sql/engine/basic/ob_arrow_data_loader.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_get_path.h"

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include <cstring>
#include <string>
#include <vector>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

namespace
{

void release_arrow_struct_if_owned(struct ArrowSchema *s)
{
  if (OB_NOT_NULL(s) && s->release) {
    s->release(s);
    s->release = nullptr;
  }
}
void release_arrow_struct_if_owned(struct ArrowArray *a)
{
  if (OB_NOT_NULL(a) && a->release) {
    a->release(a);
    a->release = nullptr;
  }
}

// Safely fetch the loaded plugin's format name (for per-plugin memory labels),
// or nullptr if no plugin is loaded yet (e.g. close_ext before init).
inline const char *fmt_name(const ObExtTablePluginApi *api)
{
  return (OB_NOT_NULL(api) && OB_NOT_NULL(api->format_name)) ? api->format_name() : nullptr;
}

} // namespace

ObExtTablePluginRowIterator::ObExtTablePluginRowIterator()
    : api_(nullptr),
      reader_worker_state_(nullptr),
      reader_scan_state_(nullptr),
      reader_task_state_(nullptr),
      scan_active_(false),
      has_open_task_(false),
      cur_batch_row_idx_(0),
      cur_batch_row_count_(0),
      column_index_map_(),
      column_loaders_(),
      bit_vector_cache_(nullptr),
      filter_eval_inited_(false),
      reader_predicate_built_(false),
      reader_predicate_fully_pushed_(false),
      reader_predicate_json_(),
      allocator_(lib::ObMemAttr(MTL_ID(), "ExtRowIter"))
{
}

ObExtTablePluginRowIterator::~ObExtTablePluginRowIterator()
{
  close_ext();
}

int ObExtTablePluginRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  filter_eval_inited_ = false;
  reader_predicate_built_ = false;
  reader_predicate_fully_pushed_ = false;
  reader_predicate_json_.reset();
  reader_projection_json_.reset();
  close_ext();  // has its own guard
  if (OB_ISNULL(scan_param) || OB_ISNULL(scan_param->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid scan param", K(ret), KP(scan_param));
  } else if (OB_FAIL(ObExternalTableRowIterator::init(scan_param))) {
    LOG_WARN("failed to init external table row iterator", K(ret));
  }

  if (OB_SUCC(ret)) {
    allocator_.set_attr(lib::ObMemAttr(MTL_ID(), "ExtRowIter"));
    column_index_map_.set_label("ExtRowIter");
    column_loaders_.set_label("ExtRowIter");
  }

  if (OB_SUCC(ret)) {
    // The plugin identity is carried as the slot encoded in lake_table_format_;
    // resolve it to the vtable by slot, not by name.
    const share::ObPluginSlot plugin_slot
        = share::lake_plugin_slot_of(scan_param_->lake_table_format_);
    api_ = share::ObExtFormatRegistry::get_instance().get_plugin_by_slot(plugin_slot);
    if (OB_ISNULL(api_) || OB_ISNULL(api_->plan_create) || OB_ISNULL(api_->reader_close)
        || OB_ISNULL(api_->reader_create) || OB_ISNULL(api_->reader_open_scan)
        || OB_ISNULL(api_->reader_open_task) || OB_ISNULL(api_->reader_next_batch)
        || OB_ISNULL(api_->reader_close_task) || OB_ISNULL(api_->reader_close_scan)) {
      // The plugin .so for this format is not present -> the format is not selectable.
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ext plugin not loaded for format", K(ret), K(plugin_slot),
               K(scan_param_->lake_table_format_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "external table plugin not loaded");
    }
  }

  if (OB_SUCC(ret)) {
    ObExtMallocGuard guard(fmt_name(api_));  // tag plugin mallocs from reader_create onward
    if (OB_FAIL(host_ctx_.default_fs.set_path(scan_param_->external_file_location_))) {
      LOG_WARN("failed to set fs path", K(ret));
    } else if (OB_FAIL(host_ctx_.default_fs.set_access_info(
                   scan_param_->external_file_access_info_))) {
      LOG_WARN("failed to set fs access info", K(ret));
    }
    host_ctx_.default_fs.set_cache_options(ObExternalFileCacheOptions(true /*enable_page_cache*/,
                                                                      true /*enable_disk_cache*/));
    host_ctx_.default_fs.set_tenant_id(MTL_ID());
    host_ctx_.select_arrow_pool(true);
    host_ctx_.pool->set_attr(get_ext_mem_attr(fmt_name(api_)));
    host_ctx_.executor = nullptr;  // read path runs inline
    build_ext_host_api(host_, &host_ctx_);
    reader_worker_state_ = nullptr;
    reader_scan_state_ = nullptr;
    reader_task_state_ = nullptr;
    scan_active_ = false;
    has_open_task_ = false;
    // table_uri / options / projection are iterator-lifetime: hand them to
    // reader_create; the predicate (scan-varying) goes to reader_open_scan.
    if (OB_FAIL(build_reader_column_set_and_projection())) {
      LOG_WARN("failed to build reader column set / projection", K(ret));
    }
    std::string loc_str;
    ObString options_json;
    if (OB_SUCC(ret)) {
      loc_str.assign(scan_param_->external_file_location_.ptr(),
                     scan_param_->external_file_location_.length());
      // Reader options ride inside the format string (built at codegen); the
      // plugin needs a NUL-terminated buffer.
      const ObString &fmt_options
          = scan_param_->external_file_format_.get_cpp_plugin_format().reader_options_json_;
      if (!fmt_options.empty()) {
        char *buf = static_cast<char *>(allocator_.alloc(fmt_options.length() + 1));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc reader options json failed", K(ret), K(fmt_options.length()));
        } else {
          MEMCPY(buf, fmt_options.ptr(), fmt_options.length());
          buf[fmt_options.length()] = '\0';
          options_json.assign_ptr(buf, fmt_options.length());
        }
      }
    }
    if (OB_SUCC(ret)) {
      const char *projection_cstr
          = reader_projection_json_.empty() ? nullptr : reader_projection_json_.ptr();
      const int rc = api_->reader_create(
          &host_,
          loc_str.c_str(),
          options_json.ptr(),
          projection_cstr,
          &reader_worker_state_,
          &reader_scan_state_,
          &reader_task_state_);
      if (OB_SUCCESS != rc || OB_ISNULL(reader_worker_state_) || OB_ISNULL(reader_scan_state_)
          || OB_ISNULL(reader_task_state_)) {
        ret = OB_SUCCESS != rc ? rc : OB_ERR_UNEXPECTED;
        LOG_WARN("plugin reader_create failed", K(ret));
        if (OB_NOT_NULL(api_->reader_close)
            && (OB_NOT_NULL(reader_worker_state_) || OB_NOT_NULL(reader_scan_state_)
                || OB_NOT_NULL(reader_task_state_))) {
          api_->reader_close(reader_worker_state_, reader_scan_state_, reader_task_state_);
        }
        reader_worker_state_ = nullptr;
        reader_scan_state_ = nullptr;
        reader_task_state_ = nullptr;
      }
    }
    // Push model: scan tasks (one task_json each) arrive via scan_param_->scan_tasks_,
    // produced by ObExtFilePruner's plan_create and distributed by the existing PX
    // plumbing. The iterator does NOT call plan_create itself.
  }

  if (OB_SUCC(ret)) {
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    void *buf = nullptr;
    if (OB_UNLIKELY(eval_ctx.max_batch_size_ <= 0)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-vectorized external table plugin scan is not supported",
               K(ret), K(eval_ctx.max_batch_size_));
    } else if (OB_ISNULL(buf = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc bit vector cache", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(buf);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::build_reader_column_set_and_projection()
{
  int ret = OB_SUCCESS;
  reader_file_column_ids_.reset();
  reader_file_column_names_.reset();
  reader_projection_json_.reset();
  // The file-column set backs the projection (reader_create) and the
  // per-scan predicate's column mapping. Iterator-lifetime: built once.
  for (int64_t i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count(); ++i) {
    const uint64_t column_id = mapping_column_ids_.at(i).first;
    ObExpr *file_expr = i < file_column_exprs_.count() ? file_column_exprs_.at(i) : nullptr;
    if (OB_INVALID_ID == column_id || OB_ISNULL(file_expr)
        || OB_ISNULL(file_expr->extra_info_)) {
      // Derived/default columns have no plugin field to reference.
    } else {
      const ObDataAccessPathExtraInfo *data_access_info =
          static_cast<const ObDataAccessPathExtraInfo *>(file_expr->extra_info_);
      if (!data_access_info->data_access_path_.empty()) {
        if (OB_FAIL(reader_file_column_ids_.push_back(column_id))) {
        } else if (OB_FAIL(reader_file_column_names_.push_back(data_access_info->data_access_path_))) {
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (reader_file_column_ids_.count() > 0) {
      // Column projection for reader_create ({"field_ids":[...]}). Optional:
      // on failure degrade to reading all columns (correct, just more I/O).
      if (OB_FAIL(build_projection_json(reader_file_column_ids_, reader_projection_json_))) {
        LOG_WARN("failed to build reader projection json, read all columns", K(ret));
        ret = OB_SUCCESS;
        reader_projection_json_.reset();
      }
    } else {
      // No file column is referenced (e.g. count(*)): push only the first
      // field, otherwise the plugin reads ALL columns (the protocol treats a
      // missing projection as "read everything"). Field id 0 is OB_APP_MIN_COLUMN_ID.
      common::ObSEArray<uint64_t, 1> first_column_id;
      if (OB_FAIL(first_column_id.push_back(OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("failed to build first-column projection", K(ret));
      } else if (OB_FAIL(build_projection_json(first_column_id, reader_projection_json_))) {
        LOG_WARN("failed to build first-column projection json, read all columns", K(ret));
        ret = OB_SUCCESS;
        reader_projection_json_.reset();
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::build_reader_predicate_json()
{
  int ret = OB_SUCCESS;
  reader_predicate_json_.reset();
  ObPushdownFilterExecutor *filter = scan_param_->pd_storage_filters_;
  if (OB_NOT_NULL(filter)) {
    if (OB_FAIL(ensure_filter_eval_inited_once(filter))) {
      LOG_WARN("failed to init runtime predicate datums", K(ret));
    } else {
      // fully_converted = the WHOLE pushdown filter tree was turned into the
      // plugin JSON predicate (only white/logic nodes; black/sample/other nodes
      // are never emitted — those are the "black box" residual). Stored so
      // get_next_rows can SKIP calc_filters when true (we trust the plugin once
      // it accepts SetPredicate; paimon owns the filtering) and MUST run
      // calc_filters when false (a black-box part was never pushed; OB evaluates
      // it for correctness). Any build failure / non-convertible node forces
      // false -> OB re-filters.
      bool fully_converted = false;
      const int build_ret = ext_predicate::build_predicate_json_from_pushdown_filter(
          allocator_, filter, reader_file_column_ids_, reader_file_column_names_,
          reader_predicate_json_, fully_converted);
      if (OB_SUCCESS != build_ret) {
        // Reader pushdown is optional. OB still evaluates the complete filter.
        LOG_WARN("failed to build reader predicate json, continue without reader pushdown",
                 K(build_ret));
        reader_predicate_json_.reset();
        fully_converted = false;
      }
      reader_predicate_fully_pushed_ = fully_converted;
    }
  }
  if (OB_SUCC(ret)) {
    reader_predicate_built_ = true;
  }
  return ret;
}

int ObExtTablePluginRowIterator::build_projection_json(const common::ObIArray<uint64_t> &column_ids,
                                                 common::ObString &out_projection_json)
{
  int ret = OB_SUCCESS;
  out_projection_json.reset();
  // {"field_ids":[...]} — field_id = OB column id - OB_APP_MIN_COLUMN_ID, the
  // plugin's protocol field id. The plugin parses the JSON as a C string, so
  // the buffer is NUL-terminated while the ObString length stays exact.
  common::ObSqlString proj;
  if (OB_FAIL(proj.append_fmt("{\"%s\":[", OB_EXT_K_FIELD_IDS))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      if (column_ids.at(i) < OB_APP_MIN_COLUMN_ID) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ext file column id below app min column id", K(ret), K(column_ids.at(i)));
      } else if (i > 0 && OB_FAIL(proj.append(","))) {
      } else if (OB_FAIL(proj.append_fmt("%ld", static_cast<long>(
                                             column_ids.at(i) - OB_APP_MIN_COLUMN_ID)))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(proj.append("]}"))) {
    }
  }
  if (OB_SUCC(ret)) {
    char *dst = static_cast<char *>(allocator_.alloc(proj.length() + 1));
    if (OB_ISNULL(dst)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projection json failed", K(ret), K(proj.length()));
    } else {
      MEMCPY(dst, proj.ptr(), proj.length());
      dst[proj.length()] = '\0';
      out_projection_json.assign_ptr(
          dst, static_cast<common::ObString::obstr_size_t>(proj.length()));
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::open_next_task()
{
  int ret = OB_SUCCESS;
  // Release the current batch before closing the task: its Arrow buffers use
  // reader-owned memory, so the BatchReader must outlive the ArrowArray.
  cur_record_batch_.reset();
  cur_batch_row_idx_ = 0;
  cur_batch_row_count_ = 0;
  if (has_open_task_ && OB_NOT_NULL(reader_worker_state_) && OB_NOT_NULL(reader_task_state_)
      && OB_NOT_NULL(api_) && OB_NOT_NULL(api_->reader_close_task)) {
    api_->reader_close_task(reader_task_state_);
    has_open_task_ = false;
  }

  if (OB_ISNULL(api_) || OB_ISNULL(reader_worker_state_) || OB_ISNULL(reader_scan_state_)
      || OB_ISNULL(reader_task_state_) || OB_ISNULL(scan_param_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ext reader not initialized", K(ret));
  } else if (state_.file_idx_ >= scan_param_->scan_tasks_.count()) {
    if (scan_active_ && OB_NOT_NULL(api_->reader_close_scan)) {
      api_->reader_close_scan(reader_worker_state_, reader_scan_state_);
      scan_active_ = false;
    }
    ret = OB_ITER_END;
  } else if (!reader_predicate_built_ && OB_FAIL(build_reader_predicate_json())) {
    // Build only when a reader is actually needed. reset()/rescan does not call
    // init(), so the first task of each scan also refreshes execution parameters.
    LOG_WARN("failed to prepare plugin reader predicate", K(ret));
  } else {
    // Push model: the per-thread scan task was produced by ObExtFilePruner's
    // plan_create and stashed in scan_param_->scan_tasks_ by the lake-table
    // plumbing. It is an ObPluginScanTask whose task_json_ carries the contract
    // single-scan-task JSON text (incl. payload_b64).
    ObIExtTblScanTask *base_task = scan_param_->scan_tasks_.at(state_.file_idx_);
    ObPluginScanTask *scan_task = dynamic_cast<ObPluginScanTask *>(base_task);
    if (OB_ISNULL(scan_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("scan task is not an ObPluginScanTask (expected task_json)",
               K(ret), K(state_.file_idx_));
    } else {
      if (!scan_active_) {
        // The predicate is the only scan-scope input.
        const char *predicate_cstr
            = reader_predicate_json_.empty() ? nullptr : reader_predicate_json_.ptr();
        const int rc_scan = api_->reader_open_scan(
            reader_worker_state_, reader_scan_state_, predicate_cstr);
        if (rc_scan != OB_SUCCESS) {
          ret = rc_scan;
          LOG_WARN("plugin reader_open_scan failed", K(ret), K(state_.file_idx_));
        } else {
          scan_active_ = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      const ObString &task_json = scan_task->task_json_;
      // start_row=0, row_count = whole task (read to EOF; row subdivision is a follow-up).
      // The plugin returns an OB errno verbatim and logs its own diagnostic (with
      // plugin-side source location) via host->log before returning.
      uint64_t rows = ~(uint64_t)0;
      int rc2 = api_->reader_open_task(
          reader_worker_state_,
          reader_scan_state_,
          reader_task_state_,
          task_json.ptr(),
          static_cast<int32_t>(task_json.length()),
          /*start_row*/ 0,
          rows);
      if (rc2 != OB_SUCCESS) {
        ret = rc2;
        LOG_WARN("plugin reader_open_task failed", K(ret), K(state_.file_idx_));
      } else {
        has_open_task_ = true;
        state_.file_idx_++;
        state_.cur_file_url_ = scan_param_->external_file_location_;
        state_.cur_line_number_ = 0;
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::fetch_next_batch()
{
  int ret = OB_SUCCESS;
  bool has_load_batch = false;
  // NEVER let a zero-row successful batch escape this function: the caller
  // chain (ObOperator::get_next_batch's "all-filtered -> continue" loop) treats
  // a 0-row OB_SUCCESS batch as "pull again" and spins forever. EOF therefore
  // advances to the next task INSIDE this loop; only OB_ITER_END (all tasks
  // exhausted) or a real error leaves without rows.
  while (OB_SUCC(ret) && !has_load_batch) {
    if (!has_open_task_) {
      if (OB_FAIL(open_next_task())) {
        // OB_ITER_END is expected when all splits exhausted.
      }
    } else {
      ArrowArray arrow_array;
      ArrowSchema arrow_schema;
      MEMSET(&arrow_array, 0, sizeof(arrow_array));
      MEMSET(&arrow_schema, 0, sizeof(arrow_schema));
      // rc: 0 = batch available, 1 = EOF, <0 = OB errno (plugin logs via host->log).
      int rc = api_->reader_next_batch(
          reader_worker_state_,
          reader_task_state_,
          &arrow_array,
          &arrow_schema);
      if (rc < 0) {
        ret = rc;  // rc is the OB errno
        LOG_WARN("plugin reader_next_batch failed", K(ret));
      } else if (rc == 1) {
        // Release the consumed batch before closing the task; its Arrow buffers
        // use reader-owned memory (the same ordering as open_next_task/reset).
        cur_record_batch_.reset();
        cur_batch_row_idx_ = 0;
        cur_batch_row_count_ = 0;
        if (OB_NOT_NULL(api_->reader_close_task)) {
          api_->reader_close_task(reader_task_state_);
        }
        has_open_task_ = false;
      } else if (OB_FAIL(import_current_arrow_batch(&arrow_array, &arrow_schema))) {
        LOG_WARN("failed to import arrow batch", K(ret));
      } else if (cur_batch_row_count_ <= 0) {
        // Empty batches are legal; keep pulling from the SAME reader.
        cur_record_batch_.reset();
      } else {
        has_load_batch = true;
      }
      // Ensure any unconsumed Arrow C Data is released (import consumes on success).
      release_arrow_struct_if_owned(&arrow_array);
      release_arrow_struct_if_owned(&arrow_schema);
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::import_current_arrow_batch(struct ArrowArray *arrow_array,
                                                       struct ArrowSchema *arrow_schema)
{
  int ret = OB_SUCCESS;
  cur_record_batch_.reset();
  cur_batch_row_idx_ = 0;
  cur_batch_row_count_ = 0;
  if (OB_ISNULL(arrow_array) || OB_ISNULL(arrow_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arrow batch", K(ret), KP(arrow_array), KP(arrow_schema));
  } else {
    arrow::Result<std::shared_ptr<arrow::Schema>> schema_result = arrow::ImportSchema(arrow_schema);
    if (!schema_result.ok()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to import arrow schema", K(ret),
               "status", schema_result.status().message().c_str());
    } else {
      std::shared_ptr<arrow::Schema> schema = schema_result.ValueOrDie();
      arrow::Result<std::shared_ptr<arrow::Array>> array_result =
          arrow::ImportArray(arrow_array, arrow::struct_(schema->fields()));
      if (!array_result.ok()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to import arrow array", K(ret),
                 "status", array_result.status().message().c_str());
      } else {
        std::shared_ptr<arrow::StructArray> struct_array =
            std::dynamic_pointer_cast<arrow::StructArray>(array_result.ValueOrDie());
        if (OB_ISNULL(struct_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("arrow batch is not struct array", K(ret));
        } else {
          cur_record_batch_ =
              arrow::RecordBatch::Make(schema, struct_array->length(), struct_array->fields());
          cur_batch_row_count_ = cur_record_batch_->num_rows();
        }
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::init_column_mapping_if_need()
{
  int ret = OB_SUCCESS;
  if (column_index_map_.count() > 0) {
  } else if (OB_FAIL(column_index_map_.reserve(file_column_exprs_.count()))
             || OB_FAIL(column_loaders_.reserve(file_column_exprs_.count()))) {
    LOG_WARN("failed to reserve column mapping arrays", K(ret));
  } else {
    std::shared_ptr<arrow::Schema> schema = cur_record_batch_->schema();
    ObArrowDataLoaderFactory loader_factory;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      ObExpr *expr = file_column_exprs_.at(i);
      int64_t column_index = -1;
      ObArrowDataLoader *loader = nullptr;
      ObExpr *load_expr = get_column_expr_by_id(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null file column expr", K(ret), K(i));
      } else if (OB_ISNULL(load_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null load expr", K(ret), K(i));
      } else {
        const ObDataAccessPathExtraInfo *data_access_info =
            static_cast<const ObDataAccessPathExtraInfo *>(expr->extra_info_);
        for (int64_t j = 0; OB_SUCC(ret) && j < schema->num_fields() && column_index < 0; ++j) {
          std::shared_ptr<arrow::Field> field = schema->field(j);
          if (OB_ISNULL(expr->extra_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr's column name not found");
          } else {
            ObString field_name(field->name().length(), field->name().data());
            if (0 == data_access_info->data_access_path_.compare(field_name)) {
              column_index = j;
            }
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (column_index < 0 || column_index >= cur_record_batch_->num_columns()) {
        ret = OB_INVALID_EXTERNAL_FILE_COLUMN_PATH;
        LOG_WARN("failed to find column in arrow batch", K(ret), K(column_index),
                 K(schema->ToString(true).c_str()));
      } else if (OB_FAIL(loader_factory.select_loader(
                     allocator_, *schema->field(column_index)->type(), load_expr->datum_meta_,
                     loader))) {
        LOG_WARN("failed to select arrow data loader", K(ret), K(column_index),
                 K(schema->field(column_index)->type()->ToString().c_str()), K(load_expr->datum_meta_));
      } else if (OB_FAIL(column_index_map_.push_back(column_index))) {
        LOG_WARN("failed to push column index mapping", K(ret));
        if (OB_NOT_NULL(loader)) {
          loader->destroy();
          OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
          loader = nullptr;
        }
      } else if (OB_FAIL(column_loaders_.push_back(loader))) {
        LOG_WARN("failed to push column loader mapping", K(ret));
        (void)column_index_map_.pop_back();
        if (OB_NOT_NULL(loader)) {
          loader->destroy();
          OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
          loader = nullptr;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    (void)reset_column_mapping();
  }
  return ret;
}

int ObExtTablePluginRowIterator::get_next_row(common::ObNewRow *&row)
{
  UNUSED(row);
  return common::OB_NOT_SUPPORTED;
}

int ObExtTablePluginRowIterator::get_next_row()
{
  return common::OB_NOT_SUPPORTED;
}

int ObExtTablePluginRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObExtMallocGuard guard(fmt_name(api_));  // covers fetch_next_batch / open_next_task / reader_next_batch
  count = 0;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObPushdownFilterExecutor *filter = scan_param_->pd_storage_filters_;

  if (capacity <= 0) {
  } else {
    // Keep pulling until at least one row survives (a batch may be emptied by
    // the residual filters) or the scan ends/errors. NEVER return a zero-row
    // OB_SUCCESS batch from here: ObOperator::get_next_batch treats
    // size_ == skipped (0 == 0) as "all filtered -> continue" and spins
    // forever (this is the original design's "no caller-visible 0-row" rule).
    while (OB_SUCC(ret) && 0 == count) {
      if (cur_batch_row_idx_ >= cur_batch_row_count_) {
        if (OB_FAIL(fetch_next_batch())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to fetch next batch", K(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(cur_record_batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("current arrow record batch is null", K(ret));
      } else {
        const int64_t rows_available = cur_batch_row_count_ - cur_batch_row_idx_;
        const int64_t rows_to_read = MIN(rows_available, capacity);
        std::shared_ptr<arrow::RecordBatch> batch_slice =
            cur_record_batch_->Slice(cur_batch_row_idx_, rows_to_read);
        scan_param_->op_->clear_evaluated_flag();
        if (OB_ISNULL(batch_slice)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to slice record batch", K(ret), K(rows_to_read));
        } else if (OB_FAIL(init_column_mapping_if_need())) {
          LOG_WARN("failed to init column mapping", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
            ObExpr *expr = get_column_expr_by_id(i);
            const int64_t column_index = column_index_map_.at(i);
            ObArrowDataLoader *loader = column_loaders_.at(i);
            if (OB_ISNULL(expr) || OB_ISNULL(loader)
                || column_index < 0 || column_index >= batch_slice->num_columns()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid column mapping", K(ret), K(i), K(column_index), KP(loader));
            } else if (OB_FAIL(expr->init_vector_for_write(eval_ctx, expr->get_default_res_format(),
                                                           rows_to_read))) {
              LOG_WARN("failed to init file column vector", K(ret), KPC(expr));
            } else if (OB_FAIL(loader->load(*batch_slice->column(column_index), eval_ctx, expr))) {
              LOG_WARN("failed to load arrow column", K(ret), K(i), K(column_index));
            } else {
              expr->set_evaluated_projected(eval_ctx);
            }
          }
          int64_t read_count = rows_to_read;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(project_output_columns(read_count))) {
            LOG_WARN("failed to project output columns", K(ret));
          } else if (OB_FAIL(calc_exprs_for_rowid(read_count, state_))) {
            LOG_WARN("failed to calc row id exprs", K(ret));
          } else if (OB_NOT_NULL(filter)) {
            // need_dup_filter is already false on the CPP_PLUGIN path (see
            // ObLogTableScan::extract_pushdown_filters), so OB does NOT also copy
            // all predicates into spec.filters_ — the plugin's SetPredicate is the
            // only filterer of the pushed tree. For now we run calc_filters +
            // reorder_output UNCONDITIONALLY here as the OB-side backstop, ignoring
            // reader_predicate_fully_pushed_. The fully_pushed_ flag is still
            // computed and kept (see build_reader_predicate_json) so that, once we
            // trust the plugin's SetPredicate as the sole backstop, we can skip
            // this block on fully_pushed_=true to avoid double filtering. Kept for
            // future optimization — do NOT delete reader_predicate_fully_pushed_.
            const common::ObBitmap *filter_result = nullptr;
            if (OB_FAIL(ensure_filter_eval_inited_once(filter))) {
              LOG_WARN("failed to init filter evaluated datums once", K(ret));
            } else if (OB_FAIL(calc_filters(read_count, filter, nullptr))) {
              LOG_WARN("failed to calc ext plugin filters", K(ret));
            } else if (OB_ISNULL(filter_result = filter->get_result())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null filter result", K(ret));
            } else if (OB_FAIL(reorder_output(*filter_result, eval_ctx, read_count))) {
              LOG_WARN("failed to reorder filtered output", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            cur_batch_row_idx_ += rows_to_read;
            count = read_count;
          }
        }
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::project_output_columns(const int64_t read_count)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_dependent_exprs_);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
    // A non-output column can still be referenced by pd_storage_filters_.
    // Project all conversion-needed columns so residual filter evaluation sees
    // OB-typed values, not raw file-column vectors.
    if (column_need_conv_.at(i)) {
      if (OB_FAIL(project_column(eval_ctx, column_conv_exprs.at(i), column_exprs_.at(i), read_count))) {
        LOG_WARN("failed to project output column", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::project_column(ObEvalCtx &eval_ctx, const ObExpr *from,
                                          const ObExpr *to, const int64_t read_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(from) || OB_ISNULL(to)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(from), KP(to));
  } else {
    if (!from->get_eval_info(eval_ctx).is_evaluated(eval_ctx)) {
      if (OB_ISNULL(bit_vector_cache_)) {
        ret = OB_NOT_INIT;
        LOG_WARN("bit vector cache is null", K(ret));
      } else if (OB_FAIL(from->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true))) {
        LOG_WARN("failed to eval convert expr", K(ret), KPC(from));
      } else {
        from->set_evaluated_projected(eval_ctx);
      }
    }
    if (OB_SUCC(ret)) {
      VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
      VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
      if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
        ObDatum *from_datum = static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
        if (OB_ISNULL(from_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("from datum is null", K(ret));
        } else if (OB_FAIL(to->init_vector(eval_ctx, VEC_UNIFORM, read_count))) {
          LOG_WARN("failed to init to vector", K(ret), KPC(to));
        } else {
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; OB_SUCC(ret) && j < read_count; ++j) {
            to_datums[j] = *from_datum;
          }
        }
      } else if (from_vec_header.format_ == VEC_UNIFORM) {
        ObUniformBase *from_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
        if (OB_ISNULL(from_vec)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("from vector is null", K(ret));
        } else if (OB_FAIL(to->init_vector(eval_ctx, VEC_UNIFORM, read_count))) {
          LOG_WARN("failed to init projected vector", K(ret), KPC(to));
        } else {
          ObDatum *src = from_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx);
          if (OB_ISNULL(src) || OB_ISNULL(dst)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null datum array", K(ret), KP(src), KP(dst));
          } else if (src != dst) {
            MEMCPY(dst, src, read_count * sizeof(ObDatum));
          }
        }
      } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
        LOG_WARN("failed to assign vector header", K(ret));
      }
      if (OB_SUCC(ret)) {
        to->set_evaluated_projected(eval_ctx);
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::init_filter_evaluated_datums(ObPushdownFilterExecutor *curr_filter)
{
  int ret = OB_SUCCESS;
  bool filter_valid = true;
  if (OB_ISNULL(curr_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null filter", K(ret));
  } else if (curr_filter->is_filter_node()) {
    if (curr_filter->is_filter_black_node()) {
      if (OB_FAIL(curr_filter->init_evaluated_datums(filter_valid))) {
        LOG_WARN("failed to init evaluated datums", K(ret));
      } else if (!filter_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
      }
    } else if (curr_filter->is_filter_white_node()) {
      ObWhiteFilterExecutor *white_filter = static_cast<ObWhiteFilterExecutor *>(curr_filter);
      ObWhiteFilterOperatorType op_type = white_filter->get_op_type();
      if (WHITE_OP_IN == op_type) {
        if (OB_FAIL(white_filter->init_in_eval_datums(filter_valid))) {
          LOG_WARN("failed to init in filter evaluated datums", K(ret));
        }
      } else if (OB_FAIL(white_filter->init_compare_eval_datums(filter_valid))) {
        LOG_WARN("failed to init compare filter evaluated datums", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (!filter_valid) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
    }
  } else if (curr_filter->is_logic_op_node()) {
    sql::ObPushdownFilterExecutor **children = curr_filter->get_childs();
    for (uint32_t i = 0; OB_SUCC(ret) && i < curr_filter->get_child_count(); ++i) {
      if (OB_ISNULL(children[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null child filter", K(ret), K(i));
      } else if (OB_FAIL(init_filter_evaluated_datums(children[i]))) {
        LOG_WARN("failed to init child filter evaluated datums", K(ret), K(i), KP(children[i]));
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::ensure_filter_eval_inited_once(ObPushdownFilterExecutor *root_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_filter)) {
  } else if (filter_eval_inited_) {
  } else if (OB_FAIL(init_filter_evaluated_datums(root_filter))) {
    LOG_WARN("failed to init filter evaluated datums", K(ret));
  } else {
    filter_eval_inited_ = true;
  }
  return ret;
}

int ObExtTablePluginRowIterator::calc_filters(const int64_t count,
                                        ObPushdownFilterExecutor *curr_filter,
                                        ObPushdownFilterExecutor *parent_filter)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(curr_filter)) {
    common::ObBitmap *result = nullptr;
    if (curr_filter->is_filter_node()) {
      if (OB_FAIL(curr_filter->init_bitmap(count, result))) {
        LOG_WARN("failed to init filter bitmap", K(ret));
      } else if (OB_NOT_NULL(parent_filter)
                 && OB_FAIL(parent_filter->prepare_skip_filter(false))) {
        LOG_WARN("failed to prepare parent skip filter", K(ret));
      } else {
        ObPhysicalFilterExecutor *phy_filter =
            static_cast<ObPhysicalFilterExecutor *>(curr_filter);
        if (OB_FAIL(phy_filter->filter_batch(parent_filter, 0, count, *result))) {
          LOG_WARN("failed to filter batch", K(ret));
        }
      }
    } else if (curr_filter->is_logic_op_node()) {
      if (OB_FAIL(curr_filter->init_bitmap(count, result))) {
        LOG_WARN("failed to init logic filter bitmap", K(ret));
      } else {
        sql::ObPushdownFilterExecutor **children = curr_filter->get_childs();
        if (OB_NOT_NULL(parent_filter) && parent_filter->is_logic_and_node()
            && curr_filter->is_logic_and_node()) {
          MEMCPY(result->get_data(), parent_filter->get_result()->get_data(), count);
        }
        for (uint32_t i = 0; OB_SUCC(ret) && i < curr_filter->get_child_count(); ++i) {
          const common::ObBitmap *child_result = nullptr;
          if (OB_ISNULL(children[i])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null child filter", K(ret), K(i));
          } else if (OB_FAIL(calc_filters(count, children[i], curr_filter))) {
            LOG_WARN("failed to calc child filter", K(ret), K(i), KP(children[i]));
          } else if (OB_ISNULL(child_result = children[i]->get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null child filter bitmap", K(ret));
          } else if (curr_filter->is_logic_and_node()) {
            if (OB_FAIL(result->bit_and(*child_result))) {
              LOG_WARN("failed to merge AND filter bitmap", K(ret), KP(child_result));
            } else if (result->is_all_false()) {
              break;
            }
          } else {
            if (OB_FAIL(result->bit_or(*child_result))) {
              LOG_WARN("failed to merge OR filter bitmap", K(ret), KP(child_result));
            } else if (result->is_all_true()) {
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::reorder_expr_vector(ObExpr *expr,
                                               ObEvalCtx &ctx,
                                               const common::ObBitmap &bitmap,
                                               const int64_t read_count,
                                               const int64_t real_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
  } else if (!expr->get_eval_info(ctx).is_evaluated(ctx)) {
  } else {
    ObIVector *vec = expr->get_vector(ctx);
    if (OB_ISNULL(vec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null vector when reorder ext output", K(ret), KPC(expr));
    } else {
      switch (vec->get_format()) {
        case VEC_FIXED: {
          ObFixedLengthBase *fix_vec = static_cast<ObFixedLengthBase *>(vec);
          ObBitVector *nulls = fix_vec->get_nulls();
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              if (nulls->at(i)) {
                nulls->set(project_count);
              } else {
                nulls->unset(project_count);
                if (project_count != i) {
                  MEMCPY(fix_vec->get_data() + fix_vec->get_length() * project_count,
                         fix_vec->get_data() + fix_vec->get_length() * i,
                         fix_vec->get_length());
                }
              }
              ++project_count;
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_DISCRETE: {
          ObDiscreteBase *dis_vec = static_cast<ObDiscreteBase *>(vec);
          ObBitVector *nulls = dis_vec->get_nulls();
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              if (nulls->at(i)) {
                nulls->set(project_count);
              } else {
                nulls->unset(project_count);
                if (project_count != i) {
                  dis_vec->get_lens()[project_count] = dis_vec->get_lens()[i];
                  dis_vec->get_ptrs()[project_count] = dis_vec->get_ptrs()[i];
                }
              }
              ++project_count;
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_UNIFORM: {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
          int64_t project_count = 0;
          for (int64_t i = 0; i < read_count; ++i) {
            if (bitmap[i]) {
              uni_vec->get_datums()[project_count++] = uni_vec->get_datums()[i];
            }
          }
          if (OB_UNLIKELY(project_count != real_count)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filtered row count", K(ret), K(project_count), K(real_count));
          }
          break;
        }
        case VEC_UNIFORM_CONST: {
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector format when reorder ext output", K(ret), K(vec->get_format()));
          break;
        }
      }
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::reorder_output(const common::ObBitmap &bitmap,
                                          ObEvalCtx &ctx,
                                          int64_t &read_count)
{
  int ret = OB_SUCCESS;
  const int64_t real_count = bitmap.popcnt();
  if (OB_UNLIKELY(real_count > read_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected filter bitmap popcnt", K(ret), K(real_count), K(read_count));
  } else if (real_count < read_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
      if (OB_FAIL(reorder_expr_vector(column_exprs_.at(i), ctx, bitmap, read_count, real_count))) {
        LOG_WARN("failed to reorder column expr vector", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(reorder_expr_vector(file_id_expr_, ctx, bitmap, read_count, real_count))) {
      LOG_WARN("failed to reorder file id vector", K(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(reorder_expr_vector(line_number_expr_, ctx, bitmap, read_count, real_count))) {
      LOG_WARN("failed to reorder line number vector", K(ret));
    }
    if (OB_SUCC(ret)) {
      read_count = real_count;
    }
  }
  return ret;
}

int ObExtTablePluginRowIterator::reset_column_mapping()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < column_loaders_.count(); ++i) {
    ObArrowDataLoader *loader = column_loaders_.at(i);
    if (OB_NOT_NULL(loader)) {
      loader->destroy();
      OB_DELETEx(ObArrowDataLoader, &allocator_, loader);
    }
  }
  column_loaders_.reset();
  column_index_map_.reset();
  return ret;
}

void ObExtTablePluginRowIterator::reset()
{
  cur_record_batch_ = nullptr;
  cur_batch_row_idx_ = 0;
  cur_batch_row_count_ = 0;
  filter_eval_inited_ = false;
  reader_predicate_built_ = false;
  reader_predicate_fully_pushed_ = false;
  reader_predicate_json_.reset();
  reader_projection_json_.reset();
  if (has_open_task_ && OB_NOT_NULL(reader_worker_state_) && OB_NOT_NULL(reader_task_state_)
      && OB_NOT_NULL(api_) && OB_NOT_NULL(api_->reader_close_task)) {
    api_->reader_close_task(reader_task_state_);
  }
  has_open_task_ = false;
  if (scan_active_ && OB_NOT_NULL(reader_worker_state_) && OB_NOT_NULL(reader_scan_state_)
      && OB_NOT_NULL(api_) && OB_NOT_NULL(api_->reader_close_scan)) {
    api_->reader_close_scan(reader_worker_state_, reader_scan_state_);
  }
  scan_active_ = false;
  (void)reset_column_mapping();
  state_.reuse();
  // Drop the instance-scoped stat cache so each (re)scan starts from fresh
  // storage metadata; the cache only needs to be valid within one scan.
  host_ctx_.default_fs.reset_stat_cache();
}

void ObExtTablePluginRowIterator::close_ext()
{
  ObExtMallocGuard guard(fmt_name(api_));  // tag plugin mallocs during reader_close
  reset();
  if (OB_NOT_NULL(api_) && OB_NOT_NULL(api_->reader_close)
      && (OB_NOT_NULL(reader_worker_state_) || OB_NOT_NULL(reader_scan_state_)
          || OB_NOT_NULL(reader_task_state_))) {
    api_->reader_close(reader_worker_state_, reader_scan_state_, reader_task_state_);
  }
  reader_worker_state_ = nullptr;
  reader_scan_state_ = nullptr;
  reader_task_state_ = nullptr;
  // Scan tasks live in scan_param_->scan_tasks_ and are not freed here.
  if (OB_NOT_NULL(bit_vector_cache_)) {
    allocator_.free(bit_vector_cache_);
    bit_vector_cache_ = nullptr;
  }
  allocator_.reset();
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase
