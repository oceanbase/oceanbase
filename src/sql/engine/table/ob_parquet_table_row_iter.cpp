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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_parquet_table_row_iter.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include <parquet/api/reader.h>
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "lib/udt/ob_array_type.h"

namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

bool mem_zero_detect(void *buf, size_t n)
{
  size_t size = n;
  if (size == 0)
      return true;
  uint8_t * ptr = (uint8_t *)buf;
  if (*ptr == 0 && memcmp(ptr, ptr + 1, size - 1) == 0)
    return true;
  return false;
}

ObParquetTableRowIterator::~ObParquetTableRowIterator()
{
  reset_column_readers();
  file_prebuffer_.destroy();
  eager_file_prebuffer_.destroy();
  column_range_slices_.destroy();
  if (nullptr != rg_bitmap_) {
    malloc_allocator_.free(rg_bitmap_);
    rg_bitmap_ = nullptr;
  }
  malloc_allocator_.reset();
  reader_profile_.dump_metrics();
  reader_profile_.update_profile();
  LOG_TRACE("print stat", K(stat_), K(mode_));
}

int ObParquetTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
  mem_attr_ = ObMemAttr(MTL_ID(), "ParquetRowIter");
  allocator_.set_attr(mem_attr_);
  str_res_mem_.set_attr(mem_attr_);
  malloc_allocator_.set_attr(mem_attr_);
  arrow_alloc_.init(MTL_ID());
  make_external_table_access_options(eval_ctx.exec_ctx_.get_my_session()->get_stmt_type());
  if (options_.enable_prebuffer_) {
    OZ(file_prebuffer_.init(options_.cache_options_, scan_param->timeout_));
    OZ (eager_file_prebuffer_.init(options_.cache_options_, scan_param->timeout_));
  }
  OZ (ObExternalTableRowIterator::init(scan_param));
  OZ (ObExternalTablePushdownFilter::init(scan_param->pd_storage_filters_,
                                          scan_param->ext_tbl_filter_pd_level_,
                                          scan_param->column_ids_,
                                          eval_ctx));
  OZ (reader_profile_.register_metrics(&reader_metrics_, "READER_METRICS"));
  OZ (data_access_driver_.register_io_metrics(reader_profile_, "IO_METRICS"));
  OZ (file_prebuffer_.register_metrics(reader_profile_, "PREBUFFER_METRICS"));
  OZ (eager_data_access_driver_.register_io_metrics(reader_profile_, "EAGER_IO_METRICS"));
  OZ (eager_file_prebuffer_.register_metrics(reader_profile_, "EAGER_PREBUFFER_METRICS"));

  if (OB_SUCC(ret)) {
    if (!scan_param->ext_enable_late_materialization_
        || nullptr == scan_param->pd_storage_filters_) {
      mode_ = FilterCalcMode::FORCE_LAZY_CALC;
    } else if (OB_FAIL(ObExternalTablePushdownFilter::gather_eager_exprs(mapping_column_ids_,
                                                              scan_param->pd_storage_filters_))) {
      if (OB_SEARCH_NOT_FOUND != ret) {
        LOG_WARN("failed to gather eager exprs", K(ret));
      } else {
        ret = OB_SUCCESS;
        mode_ = FilterCalcMode::FORCE_LAZY_CALC;
      }
    } else if (FALSE_IT(lib::ob_sort(eager_columns_.begin(), eager_columns_.end()))) {
    } else if (OB_FAIL(ObExternalTablePushdownFilter::generate_lazy_exprs(mapping_column_ids_,
                                                                          column_exprs_,
                                                                          column_sel_mask_))) {
      if (OB_SEARCH_NOT_FOUND != ret) {
        LOG_WARN("failed to gather lazy exprs", K(ret));
      } else {
        ret = OB_SUCCESS;
        mode_ = FilterCalcMode::FORCE_LAZY_CALC;
      }
    } else if (FALSE_IT(lib::ob_sort(lazy_columns_.begin(), lazy_columns_.end()))) {
    }
    OZ (state_.init(file_column_exprs_.count(), get_eager_count(), allocator_));
    if (file_column_exprs_.count() > 0) {
      OZ (column_indexs_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (column_readers_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (record_readers_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (load_funcs_.allocate_array(allocator_, file_column_exprs_.count()));
      if (get_eager_count() > 0) {
        OZ (eager_column_readers_.allocate_array(allocator_, get_eager_count()));
        OZ (eager_record_readers_.allocate_array(allocator_, get_eager_count()));
      }
    }
    int64_t err_sim = OB_E(EventTable::EN_DISK_ERROR) 0;
    if (0 != err_sim) {
      mode_ = FilterCalcMode::FORCE_LAZY_CALC;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      ObExpr *meta_expr = file_meta_column_exprs_.at(i);
      if (OB_ISNULL(meta_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null meta expr");
      } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        need_partition_info_ = true;
        break;
      }
    }
    LOG_DEBUG("check exprs", K(file_column_exprs_), K(file_meta_column_exprs_), KPC(scan_param->ext_file_column_exprs_));
  }

  if (is_lake_table()) {
    OZ(ObExternalTableRowIterator::init_default_batch(file_column_exprs_));
  }
  if (is_iceberg_lake_table()) {
    OZ(ObExternalTableRowIterator::init_for_iceberg(&options_));
  }

  if (OB_SUCC(ret) && OB_ISNULL(bit_vector_cache_)) {
    void *mem = nullptr;
    if (OB_ISNULL(mem = allocator_.alloc(ObBitVector::memory_size(eval_ctx.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(eval_ctx.max_batch_size_));
    } else {
      bit_vector_cache_ = to_bit_vector(mem);
      bit_vector_cache_->reset(eval_ctx.max_batch_size_);
    }
  }

  if (OB_SUCC(ret)) {
    OZ (def_levels_buf_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ (rep_levels_buf_.allocate_array(allocator_, eval_ctx.max_batch_size_));
  }

  if (OB_SUCC(ret)) {
    OZ (file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ (file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    OZ (malloc_allocator_.init(lib::ObMallocAllocator::get_instance(),
                               OB_MALLOC_NORMAL_BLOCK_SIZE,
                               mem_attr_));
  }

  return ret;
}

int ObParquetTableRowIterator::compute_column_id_by_index_type(int index, int &file_col_id,
                                                               const bool is_collection_column)
{
  int ret = OB_SUCCESS;
  switch (scan_param_->external_file_format_.parquet_format_.column_index_type_) {
    case sql::ColumnIndexType::NAME: {
      ObDataAccessPathExtraInfo *data_access_info =
        static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(index)->extra_info_);
      file_col_id = -1;
      if (is_collection_column) {
        for (int i = 0; i < file_meta_->schema()->num_columns(); i++) {
          const std::string& field_path = file_meta_->schema()->GetColumnRoot(i)->name();
          if (field_path.compare(0, field_path.length(),
                                data_access_info->data_access_path_.ptr(),
                                data_access_info->data_access_path_.length()) == 0) {
            file_col_id = i;
            break;
          }
        }
      } else {
        file_col_id = file_meta_->schema()->ColumnIndex(std::string(
          data_access_info->data_access_path_.ptr(), data_access_info->data_access_path_.length()));
      }
      break;
    }
    case sql::ColumnIndexType::POSITION: {
      file_col_id = file_column_exprs_.at(index)->extra_ - 1;
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unknown orc column_index_type", K(ret),
                              K(scan_param_->external_file_format_.orc_format_.column_index_type_));
      break;
  }
  return ret;
}

bool ObParquetTableRowIterator::is_contain_field_id(std::shared_ptr<parquet::FileMetaData> file_meta)
{
  bool contains_id = true;
  for (int i = 0; i < file_meta->schema()->num_columns() && contains_id; ++i) {
    const parquet::schema::Node* root_node = file_meta->schema()->GetColumnRoot(i);
    if (root_node->field_id() < 0) {
      contains_id = false;
    }
  }
  return contains_id;
}
int ObParquetTableRowIterator::next_file()
{
#define BEGIN_CATCH_EXCEPTIONS try {
#define END_CATCH_EXCEPTIONS                                                                       \
  } catch (const ObErrorCodeException &ob_error) {                                                 \
    if (OB_SUCC(ret)) {                                                                            \
      ret = ob_error.get_error_code();                                                             \
      LOG_WARN("fail to read file", K(ret));                                                       \
    }                                                                                              \
  } catch (const ::parquet::ParquetStatusException &e) {                                           \
    if (OB_SUCC(ret)) {                                                                            \
      status = e.status();                                                                         \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_WARN("unexpected error", K(ret), "Info", e.what());                                      \
    }                                                                                              \
  } catch (const ::parquet::ParquetException &e) {                                                 \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_INVALID_EXTERNAL_FILE;                                                              \
      LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());                                          \
      LOG_WARN("unexpected error", K(ret), "Info", e.what());                                      \
    }                                                                                              \
  } catch (...) {                                                                                  \
    if (OB_SUCC(ret)) {                                                                            \
      ret = OB_ERR_UNEXPECTED;                                                                     \
      LOG_WARN("unexpected error", K(ret));                                                        \
    }                                                                                              \
  }                                                                                                \

  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  ObString location = scan_param_->external_file_location_;
  int64_t task_idx = 0;
  int64_t file_size = -1;
  arrow::Status status = arrow::Status::OK();
  bool is_count_aggr = file_column_exprs_.count() == 0;
  ObFileScanTask *scan_task = nullptr;
  bool skip_create_file_reader = false;

  do {
    ret = OB_SUCCESS;
    reset_column_readers();
    file_meta_.reset();
    file_reader_.reset();
    eager_file_reader_.reset();
    status = arrow::Status::OK();
    if ((task_idx = state_.file_idx_++) >= scan_param_->scan_tasks_.count()) {
      ret = OB_ITER_END;
    } else {
      state_.is_delete_file_loaded_ = false;
      state_.cur_row_group_begin_row_id_ = 0;
      file_size = -1;
      int64_t modify_time = 0;
      ObString file_content_digest;
      url_.reuse();

      scan_task = static_cast<ObFileScanTask *>(scan_param_->scan_tasks_.at(task_idx));
      if (OB_ISNULL(scan_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null scan task", K(ret), K(task_idx));
      } else {
        state_.cur_file_url_ = scan_task->file_url_;
      }

      if (OB_FAIL(ret)) {
      } else if (!is_abs_url(state_.cur_file_url_)) {
        const char *split_char = "/";
        OZ(url_.append_fmt(
            "%.*s%s%.*s",
            location.length(),
            location.ptr(),
            (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
            state_.cur_file_url_.length(),
            state_.cur_file_url_.ptr()));
      } else {
        OZ(url_.assign(state_.cur_file_url_));
      }

      if (OB_FAIL(ret)) {
      } else if (is_dummy_file(state_.cur_file_url_)) {
        // do nothing
      } else {
        if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
          file_size = scan_task->file_size_;
          modify_time = scan_task->modification_time_;
          file_content_digest = scan_task->content_digest_;
        }
        if (file_size < 0 || modify_time <= 0) {
          OZ(ObExternalTableUtils::collect_file_basic_info(
            scan_param_->external_file_location_, scan_param_->external_file_access_info_,
            url_.string(), allocator_, file_size, modify_time, file_content_digest));
        }
        if (OB_FAIL(ret)) {
        } else if (file_size > 0) {
          skip_create_file_reader = scan_task->record_count_ > 0 && is_count_aggr;
          if (!skip_create_file_reader) {
            OZ(create_file_reader(url_.string(), file_content_digest, file_size, modify_time,
                                  data_access_driver_, file_prebuffer_, file_reader_, eager_file_reader_));
            OX(file_meta_ = file_reader_->metadata());
          }

          if (OB_SUCC(ret)) {
            ObString expr_file_url;
            if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
              ObSqlString full_name;
              if (ip_port_.empty()) {
                OZ(gen_ip_port(allocator_));
              }
              OZ(full_name.append_fmt("%.*s%%%.*s", ip_port_.length(), ip_port_.ptr(),
                                      state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
              OZ(ob_write_string(allocator_, full_name.string(), expr_file_url));
            } else {
              expr_file_url = state_.cur_file_url_;
            }
            for (int i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
              file_url_ptrs_.at(i) = expr_file_url.ptr();
              file_url_lens_.at(i) = expr_file_url.length();
            }
          }
        }
      }

      LOG_DEBUG("current external file", K(url_), K(ret));
    }
  } while (OB_OBJECT_NOT_EXIST == ret || OB_HDFS_PATH_NOT_FOUND == ret
           || (OB_SUCC(ret) && 0 == file_size)); // skip not exist or empty file

  if (OB_SUCC(ret)) {
    ++reader_metrics_.selected_file_count_;
    int64_t part_id = scan_task->part_id_;
    if (need_partition_info_ && part_id != 0 && state_.part_id_ != part_id) {
      state_.part_id_ = part_id;
      bool is_external_object = is_external_object_id(scan_param_->table_param_->get_table_id());
      if (OB_LIKELY(is_external_object)) {
        OZ(calc_file_part_list_value_by_array(part_id,
                                              allocator_,
                                              scan_param_->partition_infos_,
                                              state_.part_list_val_));
      } else {
        OZ(calc_file_partition_list_value(part_id, allocator_, state_.part_list_val_));
      }
    }

    state_.cur_file_id_ = scan_task->file_id_;
    state_.cur_row_group_idx_ = scan_task->first_lineno_;
    state_.end_row_group_idx_ = scan_task->last_lineno_;
    if (scan_task->record_count_ > 0 && is_count_aggr) {
      state_.end_row_group_idx_ = scan_task->first_lineno_;
    } else {
      OX (state_.end_row_group_idx_ = std::min((int64_t)(file_meta_->num_row_groups()), state_.end_row_group_idx_));
    }

    BEGIN_CATCH_EXCEPTIONS
      bool contains_field_id = !skip_create_file_reader ? is_contain_field_id(file_meta_) : false;
      for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
        ObDataAccessPathExtraInfo *data_access_info =
            static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
        int column_index = -1;
        if (contains_field_id) {
          int64_t target_field_id = file_column_exprs_.at(i)->extra_;
          bool is_found = false;
          for (int j = 0;
                        OB_SUCC(ret) && j < file_meta_->schema()->num_columns() && !is_found; ++j) {
            const parquet::schema::Node* root_node = file_meta_->schema()->GetColumnRoot(j);
            if (root_node->field_id() == target_field_id) {
              is_found = true;
              column_index = j;
            }
          }
        } else {
          OZ (compute_column_id_by_index_type(i, column_index,
                           ob_is_collection_sql_type(file_column_exprs_.at(i)->datum_meta_.type_)));
        }

        if (OB_SUCC(ret)) {
          const parquet::ColumnDescriptor *col_desc = NULL;
          if (column_index < 0 || column_index >= file_meta_->schema()->num_columns()) {
            if (!is_lake_table()) {
              if (scan_param_->external_file_format_.parquet_format_.column_index_type_ ==
                                                                    sql::ColumnIndexType::POSITION) {
                ret = OB_ERR_INVALID_COLUMN_ID;
                LOG_WARN("invalid column index", K(ret), K(column_index),
                        K(file_meta_->schema()->num_columns()));
              } else {
                ret = OB_INVALID_EXTERNAL_FILE_COLUMN_PATH;
                LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE_COLUMN_PATH,
                            data_access_info->data_access_path_.length(),
                            data_access_info->data_access_path_.ptr());
              }
            } else {
              load_funcs_.at(i) = &DataLoader::load_default;
              column_indexs_.at(i) = -1;
            }
          } else {
            ObDatumMeta &meta = file_column_exprs_.at(i)->datum_meta_;
            const ObSqlCollectionInfo *coll_info = NULL;
            uint16_t type_id = 0;
            if (ob_is_collection_sql_type(meta.type_)) {
              uint16_t subschema_id = file_column_exprs_.at(i)->obj_meta_.get_subschema_id();
              ObSubSchemaValue value;
              if (OB_FAIL(eval_ctx.exec_ctx_.get_sqludt_meta_by_subschema_id(subschema_id, value))) {
                LOG_WARN("failed to get subschema ctx", K(ret));
              } else if (FALSE_IT(coll_info = reinterpret_cast<const ObSqlCollectionInfo *>(value.value_))) {
              } else {
                type_id = coll_info->collection_meta_->type_id_;
              }
            }
            col_desc = file_meta_->schema()->Column(column_index);
            const ::parquet::schema::Node* node = file_meta_->schema()->GetColumnRoot(column_index);
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(col_desc)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null column desc", K(ret), K(column_index));
            } else {
              load_funcs_.at(i) = DataLoader::select_load_function(file_column_exprs_.at(i)->datum_meta_, col_desc, node, type_id);
            }
            if (OB_SUCC(ret)) {
              if (OB_ISNULL(load_funcs_.at(i))) {
                ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
                std::string log_type = col_desc->logical_type()->ToString();
                std::string phy_type = TypeToString(col_desc->physical_type());
                int64_t pos = 0;
                ObArrayWrap<char> buf;
                const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
                if (OB_SUCCESS == buf.allocate_array(allocator_, 100)) {
                  ObArray<ObString> extended_type_info;
                  if (ob_is_collection_sql_type(meta.type_)) {
                    int tmp_ret = OB_SUCCESS;
                    if (OB_SUCCESS != (tmp_ret = extended_type_info.push_back(coll_info->get_def_string()))) {
                      LOG_WARN("failed to push back to array", K(tmp_ret), KPC(coll_info));
                    }
                  }
                  ob_sql_type_str(buf.get_data(), buf.count(), pos, meta.type_,
                                  OB_MAX_VARCHAR_LENGTH, meta.precision_, meta.scale_, meta.cs_type_, extended_type_info);
                  if (pos < buf.count()) {
                    buf.at(pos++) = '\0';
                    ob_type = buf.get_data();
                  }
                }
                LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_),
                        K(ObString(log_type.length(), log_type.data())),
                        K(col_desc->physical_type()),
                        "rep_level", col_desc->max_repetition_level());
                LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH,
                               !col_desc->logical_type()->is_none() ? log_type.c_str() : phy_type.c_str(),
                               ob_type);
              } else {
                column_indexs_.at(i) = column_index;
                LOG_DEBUG("mapped ob type", K(column_index), "column type",
                          file_meta_->schema()->Column(column_index)->physical_type(), "path",
                          data_access_info->data_access_path_);
              }
            }
          }
        }
      }
    END_CATCH_EXCEPTIONS
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObColumnMeta, 8> column_metas;
    ObSEArray<uint64_t, 8> column_ids;
    if (OB_FAIL(column_metas.prepare_allocate(mapping_column_ids_.count()))) {
      LOG_WARN("failed to prepare allocate column metas");
    } else if (OB_FAIL(column_ids.prepare_allocate(mapping_column_ids_.count()))) {
      LOG_WARN("failed to prepare allocate column ids");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < mapping_column_ids_.count(); ++i) {
      column_ids.at(i) = mapping_column_ids_.at(i).first;
      if (OB_INVALID_ID == mapping_column_ids_.at(i).first) {
        // do nothing
      } else if (OB_FAIL(column_metas.at(i).from_ob_expr(
                                            column_exprs_.at(mapping_column_ids_.at(i).second)))) {
        LOG_WARN("failed to init column meta from ob expr");
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(prepare_filter_col_meta(column_indexs_, column_ids, column_metas))) {
      LOG_WARN("fail to prepare filter col meta",
              K(ret), K(column_indexs_.count()), K(column_metas.count()));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::next_row_group()
{
  int ret = OB_SUCCESS;
  bool find_row_group = false;
  dynamic_switch_calc_mode();
  //init all meta
  while (OB_SUCC(ret) && !find_row_group) {
    while (OB_SUCC(ret) && state_.cur_row_group_idx_ > state_.end_row_group_idx_) {
      if (OB_FAIL(next_file())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to next row group", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t cur_row_group = (state_.cur_row_group_idx_++) - 1;
      if (cur_row_group > 0) {
        state_.cur_row_group_begin_row_id_ += file_meta_->RowGroup(cur_row_group - 1)->num_rows();
      }
      bool is_count_aggr = file_column_exprs_.count() == 0;
      if (is_iceberg_lake_table() && is_count_aggr) {
        int64_t task_idx = state_.file_idx_ - 1;
        ObIcebergScanTask *iceberg_scan_task =
          static_cast<ObIcebergScanTask *>(scan_param_->scan_tasks_.at(task_idx));
        if (OB_ISNULL(iceberg_scan_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null scan task", K(ret), K(task_idx));
        } else if (OB_FAIL(iceberg_scan_task->delete_files_.count() > 0 &&
                  !state_.is_delete_file_loaded_ &&
                  build_delete_bitmap(state_.cur_file_url_, task_idx))) {
          LOG_WARN("failed to read position delete", K(ret));
        } else {
          state_.cur_row_group_row_count_ = iceberg_scan_task->record_count_;
          state_.cur_row_group_row_count_ -=
            iceberg_scan_task->delete_files_.count() > 0 ? delete_bitmap_->get_cardinality() : 0;
          state_.is_delete_file_loaded_ = true;
          state_.logical_read_row_count_ = 0;
          state_.logical_eager_read_row_count_ = 0;
          find_row_group = true;
        }
      } else {
        bool can_skip = false;
        std::shared_ptr<parquet::RowGroupReader> rg_reader = file_reader_->RowGroup(cur_row_group);
        std::shared_ptr<parquet::RowGroupReader> eager_rg_reader = eager_file_reader_->RowGroup(cur_row_group);
        ObEvalCtx::TempAllocGuard alloc_guard(scan_param_->op_->get_eval_ctx());
        ParquetMinMaxFilterParamBuilder param_builder(this, rg_reader, file_meta_, alloc_guard.get_allocator());
        if (OB_FAIL(ObExternalTablePushdownFilter::apply_skipping_index_filter(PushdownLevel::ROW_GROUP, param_builder, can_skip))) {
          LOG_WARN("failed to apply skip index", K(ret));
        } else if (0 == rg_reader->metadata()->num_rows() || can_skip) {
          ++reader_metrics_.skipped_row_group_count_;
          LOG_TRACE("print skip rg", K(state_.cur_row_group_idx_), K(state_.end_row_group_idx_));
          continue;
        } /*else if (options_.enable_prebuffer_ && OB_FAIL(pre_buffer(rg_reader))) {
          LOG_WARN("failed to pre buffer", K(ret));
        }*/ else {
          ++reader_metrics_.selected_row_group_count_;
          sector_iter_.reset();
          find_row_group = true;
          if (is_iceberg_lake_table() && !state_.is_delete_file_loaded_) {
            if (OB_FAIL(build_delete_bitmap(state_.cur_file_url_, state_.file_idx_ - 1))) {
              LOG_WARN("failed to read position delete", K(ret));
            } else {
              state_.is_delete_file_loaded_ = true;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(prepare_rg_bitmap(rg_reader))) {
            LOG_WARN("failed to prepare bitmap", K(ret));
          } else if (OB_FAIL(prepare_page_index(cur_row_group, rg_reader, eager_rg_reader))) {
            LOG_WARN("failed to prepare page index", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::create_file_reader(const ObString& data_path,
                                                  const ObString file_content_digest,
                                                  const int64_t file_size,
                                                  const int64_t modify_time,
                                                  ObExternalFileAccess& file_access_driver,
                                                  ObFilePreBuffer& file_prebuffer,
                                                  std::unique_ptr<parquet::ParquetFileReader>& file_reader,
                                                  std::unique_ptr<parquet::ParquetFileReader>& eager_file_reader)
{
  int ret = OB_SUCCESS;
  arrow::Status status = arrow::Status::OK();
  BEGIN_CATCH_EXCEPTIONS
    std::shared_ptr<ObArrowFile> cur_file = std::make_shared<ObArrowFile>(file_access_driver,
                                                                          data_path.ptr(),
                                                                          &arrow_alloc_);
    std::shared_ptr<ObArrowFile> eager_file = std::make_shared<ObArrowFile>(eager_data_access_driver_,
                                                                            data_path.ptr(),
                                                                            &arrow_alloc_);
    ObExternalFileUrlInfo file_info(scan_param_->external_file_location_,
                                    scan_param_->external_file_access_info_, data_path,
                                    file_content_digest, file_size, modify_time);
    ObExternalFileCacheOptions cache_options(options_.enable_page_cache_,
                                            options_.enable_disk_cache_);
    if (options_.enable_prebuffer_) {
      cur_file->set_file_prebuffer(&file_prebuffer);
      eager_file->set_file_prebuffer(&eager_file_prebuffer_);
    }
    cur_file->set_timeout_timestamp(scan_param_->timeout_);
    eager_file->set_timeout_timestamp(scan_param_->timeout_);
    read_props_.enable_buffered_stream();
    if (OB_FAIL(cur_file.get()->open(file_info, cache_options))) {
      LOG_WARN("failed to open file", K(ret));
    } else if (OB_FAIL(eager_file.get()->open(file_info, cache_options))) {
      LOG_WARN("failed to open eager file", K(ret));
    } else {
      file_reader = parquet::ParquetFileReader::Open(cur_file, read_props_);
      eager_file_reader_ = parquet::ParquetFileReader::Open(eager_file, read_props_);
      if (!file_reader || !eager_file_reader_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create row reader failed", K(ret));
      } else {
        page_index_reader_ = file_reader->GetPageIndexReader();
      }
    }
  END_CATCH_EXCEPTIONS
  return ret;
}

ObExternalTableAccessOptions&
ObParquetTableRowIterator::make_external_table_access_options(stmt::StmtType stmt_type)
{
  if (stmt::T_INSERT == stmt_type) {
    options_ = ObExternalTableAccessOptions::disable_cache_defaults();
  } else {
    options_ = ObExternalTableAccessOptions::lazy_defaults();
  }
  return options_;
}

int ObParquetTableRowIterator::DataLoader::load_data_for_col(LOAD_FUNC &func)
{
  return (this->*func)();
}

bool ObParquetTableRowIterator::DataLoader::check_array_column_schema(const ::parquet::schema::Node* node) {
  // 1st level.
  // <list-repetition> group <name> (LIST)
  int ret = OB_SUCCESS;
  bool bret = false;
  if (!OB_ISNULL(node) && node->is_group() && node->logical_type()->is_list()) {
    const ::parquet::schema::GroupNode *group_node =
                                  reinterpret_cast<const ::parquet::schema::GroupNode *>(node);
    if (group_node->field_count() == 1) {
      // 2nd level.
      // repeated group list {
      const ::parquet::schema::NodePtr list_node = group_node->field(0);
      std::shared_ptr<parquet::schema::GroupNode> list_group_node =
                                  std::static_pointer_cast<::parquet::schema::GroupNode>(list_node);
      if (list_group_node->field_count() == 1) {
        // 3rd level.
        // <list-repetition> group <name> (LIST)
        const ::parquet::schema::NodePtr child_node = list_group_node->field(0);
        if (child_node->is_primitive()) {
          bret = true;
        } else {
          bret = check_array_column_schema(child_node.get());
          if (!bret) {
            LOG_WARN("check array column schema failed", K(bret));
          }
        }
      }
    }
  }
  return bret;
}

ObParquetTableRowIterator::DataLoader::LOAD_FUNC ObParquetTableRowIterator::DataLoader::select_load_function(
    const ObDatumMeta &datum_type, const parquet::ColumnDescriptor *col_desc, const ::parquet::schema::Node* node, const uint16_t type_id)
{
  LOAD_FUNC func = NULL;
  const parquet::LogicalType* log_type = col_desc->logical_type().get();
  parquet::Type::type phy_type = col_desc->physical_type();
  bool no_log_type = log_type->is_none();
  if (ob_is_collection_sql_type(datum_type.type_) && type_id == ObNestedType::OB_ARRAY_TYPE
      && check_array_column_schema(node)) {
    func = &DataLoader::load_list_to_array;
  } else if (no_log_type && parquet::Type::BOOLEAN == phy_type) {
    if (ob_is_number_or_decimal_int_tc(datum_type.type_)) {
      func = &DataLoader::load_decimal_any_col;
    } else if (ob_is_integer_type(datum_type.type_)) {
      func = &DataLoader::load_bool_to_int64_vec;
    }
  } else if ((no_log_type || log_type->is_int()) && ob_is_integer_type(datum_type.type_)) {
    //convert parquet int storing as int32/int64 to
    // ObTinyIntType/ObSmallIntType/ObMediumIntType/ObInt32Type/ObIntType using int64_t memory layout
    // ObUTinyIntType/ObUSmallIntType/ObUMediumIntType/ObUInt32Type/ObUInt64Type using uint64_t memory layout
    if (parquet::Type::INT64 == phy_type) {
      func = &DataLoader::load_int64_to_int64_vec;
    } else if (parquet::Type::INT32 == phy_type) {
      if (log_type->is_int() && !static_cast<const parquet::IntLogicalType*>(log_type)->is_signed()) {
        func = &DataLoader::load_uint32_to_int64_vec;
      } else {
        func = &DataLoader::load_int32_to_int64_vec;
      }
    }
    //sign and width
    ObObj temp_obj;
    temp_obj.set_int(datum_type.type_, 0);
    if ((no_log_type || static_cast<const parquet::IntLogicalType*>(log_type)->is_signed()) != temp_obj.is_signed_integer()) {
      func = NULL;
    }
    if (no_log_type ? (temp_obj.get_tight_data_len() < (parquet::Type::INT32 == phy_type ? 4 : 8))
                    : static_cast<const parquet::IntLogicalType*>(log_type)->bit_width() > temp_obj.get_tight_data_len() * 8) {
      func = NULL;
    }
  } else if ((no_log_type || log_type->is_string() || log_type->is_enum())
             && (ob_is_string_type(datum_type.type_) || ObRawType == datum_type.type_ || ob_is_large_text(datum_type.type_))) {
    //convert parquet enum/string to string vector
    if (parquet::Type::BYTE_ARRAY == phy_type) {
      func = &DataLoader::load_string_col;
    } else if (parquet::Type::FIXED_LEN_BYTE_ARRAY == phy_type) {
      func = &DataLoader::load_fixed_string_col;
    }
  } else if ((no_log_type || log_type->is_int() || log_type->is_decimal())
             && ob_is_number_or_decimal_int_tc(datum_type.type_)) {
    // no_log_type || log_type->is_int() for oracle int, phy_type should be int32 or int64
    //convert parquet int storing as int32/int64 to number/decimal vector
    if (log_type->is_decimal() && (col_desc->type_precision() > ((datum_type.precision_ == -1) ? 38 : datum_type.precision_)
                                   || col_desc->type_scale() != datum_type.scale_)) {
      func = NULL;
    } else if (!log_type->is_decimal() && parquet::Type::INT32 != phy_type
               && parquet::Type::INT64 != phy_type) {
      func = NULL;
    } else {
      //there is 4 kinds of physical format in parquet(int32/int64/fixedbytearray/bytearray)
      // and 2 class of types for OB vector(decimalint/number)
      if (parquet::Type::INT32 == phy_type && ob_is_decimal_int_tc(datum_type.type_)
          && DECIMAL_INT_32 == get_decimalint_type(datum_type.precision_)) {
        func = &DataLoader::load_int32_to_int32_vec;
      } else if (parquet::Type::INT64 == phy_type && ob_is_decimal_int_tc(datum_type.type_)
                 && DECIMAL_INT_64 == get_decimalint_type(datum_type.precision_)) {
        func = &DataLoader::load_int64_to_int64_vec;
      } else if (parquet::Type::INT32 == phy_type
                 || parquet::Type::INT64 == phy_type
                 || parquet::Type::BYTE_ARRAY == phy_type
                 || parquet::Type::FIXED_LEN_BYTE_ARRAY == phy_type) {
        func = &DataLoader::load_decimal_any_col;
      }
    }
  } else if ((no_log_type || log_type->is_date())
             && (ob_is_datetime_or_mysql_datetime(datum_type.type_)
                 || ob_is_date_or_mysql_date(datum_type.type_))) {
    if (parquet::Type::INT32 == phy_type && ob_is_date_tc(datum_type.type_)) {
      func = &DataLoader::load_int32_to_int32_vec;
    } else if (parquet::Type::INT32 == phy_type && ob_is_mysql_date_tc(datum_type.type_)) {
      func = &DataLoader::load_date_to_mysql_date;
    } else if (parquet::Type::INT32 == phy_type) {
      if(ob_is_datetime(datum_type.type_)) {
        func = &DataLoader::load_date_col_to_datetime;
      } else if (ob_is_mysql_datetime(datum_type.type_)) {
        func = &DataLoader::load_date_col_to_mysql_datetime;
      }
    }
  } else if ((no_log_type || log_type->is_int()) && parquet::Type::INT32 == phy_type
             && ob_is_year_tc(datum_type.type_)) {
    func = &DataLoader::load_year_col;
  } else if (log_type->is_time() && ob_is_time_tc(datum_type.type_)) {
    switch (static_cast<const parquet::TimeLogicalType*>(log_type)->time_unit()) {
      case parquet::LogicalType::TimeUnit::unit::MILLIS: {
        if (parquet::Type::INT32 == phy_type) {
          func = &DataLoader::load_time_millis_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::MICROS: {
        if (parquet::Type::INT64 == phy_type) {
          func = &DataLoader::load_int64_to_int64_vec;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::NANOS: {
        if (parquet::Type::INT64 == phy_type) {
          func = &DataLoader::load_time_nanos_col;
        }
        break;
      }
      default: {
        func = NULL;
      }
    }
  } else if (log_type->is_timestamp() && parquet::Type::INT64 == phy_type
             && (ob_is_otimestamp_type(datum_type.type_) || ob_is_datetime_or_mysql_datetime_tc(datum_type.type_))) {
    switch (static_cast<const parquet::TimestampLogicalType*>(log_type)->time_unit()) {
      case parquet::LogicalType::TimeUnit::unit::MILLIS: {
        if (ob_is_datetime_or_mysql_datetime_tc(datum_type.type_)
            || ObTimestampLTZType == datum_type.type_
            || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_millis_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::MICROS: {
        if ((ObTimestampType == datum_type.type_ && is_parquet_store_utc(log_type))
            || (ObDateTimeType == datum_type.type_ && !is_parquet_store_utc(log_type))) {
          //mysql timestamp storing utc timestamp as int64 values
          func = &DataLoader::load_int64_to_int64_vec;
        } else if (ob_is_datetime_or_mysql_datetime_tc(datum_type.type_)
                   || ObTimestampLTZType == datum_type.type_
                   || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_micros_col;
        }
        break;
      }
      case parquet::LogicalType::TimeUnit::unit::NANOS: {
        if (ob_is_datetime_or_mysql_datetime_tc(datum_type.type_)
            || ObTimestampLTZType == datum_type.type_
            || ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_nanos_col;
        }
        break;
      }
      default: {
        func = NULL;
      }
    }
  } else if ((no_log_type || log_type->is_timestamp()) && parquet::Type::INT96 == phy_type
             && (ob_is_otimestamp_type(datum_type.type_) || ObTimestampType == datum_type.type_)) {
    func = &DataLoader::load_timestamp_hive;
  } else if (no_log_type && parquet::Type::FLOAT == phy_type) {
    if (ob_is_float_tc(datum_type.type_)) {
      func = &DataLoader::load_float;
    } else if (ob_is_double_tc(datum_type.type_)) {
      func = &DataLoader::load_float_to_double;
    }
  } else if (no_log_type && parquet::Type::DOUBLE == phy_type && ob_is_double_tc(datum_type.type_)) {
    func = &DataLoader::load_double;
  } else if (log_type->is_interval() || log_type->is_map() || log_type->is_JSON()) {
    func = NULL;
  }
  return func;
}


#define IS_PARQUET_COL_NOT_NULL (0 == max_def_level)
#define IS_PARQUET_COL_VALUE_IS_NULL(V) (V < max_def_level)

int ObParquetTableRowIterator::DataLoader::load_int32_to_int32_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<int32_t*>(dec_vec->get_data()) + row_offset_, values.get_data(), sizeof(int32_t) * row_count_);
    } else {
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          dec_vec->set_null(i + row_offset_);
        } else {
          dec_vec->set_int32(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_date_to_mysql_date()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<int32_t*>(dec_vec->get_data()) + row_offset_, values.get_data(), sizeof(int32_t) * row_count_);
    } else {
      ObMySQLDate md_value = 0;
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          dec_vec->set_null(i + row_offset_);
        } else if (OB_FAIL(ObTimeConverter::date_to_mdate(values.at(j++), md_value))) {
          LOG_WARN("date_to_mdate fail", K(ret));
        } else {
          dec_vec->set_mysql_date(i + row_offset_, md_value);
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_default()
{
  int ret = OB_SUCCESS;
  ObIVector *vec = file_col_expr_->get_vector(eval_ctx_);
  int64_t value_cnt = std::min(batch_size_, cur_row_group_row_cnt_ - row_offset_);
  if (OB_FAIL(ObExternalTableRowIterator::set_default_batch(file_col_expr_->datum_meta_,
                                                            col_def_, vec))) {
    LOG_WARN("fail to set default", K(ret));
  } else {
    row_count_ = value_cnt;
    read_progress_ += row_count_;
  }
  return ret;
}

// convert int value to decimal int or number
int ObParquetTableRowIterator::DataLoader::to_numeric(const int64_t idx, const int64_t int_value)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    ObDecimalInt *decint = NULL;
    int32_t int_bytes = 0;
    if (OB_FAIL(wide::from_integer(int_value, tmp_alloc_g.get_allocator(), decint,
                                   int_bytes, file_col_expr_->datum_meta_.precision_))) {
      LOG_WARN("fail to from integer", K(ret));
    } else {
      vec->set_decimal_int(idx, decint, int_bytes);
    }
  } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
    ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
    number::ObNumber res_nmb;
    if (OB_FAIL(res_nmb.from(int_value, tmp_alloc_g.get_allocator()))) {
      LOG_WARN("fail to from number", K(ret));
    } else {
      vec->set_number(idx, res_nmb);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
  }
  return ret;
}

// convert string value to decimal int or number
int ObParquetTableRowIterator::DataLoader::to_numeric(
    const int64_t idx,
    const char *str,
    const int32_t length)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObDecimalInt *decint = NULL;
  int32_t val_len = 0;
  int16_t in_precision = 0;
  int16_t in_scale = 0;
  int16_t out_precision = file_col_expr_->datum_meta_.precision_;
  int16_t out_scale = file_col_expr_->datum_meta_.scale_;
  if (OB_FAIL(wide::from_string(str, length, tmp_alloc_g.get_allocator(), in_scale, in_precision, val_len, decint))) {
    LOG_WARN("fail to from number", K(ret), KPHEX(str, length));
  } else {
    if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
      if (in_precision > out_precision) {
        ret = OB_DECIMAL_PRECISION_OVERFLOW;
      } else {
        ObDecimalIntBuilder res_val;
        if (ObDatumCast::need_scale_decimalint(in_scale, in_precision, out_scale, out_precision)) {
          if (OB_FAIL(ObDatumCast::common_scale_decimalint(decint, val_len, in_scale, out_scale,
                                                           out_precision, 0, res_val))) {
            LOG_WARN("scale decimal int failed", K(ret));
          } else {
            vec->set_decimal_int(idx, res_val.get_decimal_int(), res_val.get_int_bytes());
          }
        } else {
          vec->set_decimal_int(idx, decint, val_len);
        }
      }
    } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
      ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
      number::ObNumber res_nmb;
      if (OB_FAIL(wide::to_number(decint, val_len, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
        LOG_WARN("fail to from", K(ret));
      } else {
        vec->set_number(idx, res_nmb);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
    }
  }

  return ret;
}

int ObParquetTableRowIterator::DataLoader::to_numeric_hive(
    const int64_t idx,
    const char *str,
    const int32_t length,
    char *buf,
    const int64_t data_len)
{
  int ret = OB_SUCCESS;
  ObDecimalInt *decint = NULL;
  int32_t val_len = 0;
  if (OB_UNLIKELY(length > data_len)) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
    LOG_WARN("overflow", K(length), K(data_len));
  } else {
    //to little endian
    MEMSET(buf, (*str >> 8), data_len); // fill 1 when the input value is negetive, otherwise fill 0
    if (data_len <= 4) {
      //for precision <= 9
      MEMCPY(buf + 4 - length, str, length);
      uint32_t *res = pointer_cast<uint32_t*>(buf);
      uint32_t temp_v = *res;
      *res = ntohl(temp_v);
    } else {
      int64_t pos = 0;
      int64_t temp_len = length;
      while (temp_len >= 8) {
        uint64_t temp_v = *(pointer_cast<const uint64_t*>(str + temp_len - 8));
        *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
        pos+=8;
        temp_len-=8;
      }
      if (temp_len > 0) {
        MEMCPY(buf + pos + 8 - temp_len, str, temp_len);
        uint64_t temp_v = *(pointer_cast<uint64_t*>(buf + pos));
        *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
      }
    }
    decint = pointer_cast<ObDecimalInt *>(buf);
    val_len = static_cast<int32_t>(data_len);
    if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
      ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
      vec->set_decimal_int(idx, decint, val_len);
    } else if (ObNumberType == file_col_expr_->datum_meta_.type_
               || ObUNumberType == file_col_expr_->datum_meta_.type_) {
      ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
      ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
      number::ObNumber res_nmb;
      if (OB_FAIL(wide::to_number(decint, val_len, file_col_expr_->datum_meta_.scale_,
                                  tmp_alloc_g.get_allocator(), res_nmb))) {
        LOG_WARN("fail to from", K(ret));
      } else {
        vec->set_number(idx, res_nmb);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported type", K(file_col_expr_->datum_meta_));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::to_numeric_hive(
    const char *str,
    const int32_t length,
    char *buf,
    const int64_t data_len,
    const ObDatumMeta &meta,
    ObIAllocator &alloc,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  ObDecimalInt *decint = NULL;
  int32_t val_len = 0;
  if (OB_UNLIKELY(length > data_len)) {
    ret = OB_DECIMAL_PRECISION_OVERFLOW;
    LOG_WARN("overflow", K(length), K(data_len));
  } else {
    //to little endian
    MEMSET(buf, (*str >> 8), data_len); // fill 1 when the input value is negetive, otherwise fill 0
    if (data_len <= 4) {
      //for precision <= 9
      MEMCPY(buf + 4 - length, str, length);
      uint32_t *res = pointer_cast<uint32_t*>(buf);
      uint32_t temp_v = *res;
      *res = ntohl(temp_v);
    } else {
      int64_t pos = 0;
      int64_t temp_len = length;
      while (temp_len >= 8) {
        uint64_t temp_v = *(pointer_cast<const uint64_t*>(str + temp_len - 8));
        *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
        pos+=8;
        temp_len-=8;
      }
      if (temp_len > 0) {
        MEMCPY(buf + pos + 8 - temp_len, str, temp_len);
        uint64_t temp_v = *(pointer_cast<uint64_t*>(buf + pos));
        *(pointer_cast<uint64_t*>(buf + pos)) = ntohll(temp_v);
      }
    }
    decint = pointer_cast<ObDecimalInt *>(buf);
    val_len = static_cast<int32_t>(data_len);
    if (ObDecimalIntType == meta.type_) {
      datum.set_decimal_int(decint, val_len);
    } else if (ObNumberType == meta.type_
               || ObUNumberType == meta.type_) {
      number::ObNumber res_nmb;
      if (OB_FAIL(wide::to_number(decint, val_len, meta.scale_,
                                  alloc, res_nmb))) {
        LOG_WARN("fail to from", K(ret));
      } else {
        datum.set_number(res_nmb);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not supported type", K(meta));
    }
  }
  return ret;
}


//convert int32/int64/string value(from parquet file) to decimal int or number(ob types)
int ObParquetTableRowIterator::DataLoader::load_decimal_any_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  //int16_t def_precision = static_cast<int16_t>(reader_->descr()->type_precision());
  //int16_t def_scale = static_cast<int16_t>(reader_->descr()->type_precision());

  if (reader_->descr()->physical_type() == parquet::Type::type::INT32) {
    ObArrayWrap<int32_t> values;
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    if (OB_SUCC(ret)) {
      row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
            batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
            values.get_data(), &values_cnt);
      read_progress_ += row_count_;
    }
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        OZ (to_numeric(i + row_offset_, values.at(j++)));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::type::INT64) {
    ObArrayWrap<int64_t> values;
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    if (OB_SUCC(ret)) {
      row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
            batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
            values.get_data(), &values_cnt);
      read_progress_ += row_count_;
    }
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        OZ (to_numeric(i + row_offset_, values.at(j++)));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::type::BOOLEAN) {
    ObArrayWrap<bool> values;
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    if (OB_SUCC(ret)) {
      row_count_ = static_cast<parquet::BoolReader*>(reader_)->ReadBatch(
            batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
            values.get_data(), &values_cnt);
      read_progress_ += row_count_;
    }
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        OZ (to_numeric(i + row_offset_, values.at(j++)));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::Type::FIXED_LEN_BYTE_ARRAY) {
    ObArrayWrap<parquet::FixedLenByteArray> values;
    int32_t fixed_length = reader_->descr()->type_length();
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
                                                    (file_col_expr_->datum_meta_.precision_ == -1)
                                                    ? 38 : file_col_expr_->datum_meta_.precision_);
    ObArrayWrap<char> buffer;
    OZ (buffer.allocate_array(tmp_alloc_g.get_allocator(), int_bytes));
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    if (OB_SUCC(ret)) {
      row_count_ = static_cast<parquet::FixedLenByteArrayReader*>(reader_)->ReadBatch(
            batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
            values.get_data(), &values_cnt);
      read_progress_ += row_count_;
    }
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        parquet::FixedLenByteArray &cur_v = values.at(j++);
        OZ (to_numeric_hive(i + row_offset_, pointer_cast<const char*>(cur_v.ptr), fixed_length, buffer.get_data(), buffer.count()));
        //OZ (to_numeric(i, pointer_cast<const char*>(cur_v.ptr), fixed_length));
      }
    }
  } else if (reader_->descr()->physical_type() == parquet::Type::Type::BYTE_ARRAY) {
    ObArrayWrap<parquet::ByteArray> values;
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
                                                    (file_col_expr_->datum_meta_.precision_ == -1)
                                                    ? 38 : file_col_expr_->datum_meta_.precision_);
    ObArrayWrap<char> buffer;
    OZ (buffer.allocate_array(tmp_alloc_g.get_allocator(), int_bytes));
    OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
    if (OB_SUCC(ret)) {
      row_count_ = static_cast<parquet::ByteArrayReader*>(reader_)->ReadBatch(
            batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
            values.get_data(), &values_cnt);
      read_progress_ += row_count_;
    }
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        parquet::ByteArray &cur_v = values.at(j++);
        OZ (to_numeric_hive(i + row_offset_, pointer_cast<const char*>(cur_v.ptr), cur_v.len, buffer.get_data(), buffer.count()));
        //OZ (to_numeric(i, pointer_cast<const char*>(cur_v.ptr), cur_v.len));
      }
    }
  }

  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_fixed_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<parquet::FixedLenByteArray> values;

  CK (VEC_DISCRETE == text_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    int32_t fixed_length = reader_->descr()->type_length();
    row_count_ = static_cast<parquet::FixedLenByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      bool is_byte_length = is_oracle_byte_length(
            lib::is_oracle_mode(), file_col_expr_->datum_meta_.length_semantics_);
      int j = 0;
      stat_.cross_page_cnt_ += cross_page_;
      stat_.in_page_cnt_ += (!cross_page_);
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          text_vec->set_null(i + row_offset_);
        } else {
          void *res_ptr = NULL;
          parquet::FixedLenByteArray &cur_v = values.at(j++);
          if (OB_UNLIKELY(fixed_length > file_col_expr_->max_length_
                          && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                       pointer_cast<const char *>(cur_v.ptr),
                                                                       fixed_length) > file_col_expr_->max_length_))) {
            ret = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("data too long", K(file_col_expr_->max_length_), K(fixed_length), K(is_byte_length), K(ret));
          } else if (ob_is_large_text(file_col_expr_->datum_meta_.type_)) {
            if (OB_FAIL(ObTextStringHelper::string_to_templob_result(*file_col_expr_, eval_ctx_,
                                      ObString(fixed_length, pointer_cast<const char *>(cur_v.ptr)),
                                                                     i + row_offset_))) {
              LOG_WARN("fail to lob result", K(ret));
            }
          } else {
            if (!cross_page_) {
              res_ptr = (void*)(cur_v.ptr);
            } else if (fixed_length > 0) {
              //when row_count_ less than batch_size_, it may reach page end and reload next page
              //string values need deep copy
              if (OB_ISNULL(res_ptr = str_res_mem_.alloc(fixed_length))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("fail to allocate memory", K(fixed_length));
              } else {
                MEMCPY(res_ptr, cur_v.ptr, fixed_length);
              }
            }
            text_vec->set_string(i + row_offset_, pointer_cast<const char *>(res_ptr), fixed_length);
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<parquet::ByteArray> values;

  CK (VEC_DISCRETE == text_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::ByteArrayReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      bool is_oracle_mode = lib::is_oracle_mode();
      bool is_byte_length = is_oracle_byte_length(
            is_oracle_mode, file_col_expr_->datum_meta_.length_semantics_);
      int j = 0;
      stat_.cross_page_cnt_ += cross_page_;
      stat_.in_page_cnt_ += (!cross_page_);
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          text_vec->set_null(i + row_offset_);
        } else {
          parquet::ByteArray &cur_v = values.at(j++);
          if (is_oracle_mode && 0 == cur_v.len) {
            text_vec->set_null(i + row_offset_);
          } else {
            void *res_ptr = NULL;
            if (OB_UNLIKELY(cur_v.len > file_col_expr_->max_length_
                            && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                        pointer_cast<const char *>(cur_v.ptr),
                                                                        cur_v.len) > file_col_expr_->max_length_))) {
              ret = OB_ERR_DATA_TOO_LONG;
              LOG_WARN("data too long", K(file_col_expr_->max_length_), K(cur_v.len), K(is_byte_length), K(ret));
            } else if (ob_is_large_text(file_col_expr_->datum_meta_.type_)) {
              if (OB_FAIL(ObTextStringHelper::string_to_templob_result(*file_col_expr_, eval_ctx_,
                                        ObString(cur_v.len, pointer_cast<const char *>(cur_v.ptr)),
                                                                       i + row_offset_))) {
                LOG_WARN("fail to lob result", K(ret));
              }
            } else {
              if (!cross_page_) {
                res_ptr = (void *)(cur_v.ptr);
              } else if (cur_v.len > 0) {
                //when row_count_ less than batch_size_, it may reach page end and reload next page
                //string values need deep copy
                if (OB_ISNULL(res_ptr = str_res_mem_.alloc(cur_v.len))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("fail to allocate memory", K(cur_v.len));
                } else {
                  MEMCPY(res_ptr, cur_v.ptr, cur_v.len);
                }
              }
              text_vec->set_string(i + row_offset_, pointer_cast<const char *>(res_ptr), cur_v.len);
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_int32_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<int32_t> values;

  CK (VEC_FIXED == int64_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int64_vec->set_null(i + row_offset_);
        } else {
          int64_vec->set_int(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_bool_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<bool> values;

  CK (VEC_FIXED == int64_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::BoolReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int64_vec->set_null(i + row_offset_);
        } else {
          int64_vec->set_int(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_uint32_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<int32_t> values;

  CK (VEC_FIXED == int32_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int32_vec->set_null(i + row_offset_);
        } else {
          uint32_t uint_value = static_cast<uint32_t>(values.at(j));
          int32_vec->set_int(i + row_offset_, static_cast<int64_t>(uint_value));
          j++;
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_int64_to_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<int64_t> values;

  CK (VEC_FIXED == int64_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<int64_t*>(int64_vec->get_data()) + row_offset_, values.get_data(), sizeof(int64_t) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          int64_vec->set_null(i + row_offset_);
        } else {
          int64_vec->set_int(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_date_col_to_datetime()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        dec_vec->set_datetime(i + row_offset_, values.at(j++) * USECS_PER_DAY);
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_date_col_to_mysql_datetime()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    ObMySQLDateTime mdt_value = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else if (OB_FAIL(ObTimeConverter::date_to_mdatetime(values.at(j++), mdt_value))) {
        LOG_WARN("date_to_mdatetime fail", K(ret));
      } else {
        dec_vec->set_mysql_datetime(i + row_offset_, mdt_value);
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_year_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        file_col_expr_->get_vector(eval_ctx_)->set_null(i + row_offset_);
      } else {
        dec_vec->set_year(i + row_offset_, values.at(j++));
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_time_millis_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int32_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int32Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        dec_vec->set_time(i + row_offset_, values.at(j++) * USECS_PER_MSEC);
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_time_nanos_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        dec_vec->set_time(i + row_offset_, values.at(j++) / NSECS_PER_USEC);
      }
    }
  }
  return ret;
}

bool ObParquetTableRowIterator::is_parquet_store_utc(const parquet::LogicalType *logtype)
{
  return logtype->is_timestamp() ? static_cast<const parquet::TimestampLogicalType*>(logtype)->is_adjusted_to_utc() : true;
}

bool ObParquetTableRowIterator::DataLoader::is_ob_type_store_utc(const ObDatumMeta &meta)
{
  return (lib::is_mysql_mode() && ObTimestampType == meta.type_)
         || (lib::is_oracle_mode() && ObTimestampLTZType == meta.type_);
}

int64_t ObParquetTableRowIterator::DataLoader::calc_tz_adjust_us(const parquet::LogicalType *logtype,
                                                                 const ObDatumMeta &meta,
                                                                 ObSQLSessionInfo *session)
{
  int64_t res = 0;
  bool is_utc_src = is_parquet_store_utc(logtype);
  bool is_utc_dst = is_ob_type_store_utc(meta);
  if (is_utc_src != is_utc_dst) {
    int32_t tmp_offset = 0;
    if (OB_NOT_NULL(session)
        && OB_NOT_NULL(session->get_timezone_info())
        && OB_SUCCESS == session->get_timezone_info()->get_timezone_offset(0, tmp_offset)) {
      res = SEC_TO_USEC(tmp_offset) * (is_utc_src ? 1 : -1);
    }
  }
  return res;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_millis_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us(reader_->descr()->logical_type().get(),
                                        file_col_expr_->datum_meta_,
                                        eval_ctx_.exec_ctx_.get_my_session());

  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        int64_t adjusted_value = values.at(j++) * USECS_PER_MSEC + adjust_us;
        if (ob_is_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          dec_vec->set_timestamp(i + row_offset_, adjusted_value);
        } else if (ob_is_mysql_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          ObMySQLDateTime mdatetime;
          ObTimeConverter::datetime_to_mdatetime(adjusted_value, mdatetime);
          dec_vec->set_mysql_datetime(i + row_offset_, mdatetime);
        } else {
          ObOTimestampData data;
          data.time_us_ = adjusted_value;
          dec_vec->set_otimestamp_tiny(i + row_offset_, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_micros_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us(reader_->descr()->logical_type().get(),
                                        file_col_expr_->datum_meta_,
                                        eval_ctx_.exec_ctx_.get_my_session());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        int64_t adjusted_value = (values.at(j++) + adjust_us);
        if (ob_is_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          dec_vec->set_timestamp(i + row_offset_, adjusted_value);
        } else if (ob_is_mysql_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          ObMySQLDateTime mdatetime;
          ObTimeConverter::datetime_to_mdatetime(adjusted_value, mdatetime);
          dec_vec->set_mysql_datetime(i + row_offset_, mdatetime);
        } else {
          ObOTimestampData data;
          data.time_us_ = adjusted_value;
          dec_vec->set_otimestamp_tiny(i + row_offset_, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_nanos_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<int64_t> values;
  int64_t adjust_us = calc_tz_adjust_us(reader_->descr()->logical_type().get(),
                                        file_col_expr_->datum_meta_,
                                        eval_ctx_.exec_ctx_.get_my_session());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int64Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        if (ob_is_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          dec_vec->set_timestamp(i + row_offset_, values.at(j++) / NSECS_PER_USEC + adjust_us);
        } else if (ob_is_mysql_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          ObMySQLDateTime mdatetime;
          ObTimeConverter::datetime_to_mdatetime(values.at(j++) / NSECS_PER_USEC + adjust_us, mdatetime);
          dec_vec->set_mysql_datetime(i + row_offset_, mdatetime);
        } else {
          ObOTimestampData data;
          int64_t cur_value = values.at(j++);
          data.time_us_ = cur_value / NSECS_PER_USEC + adjust_us;
          data.time_ctx_.set_tail_nsec(cur_value % NSECS_PER_USEC);
          dec_vec->set_otimestamp_tiny(i + row_offset_, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_timestamp_hive()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObArrayWrap<parquet::Int96> values;
  int64_t adjust_us = calc_tz_adjust_us(reader_->descr()->logical_type().get(),
                                        file_col_expr_->datum_meta_,
                                        eval_ctx_.exec_ctx_.get_my_session());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::Int96Reader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    int j = 0;
    for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
      if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
        dec_vec->set_null(i + row_offset_);
      } else {
        parquet::Int96 &value = values.at(j++);
        uint64_t nsec_time_value = ((uint64_t)value.value[1] << 32) + (uint64_t)value.value[0];
        uint32_t julian_date_value = value.value[2];
        int64_t utc_timestamp =((int64_t)julian_date_value - 2440588LL) * 86400000000LL + (int64_t)(nsec_time_value / NSECS_PER_USEC);
        if (ob_is_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          dec_vec->set_timestamp(i + row_offset_, utc_timestamp + adjust_us);
        } else if (ob_is_mysql_datetime_tc(file_col_expr_->datum_meta_.type_)) {
          ObMySQLDateTime mdatetime;
          ObTimeConverter::datetime_to_mdatetime(utc_timestamp + adjust_us, mdatetime);
          dec_vec->set_mysql_datetime(i + row_offset_, mdatetime);
        } else {
          ObOTimestampData data;
          data.time_us_ = utc_timestamp + adjust_us;
          data.time_ctx_.set_tail_nsec((int32_t)(nsec_time_value % NSECS_PER_USEC));
          dec_vec->set_otimestamp_tiny(i + row_offset_, ObOTimestampTinyData().from_timestamp_data(data));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_float()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *float_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<float> values;

  CK (VEC_FIXED == float_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::FloatReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<float*>(float_vec->get_data()) + row_offset_, values.get_data(), sizeof(float) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          float_vec->set_null(i + row_offset_);
        } else {
          float_vec->set_float(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_double()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<double> values;

  CK (VEC_FIXED == double_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::DoubleReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      MEMCPY(pointer_cast<double*>(double_vec->get_data()) + row_offset_, values.get_data(), sizeof(double) * row_count_);
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          double_vec->set_null(i + row_offset_);
        } else {
          double_vec->set_double(i + row_offset_, values.at(j++));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_float_to_double()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  int16_t max_def_level = reader_->descr()->max_definition_level();
  ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  ObArrayWrap<float> values;

  CK (VEC_FIXED == double_vec->get_format());
  OZ (values.allocate_array(tmp_alloc_g.get_allocator(), batch_size_));
  if (OB_SUCC(ret)) {
    row_count_ = static_cast<parquet::FloatReader*>(reader_)->ReadBatch(
          batch_size_, def_levels_buf_.get_data(), rep_levels_buf_.get_data(),
          values.get_data(), &values_cnt);
    read_progress_ += row_count_;
    if (OB_UNLIKELY(values_cnt > row_count_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("repeated data not support");
    } else if (IS_PARQUET_COL_NOT_NULL && values_cnt == row_count_) {
      for (int i = 0; i < row_count_; i++) {
        double_vec->set_double(i + row_offset_, static_cast<double>(values.at(i)));
      }
    } else {
      int j = 0;
      for (int i = 0; i < row_count_; i++) {
        if (IS_PARQUET_COL_VALUE_IS_NULL(def_levels_buf_.at(i))) {
          double_vec->set_null(i + row_offset_);
        } else {
          double_vec->set_double(i + row_offset_, static_cast<double>(values.at(j++)));
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::get_offset_nulls_from_levels(
  const ObArrayWrap<int16_t> &def_levels_buf, const ObArrayWrap<int16_t> &rep_levels_buf,
  const int64_t parent_def_level, const int64_t max_rep_level, const int64_t max_def_level,
  const int64_t levels_count, uint32_t *offsets, uint8_t *nulls)
{
  int ret = OB_SUCCESS;
  int64_t offset_idx = -1;
  bool skip_null = false;
  for (int i = 0; i < levels_count; ++i) {
    // belong to parent level, do not need to deal in this level, skip it
    if (def_levels_buf.at(i) <= parent_def_level || rep_levels_buf.at(i) > max_rep_level) {
      if (rep_levels_buf.at(i) == 0) {
        skip_null = true;
      }
      continue;
    }

    int def_offset = (def_levels_buf.at(i) > max_def_level) ? 1 : 0;
    if (rep_levels_buf.at(i) == 0) { // Line separation point, rep_level = 0
      offset_idx++;
      offsets[offset_idx] = def_offset;
      skip_null = false;
    } else if (rep_levels_buf.at(i) == max_rep_level) {
      offsets[offset_idx] += def_offset;
    } else { // Line break points
      offset_idx++;
      offsets[offset_idx] = (skip_null ? 0 : offsets[offset_idx - 1]) + def_offset;
      skip_null = false;
    }
    if ((offsets[offset_idx] == 0
         || (offset_idx > 0 && offsets[offset_idx] - offsets[offset_idx - 1] == 0))
        && def_levels_buf.at(i) < max_def_level) {
      nulls[offset_idx] = 1;
    } else {
      nulls[offset_idx] = 0;
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::set_data_attr_vector_payload_for_varchar_array(
  const uint32_t attrs_idx, common::ObIArrayWrap<uint32_t> &parent_offsets, uint8_t *nulls,
  uint32_t *offsets, const ObLength &max_accuracy_len)
{
  int ret = OB_SUCCESS;
  int64_t offset_idx = 0;
  ObExpr **attrs = file_col_expr_->attrs_;
  int64_t max_rep_level = reader_->descr()->max_repetition_level();
  if (attrs_idx + 2 >= file_col_expr_->attrs_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attrs_idx out of range", K(attrs_idx), K(file_col_expr_->attrs_cnt_));
  } else {
    StrDiscVec *vec0 = static_cast<StrDiscVec *>(attrs[attrs_idx]->get_vector(eval_ctx_));
    StrDiscVec *vec1 = static_cast<StrDiscVec *>(attrs[attrs_idx + 1]->get_vector(eval_ctx_));
    StrDiscVec *vec2 = static_cast<StrDiscVec *>(attrs[attrs_idx + 2]->get_vector(eval_ctx_));
    parquet::internal::BinaryRecordReader *binary_reader =
      dynamic_cast<parquet::internal::BinaryRecordReader *>(record_reader_);
    std::vector<std::shared_ptr<::arrow::Array>> array_vector = binary_reader->GetBuilderChunks();
    if (array_vector.size() < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array_vector size is less than 1", K(array_vector.size()));
    } else {
      ::arrow::BinaryArray *binary_array =
        dynamic_cast<::arrow::BinaryArray *>(array_vector[0].get());
      char *values = reinterpret_cast<char *>(const_cast<uint8_t *>(binary_array->raw_data()));
      const ::arrow::BinaryArray::offset_type *raw_offsets = binary_array->raw_value_offsets();
      if (OB_ISNULL(raw_offsets)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get raw offsets or values", K(ret));
      }
      int32_t start = 0;
      int32_t cur_offset = 0;
      for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
        void *res_ptr = NULL;
        uint32_t count = parent_offsets.at(i);
        int64_t idx = i + row_offset_;
        int64_t row_str_length = raw_offsets[start + count] - cur_offset;
        for (int j = start; OB_SUCC(ret) && j < start + count; j++) {
          int64_t value_str_length = raw_offsets[j + 1] - raw_offsets[j];
          offsets[j] = raw_offsets[j + 1] - cur_offset;
          if (OB_UNLIKELY(value_str_length > max_accuracy_len
                          && ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                              pointer_cast<const char *>(values + raw_offsets[j]),
                              value_str_length) > max_accuracy_len)) {
            ret = OB_ERR_DATA_TOO_LONG;
            LOG_WARN("data too long", K(max_accuracy_len), K(value_str_length), K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (row_str_length != 0 && OB_ISNULL(res_ptr = str_res_mem_.alloc(row_str_length))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(row_str_length));
        } else {
          MEMCPY(res_ptr, values + cur_offset, row_str_length);
          vec0->set_string(idx, reinterpret_cast<char *>(&nulls[start]), sizeof(uint8_t) * count);
          vec1->set_string(idx, reinterpret_cast<char *>(&offsets[start]), sizeof(uint32_t) * count);
          vec2->set_string(idx, pointer_cast<const char *>(res_ptr), row_str_length);
          start += count;
          cur_offset = raw_offsets[start];
        }
      }
    }
  }
  return ret;
}

template <typename T>
int ObParquetTableRowIterator::DataLoader::set_attr_vector_payload(
  const uint32_t attrs_idx, common::ObIArrayWrap<uint32_t> &parent_offsets, uint8_t *nulls,
  T *values)
{
  int ret = OB_SUCCESS;
  int64_t offset_idx = 0;
  int64_t max_rep_level = reader_->descr()->max_repetition_level();
  ObExpr **attrs = file_col_expr_->attrs_;
  if (attrs_idx + 1 >= file_col_expr_->attrs_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attrs_idx out of range", K(attrs_idx), K(file_col_expr_->attrs_cnt_));
  } else {
    StrDiscVec *vec0 = static_cast<StrDiscVec *>(attrs[attrs_idx]->get_vector(eval_ctx_));
    StrDiscVec *vec1 = static_cast<StrDiscVec *>(attrs[attrs_idx + 1]->get_vector(eval_ctx_));
    for (int i = 0; i < row_count_; ++i) {
      uint32_t offset = parent_offsets.at(i);
      const int64_t idx = i + row_offset_;
      vec0->set_string(idx, reinterpret_cast<char *>(&nulls[offset_idx]), sizeof(uint8_t) * offset);
      vec1->set_string(idx, reinterpret_cast<char *>(&values[offset_idx]), sizeof(T) * offset);
      offset_idx += offset;
      parent_offsets.at(i) = offset == 0 ? 0 : values[offset_idx - 1];
    }
  }
  return ret;
}

template <typename SrcType, typename DstType>
int ObParquetTableRowIterator::DataLoader::decode_list_to_array(
  const ObArrayWrap<int16_t> &def_levels_buf, const ObArrayWrap<int16_t> &rep_levels_buf,
  const ::parquet::schema::Node *&node, ObArrayWrap<uint32_t> &parent_offsets, ObExpr **attrs,
  uint32_t &attrs_idx, int64_t &parent_def_level, int64_t &max_rep_level, int64_t &max_def_level,
  const ObLength &max_accuracy_len)
{
  int ret = OB_SUCCESS;
  const int64_t levels_count = record_reader_->levels_position();
  const ::parquet::schema::GroupNode *group_node =
    reinterpret_cast<const ::parquet::schema::GroupNode *>(node);
  std::shared_ptr<parquet::schema::GroupNode> list_group_node =
    std::static_pointer_cast<::parquet::schema::GroupNode>(group_node->field(0));
  node = list_group_node->field(0).get();

  max_rep_level += 1;
  parent_def_level = max_def_level;
  max_def_level += node->is_optional() ? 2 : 1;
  bool set_innermost_value = max_def_level == record_reader_->descr()->max_definition_level();
  int64_t values_size = std::max(sizeof(DstType), sizeof(uint32_t)) * levels_count;
  uint8_t* nulls = reinterpret_cast<uint8_t *>(
                    attrs[attrs_idx]->get_str_res_mem(eval_ctx_, sizeof(uint8_t) * levels_count));
  uint32_t *values = reinterpret_cast<uint32_t *>(
                    attrs[attrs_idx + 1]->get_str_res_mem(eval_ctx_, values_size));
  if (OB_ISNULL(nulls) || OB_ISNULL(values)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(levels_count));
  } else if (OB_FAIL(get_offset_nulls_from_levels(def_levels_buf, rep_levels_buf, parent_def_level,
                                    max_rep_level, max_def_level, levels_count, values, nulls))) {
    LOG_WARN("fail to get offset nulls from levels", K(ret));
  } else if (set_innermost_value) {
    if constexpr (std::is_same_v<SrcType, parquet::ByteArray>) {
      if (OB_FAIL(set_data_attr_vector_payload_for_varchar_array(attrs_idx, parent_offsets, nulls,
                                                                 values, max_accuracy_len))) {
        LOG_WARN("fail to set attr vector payload", K(ret));
      }
    } else {
      if (sizeof(SrcType) == sizeof(DstType)) {
        MEMCPY(values, record_reader_->values(), sizeof(DstType) * record_reader_->values_written());
      } else {
        SrcType *src_values = reinterpret_cast<SrcType *>(record_reader_->values());
        for (int i = 0; i < record_reader_->values_written(); i++) {
          reinterpret_cast<DstType *>(values)[i] = static_cast<DstType>(src_values[i]);
        }
      }
      if (OB_FAIL(set_attr_vector_payload<DstType>(attrs_idx, parent_offsets, nulls,
                                                   reinterpret_cast<DstType *>(values)))) {
        LOG_WARN("fail to set attr vector payload", K(ret));
      }
    }
  } else if (OB_FAIL(set_attr_vector_payload(attrs_idx, parent_offsets, nulls, values))) {
    LOG_WARN("fail to set attr vector payload", K(ret));
  }
  attrs_idx += 2;
  return ret;
}

template <typename SrcType, typename DstType>
int ObParquetTableRowIterator::DataLoader::load_list_to_array_dispatch_type(
                                                                  const ObLength &max_accuracy_len)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  ObArrayWrap<uint32_t> parent_offsets;
  ObArrayWrap<int16_t> def_levels_buf;
  ObArrayWrap<int16_t> rep_levels_buf;
  ObExpr **attrs = file_col_expr_->attrs_;
  CollDiscVec *array_vec = static_cast<CollDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  ObFixedLengthBase *vec0 = static_cast<ObFixedLengthBase *>(attrs[0]->get_vector(eval_ctx_));

  record_reader_->Reset();
  row_count_ = record_reader_->ReadRecords(batch_size_);
  int64_t levels_count = record_reader_->levels_position();
  OZ(def_levels_buf.allocate_array(allocator, levels_count));
  OZ(rep_levels_buf.allocate_array(allocator, levels_count));
  OZ (parent_offsets.allocate_array(allocator, row_count_));
  if (OB_SUCC(ret)) {
    MEMCPY(def_levels_buf.get_data(), record_reader_->def_levels(), sizeof(int16_t) * levels_count);
    MEMCPY(rep_levels_buf.get_data(), record_reader_->rep_levels(), sizeof(int16_t) * levels_count);
  }

  const ::parquet::schema::Node* node = record_reader_->descr()->schema_node().get();
  for (int i = 0; i < record_reader_->descr()->max_repetition_level(); ++i) {
    node = node->parent()->parent();
  }
  uint32_t attrs_idx = 1;
  int64_t max_rep_level = 1;
  int64_t parent_def_level = -1;
  int64_t max_def_level = node->is_optional() ? 1 : 0;

  // Set the outermost size
  uint8_t *nulls = reinterpret_cast<uint8_t *>(
                attrs[attrs_idx]->get_str_res_mem(eval_ctx_, sizeof(uint8_t) * levels_count));
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(nulls)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(levels_count));
  } else if (OB_FAIL(get_offset_nulls_from_levels(def_levels_buf, rep_levels_buf, parent_def_level,
                                                  max_rep_level, max_def_level, levels_count,
                                                  parent_offsets.get_data(), nulls))) {
    LOG_WARN("fail to get offset nulls from levels", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < row_count_; ++i) {
    vec0->set_int(i + row_offset_, parent_offsets.at(i));
    if (nulls[i] == 1) {
      array_vec->set_null(i + row_offset_);
    }
  }

  // Set the nulls and offsets of each layer in the middle of the multidimensional array
  while (OB_SUCC(ret) && max_def_level < record_reader_->descr()->max_definition_level()) {
    if (OB_FAIL((decode_list_to_array<SrcType, DstType>(def_levels_buf, rep_levels_buf, node,
                                                      parent_offsets, attrs, attrs_idx,
                                                      parent_def_level, max_rep_level,
                                                      max_def_level, max_accuracy_len)))) {
      LOG_WARN("fail to decode list to array", K(ret));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::DataLoader::load_list_to_array()
{
  int ret = OB_SUCCESS;
  parquet::Type::type phy_type = record_reader_->descr()->physical_type();
  CollDiscVec *array_vec = static_cast<CollDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
  CK(VEC_DISCRETE == array_vec->get_format());

  const parquet::LogicalType* log_type = record_reader_->descr()->logical_type().get();
  bool no_log_type = log_type->is_none();
  if (OB_ISNULL(arr_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arr_type_ is null", K(ret));
  } else {
    uint32_t depth = 0;
    const ObDataType &basic_meta = arr_type_->get_basic_meta(depth);
    const ObLength max_accuracy_len = basic_meta.get_accuracy().get_length();
    ObObjType obj_type = basic_meta.get_obj_type();
    ObCollationType cs_type = basic_meta.get_collation_type();

    if (depth != record_reader_->descr()->max_repetition_level()) {
      ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
      LOG_WARN("not supported type", K(depth), K(record_reader_->descr()->max_repetition_level()), K(ret));
    } else if ((no_log_type || log_type->is_int()) && ob_is_integer_type(obj_type)) {
      //sign and width
      ObObj temp_obj;
      temp_obj.set_int(obj_type, 0);
      if (((no_log_type || static_cast<const parquet::IntLogicalType *>(log_type)->is_signed())
           != temp_obj.is_signed_integer())
          || (no_log_type ?
                (temp_obj.get_tight_data_len() != (parquet::Type::INT32 == phy_type ? 4 : 8)) :
                static_cast<const parquet::IntLogicalType *>(log_type)->bit_width()
                  > temp_obj.get_tight_data_len() * 8)) {
        ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
        LOG_WARN("not supported type", K(phy_type), K(obj_type), K(ret));
      } else if (parquet::Type::INT64 == phy_type) {
        ret = load_list_to_array_dispatch_type<int64_t, int64_t>(0);
      } else if (parquet::Type::INT32 == phy_type) {
        if (obj_type == ObTinyIntType || obj_type == ObUTinyIntType) {
          ret = load_list_to_array_dispatch_type<int32_t, int8_t>(0);
        } else if (obj_type == ObSmallIntType || obj_type == ObUSmallIntType) {
          ret = load_list_to_array_dispatch_type<int32_t, int16_t>(0);
        } else {
          ret = load_list_to_array_dispatch_type<int32_t, int32_t>(0);
        }
      }
    } else if (phy_type == parquet::Type::FLOAT && ob_is_float_tc(obj_type)) {
      ret = load_list_to_array_dispatch_type<float, float>(0);
    } else if (phy_type == parquet::Type::DOUBLE && ob_is_double_tc(obj_type)) {
      ret = load_list_to_array_dispatch_type<double, double>(0);
    } else if (phy_type == parquet::Type::BYTE_ARRAY && ob_is_varchar_or_char(obj_type, cs_type)) {
      ret = load_list_to_array_dispatch_type<parquet::ByteArray, int64_t>(max_accuracy_len);
    } else {
      ret = OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH;
      LOG_WARN("not supported type", K(phy_type), K(obj_type), K(ret));
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < row_count_; i++) {
    if (!array_vec->is_null(i + row_offset_)) {
      ObCollectionExprCell cell;
      cell.row_idx_ = i + row_offset_;
      cell.expr_ = file_col_expr_;
      cell.eval_ctx_ = &eval_ctx_;
      cell.format_ = ObCollectionExprCell::COLLECTION_VEC_FORMAT;
      array_vec->set_payload(i + row_offset_, &cell, sizeof(cell));
    }
  }
  const ObBitVector *tmp_skip = NULL;
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to load list to array", K(ret));
  } else if (OB_FAIL(ObCollectionExprUtil::cast_vector2compact_fmt(
               static_cast<ObDiscreteFormat *>(array_vec), row_offset_, row_offset_ + row_count_,
               tmp_skip))) {
    LOG_WARN("fail to cast vector to compact fmt", K(ret));
  }
  return ret;
}

#undef IS_PARQUET_COL_NOT_NULL
#undef IS_PARQUET_COL_VALUE_IS_NULL

int ObParquetTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);

  if (OB_FAIL(next_sector(capacity, eval_ctx, read_count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next sector", K(ret));
    }
  } else if (OB_FAIL(calc_file_meta_column(read_count, eval_ctx))) {
    LOG_WARN("failed to calc file meta column", K(ret));
  } else if (OB_FAIL(calc_column_convert(read_count, false, eval_ctx))) {
    LOG_WARN("failed to calc column convert", K(ret));
  } else if (OB_FAIL(calc_exprs_for_rowid(read_count, state_))) {
    LOG_WARN("failed to calc rowid", K(ret));
  } else if (is_lazy_calc()
             && scan_param_->ext_enable_late_materialization_
             && nullptr != scan_param_->pd_storage_filters_) {
    if (OB_FAIL(calc_filters(read_count, scan_param_->pd_storage_filters_, nullptr))) {
      LOG_WARN("failed to calc lazy filters", K(ret));
    } else if (OB_FAIL(reorder_output(*scan_param_->pd_storage_filters_->get_result(),
                                      eval_ctx, read_count))) {
      LOG_WARN("failed to reorder", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    count = read_count;
    reader_metrics_.read_rows_count_ += read_count;
    stat_.projected_lazy_cnt_ = reader_metrics_.read_rows_count_;
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("fail to get next rows from parquet file", K(ret), K(state_));
  }
  return ret;
}

int ObParquetTableRowIterator::get_next_row()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

void ObParquetTableRowIterator::reset() {
  // reset state_ to initial values for rescan
  state_.reuse();
  file_prebuffer_.destroy();
  eager_file_prebuffer_.destroy();
  if (nullptr != rg_bitmap_) {
    malloc_allocator_.free(rg_bitmap_);
    rg_bitmap_ = nullptr;
  }
}

int ObParquetTableRowIterator::read_min_max_datum(const std::string &min_val, const std::string &max_val,
                                                  const parquet::Type::type physical_type, const parquet::LogicalType* log_type,
                                                  const ObColumnMeta &column_meta, const parquet::SortOrder::type sort_order,
                                                  ObEvalCtx &eval_ctx, blocksstable::ObStorageDatum &min_datum,
                                                  blocksstable::ObStorageDatum &max_datum, ObIAllocator &tmp_alloc)
{
  int ret = OB_SUCCESS;
  bool no_log_type = log_type->is_none();
  switch (physical_type) {
    case parquet::Type::BOOLEAN: {
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if ((no_log_type || log_type->is_int()) && ob_is_int_tc(column_meta.type_)) {
        min_datum.set_bool(reinterpret_cast<const bool*>(min_val.data())[0]);
        max_datum.set_bool(reinterpret_cast<const bool*>(max_val.data())[0]);
      }
      break;
    }
    case parquet::Type::INT32: {
      int32_t tmp_min32 = reinterpret_cast<const int32_t*>(min_val.data())[0];
      int32_t tmp_max32 = reinterpret_cast<const int32_t*>(max_val.data())[0];
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if (no_log_type || log_type->is_int()) {
        if (ob_is_int_tc(column_meta.type_)
            || (ob_is_decimal_int_tc(column_meta.type_)
                && DECIMAL_INT_32 == get_decimalint_type(column_meta.precision_))) {
          min_datum.set_int32(tmp_min32);
          max_datum.set_int32(tmp_max32);
        } else if (ob_is_year_tc(column_meta.type_)) {
          min_datum.set_year(tmp_min32);
          max_datum.set_year(tmp_max32);
        }
      } else if (no_log_type || log_type->is_date()) {
        if (ObDateType == column_meta.type_) {
          min_datum.set_date(tmp_min32);
          max_datum.set_date(tmp_max32);
        } else if (ObMySQLDateType == column_meta.type_) {
          ObMySQLDate min_md_value = 0;
          ObMySQLDate max_md_value = 0;
          if (OB_FAIL(ObTimeConverter::date_to_mdate(tmp_min32, min_md_value))) {
            LOG_WARN("date_to_mdate fail", K(ret));
          } else if (OB_FAIL(ObTimeConverter::date_to_mdate(tmp_max32, max_md_value))) {
            LOG_WARN("date_to_mdate fail", K(ret));
          } else {
            min_datum.set_mysql_date(min_md_value);
            max_datum.set_mysql_date(max_md_value);
          }
        }
      } else if (log_type->is_time() && ob_is_time_tc(column_meta.type_)
                && parquet::LogicalType::TimeUnit::unit::MILLIS
                   == static_cast<const parquet::TimeLogicalType*>(log_type)->time_unit()) {
        min_datum.set_time(tmp_min32 * USECS_PER_MSEC);
        max_datum.set_time(tmp_max32 * USECS_PER_MSEC);
      }
      break;
    }
    case parquet::Type::INT64: {
      int64_t tmp_min64 = reinterpret_cast<const int64_t*>(min_val.data())[0];
      int64_t tmp_max64 = reinterpret_cast<const int64_t*>(max_val.data())[0];
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if ((no_log_type || log_type->is_int())
          && (ob_is_int_tc(column_meta.type_)
              || (ob_is_decimal_int_tc(column_meta.type_)
                  && DECIMAL_INT_64 == get_decimalint_type(column_meta.precision_)))) {
        min_datum.set_int(tmp_min64);
        max_datum.set_int(tmp_max64);
      } else if (log_type->is_time() && ob_is_time_tc(column_meta.type_)) {
        if (parquet::LogicalType::TimeUnit::unit::MICROS
            == static_cast<const parquet::TimeLogicalType*>(log_type)->time_unit()) {
          min_datum.set_time(tmp_min64);
          max_datum.set_time(tmp_max64);
        } else if (parquet::LogicalType::TimeUnit::unit::NANOS
                   == static_cast<const parquet::TimeLogicalType*>(log_type)->time_unit()) {
          min_datum.set_time(tmp_min64 / NSECS_PER_USEC);
          max_datum.set_time(tmp_max64 / NSECS_PER_USEC);
        }
      } else if (log_type->is_timestamp() && (ob_is_otimestamp_type(column_meta.type_)
                                              || ob_is_datetime_or_mysql_datetime_tc(column_meta.type_))) {
          int64_t adjust_us = DataLoader::calc_tz_adjust_us(log_type, column_meta, eval_ctx.exec_ctx_.get_my_session());
          if (parquet::LogicalType::TimeUnit::unit::MILLIS == static_cast<const parquet::TimestampLogicalType*>(log_type)->time_unit()) {
            int64_t adjusted_min_value = tmp_min64 * USECS_PER_MSEC + adjust_us;
            int64_t adjusted_max_value = tmp_max64 * USECS_PER_MSEC + adjust_us;
            if (ob_is_datetime_or_mysql_datetime_tc(column_meta.type_)
                || ObTimestampLTZType == column_meta.type_
                || ObTimestampNanoType == column_meta.type_) {
              if (OB_FAIL(convert_timestamp_datum(column_meta, adjusted_min_value,
                                                  adjusted_max_value, min_datum,
                                                  max_datum))) {
                LOG_WARN("failed to convert timestamp", K(ret));
              }
            }
          } else if (parquet::LogicalType::TimeUnit::unit::MICROS == static_cast<const parquet::TimestampLogicalType*>(log_type)->time_unit()) {
            if ((ObTimestampType == column_meta.type_ && is_parquet_store_utc(log_type))
                || (ObDateTimeType == column_meta.type_ && !is_parquet_store_utc(log_type))) {
              min_datum.set_timestamp(tmp_min64);
              max_datum.set_timestamp(tmp_max64);
            } else if (ob_is_datetime_or_mysql_datetime_tc(column_meta.type_)
                      || ObTimestampLTZType == column_meta.type_
                      || ObTimestampNanoType == column_meta.type_) {
              int64_t adjusted_min_value = tmp_min64 + adjust_us;
              int64_t adjusted_max_value = tmp_max64 + adjust_us;
              if (OB_FAIL(convert_timestamp_datum(column_meta, adjusted_min_value,
                                                  adjusted_max_value, min_datum,
                                                  max_datum))) {
                LOG_WARN("failed to convert timestamp", K(ret));
              }
            }
          } else if (parquet::LogicalType::TimeUnit::unit::NANOS == static_cast<const parquet::TimestampLogicalType*>(log_type)->time_unit()) {
            if (ob_is_datetime_or_mysql_datetime_tc(column_meta.type_)
                || ObTimestampLTZType == column_meta.type_
                || ObTimestampNanoType == column_meta.type_) {
              int64_t adjusted_min_value = tmp_min64 / NSECS_PER_USEC + adjust_us;
              int64_t adjusted_max_value = tmp_max64 / NSECS_PER_USEC + adjust_us;
              if (OB_FAIL(convert_timestamp_datum(column_meta, adjusted_min_value,
                                                  adjusted_max_value, min_datum,
                                                  max_datum))) {
                LOG_WARN("failed to convert timestamp", K(ret));
              }
            }
          }
      }
      break;
    }
    case parquet::Type::DOUBLE: {
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if (no_log_type && ob_is_double_tc(column_meta.type_)) {
        min_datum.set_double(reinterpret_cast<const double*>(min_val.data())[0]);
        max_datum.set_double(reinterpret_cast<const double*>(max_val.data())[0]);
      }
      break;
    }
    case parquet::Type::FLOAT: {
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if (no_log_type && ob_is_float_tc(column_meta.type_)) {
        min_datum.set_float(reinterpret_cast<const float*>(min_val.data())[0]);
        max_datum.set_float(reinterpret_cast<const float*>(max_val.data())[0]);
      }
      break;
    }
    case parquet::Type::INT96: {
      break;
    }
    case parquet::Type::BYTE_ARRAY: {
      if (ObCharset::is_bin_sort(column_meta.cs_type_)) {
        ObString min_ob_str(min_val.size(), min_val.c_str());
        ObString max_ob_str(max_val.size(), max_val.c_str());
        if ((ob_is_varbinary_or_binary(column_meta.type_, column_meta.cs_type_)
              || ob_is_varchar_or_char(column_meta.type_, column_meta.cs_type_))
            && min_val.size() <= column_meta.max_length_
            && max_val.size() <= column_meta.max_length_) {
          ObString tmp_min;
          ObString tmp_max;
          if (OB_FAIL(ob_write_string(tmp_alloc, min_ob_str, tmp_min))) {
            LOG_WARN("failed to copy string", K(ret));
          } else if (OB_FAIL(ob_write_string(tmp_alloc, max_ob_str, tmp_max))) {
            LOG_WARN("failed to copy string", K(ret));
          } else {
            min_datum.set_string(tmp_min);
            max_datum.set_string(tmp_max);
          }
        } else if (ob_is_text_tc(column_meta.type_)) {
          if (OB_FAIL(ObTextStringHelper::string_to_templob_result(column_meta.type_, column_meta.has_lob_header_,
              tmp_alloc, min_ob_str, min_datum))) {
            LOG_WARN("fail to string to templob result", K(ret));
          } else if (OB_FAIL(ObTextStringHelper::string_to_templob_result(column_meta.type_, column_meta.has_lob_header_,
              tmp_alloc, max_ob_str, max_datum))) {
            LOG_WARN("fail to string to templob result", K(ret));
          }
        }
      }
      break;
    }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      if (parquet::SortOrder::type::SIGNED != sort_order) {
      } else if (log_type->is_decimal() && ob_is_number_or_decimal_int_tc(column_meta.type_)) {
        if (ob_is_decimal_int_tc(column_meta.type_)
            && get_decimalint_type(column_meta.precision_) > DECIMAL_INT_256) {
          // ObStorageDatum only support 40 byte memory
          // do nothing
        } else {
          ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx);
          int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(
                                                    (column_meta.precision_ == -1)
                                                    ? 38 : column_meta.precision_);
          ObArrayWrap<char> buffer;
          OZ (buffer.allocate_array(tmp_alloc_g.get_allocator(), int_bytes));
          if (OB_FAIL(to_numeric_hive(pointer_cast<const char*>(min_val.data()), min_val.size(),
                                                                buffer.get_data(), buffer.count(),
                                                                column_meta, tmp_alloc_g.get_allocator(), min_datum))) {
            LOG_WARN("failed to convert numeric", K(ret));
          } else if (OB_FAIL(to_numeric_hive(pointer_cast<const char*>(max_val.data()), max_val.size(),
                                                                       buffer.get_data(), buffer.count(),
                                                                       column_meta, tmp_alloc_g.get_allocator(), max_datum))) {
            LOG_WARN("failed to convert numeric", K(ret));
          }
        }
      }
      break;
    }
    case parquet::Type::UNDEFINED: {
      break;
    }
    default:
      break;
  }
  return ret;
}

int ObParquetTableRowIterator::convert_timestamp_datum(const ObDatumMeta &datum_type, int64_t adjusted_min_value,
                                                       int64_t adjusted_max_value, ObStorageDatum &min_datum,
                                                       ObStorageDatum &max_datum)
{
  int ret = OB_SUCCESS;
  if (ob_is_datetime_tc(datum_type.type_)) {
    min_datum.set_timestamp(adjusted_min_value);
    max_datum.set_timestamp(adjusted_max_value);
  } else if (ob_is_mysql_datetime_tc(datum_type.type_)) {
    ObMySQLDateTime min_mdatetime;
    ObMySQLDateTime max_mdatetime;
    if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(adjusted_min_value, min_mdatetime))) {
      LOG_WARN("failed to convert mysql datetime", K(ret));
    } else if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(adjusted_max_value, max_mdatetime))) {
      LOG_WARN("failed to convert mysql datetime", K(ret));
    } else {
      min_datum.set_mysql_datetime(min_mdatetime);
      max_datum.set_mysql_datetime(max_mdatetime);
    }
  } else {
    ObOTimestampData min_data;
    ObOTimestampData max_data;
    min_data.time_us_ = adjusted_min_value;
    max_data.time_us_ = adjusted_max_value;
    min_datum.set_otimestamp_tiny(min_data);
    max_datum.set_otimestamp_tiny(max_data);
  }
  return ret;
}

int ObParquetTableRowIterator::ParquetMinMaxFilterParamBuilder::build(const int32_t ext_col_id,
                                                                      const ObColumnMeta &column_meta,
                                                                      blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<parquet::Statistics> stat
                      = rg_reader_->metadata()->ColumnChunk(ext_col_id)->statistics();
  param.set_uncertain();
  if (nullptr != stat) {
    std::string min_val = stat->EncodeMin();
    std::string max_val = stat->EncodeMax();
    parquet::Type::type parquet_type = file_meta_->schema()->Column(ext_col_id)->physical_type();
    parquet::SortOrder::type sort_order = stat->descr()->sort_order();
    const parquet::LogicalType* log_type
                      = file_meta_->schema()->Column(ext_col_id)->logical_type().get();
    if (stat->HasMinMax()) {
      if (OB_FAIL(ObParquetTableRowIterator::read_min_max_datum(min_val, max_val,
                                            parquet_type, log_type,
                                            column_meta, sort_order,
                                            row_iter_->scan_param_->op_->get_eval_ctx(),
                                            param.min_datum_, param.max_datum_, tmp_alloc_))) {
        LOG_WARN("failed to read min/max value", K(ret));
      }
    }
    if (!param.min_datum_.is_null() && !param.max_datum_.is_null()) {
      param.null_count_.set_int(1);
    }
    LOG_TRACE("print min/max info", K(parquet_type), K(sort_order), K(log_type->type()),
                                    K(ObString(min_val.size(), min_val.c_str())),
                                    K(ObString(max_val.size(), max_val.c_str())),
                                    K(column_meta), K(param.min_datum_), K(param.max_datum_));
  }
  return ret;
}

int ObParquetTableRowIterator::pre_buffer(ObFilePreBuffer &file_prebuffer,
                                    ObFilePreBuffer::ColumnRangeSlicesList &column_range_slice_list)
{
  int ret = OB_SUCCESS;
  try {
    ret = file_prebuffer.pre_buffer(column_range_slice_list);
  } catch (const ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("fail to read file", K(ret));
    }
  } catch(const std::exception& e) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), "Info", e.what());
    }
  } catch(...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::ParquetPageMinMaxFilterParamBuilder::build(const int32_t ext_col_id,
                                                                          const ObColumnMeta &column_meta,
                                                                          blocksstable::ObMinMaxFilterParam &param)
{
  int ret = OB_SUCCESS;
  std::shared_ptr<parquet::Statistics> stat
                  = rg_reader_->metadata()->ColumnChunk(ext_col_id)->statistics();
  param.set_uncertain();
  std::shared_ptr<parquet::ColumnIndex> column_index
                  = rg_page_index_reader_->GetColumnIndex(ext_col_id);
  if (nullptr != stat && nullptr != column_index && !column_index->null_pages().at(page_id_)) {
    const std::string &min_val = column_index->encoded_min_values().at(page_id_);
    const std::string &max_val = column_index->encoded_max_values().at(page_id_);
    parquet::Type::type parquet_type = file_meta_->schema()->Column(ext_col_id)->physical_type();
    parquet::SortOrder::type sort_order = stat->descr()->sort_order();
    const parquet::LogicalType* log_type = file_meta_->schema()->Column(ext_col_id)->logical_type().get();
    if (OB_FAIL(ObParquetTableRowIterator::read_min_max_datum(min_val, max_val,
                                          parquet_type, log_type,
                                          column_meta, sort_order,
                                          row_iter_->scan_param_->op_->get_eval_ctx(),
                                          param.min_datum_, param.max_datum_, tmp_alloc_))) {
      LOG_WARN("failed to read min/max value", K(ret));
    }
    if (!param.min_datum_.is_null() && !param.max_datum_.is_null()) {
      param.null_count_.set_int(1);
    }
    LOG_TRACE("print min/max info", K(parquet_type), K(sort_order),
                                    K(log_type->type()), K(column_meta.type_),
                                    K(ObString(min_val.size(), min_val.c_str())),
                                    K(ObString(max_val.size(), max_val.c_str())),
                                    K(param.min_datum_), K(param.max_datum_));
  }
  return ret;
}

int ObParquetTableRowIterator::ParquetPageMinMaxFilterParamBuilder::next_range(const int64_t column_id,
                                                                               int64_t &offset,
                                                                               int64_t &rows)
{
  int ret = OB_SUCCESS;
  if (nullptr != rg_page_index_reader_ && nullptr != page_index_reader_) {
    if (last_column_id_ != column_id) {
      page_id_ = -1;
      last_column_id_ = column_id;
    }
    std::shared_ptr<parquet::OffsetIndex> offset_index
                            = rg_page_index_reader_->GetOffsetIndex(column_id);
    if (++page_id_ >= offset_index->page_locations().size()) {
      ret = OB_ITER_END;
      LOG_TRACE("no range left", K(ret), K(page_id_),
                                 K(offset_index->page_locations().size()));
    } else {
      offset = offset_index->page_locations().at(page_id_).first_row_index;
      rows = (page_id_ == offset_index->page_locations().size() - 1)
                          ? rg_reader_->metadata()->num_rows() - offset
                            : offset_index->page_locations().at(page_id_ + 1).first_row_index - offset;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get range", K(ret),
                                    KP(rg_page_index_reader_.get()),
                                    KP(page_index_reader_.get()));
  }
  return ret;
}

int ObParquetTableRowIterator::pre_buffer(std::shared_ptr<parquet::RowGroupReader> rg_reader)
{
  int ret = OB_SUCCESS;
  ObFilePreBuffer::ColumnRangeSlicesList column_range_slice_list;
  ObFilePreBuffer::ColumnRangeSlicesList eager_column_range_slice_list;
  if (OB_UNLIKELY(column_range_slices_.empty())) {
    if (OB_FAIL(column_range_slices_.prepare_allocate(column_indexs_.count()))) {
      LOG_WARN("fail to prepare allocate array", K(ret));
    } else {
      // init slice for reuse.
      for (int64_t i = 0; OB_SUCC(ret) && i < column_range_slices_.count(); ++i) {
        void *buf = nullptr;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObFilePreBuffer::ColumnRangeSlices)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate memory", K(ret));
        } else {
          column_range_slices_.at(i) = new(buf)ObFilePreBuffer::ColumnRangeSlices;
        }
      }
    }
  }
  int64_t eager_column_cnt = 0;
  for (int i = 0; OB_SUCC(ret) && i < column_indexs_.count(); i++) {
    if (column_indexs_.at(i) >= 0) {
      std::unique_ptr<parquet::ColumnChunkMetaData> metadata = rg_reader->metadata()->ColumnChunk(column_indexs_.at(i));
      int64_t col_start = metadata->data_page_offset();
      if (metadata->has_dictionary_page()
          && metadata->dictionary_page_offset() > 0
          && col_start > metadata->dictionary_page_offset()) {
        col_start = metadata->dictionary_page_offset();
      }
      int64_t read_size = rg_reader->metadata()->ColumnChunk(column_indexs_.at(i))->total_compressed_size();
      column_range_slices_.at(i)->range_list_.reuse();
      OZ (column_range_slices_.at(i)->range_list_.push_back(ObFilePreBuffer::ReadRange(col_start, read_size)));
      OZ (column_range_slice_list.push_back(column_range_slices_.at(i)));
      if (has_eager_columns() && eager_column_cnt < get_eager_count()
      && eager_columns_.at(eager_column_cnt) == i) {
        OZ (eager_column_range_slice_list.push_back(column_range_slices_.at(i)));
      }
    } else {
      column_range_slices_.at(i)->range_list_.reuse();
      OZ (column_range_slice_list.push_back(column_range_slices_.at(i)));
    }
  }
  if (OB_SUCC(ret) && !column_range_slice_list.empty()) {
    OZ (pre_buffer(file_prebuffer_, column_range_slice_list));
    //TODO : prebuffer for eager
    //OZ (pre_buffer(eager_file_prebuffer_, eager_column_range_slice_list));
  }

  return ret;
}

void ObParquetTableRowIterator::reset_column_readers()
{
  for (int i = 0; i < column_readers_.count(); i++) {
    column_readers_.at(i) = NULL;
    record_readers_.at(i) = NULL;
  }
  for (int i = 0; i < eager_column_readers_.count(); ++i) {
    eager_column_readers_.at(i) = NULL;
    eager_record_readers_.at(i) = NULL;
  }
}

int ObParquetTableRowIterator::project_eager_columns(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_dependent_exprs_);
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);
  str_res_mem_.reuse();
  if (!has_eager_columns()) {
    read_count = capacity;
  } else {
    try {
      //load vec data from parquet file to file column expr
      for (int i = 0; OB_SUCC(ret) && i < get_eager_count(); ++i) {
        // use class member variables to apply data page filter
        cur_col_id_ = eager_columns_.at(i);
        cur_eager_id_ = i;
        int64_t load_row_count = 0;
        ObColumnDefaultValue default_value = is_lake_table() ?
                                  colid_default_value_arr_.at(cur_col_id_) : ObColumnDefaultValue();
        ObExpr* column_expr = get_column_expr_by_id(cur_col_id_);
        OZ (column_expr->init_vector_for_write(
                eval_ctx,
                column_expr->get_default_res_format(),
                eval_ctx.max_batch_size_));
        std::shared_ptr<parquet::ColumnReader> eager_col_reader = eager_column_readers_.at(i);
        ObCollectionArrayType *arr_type = NULL;
        if (ob_is_collection_sql_type(column_expr->datum_meta_.type_)) {
          OZ (ObArrayExprUtils::get_array_type_by_subschema_id(eval_ctx,
                                            column_expr->datum_meta_.get_subschema_id(), arr_type));
        }
        while (OB_SUCC(ret) && load_row_count < capacity &&
              ((eager_col_reader == nullptr && load_row_count < state_.cur_row_group_row_count_) ||
              (eager_col_reader != nullptr && eager_col_reader->HasNext()))) {
          int64_t temp_row_count = 0;
          DataLoader loader(eval_ctx, column_expr, arr_type,
                            eager_column_readers_.at(i).get(),
                            eager_record_readers_.at(i).get(),
                            def_levels_buf_, rep_levels_buf_, str_res_mem_,
                            capacity - load_row_count, load_row_count,
                            temp_row_count, state_.cur_row_group_row_count_,
                            default_value, state_.eager_read_row_counts_[i],
                            sector_iter_.is_cross_page(cur_col_id_), stat_);
          MEMSET(def_levels_buf_.get_data(), 0, sizeof(def_levels_buf_.at(0)) * eval_ctx.max_batch_size_);
          MEMSET(rep_levels_buf_.get_data(), 0, sizeof(rep_levels_buf_.at(0)) * eval_ctx.max_batch_size_);
          OZ (loader.load_data_for_col(load_funcs_.at(cur_col_id_)));
          load_row_count += temp_row_count;
        }
        if (OB_SUCC(ret)) {
          if (0 == read_count) {
            read_count = load_row_count;
          } else {
            if (read_count != load_row_count) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("row count inconsist", K(ret), K(read_count), K(load_row_count),
                                              K(state_), K(state_.eager_read_row_counts_),
                                              K(state_.read_row_counts_));
            }
          }
        }
        file_column_exprs_.at(cur_col_id_)->set_evaluated_projected(eval_ctx);
        if (0 == i) {
          stat_.projected_eager_cnt_ += load_row_count;
        }
      }
      //ObTaskController::get().allow_next_syslog();
      //LOG_INFO("print load batch", K(state_.eager_read_row_counts_[0]));
    } catch (const ObErrorCodeException &ob_error) {
      if (OB_SUCC(ret)) {
        ret = ob_error.get_error_code();
        LOG_WARN("fail to read file", K(ret));
      }
    } catch(const std::exception& e) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index", K(ret), "Info", e.what());
    } catch(...) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index", K(ret));
    }
  }

  if (OB_SUCC(ret) && read_count > 0) {
    scan_param_->op_->clear_evaluated_flag();
    if (OB_FAIL(calc_column_convert(read_count, true, eval_ctx))) {
      LOG_WARN("failed to calc column convert", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    state_.logical_eager_read_row_count_ += read_count;
    count = read_count;
  }
  return ret;
}

int ObParquetTableRowIterator::calc_filters(const int64_t count,
                                            ObPushdownFilterExecutor *curr_filter,
                                            ObPushdownFilterExecutor *parent_filter)
{
  int ret = OB_SUCCESS;
  if (nullptr != curr_filter) {
    common::ObBitmap *result = nullptr;
    bool filter_valid = true;
    if (OB_FAIL(curr_filter->init_evaluated_datums(filter_valid))) {
      LOG_WARN("failed to init eval datum", K(ret));
    } else if (!filter_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("filter is invalid", K(ret), K(curr_filter->is_logic_op_node()));
    } else if (OB_FAIL(curr_filter->init_bitmap(count, result))) {
      LOG_WARN("Failed to get filter bitmap", K(ret));
    } else if (curr_filter->is_filter_node()) {
      if ((OB_FAIL((static_cast<ObBlackFilterExecutor*>(curr_filter))->filter_batch(parent_filter,
          0, count, *result)))) {
        LOG_WARN("failed to filter batch", K(ret));
      }
    } else if (curr_filter->is_logic_op_node()) {
      sql::ObPushdownFilterExecutor **children = curr_filter->get_childs();
      if (parent_filter != nullptr
          && parent_filter->is_logic_and_node()
          && curr_filter->is_logic_and_node()) {
        MEMCPY(result->get_data(), parent_filter->get_result()->get_data(), count);
      }
      for (uint32_t i = 0; OB_SUCC(ret) && i < curr_filter->get_child_count(); i++) {
        const common::ObBitmap *child_result = nullptr;
        if (OB_ISNULL(children[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected null child filter", K(ret));
        } else if (OB_FAIL(calc_filters(count, children[i], curr_filter))) {
          LOG_WARN("Failed to filter micro block", K(ret), K(i), KP(children[i]));
        } else if (OB_ISNULL(child_result = children[i]->get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected get null filter bitmap", K(ret));
        } else {
          if (curr_filter->is_logic_and_node()) {
            if (OB_FAIL(result->bit_and(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
            } else if (result->is_all_false()) {
              break;
            }
          } else  {
            if (OB_FAIL(result->bit_or(*child_result))) {
              LOG_WARN("Failed to merge result bitmap", K(ret), KP(child_result));
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

int ObParquetTableRowIterator::project_lazy_columns(int64_t &read_count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);
  str_res_mem_.reuse();
  try {
    //load vec data from parquet file to file column expr
    for (int i = 0; OB_SUCC(ret) && i < get_lazy_file_count(); ++i) {
      cur_col_id_ = get_lazy_column_id(i);
      int64_t load_row_count = 0;
      int64_t tmp_logical_read = 0;
      ObColumnDefaultValue default_value = is_lake_table() ?
                                  colid_default_value_arr_.at(cur_col_id_) : ObColumnDefaultValue();
      ObExpr* column_expr = get_column_expr_by_id(cur_col_id_);
      OZ (column_expr->init_vector_for_write(
              eval_ctx, column_expr->get_default_res_format(), eval_ctx.max_batch_size_));
      ObIArray<int64_t> &skip_range = sector_iter_.get_skip_ranges();
      ObIArray<int64_t> &read_range = sector_iter_.get_read_ranges();
      ObCollectionArrayType *arr_type = NULL;
      if (ob_is_collection_sql_type(column_expr->datum_meta_.type_)) {
        OZ (ObArrayExprUtils::get_array_type_by_subschema_id(
          eval_ctx, column_expr->datum_meta_.get_subschema_id(), arr_type));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < skip_range.count(); ++j) {
        int64_t skip_count = SkipRowsInColumn(cur_col_id_, skip_range.at(j),
                                              state_.logical_read_row_count_ + tmp_logical_read,
                                              true, state_.read_row_counts_[cur_col_id_],
                                              column_readers_.at(cur_col_id_).get(),
                                              record_readers_.at(cur_col_id_).get(),
                                              ob_is_collection_sql_type(column_expr->datum_meta_.type_));
        tmp_logical_read += skip_range.at(j);
        int64_t temp_row_count = 0;
        int64_t orig_load_row_count = load_row_count;
        while (OB_SUCC(ret) && orig_load_row_count + read_range.at(j) > load_row_count) {
          DataLoader loader(eval_ctx, column_expr, arr_type,
                            column_readers_.at(cur_col_id_).get(),
                            record_readers_.at(cur_col_id_).get(),
                            def_levels_buf_, rep_levels_buf_, str_res_mem_,
                            orig_load_row_count + read_range.at(j) - load_row_count, load_row_count,
                            temp_row_count, state_.cur_row_group_row_count_,
                            default_value, state_.read_row_counts_[cur_col_id_],
                            sector_iter_.is_cross_page(cur_col_id_), stat_);
          MEMSET(def_levels_buf_.get_data(), 0, sizeof(def_levels_buf_.at(0)) * eval_ctx.max_batch_size_);
          MEMSET(rep_levels_buf_.get_data(), 0, sizeof(rep_levels_buf_.at(0)) * eval_ctx.max_batch_size_);
          OZ (loader.load_data_for_col(load_funcs_.at(cur_col_id_)));
          load_row_count += temp_row_count;
          tmp_logical_read += temp_row_count;
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == read_count) {
          read_count = load_row_count;
        } else {
          if (read_count != load_row_count) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row count inconsist", K(ret), K(read_count), K(load_row_count),
                                            K(state_), K(state_.eager_read_row_counts_),
                                            K(state_.read_row_counts_));
          }
        }
      }
      if (0 == i) {
        if (is_eager_calc()) {
        } else {
          stat_.projected_eager_cnt_ += load_row_count;
        }
      }
      column_expr->set_evaluated_projected(eval_ctx);
    }
  } catch (const ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("fail to read file", K(ret));
    }
  } catch(const std::exception& e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(ret), "Info", e.what());
  } catch(...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(ret));
  }
  for (int64_t i = 0; i < sector_iter_.get_skip_ranges().count(); ++i) {
    state_.logical_read_row_count_ += sector_iter_.get_skip_ranges().at(i);
  }
  if (0 == get_lazy_file_count()) {
    for (int64_t i = 0; i < sector_iter_.get_read_ranges().count(); ++i) {
      read_count += sector_iter_.get_read_ranges().at(i);
    }
  }
  state_.logical_read_row_count_ += read_count;
  return ret;
}

void ObParquetTableRowIterator::move_next()
{
  if (nullptr != rg_page_index_reader_) {
    if (state_.logical_read_row_count_ != state_.logical_eager_read_row_count_
        || (has_eager_columns()
            && state_.eager_read_row_counts_.at(0) > state_.logical_read_row_count_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "inconsist row count", K(state_), K(state_.eager_read_row_counts_));
    }
    bool find_sector = false;
    int64_t &curr_idx = state_.logical_read_row_count_;
    while (!find_sector && curr_idx < state_.cur_row_group_row_count_) {
      int64_t remain_size = state_.cur_row_group_row_count_ - curr_idx;
      int64_t capacity = min(SECTOR_SIZE, remain_size);
      if (rg_bitmap_->accumulate_bit_cnt(curr_idx, curr_idx + capacity) == capacity) {
        increase_read_rows(capacity, false);
      } else {
        find_sector = true;
      }
      /*ObTaskController::get().allow_next_syslog();
      LOG_INFO("print move next", K(capacity), K(find_sector), K(state_.logical_read_row_count_),
                                    K(state_.logical_eager_read_row_count_), K(curr_idx),
                                    K(state_.cur_row_group_row_count_), K(state_.read_row_counts_),
                                    K(state_.eager_read_row_counts_));*/
    }
  }
}

void ObParquetTableRowIterator::increase_read_rows(const int64_t rows, const bool only_eager)
{
  if (rows > 0) {
    int64_t eager_column_id = 0;
    for (int64_t i = 0; i < state_.read_row_counts_.count(); ++i) {
      cur_col_id_ = i;
      if (!only_eager) {
        SkipRowsInColumn( i, rows, state_.logical_read_row_count_, true, state_.read_row_counts_[i],
                  column_readers_.at(i).get(), record_readers_.at(i).get(),
                  ob_is_collection_sql_type(file_column_exprs_.at(i)->datum_meta_.type_));
      }
      if (has_eager_columns() && eager_column_id < get_eager_count()
                              && eager_columns_.at(eager_column_id) == i) {
        cur_eager_id_ = eager_column_id;
        SkipRowsInColumn(i, rows, state_.logical_eager_read_row_count_, false,
                  state_.eager_read_row_counts_[eager_column_id],
                  eager_column_readers_.at(eager_column_id).get(),
                  eager_record_readers_.at(eager_column_id).get(),
                  ob_is_collection_sql_type(file_column_exprs_.at(eager_column_id)->datum_meta_.type_));
        ++eager_column_id;
      }
    }
    state_.logical_eager_read_row_count_ += rows;
    if (!only_eager) {
      state_.logical_read_row_count_ += rows;
    }
  }
}


int ObParquetTableRowIterator::ParquetSectorIterator::prepare_next(const int64_t group_remain_count,
                                                                   const int64_t batch_size,
                                                                   ObPushdownFilterExecutor *root_filter,
                                                                   ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  int64_t capacity = std::min(capacity_, group_remain_count);
  if (is_end()) {
    if (!bitmap_.is_inited()) {
      OZ (bitmap_.init(capacity_));
      OZ (cross_pages_.prepare_allocate(iter_->file_column_exprs_.count()));
    }
    if (OB_SUCC(ret)) {
      rewind(capacity);
      if (group_remain_count > 0) {
        check_cross_pages(capacity);
        OZ (fill_eager_ranegs(*iter_->rg_bitmap_, batch_size,
                              iter_->state_.logical_eager_read_row_count_, capacity));
        CK (skip_ranges_.count() == read_ranges_.count());
        ObPushdownFilterExecutor *real_filter = iter_->has_eager_columns() ? root_filter : nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < skip_ranges_.count(); ++i) {
          iter_->increase_read_rows(skip_ranges_.at(i), true);
          if (nullptr == real_filter) {
            MEMSET(static_cast<void *> (&bitmap_.get_data()[size_]),
                                        1, sizeof(int8_t) * skip_ranges_.at(i));
          }
          size_ += skip_ranges_.at(i);
          int64_t real_batch_size = read_ranges_.at(i);
          int64_t read_count = 0;
          if (0 == real_batch_size) {
          } else if (OB_FAIL(iter_->project_eager_columns(read_count, real_batch_size))) {
            LOG_WARN("failed to project eager column", K(ret));
          } else if (OB_FAIL(iter_->calc_filters(read_count, real_filter, nullptr))) {
            LOG_WARN("failed to calc eager filters", K(ret));
          } else if (nullptr != real_filter
                      && OB_FAIL(bitmap_.append(size_, *real_filter->get_result(), 0, read_count))) {
            LOG_WARN("failed to copy bitmap", K(ret));
          } else {
            size_ += read_count;
          }
        }
        if (OB_SUCC(ret) && size_ > 0) {
          if (nullptr != real_filter && OB_FAIL(bitmap_.bit_not())) {
            LOG_WARN("failed to convert bitmap", K(ret));
          }
          ++segment_count_;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (0 == size_) {
  } else if (OB_FAIL(1 == batch_size ? fill_ranges_one() : fill_ranges(batch_size))) {
    LOG_WARN("failed to fill ranges", K(ret));
  } else if (skip_ranges_.empty()) {
    ret = OB_ERR_UNEXPECTED;
  }

  //LOG_INFO("print sector info", KP(root_filter), K(size_), K(idx_), K(capacity_), K(segment_count_), K(group_remain_count), K(batch_size));

  return ret;
}

void ObParquetTableRowIterator::ParquetSectorIterator::rewind(const int64_t capacity)
{
  size_ = 0;
  idx_ = 0;
  memset(static_cast<void *> (bitmap_.get_data()), 0, sizeof(int8_t) * capacity_);
  if (cross_pages_.count() > 0) {
    memset(pointer_cast<char *> (&cross_pages_.at(0)),
           0, sizeof(bool) * cross_pages_.count());
  }
}

void ObParquetTableRowIterator::ParquetSectorIterator::reset()
{
  size_ = 0;
  idx_ = 0;
  segment_count_ = 0;
}

int ObParquetTableRowIterator::ParquetSectorIterator::fill_ranges_one()
{
  int ret = OB_SUCCESS;
  skip_ranges_.reuse();
  read_ranges_.reuse();
  int64_t read_count = 0;
  int64_t start_idx = idx_;
  int64_t continus_len = 0;
  if (idx_ < size_) {
    while (idx_ < size_ && 1 == bitmap_.get_data()[idx_]) {
      ++idx_;
      ++continus_len;
    }
    OZ (skip_ranges_.push_back(continus_len));
    if (idx_ < size_) {
      OZ (read_ranges_.push_back(1));
      ++idx_;
    } else {
      OZ (read_ranges_.push_back(0));
    }
  }
  return ret;
}

int ObParquetTableRowIterator::ParquetSectorIterator::fill_ranges(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  skip_ranges_.reuse();
  read_ranges_.reuse();
  int64_t read_count = 0;
  if (idx_ < size_) {
    int64_t start_idx = idx_;
    for (; read_count < batch_size && idx_ < size_; ++idx_) {
      if (0 == bitmap_.get_data()[idx_]) {
        ++read_count;
      }
    }
    int64_t end_idx = idx_;
    int8_t val = bitmap_.get_data()[start_idx++];
    int64_t continus_len = 1;
    if (0 == val) {
      OZ (skip_ranges_.push_back(0));
    }
    for (; OB_SUCC(ret) && start_idx < end_idx; ++start_idx) {
      if (bitmap_.get_data()[start_idx] == val) {
        ++continus_len;
      } else if (0 == val) {
        OZ (read_ranges_.push_back(continus_len));
        continus_len = 1;
      } else {
        OZ (skip_ranges_.push_back(continus_len));
        continus_len = 1;
      }
      val = bitmap_.get_data()[start_idx];
    }
    if (0 == val) {
      OZ (read_ranges_.push_back(continus_len));
    } else {
      OZ (skip_ranges_.push_back(continus_len));
    }
  }
  if (read_ranges_.count() == skip_ranges_.count() - 1) {
    OZ (read_ranges_.push_back(0));
  }
  //LOG_INFO("print ranges", K(skip_ranges_), K(read_ranges_), K(idx_), K(size_), K(bitmap_), K(read_count));
  return ret;
}

int ObParquetTableRowIterator::ParquetSectorIterator::fill_eager_ranegs(const ObBitVector &rg_bitmap,
                                                                        const int64_t max_batch_size,
                                                                        const int64_t start_idx,
                                                                        const int64_t capacity)
{
  int ret = OB_SUCCESS;
  skip_ranges_.reuse();
  read_ranges_.reuse();
  int64_t i = start_idx;
  int64_t n = start_idx + capacity;
  if (rg_bitmap.at(i) == 0) {
    OZ (skip_ranges_.push_back(0));
  }
  while (i < n) {
    bool current_bit = rg_bitmap.at(i);
    int64_t start = i;
    while (i < n && rg_bitmap.at(i) == current_bit) {
      ++i;
    }
    int64_t length = i - start;
    if (current_bit) {
      OZ (skip_ranges_.push_back(length));
    } else {
      int64_t remaining = length;
      while (remaining > 0) {
          int64_t chunk = std::min(remaining, max_batch_size);
          if (read_ranges_.count() == skip_ranges_.count()) {
            OZ (skip_ranges_.push_back(0));
          }
          OZ (read_ranges_.push_back(chunk));
          remaining -= chunk;
      }
    }
  }
  if (read_ranges_.count() == skip_ranges_.count() - 1) {
    OZ (read_ranges_.push_back(0));
  }
  return ret;
}

void ObParquetTableRowIterator::ParquetSectorIterator::check_cross_pages(const int64_t capacity)
{
  if (cross_pages_.count() > 0) {
    if (nullptr == iter_->rg_page_index_reader_ && cross_pages_.count() > 0) {
      memset(pointer_cast<char *> (&cross_pages_.at(0)),
           1, sizeof(bool) * cross_pages_.count());
    } else {
      int64_t sector_start = iter_->state_.logical_read_row_count_;
      int64_t sector_end = sector_start + capacity;
      for (int64_t i = 0; i < cross_pages_.count(); ++i) {
        bool cross_page = true;
        if (iter_->column_indexs_.at(i) < 0) {
          cross_page = false;
        } else {
          std::shared_ptr<parquet::OffsetIndex> offset_index
                              = iter_->rg_page_index_reader_->GetOffsetIndex(iter_->column_indexs_.at(i));
          for (int64_t j = 0; cross_page && j < offset_index->page_locations().size(); ++j) {
            int64_t offset = offset_index->page_locations().at(j).first_row_index;
            if (sector_start >= offset) {
              if (j == offset_index->page_locations().size() - 1
                  || sector_end <= offset_index->page_locations().at(j + 1).first_row_index) {
                cross_page = false;
              }
            }
          }
        }
        cross_pages_.at(i) = cross_page;
      }
    }
  }
}



int64_t ObParquetTableRowIterator::SkipRowsInColumn(const int64_t column_id,
                                                    const int64_t num_rows_to_skip,
                                                    const int64_t logical_idx,
                                                    const bool is_lazy,
                                                    int64_t &curr_idx,
                                                    parquet::ColumnReader* reader,
                                                    parquet::internal::RecordReader* record_reader,
                                                    bool is_collection_column) {
  int64_t num_skipped = 0;
  int64_t need_skip_cnt = get_real_skip_count(logical_idx, num_rows_to_skip, column_id);
  if (OB_ISNULL(reader) || OB_ISNULL(record_reader)) {
    num_skipped = need_skip_cnt;
  } else if (is_collection_column) {
    num_skipped = record_reader->SkipRecords(need_skip_cnt);
  } else {
    switch (reader->type()) {
      case parquet::Type::BOOLEAN:
        num_skipped = static_cast<parquet::BoolReader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::INT32:
        num_skipped = static_cast<parquet::Int32Reader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::INT64:
        num_skipped = static_cast<parquet::Int64Reader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::BYTE_ARRAY:
        num_skipped = static_cast<parquet::ByteArrayReader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        num_skipped = static_cast<parquet::FixedLenByteArrayReader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::FLOAT:
        num_skipped = static_cast<parquet::FloatReader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::DOUBLE:
        num_skipped = static_cast<parquet::DoubleReader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::INT96:
        num_skipped = static_cast<parquet::Int96Reader*>(reader)->Skip(need_skip_cnt);
        break;
      case parquet::Type::UNDEFINED:
        break;
    }
  }
  curr_idx += num_skipped;
  if (num_skipped != need_skip_cnt) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "unexpected skip count", K(num_skipped), K(need_skip_cnt));
  }
  //ObTaskController::get().allow_next_syslog();
  //LOG_INFO("print skip zero", K(column_id), K(num_rows_to_skip), K(is_lazy), K(curr_idx), K(num_skipped), K(logical_idx));

  return num_skipped;
}

int64_t ObParquetTableRowIterator::get_real_skip_count(const int64_t curr_idx,
                                                       const int64_t num_rows_to_skip,
                                                       const int64_t column_id) {
  ObArray<std::pair<int64_t, int64_t>> *curr_page_ranges = page_skip_ranges_.at(column_id);
  int64_t target_start = curr_idx;
  int64_t target_end = curr_idx + num_rows_to_skip;
  int64_t total_covered = 0;

  for (int64_t i = 0; i < curr_page_ranges->count(); ++i) {
    std::pair<int64_t, int64_t> &range = curr_page_ranges->at(i);
    int64_t s = range.first;
    int64_t e = range.second;

    int64_t overlap_start = std::max(s, target_start);
    int64_t overlap_end = std::min(e, target_end);

    if (overlap_start < overlap_end) {
        total_covered += (overlap_end - overlap_start);
    }
  }
  return num_rows_to_skip - total_covered;
}

int ObParquetTableRowIterator::prepare_page_ranges(const int64_t num_rows)
{
  int ret = OB_SUCCESS;
  if (page_skip_ranges_.empty()) {
    OZ (page_skip_ranges_.prepare_allocate(file_column_exprs_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < page_skip_ranges_.count(); ++i) {
      void *buf = nullptr;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObArray<std::pair<int64_t, int64_t>>)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        page_skip_ranges_.at(i) = new (buf) ObArray<std::pair<int64_t, int64_t>>();
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (nullptr == rg_page_index_reader_) {
    } else {
      arrow::Status status = arrow::Status::OK();
      BEGIN_CATCH_EXCEPTIONS
        for (int64_t i = 0; OB_SUCC(ret) && i < page_skip_ranges_.count(); ++i) {
          page_skip_ranges_.at(i)->reuse();
          int column_idx = column_indexs_.at(i);
          if (column_idx >= 0) {
            std::shared_ptr<parquet::OffsetIndex> offset_index
                                  = rg_page_index_reader_->GetOffsetIndex(column_idx);
            for (int64_t j = 0; OB_SUCC(ret) && j < offset_index->page_locations().size(); ++j) {
              int64_t offset = offset_index->page_locations().at(j).first_row_index;
              int64_t rows = (j == offset_index->page_locations().size() - 1)
                                  ? num_rows - offset
                                    : offset_index->page_locations().at(j + 1).first_row_index - offset;
              int64_t begin = state_.read_row_counts_[i];
              int64_t end = begin + rows;
              bool can_skip = false;
              if (rg_bitmap_->accumulate_bit_cnt(offset, offset + rows) == rows) {
                can_skip = true;
              }
              if (can_skip) {
                if (page_skip_ranges_.at(i)->empty()
                    || offset != page_skip_ranges_.at(i)->at(page_skip_ranges_.at(i)->count() - 1).second) {
                  OZ (page_skip_ranges_.at(i)->push_back({offset, offset + rows}));
                } else {
                  page_skip_ranges_.at(i)->at(page_skip_ranges_.at(i)->count() - 1).second = offset + rows;
                }
              }
            }
          }
          //LOG_INFO("print page ranges", K(i), K(*page_skip_ranges_.at(i)));
        }
      END_CATCH_EXCEPTIONS
    }
  }
  return ret;
}

int ObParquetTableRowIterator::reorder_output(const oceanbase::common::ObBitmap &bitmap,
                                              ObEvalCtx &ctx,
                                              int64_t &read_count)
{
  int ret = OB_SUCCESS;
  int64_t real_count = bitmap.popcnt();
  if (real_count < read_count) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs_.count(); ++i) {
      ObIVector *vec = column_exprs_.at(i)->get_vector(ctx);
      int64_t project_count = 0;
      switch (vec->get_format()) {
        case VEC_FIXED : {
          ObFixedLengthBase *fix_vec = static_cast<ObFixedLengthBase *> (vec);
          for (int64_t j = 0; j < read_count; ++j) {
            if (bitmap[j]) {
              if (fix_vec->get_nulls()->at(j)) {
                fix_vec->get_nulls()->set(project_count);
              } else {
                fix_vec->get_nulls()->unset(project_count);
                MEMCPY(fix_vec->get_data() + fix_vec->get_length() * project_count,
                       fix_vec->get_data() + fix_vec->get_length() * j,
                       fix_vec->get_length());
              }
              ++project_count;
            }
          }
          break;
        }
        case VEC_DISCRETE : {
          ObDiscreteBase *dis_vec = static_cast<ObDiscreteBase *> (vec);
          for (int64_t j = 0; j < read_count; ++j) {
            if (bitmap[j]) {
              if (dis_vec->get_nulls()->at(j)) {
                dis_vec->get_nulls()->set(project_count);
              } else {
                dis_vec->get_nulls()->unset(project_count);
                dis_vec->get_lens()[project_count] = dis_vec->get_lens()[j];
                dis_vec->get_ptrs()[project_count] = dis_vec->get_ptrs()[j];
              }
              ++project_count;
            }
          }
          break;
        }
        case VEC_UNIFORM : {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *> (vec);
          for (int64_t j = 0; j < read_count; ++j) {
            if (bitmap[j]) {
              uni_vec->get_datums()[project_count] = uni_vec->get_datums()[j];
              ++project_count;
            }
          }
          break;
        }
        case VEC_UNIFORM_CONST : {
          break;
        }
        default : {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected format", K(ret), K(vec->get_format()));
        }
      }
    }
    read_count = real_count;
  }
  return ret;
}

int ObParquetTableRowIterator::prepare_rg_bitmap(std::shared_ptr<parquet::RowGroupReader> rg_reader)
{
  int ret = OB_SUCCESS;
  if (nullptr != rg_bitmap_) {
    malloc_allocator_.free(rg_bitmap_);
    rg_bitmap_ = nullptr;
  }
  if (OB_ISNULL(rg_bitmap_
          = to_bit_vector(malloc_allocator_.alloc(
                          ObBitVector::memory_size(rg_reader->metadata()->num_rows()))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc bitmap", K(ret), K(rg_reader->metadata()->num_rows()));
  } else {
    rg_bitmap_->reset(rg_reader->metadata()->num_rows());
  }

  if (OB_SUCC(ret)) {
    int64_t row_count = rg_reader->metadata()->num_rows();
    if (OB_NOT_NULL(delete_bitmap_) && !delete_bitmap_->is_empty_type()
        && delete_bitmap_->get_range_cardinality(state_.cur_row_group_begin_row_id_,
                                              state_.cur_row_group_begin_row_id_ + row_count) > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        int64_t global_row_id = i + state_.cur_row_group_begin_row_id_;
        if (delete_bitmap_->is_contains(global_row_id)) {
          rg_bitmap_->set(i);
        }
      }
    }
  }
  return ret;
}

int ObParquetTableRowIterator::prepare_page_index(const int64_t cur_row_group,
                                                  std::shared_ptr<parquet::RowGroupReader> rg_reader,
                                                  std::shared_ptr<parquet::RowGroupReader> eager_rg_reader)
{
  int ret = OB_SUCCESS;
  try {
    if (nullptr == page_index_reader_ ) {
      LOG_TRACE("do not have page index reader", K(cur_row_group));
    } else if (nullptr == (rg_page_index_reader_ = page_index_reader_->RowGroup(cur_row_group))) {
      LOG_TRACE("do not have page index reader", K(cur_row_group),
                                  K(state_.cur_row_group_idx_), K(state_.end_row_group_idx_));
    }
  } catch(const std::exception& e) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(ret), "Info", e.what(), K(cur_row_group));
  } catch(...) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index", K(ret), K(cur_row_group));
  }
  if (OB_SUCC(ret)) {
    ObEvalCtx::TempAllocGuard tmp_alloc_g(scan_param_->op_->get_eval_ctx());
    ParquetPageMinMaxFilterParamBuilder page_builder(this, rg_reader, file_meta_,
                                                     page_index_reader_, rg_page_index_reader_,
                                                     tmp_alloc_g.get_allocator());
    if (OB_FAIL(ObExternalTablePushdownFilter::apply_skipping_index_filter(PushdownLevel::PAGE, page_builder, rg_bitmap_,
        rg_reader->metadata()->num_rows(), tmp_alloc_g.get_allocator()))) {
      LOG_WARN("failed to apply skip index", K(ret));
    } else if (OB_FAIL(prepare_page_ranges(file_meta_->RowGroup(cur_row_group)->num_rows()))) {
      LOG_WARN("failed to prepare ranges", K(ret));
    } else {
      try {
        if (state_.read_row_counts_.count() > 0) {
          memset(pointer_cast<char *> (&state_.read_row_counts_.at(0)), 0,
                                       sizeof(int64_t) * state_.read_row_counts_.count());
        }
        if (state_.eager_read_row_counts_.count() > 0) {
          memset(pointer_cast<char *> (&state_.eager_read_row_counts_.at(0)), 0,
                                       sizeof(int64_t) * state_.eager_read_row_counts_.count());
        }
        state_.cur_row_group_row_count_ = file_meta_->RowGroup(cur_row_group)->num_rows();
        state_.logical_read_row_count_ = 0;
        state_.logical_eager_read_row_count_ = 0;
        //LOG_INFO("got rg", K(state_.cur_row_group_idx_), K(state_.cur_row_group_row_count_));
        int64_t eager_column_cnt = 0;
        for (int i = 0; OB_SUCC(ret) && i < column_indexs_.count(); i++) {
          if (column_indexs_.at(i) >= 0) {
            std::unique_ptr<parquet::PageReader> page_reader
                          = rg_reader->GetColumnPageReader(column_indexs_.at(i));
            std::unique_ptr<parquet::PageReader> eager_page_reader
                          = eager_rg_reader->GetColumnPageReader(column_indexs_.at(i));
            ReadPages r1(cur_col_id_, cur_col_id_, state_.read_row_counts_, page_skip_ranges_, reader_metrics_);
            ReadPages r2(cur_col_id_, cur_eager_id_, state_.eager_read_row_counts_, page_skip_ranges_, reader_metrics_);
            page_reader->set_data_page_filter(r1);
            eager_page_reader->set_data_page_filter(r2);
            column_readers_.at(i) = parquet::ColumnReader::Make(
                                    rg_reader->metadata()->schema()->Column(column_indexs_.at(i)),
                                    std::move(page_reader));
            record_readers_.at(i) = rg_reader->RecordReader(column_indexs_.at(i));
            if (has_eager_columns() && eager_column_cnt < get_eager_count()
                                    && eager_columns_.at(eager_column_cnt) == i) {
              eager_column_readers_.at(eager_column_cnt) = parquet::ColumnReader::Make(
                                    eager_rg_reader->metadata()->schema()->Column(column_indexs_.at(i)),
                                    std::move(eager_page_reader));
              eager_record_readers_.at(eager_column_cnt) = eager_rg_reader->RecordReader(column_indexs_.at(i));
              eager_column_cnt++;
            }
          } else {
            column_readers_.at(i) = nullptr;
            if (has_eager_columns() && eager_column_cnt < get_eager_count()
                                    && eager_columns_.at(eager_column_cnt) == i) {
              eager_column_readers_.at(eager_column_cnt) = nullptr;
              eager_record_readers_.at(eager_column_cnt) = nullptr;
              eager_column_cnt++;
            }
          }
        }
      } catch (const ObErrorCodeException &ob_error) {
        if (OB_SUCC(ret)) {
          ret = ob_error.get_error_code();
          LOG_WARN("fail to read file", K(ret));
        }
      } catch(const std::exception& e) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index", K(ret), "Info", e.what(), K(cur_row_group), K(column_indexs_));
      } catch(...) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index", K(ret), K(cur_row_group), K(column_indexs_));
      }
    }
  }
  return ret;
}

void ObParquetTableRowIterator::dynamic_switch_calc_mode()
{
  FilterCalcMode ori_mode = mode_;
  if (is_dynamic_calc() && stat_.projected_eager_cnt_ > 0) {
    if (stat_.projected_lazy_cnt_ > stat_.projected_eager_cnt_) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid stat project cnt", K(mode_), K(stat_));
    } else {
      double ratio = static_cast<double> (stat_.projected_lazy_cnt_ ) / stat_.projected_eager_cnt_;
      if (is_eager_calc()) {
        if (ratio > EAGER_CALC_CUT_RATIO) {
          mode_ = FilterCalcMode::DYNAMIC_LAZY_CALC;
        }
      } else if (ratio < EAGER_CALC_CUT_RATIO) {
        mode_ = FilterCalcMode::DYNAMIC_EAGER_CALC;
      }
    }
  }
  if (ori_mode != mode_) {
    LOG_TRACE("trace switch mode", K(mode_), K(stat_));
  }
}

int ObParquetTableRowIterator::calc_file_meta_column(const int64_t read_count,
                                                     ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  //fill expr results from metadata
  for (int i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
    ObExpr *meta_expr = file_meta_column_exprs_.at(i);
    if (meta_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
      StrDiscVec *text_vec = static_cast<StrDiscVec *>(meta_expr->get_vector(eval_ctx));
      OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_DISCRETE, read_count));
      if (OB_SUCC(ret)) {
        text_vec->set_ptrs(file_url_ptrs_.get_data());
        text_vec->set_lens(file_url_lens_.get_data());
      }
    } else if (meta_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
      OZ (meta_expr->init_vector_for_write(eval_ctx, VEC_UNIFORM, read_count));
      OZ (fill_file_partition_expr(meta_expr, state_.part_list_val_, read_count));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", KPC(meta_expr));
    }
    meta_expr->set_evaluated_projected(eval_ctx);
  }
  return ret;
}

int ObParquetTableRowIterator::calc_column_convert(const int64_t read_count,
                                                   const bool is_eager,
                                                   ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_dependent_exprs_);
  int64_t column_cnt = is_eager ? get_eager_count() : get_lazy_access_count();
  for (int i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
    int64_t cur_col_id = is_eager ? mapping_column_ids_.at(eager_columns_.at(i)).second : get_lazy_column_id(i);
    if (cur_col_id == OB_INVALID_ID || column_need_conv_.at(cur_col_id)) {
      //column_conv_exprs is 1-1 mapped to column_exprs
      //calc gen column exprs
      if (!column_conv_exprs.at(cur_col_id)->get_eval_info(eval_ctx).evaluated_) {
        OZ (column_conv_exprs.at(cur_col_id)->init_vector_default(eval_ctx, read_count));
        OZ (column_conv_exprs.at(cur_col_id)->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
        column_conv_exprs.at(cur_col_id)->set_evaluated_projected(eval_ctx);
      }
      //assign gen column exprs value to column exprs(output exprs)
      if (OB_SUCC(ret)) {
        ObExpr *to = column_exprs_.at(cur_col_id);
        ObExpr *from = column_conv_exprs.at(cur_col_id);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < read_count && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx);
          if (src != dst) {
            MEMCPY(dst, src, read_count * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
        } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
          LOG_WARN("assign vector header failed", K(ret));
        }
        column_exprs_.at(cur_col_id)->set_evaluated_projected(eval_ctx);
      }
    } else {
      // do nothing

    }
  }
  return ret;
}

int ObParquetTableRowIterator::next_sector(const int64_t capacity, ObEvalCtx &eval_ctx, int64_t &read_count)
{
  int ret = OB_SUCCESS;
  read_count = 0;
  try {
    while (OB_SUCC(ret) && 0 == read_count) {
      if (state_.logical_read_row_count_ >= state_.cur_row_group_row_count_) {
        if (OB_FAIL(next_row_group())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to next row group", K(ret));
          }
        }
      }
      if (!file_column_exprs_.count()) {
        read_count = std::min(capacity, state_.cur_row_group_row_count_ - state_.logical_read_row_count_);
        state_.logical_read_row_count_ += read_count;
        state_.logical_eager_read_row_count_ += read_count;
      } else {
        if (OB_FAIL(ret)) {
        } else if (sector_iter_.is_end() && FALSE_IT(move_next())) {
        } else if (OB_FAIL(sector_iter_.prepare_next(state_.cur_row_group_row_count_ - (has_eager_columns()
                                  ? state_.logical_eager_read_row_count_ : state_.logical_read_row_count_),
                                      capacity, scan_param_->pd_storage_filters_, eval_ctx))) {
          LOG_WARN("failed to prepare next", K(ret));
        } else if (sector_iter_.is_empty()) {
        } else if (FALSE_IT(scan_param_->op_->clear_evaluated_flag())) {
        } else if (OB_FAIL(project_lazy_columns(read_count, capacity))) {
          LOG_WARN("failed to project lazy columns", K(ret));
        }
      }
    }
  } catch (const ObErrorCodeException &ob_error) {
    if (OB_SUCC(ret)) {
      ret = ob_error.get_error_code();
      LOG_WARN("fail to read file", K(ret));
    }
  } catch(const std::exception& e) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), "Info", e.what());
    }
  } catch(...) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    }
  }
  return ret;
}

DEF_TO_STRING(ObParquetIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("ob_external_iterator_state");
  J_COLON();
  pos += ObExternalIteratorState::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(row_group_idx),
       K_(cur_row_group_idx),
       K_(end_row_group_idx),
       K_(cur_row_group_row_count));
  J_OBJ_END();
  return pos;
}

#undef BEGIN_CATCH_EXCEPTIONS
#undef END_CATCH_EXCEPTIONS
}
}
