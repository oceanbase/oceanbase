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
#include "ob_orc_table_row_iter.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "share/external_table/ob_external_table_utils.h"


namespace oceanbase
{
using namespace share::schema;
using namespace common;
using namespace share;
namespace sql {

int ObOrcTableRowIterator::to_dot_column_path(ObIArray<ObString> &col_names, ObString &path)
{
  int ret = OB_SUCCESS;
  ObSqlString tmp_string;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_names.count(); i++) {
    if (i > 0) {
      OZ (tmp_string.append("."));
    }
    OZ (tmp_string.append(col_names.at(i)));
  }
  OZ (ob_write_string(allocator_, tmp_string.string(), path));
  return ret;
}


/**
  * Recurses over a type tree and build two maps
  * map<TypeName, TypeId>, map<TypeId, Type>
  */
int ObOrcTableRowIterator::build_type_name_id_map(const orc::Type* type, ObIArray<ObString> &col_names)
{
  int ret = OB_SUCCESS;
  CK (type != nullptr);
  OZ (id_to_type_.set_refactored(type->getColumnId(), type, 0));
  if (OB_FAIL(ret)) {
  } else if (orc::TypeKind::STRUCT == type->getKind()) {
    for (size_t i = 0; OB_SUCC(ret) && i < type->getSubtypeCount(); ++i) {
      const std::string& cpp_field_name = type->getFieldName(i);
      ObString field_name;
      OZ (ob_write_string(allocator_, ObString(cpp_field_name.c_str()), field_name));
      OZ (col_names.push_back(field_name));
      ObString path;
      OZ (to_dot_column_path(col_names, path));
      OZ (name_to_id_.set_refactored(path, type->getSubtype(i)->getColumnId(), 0));
      OZ (build_type_name_id_map(type->getSubtype(i), col_names));
      if (OB_FAIL(ret)) {
      } else if (col_names.count() > 0) {
        col_names.pop_back();
      }
    }
  } else {
    // other non-primitive type
    for (size_t j = 0; OB_SUCC(ret) && j < type->getSubtypeCount(); ++j) {
      OZ (build_type_name_id_map(type->getSubtype(j), col_names));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::init(const storage::ObTableScanParam *scan_param)
{
  int ret = OB_SUCCESS;

  CK (scan_param != nullptr);
  CK (scan_param->op_ != nullptr);
  CK (scan_param->ext_column_convert_exprs_ != nullptr);
  if (OB_SUCC(ret)) {
    ObEvalCtx &eval_ctx = scan_param->op_->get_eval_ctx();
    int64_t column_cnt = scan_param->ext_column_convert_exprs_->count();
    mem_attr_ = ObMemAttr(MTL_ID(), "OrcRowIter");
    allocator_.set_attr(mem_attr_);
    orc_alloc_.init(MTL_ID());
    OZ (id_to_type_.create(512, mem_attr_));
    OZ (name_to_id_.create(512, mem_attr_));
    OZ (ObExternalTableRowIterator::init(scan_param));
    OZ (data_access_driver_.init(scan_param->external_file_location_,
                                scan_param->external_file_access_info_));

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

    ObArray<ObExpr*> file_column_exprs;
    ObArray<ObExpr*> file_meta_column_exprs;
    for (int i = 0; OB_SUCC(ret) && i < scan_param->ext_file_column_exprs_->count(); i++) {
      ObExpr* ext_file_column_expr = scan_param->ext_file_column_exprs_->at(i);
      if (OB_ISNULL(ext_file_column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ptr", K(ret));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL
                  || ext_file_column_expr->type_ == T_PSEUDO_PARTITION_LIST_COL) {
        OZ (file_meta_column_exprs.push_back(ext_file_column_expr));
      } else if (ext_file_column_expr->type_ == T_PSEUDO_EXTERNAL_FILE_COL) {
        OZ (file_column_exprs.push_back(ext_file_column_expr));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", KPC(ext_file_column_expr));
      }
    }
    OZ (file_column_exprs_.assign(file_column_exprs));
    OZ (file_meta_column_exprs_.assign(file_meta_column_exprs));

    if (OB_SUCC(ret) && file_column_exprs_.count() > 0) {
      OZ (column_indexs_.allocate_array(allocator_, file_column_exprs_.count()));
      //OZ (column_readers_.allocate_array(allocator_, file_column_exprs_.count()));
      OZ (load_funcs_.allocate_array(allocator_, file_column_exprs_.count()));
    }

    if (OB_SUCC(ret)) {
      OZ (file_url_ptrs_.allocate_array(allocator_, eval_ctx.max_batch_size_));
      OZ (file_url_lens_.allocate_array(allocator_, eval_ctx.max_batch_size_));
    }
  }
  return ret;
}


int ObOrcTableRowIterator::next_stripe()
{
  int ret = OB_SUCCESS;
  //init all meta
  if (state_.cur_stripe_idx_ > state_.end_stripe_idx_) {
    if (OB_FAIL(next_file())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next srtipe", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t cur_stripe = (state_.cur_stripe_idx_++) - 1;
    CK (cur_stripe < stripes_.count());
    if (OB_SUCC(ret)) {
      LOG_TRACE("show current stripe info", K(stripes_.at(cur_stripe)));
      try {
        // for (int i = 0; OB_SUCC(ret) && i < column_readers_.count(); i++) {
        //   if (column_readers_.at(i)) {
        //     column_readers_.at(i)->seekToRow(stripes_.at(cur_stripe).first_row_id);
        //   } else {
        //     ret = OB_ERR_UNEXPECTED;
        //     LOG_WARN("column reader is null", K(ret));
        //   }
        // }
        if (row_reader_) {
          row_reader_->seekToRow(stripes_.at(cur_stripe).first_row_id);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column reader is null", K(ret));
        }
        state_.cur_stripe_read_row_count_ = 0;
        state_.cur_stripe_row_count_ = stripes_.at(cur_stripe).num_rows;
      } catch(const std::exception& e) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index", K(ret), "Info", e.what(), K(cur_stripe), K(column_indexs_));
      } catch(...) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index", K(ret), K(cur_stripe), K(column_indexs_));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::next_file()
{
  int ret = OB_SUCCESS;
  CK (scan_param_ != nullptr);
  CK (scan_param_->op_ != nullptr);
  if (OB_SUCC(ret)) {
    ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
    ObString location = scan_param_->external_file_location_;
    int64_t task_idx = 0;
    int64_t file_size = 0;
    if (data_access_driver_.is_opened()) {
      data_access_driver_.close();
    }

    do {
      if ((task_idx = state_.file_idx_++) >= scan_param_->key_ranges_.count()) {
        ret = OB_ITER_END;
      } else {
        state_.cur_file_url_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_URL].get_string();
        url_.reuse();
        const char *split_char = "/";
        OZ (url_.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                          (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
                                          state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
        OZ (data_access_driver_.get_file_size(url_.string(), file_size));

        if (OB_SUCC(ret)) {
          ObString expr_file_url;
          if (data_access_driver_.get_storage_type() == OB_STORAGE_FILE) {
            ObSqlString full_name;
            if (ip_port_.empty()) {
              OZ(gen_ip_port(allocator_));
            }
            OZ (full_name.append_fmt("%.*s%%%.*s", ip_port_.length(), ip_port_.ptr(),
                                    state_.cur_file_url_.length(), state_.cur_file_url_.ptr()));
            OZ (ob_write_string(allocator_, full_name.string(), expr_file_url));
          } else {
            expr_file_url = state_.cur_file_url_;
          }
          for (int i = 0; OB_SUCC(ret) && i < eval_ctx.max_batch_size_; i++) {
            file_url_ptrs_.at(i) = expr_file_url.ptr();
            file_url_lens_.at(i) = expr_file_url.length();
          }
        }
        LOG_DEBUG("current external file", K(url_), K(file_size));
      }
    } while (OB_SUCC(ret) && OB_UNLIKELY(0 >= file_size)); //skip not exist or empty file

    if (OB_SUCC(ret)) {
      int64_t part_id = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::PARTITION_ID].get_int();
      if (part_id != 0 && state_.part_id_ != part_id) {
        state_.part_id_ = part_id;
        OZ (calc_file_partition_list_value(part_id, allocator_, state_.part_list_val_));
      }

      state_.cur_file_id_ = scan_param_->key_ranges_.at(task_idx).get_start_key().get_obj_ptr()[ObExternalTableUtils::FILE_ID].get_int();
      OZ (ObExternalTableUtils::resolve_line_number_range(scan_param_->key_ranges_.at(task_idx),
                                                          ObExternalTableUtils::ROW_GROUP_NUMBER,
                                                          state_.cur_stripe_idx_,
                                                          state_.end_stripe_idx_));

      try {
        OZ (data_access_driver_.open(url_.ptr()), url_);
        std::unique_ptr<ObOrcFileAccess> inStream(new ObOrcFileAccess(data_access_driver_,
                                                          url_.ptr(), file_size));
        orc::ReaderOptions options;
        options.setMemoryPool(orc_alloc_);
        std::unique_ptr<orc::Reader> reader = orc::createReader(std::move(inStream), options);
        if (!reader) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("orc create reader failed", K(ret));
          throw std::bad_exception();
        }
        int64_t nstripes = reader->getNumberOfStripes();
        LOG_TRACE("read file access: number of stipes", K(nstripes), K(url_));
        OZ (stripes_.allocate_array(allocator_, static_cast<size_t>(nstripes)));
        state_.end_stripe_idx_ = std::min(nstripes, state_.end_stripe_idx_);
        std::unique_ptr<orc::StripeInformation> stripe;
        uint64_t first_row_of_stripe = 0;
        for (int i = 0; OB_SUCC(ret) && i < nstripes; i++) {
          stripe = reader->getStripe(i);
          if (!stripe) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get orc file stripe failed", K(ret));
            throw std::bad_exception();
          }
          stripes_.at(i) = StripeInformation({static_cast<int64_t>(stripe->getOffset()),
                                              static_cast<int64_t>(stripe->getLength()),
                                              static_cast<int64_t>(stripe->getNumberOfRows()),
                                              static_cast<int64_t>(first_row_of_stripe)});
          first_row_of_stripe += stripe->getNumberOfRows();
        }

        std::list<std::string> include_names_list;
        for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
          ObDataAccessPathExtraInfo *data_access_info =
              static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
          if (OB_SUCC(ret) && (data_access_info == nullptr ||
                              data_access_info->data_access_path_.ptr() == nullptr ||
                              data_access_info->data_access_path_.length() == 0))
          {
            ret = OB_EXTERNAL_ACCESS_PATH_ERROR;
          }
          if (OB_SUCC(ret)) {
            include_names_list.push_front(std::string(data_access_info->data_access_path_.ptr(),
                                                      data_access_info->data_access_path_.length())); //x.y.z -> column_id
          }
        }
        orc::RowReaderOptions rowReaderOptions;
        rowReaderOptions.include(include_names_list);
        try {
          row_reader_ = reader->createRowReader(rowReaderOptions);
        } catch(const std::exception& e) {
          if (OB_SUCC(ret)) {
            ret = OB_ORC_READ_ERROR;
            LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
            LOG_WARN("unexpected error", K(ret), "Info", e.what());
          }
        } catch(...) {
          if (OB_SUCC(ret)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (!row_reader_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("create row reader failed", K(ret));
        } else {
          //column_indexs_.at(i) = rowReader->getSelectedType().getColumnId();
          //column_readers_.at(i) = std::move(rowReader);
          ObArray<ObString> col_names;
          id_to_type_.reuse();
          name_to_id_.reuse();
          OZ (SMART_CALL(build_type_name_id_map(&row_reader_->getSelectedType(), col_names)));
          for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); i++) {
            ObDataAccessPathExtraInfo *data_access_info =
                static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
            CK (data_access_info != nullptr);
            CK (data_access_info->data_access_path_.ptr() != nullptr);
            CK (data_access_info->data_access_path_.length() != 0);
            int64_t col_id = -1;
            OZ (name_to_id_.get_refactored(ObString(data_access_info->data_access_path_.length(), data_access_info->data_access_path_.ptr()), col_id));
            CK (col_id != -1);
            const orc::Type *type = nullptr;
            OZ (id_to_type_.get_refactored(col_id, type));
            CK (type != nullptr);

            if (OB_SUCC(ret)) {
              load_funcs_.at(i) = DataLoader::select_load_function(file_column_exprs_.at(i)->datum_meta_,
                                                                    *type);
            }
            if (OB_FAIL(ret)) {
            } else if (OB_ISNULL(load_funcs_.at(i))) {
              ret = OB_ERR_INVALID_TYPE_FOR_OP;
              if (i >= row_reader_->getSelectedType().getSubtypeCount()) {
                //error for report
                LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_));
                const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
                LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, "", ob_type);
              } else {
                std::string p_type = row_reader_->getSelectedType().getSubtype(i) == nullptr ?
                        "INVALID ORC TYPE" : row_reader_->getSelectedType().getSubtype(i)->toString();
                int64_t pos = 0;
                ObArrayWrap<char> buf;
                ObArray<ObString> extended_type_info;
                ObDatumMeta &meta = file_column_exprs_.at(i)->datum_meta_;
                const char *ob_type = ob_obj_type_str(file_column_exprs_.at(i)->datum_meta_.type_);
                if (OB_SUCCESS == buf.allocate_array(allocator_, 100)) {
                  ob_sql_type_str(buf.get_data(), buf.count(), pos, meta.type_,
                                  OB_MAX_VARCHAR_LENGTH, meta.precision_, meta.scale_, meta.cs_type_, extended_type_info);
                  if (pos < buf.count()) {
                    buf.at(pos++) = '\0';
                    ob_type = buf.get_data();
                  }
                }
                LOG_WARN("not supported type", K(ret), K(file_column_exprs_.at(i)->datum_meta_),
                          K(ObString(p_type.length(), p_type.data())));
                LOG_USER_ERROR(OB_EXTERNAL_FILE_COLUMN_TYPE_MISMATCH, p_type.c_str(), ob_type);
              }
            }
          }
        }
      } catch(const std::exception& e) {
        if (OB_SUCC(ret)) {
          ret = OB_INVALID_EXTERNAL_FILE;
          LOG_USER_ERROR(OB_INVALID_EXTERNAL_FILE, e.what());
          LOG_WARN("unexpected error", K(ret), "Info", e.what());
        }
      } catch(...) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObOrcTableRowIterator::DataLoader::is_orc_read_utc(const orc::Type *type)
{
  // TIMESTAMP_INSTANT 是utc时间
  return type->getKind() == orc::TypeKind::TIMESTAMP_INSTANT;
}

bool ObOrcTableRowIterator::DataLoader::is_ob_type_store_utc(const ObDatumMeta &meta)
{
  return (lib::is_mysql_mode() && ObTimestampType == meta.type_)
         || (lib::is_oracle_mode() && ObTimestampLTZType == meta.type_);
}

int64_t ObOrcTableRowIterator::DataLoader::calc_tz_adjust_us()
{
  int64_t res = 0;
  int ret = OB_SUCCESS;
  bool is_utc_src = is_orc_read_utc(col_type_);
  bool is_utc_dst = is_ob_type_store_utc(file_col_expr_->datum_meta_);
  if (is_utc_src != is_utc_dst) {
    int32_t tmp_offset = 0;
    if (OB_NOT_NULL(eval_ctx_.exec_ctx_.get_my_session())
        && OB_NOT_NULL(eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info())
        && OB_SUCCESS == eval_ctx_.exec_ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset)) {
      res = SEC_TO_USEC(tmp_offset) * (is_utc_src ? 1 : -1);
    }
  }
  LOG_DEBUG("tz adjust", K(is_utc_src), K(is_utc_dst), K(res), K(file_col_expr_->datum_meta_));
  return res;
}

int ObOrcTableRowIterator::DataLoader::load_data_for_col(LOAD_FUNC &func)
{
  return (this->*func)();
}

ObOrcTableRowIterator::DataLoader::LOAD_FUNC ObOrcTableRowIterator::DataLoader::select_load_function(
    const ObDatumMeta &datum_type, const orc::Type &type)
{
  LOAD_FUNC func = NULL;

  // int size = static_cast<int>(type.getSubtypeCount());
  // if (size > 1) {
  //   LOG_USER_ERROR(OB_NOT_SUPPORTED, "Non-primitive type now are");
  //   throw std::invalid_argument(type.toString());
  // }
  const orc::Type* col_desc = &type;
  orc::TypeKind type_kind = col_desc->getKind();
  if (ob_is_integer_type(datum_type.type_)) {
    func = &DataLoader::load_int64_vec;
    int orc_data_len = 0;
    switch (type_kind) {
        case orc::TypeKind::BOOLEAN:
        case orc::TypeKind::BYTE:
          orc_data_len = 1;
          break;
        case orc::TypeKind::SHORT:
          orc_data_len = 2;
          break;
        case orc::TypeKind::INT:
          orc_data_len = 4;
          break;
        case orc::TypeKind::LONG:
          orc_data_len = 8;
          break;
        default:
          func = NULL;
    }
    //sign and width
    ObObj temp_obj;
    temp_obj.set_int(datum_type.type_, 0);
    if (ob_is_unsigned_type(datum_type.type_)) {
      func = NULL;
    }
    if (temp_obj.get_tight_data_len() < orc_data_len) {
      func = NULL;
    }
  } else if (ob_is_year_tc(datum_type.type_) && orc::TypeKind::INT == type_kind) {
    func = &DataLoader::load_year_vec;
  } else if (ob_is_string_tc(datum_type.type_) || ob_is_enum_or_set_type(datum_type.type_)) {
    //convert orc enum/string to enum/string vector
    switch (type_kind) {
      case orc::TypeKind::STRING:
      case orc::TypeKind::VARCHAR:
      case orc::TypeKind::BINARY:
        func = &DataLoader::load_string_col;
        break;
      case orc::TypeKind::CHAR:
        if (ob_is_char(datum_type.type_, datum_type.cs_type_)) {
          func = &DataLoader::load_string_col;
          break;
        }
      default:
        func = NULL;
    }
  } else if (ob_is_number_or_decimal_int_tc(datum_type.type_)) {
    //convert orc int storing as int32/int64 to number/decimal vector
    if (type_kind == orc::TypeKind::DECIMAL) {
      if (col_desc->getPrecision() != ((datum_type.precision_ == -1) ? 38 : datum_type.precision_)
        || col_desc->getScale() != datum_type.scale_) {
        func = NULL;
      } else if (col_desc->getPrecision() == 0 || col_desc->getPrecision() > 18) {
        func = &DataLoader::load_dec128_vec;
      } else {
        func = &DataLoader::load_dec64_vec;
      }
    } else if (type_kind == orc::TypeKind::INT
               || type_kind == orc::TypeKind::LONG
               || type_kind == orc::TypeKind::SHORT
               || type_kind == orc::TypeKind::BYTE
               || type_kind == orc::TypeKind::BOOLEAN) {
      func = &DataLoader::load_int64_to_number_vec;
    } else {
      func = NULL;
    }
  } else if (ob_is_date_tc(datum_type.type_) ||
             ob_is_datetime(datum_type.type_) ||
             ob_is_time_tc(datum_type.type_) ||
             ob_is_otimestamp_type(datum_type.type_) ||
             ObTimestampType == datum_type.type_) {
    switch (type_kind) {
      // Values of TIMESTAMP type are stored in the writer timezone in the Orc file.
      // Values are read back in the reader timezone. However, the writer timezone
      // information in the Orc stripe footer is optional and may be missing. What is
      // more, stripes in the same Orc file may have different writer timezones (though
      // unlikely). So we cannot tell the exact timezone of values read back. In the adapter
      // implementations, we set both writer and
      // reader timezone to UTC to avoid any conversion so users can get the same values
      // as written. To get rid of this burden, TIMESTAMP_INSTANT type is always preferred
      // over TIMESTAMP type.
      case orc::TypeKind::TIMESTAMP:
      case orc::TypeKind::TIMESTAMP_INSTANT:
        LOG_DEBUG("show type kind", K(type_kind), K(orc::TypeKind::TIMESTAMP_INSTANT));
        if (ob_is_date_tc(datum_type.type_) ||
             ob_is_datetime(datum_type.type_) ||
             ob_is_time_tc(datum_type.type_) ||
             ObTimestampType == datum_type.type_ ||
             ObTimestampLTZType == datum_type.type_ ||
             ObTimestampNanoType == datum_type.type_) {
          func = &DataLoader::load_timestamp_vec;
        }
        break;
      case orc::TypeKind::DATE:
        if (ob_is_date_tc(datum_type.type_)) {
          func = &DataLoader::load_int32_vec;
        } else if (ob_is_time_tc(datum_type.type_) ||
                   ObTimestampType == datum_type.type_ ||
                   ObTimestampLTZType == datum_type.type_) {
          func = &DataLoader::load_date_to_time_or_stamp;
        }
        break;
      default:
        func = NULL;
    }
  } else if (orc::TypeKind::FLOAT == type_kind && ObFloatType == datum_type.type_) {
    func = &DataLoader::load_float;
  } else if (orc::TypeKind::DOUBLE == type_kind && ObDoubleType == datum_type.type_) {
    func = &DataLoader::load_double;
  }
  return func;
}

int ObOrcTableRowIterator::get_data_column_batch_idxs(const orc::Type *type, const int col_id, ObIArray<int> &idxs)
{
  int ret = OB_SUCCESS;
  CK (type != NULL);
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    const orc::Type *cur = type;
    for (int i = 0; OB_SUCC(ret) && !found && i < type->getSubtypeCount(); i++) {
      if (type->getSubtype(i)->getColumnId() == col_id) {
        OZ (idxs.push_back(i));
        found = true;
      } else if (type->getSubtype(i)->getColumnId() < col_id && col_id < type->getSubtype(i)->getMaximumColumnId()) {
        OZ (idxs.push_back(i));
        type = type->getSubtype(i);
      } else {
        //do nothing
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cur == type && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get data colum batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObEvalCtx &eval_ctx = scan_param_->op_->get_eval_ctx();
  const ExprFixedArray &column_conv_exprs = *(scan_param_->ext_column_convert_exprs_);
  int64_t read_count = 0;
  ObMallocHookAttrGuard guard(mem_attr_);

  if (OB_SUCC(ret) && state_.cur_stripe_read_row_count_ >= state_.cur_stripe_row_count_) {
    if (OB_FAIL(next_stripe())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to next row group", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!file_column_exprs_.count()) {
    read_count = std::min(capacity, state_.cur_stripe_row_count_ - state_.cur_stripe_read_row_count_);
  } else if (!row_reader_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row reader is null", K(ret));
  } else {
    try {
      orc_batch_ = row_reader_->createRowBatch(capacity);
      if (!orc_batch_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("create orc row batch failed", K(ret));
      } else if (row_reader_->next(*orc_batch_)) {
        //ok
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read next batch failed", K(ret), K(state_.cur_stripe_read_row_count_), K(state_.cur_stripe_row_count_));
      }
    } catch(const std::exception& e) {
      if (OB_SUCC(ret)) {
        ret = OB_ORC_READ_ERROR;
        LOG_USER_ERROR(OB_ORC_READ_ERROR, e.what());
        LOG_WARN("unexpected error", K(ret), "Info", e.what());
      }
    } catch(...) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret));
      }
    }

    //load vec data from parquet file to file column expr
    for (int i = 0; OB_SUCC(ret) && i < file_column_exprs_.count(); ++i) {
      if (OB_ISNULL(file_column_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("file column expr is null", K(ret));
      } else {
        int idx = -1;
        int64_t col_id = -1;
        const orc::Type *col_type = nullptr;
        ObDataAccessPathExtraInfo *data_access_info =
        static_cast<ObDataAccessPathExtraInfo *>(file_column_exprs_.at(i)->extra_info_);
        CK (data_access_info != nullptr);
        CK (data_access_info->data_access_path_.ptr() != nullptr);
        CK (data_access_info->data_access_path_.length() != 0);
        OZ (name_to_id_.get_refactored(ObString(data_access_info->data_access_path_.length(), data_access_info->data_access_path_.ptr()), col_id));

        OZ (id_to_type_.get_refactored(col_id, col_type));
        ObArray<int> idxs;
        OZ (get_data_column_batch_idxs(&row_reader_->getSelectedType(), col_id, idxs));
        DataLoader loader(eval_ctx, file_column_exprs_.at(i), orc_batch_, capacity, idxs, read_count, col_type);
        OZ (file_column_exprs_.at(i)->init_vector_for_write(
              eval_ctx, file_column_exprs_.at(i)->get_default_res_format(), eval_ctx.max_batch_size_));
        OZ (loader.load_data_for_col(load_funcs_.at(i)));
        if (OB_SUCC(ret)) {
          file_column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
        }
        if (OB_SUCC(ret) && read_count == 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("read result count is zero", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && read_count > 0) {
    //fill expr results from metadata
    for (int i = 0; OB_SUCC(ret) && i < file_meta_column_exprs_.count(); i++) {
      ObExpr *meta_expr = file_meta_column_exprs_.at(i);
      CK (OB_NOT_NULL(meta_expr));
      if (OB_FAIL(ret)) {
      } else if (meta_expr->type_ == T_PSEUDO_EXTERNAL_FILE_URL) {
        StrDiscVec *text_vec = static_cast<StrDiscVec *>(meta_expr->get_vector(eval_ctx));
        CK (OB_NOT_NULL(text_vec));
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

    for (int i = 0; OB_SUCC(ret) && i < column_exprs_.count(); i++) {
      //column_conv_exprs is 1-1 mapped to column_exprs
      //calc gen column exprs
      CK (OB_NOT_NULL(column_conv_exprs.at(i)));
      if (OB_FAIL(ret)) {
      } else if (!column_conv_exprs.at(i)->get_eval_info(eval_ctx).evaluated_) {
        OZ (column_conv_exprs.at(i)->init_vector_default(eval_ctx, read_count));
        CK (OB_NOT_NULL(bit_vector_cache_));
        OZ (column_conv_exprs.at(i)->eval_vector(eval_ctx, *bit_vector_cache_, read_count, true));
        OX (column_conv_exprs.at(i)->set_evaluated_projected(eval_ctx));
      }
      //assign gen column exprs value to column exprs(output exprs)
      if (OB_SUCC(ret)) {
        ObExpr *to = column_exprs_.at(i);
        ObExpr *from = column_conv_exprs.at(i);
        CK (OB_NOT_NULL(to));
        CK (OB_NOT_NULL(from));
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx);
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx);
        if (OB_FAIL(ret)) {
        } else if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx))->get_datums();
          CK (OB_NOT_NULL(from_datum));
          OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx));
          CK (OB_NOT_NULL(to_vec));
          if (OB_SUCC(ret)) {
            ObDatum *to_datums = to_vec->get_datums();
            CK (OB_NOT_NULL(to_datums));
            for (int64_t j = 0; j < read_count && OB_SUCC(ret); j++) {
              to_datums[j] = *from_datum;
            }
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx));
          CK (OB_NOT_NULL(uni_vec));
          if (OB_SUCC(ret)) {
            ObDatum *src = uni_vec->get_datums();
            ObDatum *dst = to->locate_batch_datums(eval_ctx);
            CK (OB_NOT_NULL(src));
            CK (OB_NOT_NULL(dst));
            if (OB_SUCC(ret) && src != dst) {
              MEMCPY(dst, src, read_count * sizeof(ObDatum));
            }
            OZ(to->init_vector(eval_ctx, VEC_UNIFORM, read_count));
          }
        } else {
          to_vec_header = from_vec_header;
          if (from->is_nested_expr()) {
            OZ(to->assign_nested_vector(*from, eval_ctx));
          }
        }
        column_exprs_.at(i)->set_evaluated_projected(eval_ctx);
      }
    }
    OZ (calc_exprs_for_rowid(read_count, state_));
  }
  if (OB_SUCC(ret)) {
    state_.cur_stripe_read_row_count_ += read_count;
    count = read_count;
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  row_count_ = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int64_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int64_vec));
    CK (VEC_FIXED == int64_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else if (!long_batch->hasNulls) {
            CK (OB_NOT_NULL(long_batch->data.data()));
            CK (OB_NOT_NULL(int64_vec->get_data()));
            if (OB_SUCC(ret)) {
              MEMCPY(pointer_cast<int64_t*>(int64_vec->get_data()), long_batch->data.data(), sizeof(int64_t) * row_count_);
            }
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              CK (OB_NOT_NULL(long_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                if (OB_ISNULL(valid_bytes)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("orc not null batch valid bytes is null", K(ret));
                } else if (*valid_bytes == 1) {
                  int64_vec->set_int(i, long_batch->data[i]);
                } else {
                  int64_vec->set_null(i);
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int64 vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_year_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      } else {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(long_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (long_batch->hasNulls) {
                CK (OB_NOT_NULL(long_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    int32_vec->set_year(i, long_batch->data[i]);
                  } else {
                    int32_vec->set_null(i);
                  }
                }
              } else {
                int32_vec->set_year(i, long_batch->data[i]);
              }
            }
          }
        }
      }
    }
  }
  LOG_DEBUG("load year vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_int32_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *int32_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(int32_vec));
    CK (VEC_FIXED == int32_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (long_batch->hasNulls) {
                CK (OB_NOT_NULL(long_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
                  } else {
                    int32_vec->set_null(i);
                  }
                }
              } else {
                int32_vec->set_int32(i, (int32_t)long_batch->data[i]);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int32 vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_string_col()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    StrDiscVec *text_vec = static_cast<StrDiscVec *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(text_vec));
    CK (VEC_DISCRETE == text_vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          bool is_oracle_mode = lib::is_oracle_mode();
          bool is_byte_length = is_oracle_byte_length(
              is_oracle_mode, file_col_expr_->datum_meta_.length_semantics_);
          orc::StringVectorBatch *string_batch = dynamic_cast<orc::StringVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!string_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              if (string_batch->hasNulls) {
                CK (OB_NOT_NULL(string_batch->data.data()));
                CK (OB_NOT_NULL(string_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(string_batch->notNull.data()) + i;
                  if (OB_ISNULL(valid_bytes)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("orc not null batch valid bytes is null", K(ret));
                  } else if (*valid_bytes == 1) {
                    if (string_batch->length[i] == 0 && is_oracle_mode) {
                      text_vec->set_null(i);
                    } else {
                      if (OB_UNLIKELY(string_batch->length[i] > file_col_expr_->max_length_
                                  && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                              pointer_cast<const char *>(string_batch->data[i]),
                                                                              string_batch->length[i]) > file_col_expr_->max_length_))) {
                        ret = OB_ERR_DATA_TOO_LONG;
                        LOG_WARN("data too long", K(ret));
                      } else {
                        text_vec->set_string(i, string_batch->data[i], string_batch->length[i]);
                      }
                    }
                  } else {
                    text_vec->set_null(i);
                  }
                }
              } else {
                CK (OB_NOT_NULL(string_batch->length.data()));
                if (OB_FAIL(ret)) {
                } else if (string_batch->length[i] == 0 && is_oracle_mode) {
                  text_vec->set_null(i);
                } else {
                  CK (OB_NOT_NULL(string_batch->data.data()));
                  if (OB_FAIL(ret)) {
                  } else if (OB_UNLIKELY(string_batch->length[i] > file_col_expr_->max_length_
                                && (is_byte_length || ObCharset::strlen_char(CS_TYPE_UTF8MB4_BIN,
                                                                            pointer_cast<const char *>(string_batch->data[i]),
                                                                            string_batch->length[i]) > file_col_expr_->max_length_))) {
                    ret = OB_ERR_DATA_TOO_LONG;
                    LOG_WARN("data too long", K(ret));
                  } else {
                    text_vec->set_string(i, string_batch->data[i], string_batch->length[i]);
                  }
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_timestamp_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    int64_t adjust_us = calc_tz_adjust_us();
    LOG_DEBUG("adjust value", K(adjust_us));
    CK (OB_NOT_NULL(dec_vec));
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          row_count_ = batch_->numElements;
          orc::TimestampVectorBatch *timestamp_batch = dynamic_cast<orc::TimestampVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!timestamp_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              const uint8_t* valid_bytes = nullptr;
              CK (OB_NOT_NULL(timestamp_batch->data.data()));
              CK (OB_NOT_NULL(timestamp_batch->nanoseconds.data()));
              if (OB_SUCC(ret) && timestamp_batch->hasNulls) {
                CK (OB_NOT_NULL(timestamp_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(timestamp_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!timestamp_batch->hasNulls || *valid_bytes == 1) {
                int64_t adjusted_value = timestamp_batch->data[i] * USECS_PER_SEC + timestamp_batch->nanoseconds[i] / NSECS_PER_USEC + adjust_us;
                if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_timestamp(i, adjusted_value);
                } else if (ObDateTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_datetime(i, adjusted_value);
                } else if (ObDateType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_date(i, adjusted_value / USECS_PER_DAY);
                } else if (ObTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_time(i, adjusted_value);
                } else {
                  ObOTimestampData data;
                  data.time_us_ = adjusted_value;
                  data.time_ctx_.set_tail_nsec(timestamp_batch->nanoseconds[i] % NSECS_PER_USEC);
                  dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
                }
              } else {
                dec_vec->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}


int ObOrcTableRowIterator::DataLoader::load_date_to_time_or_stamp()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *dec_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    int64_t adjust_us = calc_tz_adjust_us();
    LOG_DEBUG("show adjust value in date to ts", K(adjust_us));
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          row_count_ = batch_->numElements;
          orc::LongVectorBatch *date_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(date_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic dec64 batch cast failed", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
            const uint8_t* valid_bytes = nullptr;
            CK (OB_NOT_NULL(date_batch->data.data()));
            if (OB_SUCC(ret)) {
              if (date_batch->hasNulls) {
                CK (OB_NOT_NULL(date_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(date_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!date_batch->hasNulls || *valid_bytes == 1) {
                int64_t adjusted_value = date_batch->data[i] * USECS_PER_DAY + adjust_us;
                if (ObTimestampType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_timestamp(i, adjusted_value);
                } else if (ObDateTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_datetime(i, date_batch->data[i] * USECS_PER_DAY);
                } else if (ObDateType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_date(i, adjusted_value / USECS_PER_DAY);
                } else if (ObTimeType == file_col_expr_->datum_meta_.type_) {
                  dec_vec->set_time(i, adjusted_value);
                } else {
                  ObOTimestampData data;
                  data.time_us_ = adjusted_value;
                  dec_vec->set_otimestamp_tiny(i, ObOTimestampTinyData().from_timestamp_data(data));
                }
              } else {
                dec_vec->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec64_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
    CK (OB_NOT_NULL(vec));
    CK (VEC_FIXED == vec->get_format());
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::Decimal64VectorBatch *dec64_batch = dynamic_cast<orc::Decimal64VectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (OB_ISNULL(dec64_batch)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic dec64 batch cast failed", K(ret));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
            CK (OB_NOT_NULL(dec64_batch->values.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = nullptr;
              if (dec64_batch->hasNulls) {
                CK (OB_NOT_NULL(dec64_batch->notNull.data()));
                if (OB_SUCC(ret)) {
                  valid_bytes = reinterpret_cast<const uint8_t*>(dec64_batch->notNull.data()) + i;
                }
              }
              if (OB_FAIL(ret)) {
              } else if (!dec64_batch->hasNulls || *valid_bytes == 1) {
                if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
                  ObDecimalInt *decint = NULL;
                  int32_t int_bytes = 0;
                  if (OB_FAIL(wide::from_integer(dec64_batch->values[i], tmp_alloc_g.get_allocator(), decint,
                                                int_bytes, file_col_expr_->datum_meta_.precision_))) {
                    LOG_WARN("fail to from integer", K(ret));
                  } else if (OB_ISNULL(decint)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("int to dec failed", K(ret));
                  } else {
                    vec->set_decimal_int(i, decint, int_bytes);
                  }
                } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
                  ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
                  number::ObNumber res_nmb;
                  if (OB_FAIL(res_nmb.from(dec64_batch->values[i], tmp_alloc_g.get_allocator()))) {
                    LOG_WARN("fail to from number", K(ret));
                  } else {
                    vec->set_number(i, res_nmb);
                  }
                }
              } else {
                file_col_expr_->get_vector(eval_ctx_)->set_null(i);
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::to_numeric(const int64_t idx, const int64_t int_value)
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

int ObOrcTableRowIterator::DataLoader::load_int64_to_number_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  row_count_ = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  CK (OB_NOT_NULL(file_col_expr_));
  if (OB_SUCC(ret)) {
    if (OB_SUCC(ret)) {
      if (batch_) {
        row_count_ = batch_->numElements;
        orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
        if (OB_ISNULL(root)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
        }
        CK (root->fields.size() > 0);
        CK (idxs_.count() > 0);
        if (OB_SUCC(ret)) {
          orc::StructVectorBatch *cb = root;
          for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
            CK (root->fields.size() > idxs_.at(i));
            if (OB_SUCC(ret)) {
              cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
              CK (cb != nullptr);
            }
          }
        }
        if (OB_SUCC(ret)) {
          orc::LongVectorBatch *long_batch = dynamic_cast<orc::LongVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
          if (!long_batch) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dynamic cast orc type failed", K(ret));
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
              CK (OB_NOT_NULL(long_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(long_batch->notNull.data()) + i;
                if (OB_ISNULL(valid_bytes)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("orc not null batch valid bytes is null", K(ret));
                } else if (*valid_bytes == 1) {
                  OZ (to_numeric(i, long_batch->data[i]));
                } else {
                  file_col_expr_->get_vector(eval_ctx_)->set_null(i);
                }
              }
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read orc next batch failed", K(ret));
      }
    }
  }
  LOG_DEBUG("load int64 to number vec", K(ret), K(row_count_));
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_dec128_vec()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::Decimal128VectorBatch *dec128_batch = dynamic_cast<orc::Decimal128VectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(dec128_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic dec128 batch cast failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
          CK (OB_NOT_NULL(dec128_batch->values.data()));
          if (OB_SUCC(ret)) {
            const uint8_t* valid_bytes = nullptr;
            if (dec128_batch->hasNulls) {
              CK (OB_NOT_NULL(dec128_batch->notNull.data()));
              if (OB_SUCC(ret)) {
                valid_bytes = reinterpret_cast<const uint8_t*>(dec128_batch->notNull.data()) + i;
              }
            }
            if (OB_FAIL(ret)) {
            } else if (!dec128_batch->hasNulls || *valid_bytes == 1) {
              ObDecimalInt *decint = NULL;
              int32_t int_bytes = sizeof(int128_t);
              int128_t val = dec128_batch->values[i].getHighBits();
              val = val << 64;
              val.items_[0] = dec128_batch->values[i].getLowBits();
              void *data = nullptr;

              if (OB_ISNULL(data = tmp_alloc_g.get_allocator().alloc(int_bytes))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                COMMON_LOG(WARN, "allocate memory failed", K(ret), K(int_bytes));
              } else {
                decint = reinterpret_cast<ObDecimalInt *>(data);
                *decint->int128_v_ = val;
              }
              if (OB_FAIL(ret)) {
              } else if (ObDecimalIntType == file_col_expr_->datum_meta_.type_) {
                ObFixedLengthBase *vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
                CK (OB_NOT_NULL(vec));
                CK (VEC_FIXED == vec->get_format());
                if (OB_SUCC(ret)) {
                  vec->set_decimal_int(i, decint, int_bytes);
                }
              } else if (ObNumberType == file_col_expr_->datum_meta_.type_) {
                ObDiscreteBase *vec = static_cast<ObDiscreteBase *>(file_col_expr_->get_vector(eval_ctx_));
                CK (OB_NOT_NULL(vec));
                if (OB_SUCC(ret)) {
                  number::ObNumber res_nmb;
                  if (OB_FAIL(wide::to_number(decint, int_bytes, file_col_expr_->datum_meta_.scale_,
                                      tmp_alloc_g.get_allocator(), res_nmb))) {
                    LOG_WARN("fail to from", K(ret));
                  } else {
                    vec->set_number(i, res_nmb);
                  }
                }
              }
            } else {
              file_col_expr_->get_vector(eval_ctx_)->set_null(i);
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_float()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *float_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  CK (OB_NOT_NULL(float_vec));
  CK (VEC_FIXED == float_vec->get_format());
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::DoubleVectorBatch *double_batch = dynamic_cast<orc::DoubleVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(double_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic double batch cast failed", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < row_count_; i++) {
          CK (OB_NOT_NULL(double_batch->data.data()));
          if (OB_FAIL(ret)) {
          } else if (double_batch->hasNulls) {
            CK (OB_NOT_NULL(double_batch->notNull.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(double_batch->notNull.data()) + i;
              if (*valid_bytes == 1) {
                float_vec->set_float(i, (float)double_batch->data[i]);
              } else {
                float_vec->set_null(i);
              }
            }
          } else {
            float_vec->set_float(i, (float)double_batch->data[i]);
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}

int ObOrcTableRowIterator::DataLoader::load_double()
{
  int ret = OB_SUCCESS;
  int64_t values_cnt = 0;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(eval_ctx_);
  ObFixedLengthBase *double_vec = static_cast<ObFixedLengthBase *>(file_col_expr_->get_vector(eval_ctx_));
  CK (OB_NOT_NULL(double_vec));
  CK (VEC_FIXED == double_vec->get_format());
  if (OB_SUCC(ret)) {
    if (batch_) {
      row_count_ = batch_->numElements;
      orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch_.get());
      if (OB_ISNULL(root)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dynamic cast orc column vector batch failed", K(ret));
      }
      CK (root->fields.size() > 0);
      CK (idxs_.count() > 0);
      if (OB_SUCC(ret)) {
        orc::StructVectorBatch *cb = root;
        for (int64_t i = 0; OB_SUCC(ret) && i < idxs_.count() - 1; i++) {
          CK (root->fields.size() > idxs_.at(i));
          if (OB_SUCC(ret)) {
            cb = dynamic_cast<orc::StructVectorBatch *>(cb->fields[idxs_.at(i)]);
            CK (cb != nullptr);
          }
        }
      }
      if (OB_SUCC(ret)) {
        orc::DoubleVectorBatch *double_batch = dynamic_cast<orc::DoubleVectorBatch *>(root->fields[idxs_.at(idxs_.count() - 1)]);
        if (OB_ISNULL(double_batch)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dynamic double batch cast failed", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (!double_batch->hasNulls) {
          CK (OB_NOT_NULL(double_batch->data.data()));
          CK (OB_NOT_NULL(double_vec->get_data()));
          if (OB_SUCC(ret)) {
            MEMCPY(pointer_cast<double*>(double_vec->get_data()), double_batch->data.data(), sizeof(double) * row_count_);
          }
        } else {
          for (int64_t i = 0; i < row_count_; i++) {
            CK (OB_NOT_NULL(double_batch->notNull.data()));
            CK (OB_NOT_NULL(double_batch->data.data()));
            if (OB_SUCC(ret)) {
              const uint8_t* valid_bytes = reinterpret_cast<const uint8_t*>(double_batch->notNull.data()) + i;
              CK (OB_NOT_NULL(valid_bytes));
              if (OB_FAIL(ret)) {
              } else if (*valid_bytes == 1) {
                double_vec->set_double(i, double_batch->data[i]);
              } else {
                double_vec->set_null(i);
              }
            }
          }
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read orc next batch failed", K(ret));
    }
  }
  return ret;
}


int ObOrcTableRowIterator::get_next_row()
{
  int ret = OB_NOT_SUPPORTED;
  return ret;
}

void ObOrcTableRowIterator::reset() {
  // reset state_ to initial values for rescan
  state_.reuse();
}

DEF_TO_STRING(ObOrcIteratorState)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("ob_external_iterator_state");
  J_COLON();
  pos += ObExternalIteratorState::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(cur_stripe_idx),
       K_(end_stripe_idx),
       K_(cur_stripe_read_row_count),
       K_(cur_stripe_row_count));
  J_OBJ_END();
  return pos;
}

}
}