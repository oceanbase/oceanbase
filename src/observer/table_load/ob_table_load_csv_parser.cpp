// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   xiaotao.ht <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_csv_parser.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace sql;
using namespace table;

const char *ObTableLoadCSVParser::default_field_term_str = "|";

ObTableLoadCSVParser::ObTableLoadCSVParser()
  : allocator_("TLD_CSVParser"),
    column_count_(0),
    batch_row_count_(0),
    str_(nullptr),
    end_(nullptr),
    escape_buf_(nullptr),
    is_inited_(false)
{
}

ObTableLoadCSVParser::~ObTableLoadCSVParser()
{
}

int ObTableLoadCSVParser::init(ObTableLoadTableCtx *table_ctx, const ObString &data_buffer)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadCSVParser init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_ctx || data_buffer.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_ctx), K(data_buffer.length()),
             K(table_ctx->param_.data_type_));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    column_count_ = table_ctx->param_.column_count_;
    batch_row_count_ = table_ctx->param_.batch_size_;
    const int64_t total_obj_count = batch_row_count_ * column_count_;
    ObDataInFileStruct file_struct;
    file_struct.field_term_str_ = default_field_term_str;
    if (OB_FAIL(csv_parser_.init(file_struct, column_count_, table_ctx->schema_.collation_type_))) {
      LOG_WARN("fail to init csv general parser", KR(ret));
    } else if (OB_FAIL(store_column_objs_.create(total_obj_count, allocator_))) {
      LOG_WARN("fail to create objs", KR(ret));
    } else if (OB_ISNULL(escape_buf_ = static_cast<char *>(
                           allocator_.alloc(ObLoadFileBuffer::MAX_BUFFER_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate escape buf memory", KR(ret));
    } else {
      str_ = data_buffer.ptr();
      end_ = data_buffer.ptr() + data_buffer.length();
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadCSVParser::get_next_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row.cells_) || OB_UNLIKELY(row.count_ != column_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(row));
  } else if (str_ == end_) {
    ret = OB_ITER_END;
  } else {
    ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records;
    int64_t nrows = 1;
    auto handle_one_line = [](ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line) -> int {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    };
    ret = csv_parser_.scan<decltype(handle_one_line), true>(
      str_, end_, nrows, escape_buf_, escape_buf_ + ObLoadFileBuffer::MAX_BUFFER_SIZE,
      handle_one_line, err_records, true);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to csv parser scan", KR(ret));
    } else if (OB_UNLIKELY(!err_records.empty())) {
      ret = OB_ERR_WRONG_VALUE;
      LOG_WARN("parser error, have err records", KR(ret));
    } else if (0 == nrows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parser error 0 == nrows", KR(ret));
    } else {
      const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
        csv_parser_.get_fields_per_line();
      for (int64_t i = 0; i < row.count_; ++i) {
        const ObCSVGeneralParser::FieldValue &str_v = field_values_in_file.at(i);
        ObObj &obj = row.cells_[i];
        if (str_v.is_null_) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
      }
    }
    for (int64_t i = 0; i < err_records.count(); ++i) {
      LOG_WARN("csv parser error records", K(i), K(err_records.at(i).err_code));
    }
  }
  return ret;
}

int ObTableLoadCSVParser::get_batch_objs(ObTableLoadArray<ObObj> &store_column_objs)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadCSVParser not init", KR(ret), KP(this));
  } else {
    uint64_t processed_line_count = 0;
    ObNewRow row;
    row.cells_ = store_column_objs_.ptr();
    row.count_ = column_count_;
    while (OB_SUCC(ret) && processed_line_count < batch_row_count_) {
      if (OB_FAIL(get_next_row(row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get row objs", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ++processed_line_count;
        row.cells_ += column_count_;
      }
    }
    if (OB_SUCC(ret)) {
      if (processed_line_count == 0) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(store_column_objs.ref(store_column_objs_.ptr(),
                                               processed_line_count * column_count_))) {
        LOG_WARN("fail to ref", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase