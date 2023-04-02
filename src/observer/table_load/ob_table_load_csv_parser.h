// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   xiaotao.ht <>

#pragma once

#include "lib/string/ob_string.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace observer
{

struct ObTableLoadCSVParser
{
public:
  static const char *default_field_term_str;
  ObTableLoadCSVParser();
  ~ObTableLoadCSVParser();
  int init(ObTableLoadTableCtx *table_ctx, const ObString &data_buffer);
  int get_batch_objs(table::ObTableLoadArray<ObObj> &store_column_objs);
private:
  int get_next_row(ObNewRow &row);
private:
  sql::ObCSVGeneralParser csv_parser_;
  common::ObArenaAllocator allocator_;
  int64_t column_count_;
  int64_t batch_row_count_;
  const char *str_;
  const char *end_;
  table::ObTableLoadArray<ObObj> store_column_objs_;
  char *escape_buf_;
  bool is_inited_;
};

} // namespace observer
} // namespace oceanbase