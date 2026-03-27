/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <random>
#include <chrono>
#include <fstream>
#include "../test_sql_utils.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_operator_reg.h"

namespace oceanbase
{
namespace sql
{
class ObFakeTableScanVecOp : public ObTableScanOp
{
  friend class ObDASScanOp;
  friend class ObGlobalIndexLookupOpImpl;

public:
  static constexpr int64_t CHECK_STATUS_ROWS_INTERVAL = 1 << 13;
  ObFakeTableScanVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObTableScanOp(exec_ctx, spec, input)
  {}
  ~ObFakeTableScanVecOp() = default;

  int inner_open() override;
  int inner_get_next_batch(const int64_t max_row_cnt) override;

  int fill_random_data_into_expr_datum_frame(int expr_i, int expr_count, const ObExpr *expr, const int output_max_count,
                                             bool &is_duplicate);
  int get_random_data(int expr_i, int expr_count, const ObExpr *expr, const int round, const int batch_size,
                      const int len, bool &is_duplicate);
  void set_random_skip(const int round, const int batch_size);

public:
  int max_round_{2};
  int current_round_{1};

  // io
  std::unordered_map<uint64_t, std::ofstream> op_id_2_output_streams_;
};

} // end namespace sql
} // end namespace oceanbase
