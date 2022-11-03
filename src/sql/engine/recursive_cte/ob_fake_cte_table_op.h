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

#ifndef _OB_FAKE_CTE_TABLE_OP_H
#define _OB_FAKE_CTE_TABLE_OP_H 1

#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "ob_search_method_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{

class ObExecContext;
class ObFakeCTETableSpec : public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  explicit ObFakeCTETableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
      : ObOpSpec(alloc, type), column_involved_offset_(alloc), column_involved_exprs_(alloc)
  {
  }
  virtual ~ObFakeCTETableSpec() {}

  //数组下标指的是output_里面的下标，数组内容是在cte表中原始列的偏移位置
  common::ObFixedArray<int64_t, common::ObIAllocator> column_involved_offset_;
  common::ObFixedArray<ObExpr *, common::ObIAllocator> column_involved_exprs_;
};

class ObFakeCTETableOp : public ObOperator
{
public:
  explicit ObFakeCTETableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObOperator(exec_ctx, spec, input),
        empty_(false),
        pump_row_(nullptr),
        allocator_(exec_ctx.get_allocator())
    {
    }
  inline virtual void destroy()
  {
    ObOperator::destroy();
  }
  inline bool has_valid_data() { return !empty_; }
  void reuse();
  virtual int inner_rescan() override;
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  inline void set_empty() { empty_ = true; }
  int add_row(ObChunkDatumStore::StoredRow *row);
  int copy_datums(ObChunkDatumStore::StoredRow *row, common::ObDatum *datums,
                  int64_t cnt, const common::ObIArray<int64_t> &chosen_datums,
                  char *buf, const int64_t size, const int64_t row_size,
                  const uint32_t row_extend_size);
  //从src_row中拷贝chosen_index中包含的cell到dst_row中
  int deep_copy_row(const ObChunkDatumStore::StoredRow *src_row,
                    const ObChunkDatumStore::StoredRow *&dst_row,
                    const common::ObIArray<int64_t> &chosen_index,
                    int64_t extra_size,
                    common::ObIAllocator &allocator);
  const static int64_t ROW_EXTRA_SIZE = 0;

private:
  bool empty_;
  const ObChunkDatumStore::StoredRow* pump_row_;
  ObIAllocator &allocator_;
};


} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_FAKE_CTE_TABLE_OP_H */
