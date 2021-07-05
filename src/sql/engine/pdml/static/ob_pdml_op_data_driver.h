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

#ifndef _OB_SQL_ENGINE_PDML_OP_PDML_DATA_DRIVER_H_
#define _OB_SQL_ENGINE_PDML_OP_PDML_DATA_DRIVER_H_

#include "sql/engine/pdml/static/ob_pdml_op_batch_row_cache.h"
#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObNewRow;
}  // namespace common

namespace sql {

class ObExecContext;
class ObDMLOpTableDesc;

// The core classes that operate ObBatchRowCache and the storage layer
class ObPDMLOpDataDriver {
public:
  ObPDMLOpDataDriver(ObEvalCtx* eval_ctx, ObIAllocator& allocator, ObMonitorNode& op_monitor_info)
      : returning_ctx_(),
        op_monitor_info_(op_monitor_info),
        cache_(eval_ctx),
        reader_(nullptr),
        writer_(nullptr),
        state_(FILL_CACHE),
        eval_ctx_(eval_ctx),
        last_row_(allocator),
        last_row_part_id_(common::OB_INVALID_ID),
        last_row_expr_(nullptr),
        op_id_(common::OB_INVALID_ID),
        with_barrier_(false),
        dfo_id_(OB_INVALID_ID)
  {}

  ~ObPDMLOpDataDriver();

  int init(common::ObIAllocator& allocator, const ObDMLOpTableDesc& tdesc, ObDMLOpDataReader* reader,
      ObDMLOpDataWriter* writer);

  int destroy();

  int set_with_barrier(uint64_t op_id, const ObPxMultiPartModifyOpInput* modify_input);

  int get_next_row(ObExecContext& ctx, const ObExprPtrIArray& row);

private:
  int fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext& ctx);
  inline int try_write_last_pending_row();
  int switch_to_returning_state(ObExecContext& ctx);
  int switch_row_iter_to_next_partition();
  int barrier(ObExecContext& ctx);
  int next_row_from_cache(const ObExprPtrIArray& row);
  int write_partitions(ObExecContext& ctx);

private:
  // Because the data of multiple partitions will be cached in the cache, it is necessary in the process of iteration
  // Record the current status of which partition it is iterated to, which row in the partition it is iterated to, etc.
  // So, introduce ReturningCtx
  // Used to record the row of which partition is currently being returned to support repeated get_next_row
  struct ReturningCtx {
    ReturningCtx() : next_idx_(0), row_iter_(nullptr)
    {}
    ~ReturningCtx() = default;
    void reset()
    {
      part_id_array_.reset();
      next_idx_ = 0;
      row_iter_ = NULL;
    }

    PartitionIdArray part_id_array_;
    int64_t next_idx_;
    ObPDMLOpRowIterator* row_iter_;
  };

  /* State transition diagram:
   *
   *           start
   *             |
   *             |(0);
   *             |
   *    +---- FILL_CACHE <-----+
   *    |        |             |
   *    |        |(1);          |
   *    |        |             |
   *    |     ROW_RETURNING    |
   *    |     ROW_RETURNING    |
   *    |       ....(2);        |
   *    |     ROW_RETURNING    |
   *    |        |             | (3);
   *    |        |             |
   *    |        |             |
   *    |        +------->-----+
   *    |(4);
   *    +-----> end
   *
   *  (0); Begin;
   *  (1); Read in data from PDMLDataReader to fill the cache;
   *  (2); Spit up data, because it is a pull data model, so it will be done multiple times state (2);
   *  (3); All the cached data is spit out, the cache is filled again, back into state (1);
   *  (4); No data fills the cache, and the cache is empty, end
   *
   */
  enum DriverState {
    FILL_CACHE,   /* When the cache is filled, and the cache is full,
                     it will automatically write to the disk synchronously and enter the ROW_RETURNING state */
    ROW_RETURNING /* Return rows to the upper layer,
                     and automatically switch to the state of filling cache after all rows are returned*/
  };

private:
  ReturningCtx returning_ctx_;
  ObMonitorNode& op_monitor_info_;
  ObPDMLOpBatchRowCache cache_;  // cache data
  ObDMLOpDataReader* reader_;
  ObDMLOpDataWriter* writer_;
  ObDMLOpTableDesc tdesc_;
  DriverState state_;  // Driver current state

  ObEvalCtx* eval_ctx_;
  ObChunkDatumStore::LastStoredRow<> last_row_;
  int64_t last_row_part_id_;
  const ObExprPtrIArray* last_row_expr_;
  int64_t op_id_;
  bool with_barrier_;
  uint64_t dfo_id_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpDataDriver);
  ;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PDML_OP_PDML_DATA_DRIVER_H_ */
//// end of header file
