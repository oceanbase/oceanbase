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

#ifndef _OB_SQL_ENGINE_PDML_PDML_DATA_DRIVER_H_
#define _OB_SQL_ENGINE_PDML_PDML_DATA_DRIVER_H_

#include "sql/engine/pdml/ob_batch_row_cache.h"
#include "sql/engine/pdml/ob_px_multi_part_modify.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
class ObNewRow;
}  // namespace common

namespace sql {

class ObExecContext;
class ObDMLTableDesc;

class ObPDMLDataDriver {
public:
  ObPDMLDataDriver(ObMonitorNode& op_monitor_info)
      : op_monitor_info_(op_monitor_info),
        returning_ctx_(),
        cache_(),
        reader_(nullptr),
        writer_(nullptr),
        state_(FILL_CACHE),
        last_row_(nullptr),
        last_row_part_id_(common::OB_INVALID_ID),
        op_id_(common::OB_INVALID_ID),
        with_barrier_(false),
        modify_input_(nullptr)
  {}

  ~ObPDMLDataDriver();
  int init(common::ObIAllocator& allocator, const ObDMLTableDesc& tdesc, const ObDMLDataReader* reader,
      const ObDMLDataWriter* writer);
  void set_with_barrier(uint64_t op_id, const ObPxModifyInput* modify_input)
  {
    op_id_ = op_id;
    with_barrier_ = true;
    modify_input_ = modify_input;
  }
  int destroy();
  int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row);

private:
  int fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext& ctx);
  inline int try_write_last_pending_row();
  int switch_to_returning_state(ObExecContext& ctx);
  int switch_row_iter_to_next_partition();
  int barrier(ObExecContext& ctx);
  int next_row_from_cache(const common::ObNewRow*& row);
  int write_partitions(ObExecContext& ctx);

private:
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

    PartitionIdArray part_id_array_;  // All partition indexes to be read
    int64_t next_idx_;                // Next partition indexes to be read 0 if not started
    ObPDMLRowIterator* row_iter_;     // The row iterator of the current read partition
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
   *
   */
  enum DriverState { FILL_CACHE, ROW_RETURNING };

private:
  ObMonitorNode& op_monitor_info_;
  ReturningCtx returning_ctx_;
  ObBatchRowCache cache_;
  const ObDMLDataReader* reader_;
  const ObDMLDataWriter* writer_;
  ObDMLTableDesc tdesc_;
  DriverState state_;
  const common::ObNewRow* last_row_;
  int64_t last_row_part_id_;
  int64_t op_id_;
  bool with_barrier_;
  const ObPxModifyInput* modify_input_;
  DISALLOW_COPY_AND_ASSIGN(ObPDMLDataDriver);
  ;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* _OB_SQL_ENGINE_PDML_PDML_DATA_DRIVER_H_ */
//// end of header file
