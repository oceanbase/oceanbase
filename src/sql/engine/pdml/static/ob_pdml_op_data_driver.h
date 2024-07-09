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

#include "sql/engine/pdml/static/ob_pdml_op_batch_row_cache.h"
#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"

namespace oceanbase
{
namespace common
{
class ObNewRow;
}

namespace sql
{

class ObExecContext;
struct ObDMLBaseRtDef;
class ObDMLOpTableDesc;

// 操作 ObBatchRowCache 和存储层的核心类
class ObPDMLOpDataDriver
{
public:
  ObPDMLOpDataDriver(ObEvalCtx *eval_ctx, ObIAllocator &allocator, ObMonitorNode &op_monitor_info):
      returning_ctx_(),
      op_monitor_info_(op_monitor_info),
      cache_(eval_ctx, op_monitor_info),
      reader_(nullptr),
      writer_(nullptr),
      dml_rtdef_(nullptr),
      state_(FILL_CACHE),
      eval_ctx_(eval_ctx),
      last_row_(allocator),
      last_row_tablet_id_(),
      last_row_expr_(nullptr),
      op_id_(common::OB_INVALID_ID),
      is_heap_table_insert_(false),
      with_barrier_(false),
      dfo_id_(OB_INVALID_ID)
  {
  }

  ~ObPDMLOpDataDriver();

  int init(const ObTableModifySpec &spec,
           common::ObIAllocator &allocator,
           ObDMLBaseRtDef &dml_rtdef,
           ObDMLOpDataReader *reader,
           ObDMLOpDataWriter *writer,
           const bool is_heap_table_insert,
           const bool with_barrier = false);

  int destroy();

  int set_dh_barrier_param(uint64_t op_id, const ObPxMultiPartModifyOpInput *modify_input);

  int get_next_row(ObExecContext &ctx, const ObExprPtrIArray &row);
private:
  int fill_cache_unitl_cache_full_or_child_iter_end(ObExecContext &ctx);
  inline int try_write_last_pending_row();
  int switch_to_returning_state(ObExecContext &ctx);
  int switch_row_iter_to_next_partition();
  int barrier(ObExecContext &ctx);
  int next_row_from_cache_for_returning(const ObExprPtrIArray &row);
  int write_partitions(ObExecContext &ctx);
  int set_heap_table_hidden_pk(const ObExprPtrIArray *&row,
                               common::ObTabletID &tablet_id,
                               const bool is_direct_load = false);
  int set_heap_table_hidden_pk_value(const ObExprPtrIArray *&row,
                                     common::ObTabletID &tablet_id,
                                     const uint64_t pk_value);

private:
  // 因为 cache 中会缓存多个分区的数据，迭代的过程中需要
  // 记录当前迭代到哪个分区、迭代到分区中的哪一行等状态
  // 所以，引入 ReturningCtx
  // 用于记录当前正在返回哪个分区的行，以支持反复 get_next_row
  struct ReturningCtx {
    ReturningCtx() : next_idx_(0), row_iter_(nullptr) {}
    ~ReturningCtx() = default;
    void reset()
    {
      if (row_iter_) {
        row_iter_->close();
      }
      tablet_id_array_.reset();
      next_idx_ = 0;
      row_iter_ = NULL;
    }

    ObTabletIDArray tablet_id_array_; // 所有要读的分区索引
    int64_t next_idx_; // 下一个要读的分区索引，0 if not started
    ObPDMLOpRowIterator *row_iter_; // 当前在读分区的行迭代器
  };

  /* 状态转移图：
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
   *  (0); 开始
   *  (1); 从 PDMLDataReader 读入数据填充缓存
   *  (2); 向上吐出数据，因为是拉数据模型，所以会多次做，在状态(2);
   *  (3); 缓存数据全部吐出，再次开始填充缓存，进入状态(1);
   *  (4); 无数据填充缓存，且缓存为空，结束
   *
   */
  enum DriverState {
    FILL_CACHE,  /* 填充 cache、cache 满后同步自动写盘，并转入 ROW_RETURNING 状态 */
    ROW_RETURNING /* 返回行给上层，行全部返回后自动转入填充 cache 状态 */
  };


private:
  ReturningCtx returning_ctx_; // returning类型会使用到，目前还未使用
  ObMonitorNode &op_monitor_info_;
  ObPDMLOpBatchRowCache cache_; // 用于缓存数据，需要在init函数中初始化，并且分配alloctor
  ObDMLOpDataReader *reader_;
  ObDMLOpDataWriter *writer_;
  ObDMLBaseRtDef *dml_rtdef_;
  DriverState state_; // Driver 当前状态：读写数据状态、向上返回数据状态

  ObEvalCtx *eval_ctx_; // 用于存储 last_row 做入参
  ObChunkDatumStore::LastStoredRow last_row_; //缓存已从child读出但还没写入cache的行
  common::ObTabletID last_row_tablet_id_; // 缓存已经从child读取出来还没有写入到cache的行的part id
  const ObExprPtrIArray *last_row_expr_; // 指向表达式，用于把 row 数据恢复到表达式中
  int64_t op_id_; // 当前操作这个 driver 的算子 id，用于 barrier 场景下发消息传参
  bool is_heap_table_insert_;
  bool with_barrier_; // 当前算子需要支持 barrier，即：没有写完之前不可以对外吐出数据
                      // 这是针对 row-movement 场景下避免 insert、delete 并发写同一行
  uint64_t dfo_id_;   // with_barrier_等于true的情况下需要知道barrier对应的DFO
  DISALLOW_COPY_AND_ASSIGN(ObPDMLOpDataDriver);;
};
}
}
#endif /* _OB_SQL_ENGINE_PDML_PDML_DATA_DRIVER_H_ */
//// end of header file

