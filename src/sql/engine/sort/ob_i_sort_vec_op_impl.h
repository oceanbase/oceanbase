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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_I_VEC_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_I_VEC_OP_IMPL_H_

#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/sort/ob_sort_vec_op_context.h"

namespace oceanbase {
namespace sql {

// The abstract interface class of sort implementation, each type of sort
// implementation needs to inherit from this interface and implement its own
// implementation class.
class ObISortVecOpImpl
{
public:
  explicit ObISortVecOpImpl(ObMonitorNode &op_monitor_info, lib::MemoryContext &mem_context) :
    mem_context_(mem_context), input_rows_(OB_INVALID_ID), input_width_(OB_INVALID_ID),
    op_type_(PHY_INVALID), op_id_(UINT64_MAX), io_event_observer_(nullptr),
    profile_(ObSqlWorkAreaType::SORT_WORK_AREA), op_monitor_info_(op_monitor_info),
    sql_mem_processor_(profile_, op_monitor_info_)
  {}
  virtual ~ObISortVecOpImpl()
  {}
  virtual void reset() = 0;
  virtual int init(ObSortVecOpContext &context) = 0;
  virtual int add_batch(const ObBatchRows &input_brs, bool &sort_need_dump) = 0;
  virtual int get_next_batch(const int64_t max_cnt, int64_t &read_rows) = 0;
  virtual int sort() = 0;
  virtual int add_batch_stored_row(int64_t &row_size, const ObCompactRow **sk_stored_rows,
                                   const ObCompactRow **addon_stored_rows) = 0;
  virtual int64_t get_extra_size(bool is_sort_key) = 0;
  void unregister_profile()
  {
    sql_mem_processor_.unregister_profile();
  }
  void unregister_profile_if_necessary()
  {
    sql_mem_processor_.unregister_profile_if_necessary();
  }
  void collect_memory_dump_info(ObMonitorNode &info)
  {
    info.otherstat_1_id_ = op_monitor_info_.otherstat_1_id_;
    info.otherstat_1_value_ = op_monitor_info_.otherstat_1_value_;
    info.otherstat_2_id_ = op_monitor_info_.otherstat_2_id_;
    info.otherstat_2_value_ = op_monitor_info_.otherstat_2_value_;
    info.otherstat_3_id_ = op_monitor_info_.otherstat_3_id_;
    info.otherstat_3_value_ = op_monitor_info_.otherstat_3_value_;
    info.otherstat_4_id_ = op_monitor_info_.otherstat_4_id_;
    info.otherstat_4_value_ = op_monitor_info_.otherstat_4_value_;
    info.otherstat_6_id_ = op_monitor_info_.otherstat_6_id_;
    info.otherstat_6_value_ = op_monitor_info_.otherstat_6_value_;
  }
  void set_input_rows(int64_t input_rows)
  {
    input_rows_ = input_rows;
  }
  void set_input_width(int64_t input_width)
  {
    input_width_ = input_width;
  }
  void set_operator_type(ObPhyOperatorType op_type)
  {
    op_type_ = op_type;
  }
  void set_operator_id(uint64_t op_id)
  {
    op_id_ = op_id;
  }
  void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }

protected:
  lib::MemoryContext mem_context_;
  int64_t input_rows_;
  int64_t input_width_;
  ObPhyOperatorType op_type_;
  uint64_t op_id_;
  ObIOEventObserver *io_event_observer_;
  ObSqlWorkAreaProfile profile_;
  ObMonitorNode &op_monitor_info_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_I_VEC_OP_IMPL_H_ */
