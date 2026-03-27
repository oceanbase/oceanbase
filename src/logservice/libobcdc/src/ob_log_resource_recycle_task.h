/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 *  Define resource recovery tasks, for asynchronous recovery
 */

#ifndef OCEANBASE_LIBOBCDC_RESOURCE_RECYCLE_TASK_H__
#define OCEANBASE_LIBOBCDC_RESOURCE_RECYCLE_TASK_H__

namespace oceanbase
{
namespace libobcdc
{
class ObLogResourceRecycleTask
{
public:
  enum TaskType
  {
    UNKNOWN_TASK = 0,
    PART_TRANS_TASK = 1,
    BINLOG_RECORD_TASK = 2,
    LOB_DATA_CLEAN_TASK = 3,
  };
  OB_INLINE bool is_unknown_task() const { return UNKNOWN_TASK == task_type_; }
  OB_INLINE bool is_part_trans_task() const { return PART_TRANS_TASK == task_type_; }
  OB_INLINE bool is_binlog_record_task() const { return BINLOG_RECORD_TASK == task_type_; }
  OB_INLINE bool is_lob_data_clean_task() const { return LOB_DATA_CLEAN_TASK == task_type_; }
  OB_INLINE TaskType get_task_type() const { return task_type_; }

  static const char *print_task_type(TaskType task)
  {
    const char *str = "UNKNOWN_TASK";

    switch (task) {
      case PART_TRANS_TASK:
        str = "PartTransTask";
        break;
      case BINLOG_RECORD_TASK:
        str = "BinlogRecordTask";
        break;
      case LOB_DATA_CLEAN_TASK:
        str = "LobDataCleanTask";
      default:
        str = "UNKNOWN_TASK";
        break;
    }

    return str;
  }

public:
  ObLogResourceRecycleTask() : task_type_(UNKNOWN_TASK) {}
  ObLogResourceRecycleTask(TaskType task_type) : task_type_(task_type) {}
  ~ObLogResourceRecycleTask() { task_type_ = UNKNOWN_TASK; }

public:
  TaskType task_type_;
};

} // namespace libobcdc
} // namespace oceanbase

#endif
