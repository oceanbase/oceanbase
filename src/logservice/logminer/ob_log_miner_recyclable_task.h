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

#ifndef OCEANBASE_LOG_MINER_RECYCLABLE_TASK_H_
#define OCEANBASE_LOG_MINER_RECYCLABLE_TASK_H_

namespace oceanbase
{
namespace oblogminer
{

class ObLogMinerRecyclableTask
{
public:
  enum class TaskType
  {
    UNKNOWN = 0,
    BINLOG_RECORD,
    LOGMINER_RECORD,
    BATCH_RECORD,
    UNDO_TASK
  };

  explicit ObLogMinerRecyclableTask(TaskType type): type_(type) { }
  ~ObLogMinerRecyclableTask() { type_ = TaskType::UNKNOWN; }

  bool is_binlog_record() const {
    return TaskType::BINLOG_RECORD == type_;
  }
  bool is_logminer_record() const {
    return TaskType::LOGMINER_RECORD == type_;
  }
  bool is_batch_record() const {
    return TaskType::BATCH_RECORD == type_;
  }
  bool is_undo_task() const {
    return TaskType::UNDO_TASK == type_;
  }

  TaskType get_task_type() const
  {
    return type_;
  }

protected:
  TaskType type_;
};

}
}

#endif