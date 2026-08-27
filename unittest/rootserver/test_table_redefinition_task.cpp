/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define protected public
#define private public

#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "rootserver/ddl_task/ob_column_redefinition_task.h"

#define LOG_FILE_PATH "./test_table_redefinition_task.log"

namespace oceanbase
{
namespace unittest
{

using namespace oceanbase::common;
using namespace oceanbase::rootserver;
using namespace oceanbase::share;

#define ASSERT_SUCC(expr) ASSERT_EQ(OB_SUCCESS, (expr))

class TestTableRedefinitionTask : public ::testing::Test
{
public:
  static void prepare_table_task(ObTableRedefinitionTask &task)
  {
    task.tenant_id_ = 1002;
    task.task_id_ = 6001;
    task.task_type_ = ObDDLType::DDL_TABLE_REDEFINITION;
    task.task_version_ = ObTableRedefinitionTask::OB_TABLE_REDEFINITION_TASK_VERSION;
    task.parallelism_ = 1;
    task.data_format_version_ = 1;
    task.alter_table_arg_.alter_table_schema_.set_tenant_id(task.tenant_id_);
    task.alter_table_arg_.alter_table_schema_.origin_database_name_ =
        ObString::make_string("test_db");
    task.alter_table_arg_.alter_table_schema_.origin_table_name_ =
        ObString::make_string("test_table");
    task.alter_table_arg_.tz_info_wrap_.set_tz_info_offset(0);
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      task.alter_table_arg_.nls_formats_[i] = ObString::make_string("NLS_TEST_FORMAT");
    }
  }

  static void prepare_column_task(ObColumnRedefinitionTask &task)
  {
    task.tenant_id_ = 1002;
    task.task_id_ = 6002;
    task.task_type_ = ObDDLType::DDL_DROP_COLUMN;
    task.task_version_ = ObColumnRedefinitionTask::OB_COLUMN_REDEFINITION_TASK_VERSION;
    task.parallelism_ = 1;
    task.data_format_version_ = 1;
    task.alter_table_arg_.alter_table_schema_.set_tenant_id(task.tenant_id_);
    task.alter_table_arg_.alter_table_schema_.origin_database_name_ =
        ObString::make_string("test_db");
    task.alter_table_arg_.alter_table_schema_.origin_table_name_ =
        ObString::make_string("test_table");
    task.alter_table_arg_.tz_info_wrap_.set_tz_info_offset(0);
    for (int64_t i = 0; i < ObNLSFormatEnum::NLS_MAX; ++i) {
      task.alter_table_arg_.nls_formats_[i] = ObString::make_string("NLS_TEST_FORMAT");
    }
  }

  template <typename Task>
  static int add_child(Task &task, const uint64_t object_id,
                       const int64_t task_id, const int64_t ret_code)
  {
    typename Task::DependTaskStatus status;
    status.task_id_ = task_id;
    status.ret_code_ = ret_code;
    return task.dependent_task_result_map_.set_refactored(object_id, status);
  }

  template <typename Task>
  static void assert_recovered_child(const Task &task,
                                     const uint64_t object_id, const int64_t task_id)
  {
    typename Task::DependTaskStatus status;
    ASSERT_SUCC(task.dependent_task_result_map_.get_refactored(object_id, status));
    ASSERT_EQ(task_id, status.task_id_);
    ASSERT_EQ(INT64_MAX, status.ret_code_);
  }
};

// Recover only {object_id, task_id} from the parent message tail; ret_code stays INT64_MAX.
TEST_F(TestTableRedefinitionTask, dependent_tasks_serialize_deserialize_roundtrip)
{
  const uint64_t first_object_id = 500001;
  const uint64_t second_object_id = 500002;
  const int64_t first_task_id = 7001;
  const int64_t second_task_id = 7002;

  ObTableRedefinitionTask src_task;
  prepare_table_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, first_object_id, first_task_id, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, second_object_id, second_task_id, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  ASSERT_GT(size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObTableRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, serialize_pos, deserialize_pos));
  ASSERT_EQ(serialize_pos, deserialize_pos);
  ASSERT_EQ(2, dst_task.dependent_task_result_map_.size());
  assert_recovered_child(dst_task, first_object_id, first_task_id);
  assert_recovered_child(dst_task, second_object_id, second_task_id);
}

// Old Table messages without the dependent-task tail still deserialize; map stays empty.
TEST_F(TestTableRedefinitionTask, deserialize_legacy_message_without_dependent_tasks)
{
  ObTableRedefinitionTask src_task;
  prepare_table_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObTableRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, 500001, 7001, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, 500002, 7002, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  const int64_t dependent_size = src_task.get_dependent_task_serialize_size();
  const int64_t legacy_size = size - dependent_size;
  ASSERT_GT(dependent_size, 0);
  ASSERT_GT(legacy_size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObTableRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, legacy_size, deserialize_pos));
  ASSERT_EQ(legacy_size, deserialize_pos);
  ASSERT_EQ(0, dst_task.dependent_task_result_map_.size());
}

TEST_F(TestTableRedefinitionTask, column_dependent_tasks_serialize_deserialize_roundtrip)
{
  const uint64_t first_object_id = 500001;
  const uint64_t second_object_id = 500002;
  const int64_t first_task_id = 7001;
  const int64_t second_task_id = 7002;

  ObColumnRedefinitionTask src_task;
  prepare_column_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObColumnRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, first_object_id, first_task_id, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, second_object_id, second_task_id, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  ASSERT_GT(size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObColumnRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, serialize_pos, deserialize_pos));
  ASSERT_EQ(serialize_pos, deserialize_pos);
  ASSERT_EQ(2, dst_task.dependent_task_result_map_.size());
  assert_recovered_child(dst_task, first_object_id, first_task_id);
  assert_recovered_child(dst_task, second_object_id, second_task_id);
}

// Old Column messages end after alter_table_arg_; the missing tail leaves the map empty.
TEST_F(TestTableRedefinitionTask, column_deserialize_legacy_message_without_dependent_tasks)
{
  ObColumnRedefinitionTask src_task;
  prepare_column_task(src_task);
  ASSERT_SUCC(src_task.dependent_task_result_map_.create(
      ObColumnRedefinitionTask::MAX_DEPEND_OBJECT_COUNT, lib::ObLabel("RedefTaskUT")));
  ASSERT_SUCC(add_child(src_task, 500001, 7001, INT64_MAX));
  ASSERT_SUCC(add_child(src_task, 500002, 7002, OB_ERR_UNEXPECTED));

  const int64_t size = src_task.get_serialize_param_size();
  const int64_t dependent_size = src_task.get_dependent_task_serialize_size();
  const int64_t legacy_size = size - dependent_size;
  ASSERT_GT(dependent_size, 0);
  ASSERT_GT(legacy_size, 0);
  ObArenaAllocator allocator;
  char *buf = static_cast<char *>(allocator.alloc(size));
  ASSERT_NE(nullptr, buf);

  int64_t serialize_pos = 0;
  ASSERT_SUCC(src_task.serialize_params_to_message(buf, size, serialize_pos));
  ASSERT_EQ(size, serialize_pos);

  ObColumnRedefinitionTask dst_task;
  int64_t deserialize_pos = 0;
  ASSERT_SUCC(dst_task.deserialize_params_from_message(
      src_task.tenant_id_, buf, legacy_size, deserialize_pos));
  ASSERT_EQ(legacy_size, deserialize_pos);
  ASSERT_EQ(0, dst_task.dependent_task_result_map_.size());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf " LOG_FILE_PATH "*");
  oceanbase::common::ObLogger::get_logger().set_log_level("WDIAG");
  oceanbase::common::ObLogger::get_logger().set_file_name(LOG_FILE_PATH, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
