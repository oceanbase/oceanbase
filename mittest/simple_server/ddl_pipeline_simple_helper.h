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

#ifndef OCEANBASE_UNITTEST_DDL_PIPELINE_SIMPLE_HELPER_H_
#define OCEANBASE_UNITTEST_DDL_PIPELINE_SIMPLE_HELPER_H_

#define USING_LOG_PREFIX STORAGE

#include "test_ddl_pipeline_base.h"

namespace oceanbase
{
namespace unittest
{

// 默认列定义：两列，rowkey 各占一列
static constexpr ObObjType DEFAULT_COL_TYPES[] = {ObIntType, ObVarcharType};
static constexpr int64_t DEFAULT_COL_COUNT = 2;
static constexpr int64_t DEFAULT_ROWKEY_COUNT = 2;

// 基础参数，提供默认值，便于不同 pipeline 复用
struct DDLPipelineSimpleParam
{
  uint64_t tenant_id_{OB_INVALID_TENANT_ID};
  share::ObLSID ls_id_{1001};
  common::ObTabletID tablet_id_{common::ObTabletID(2000001)};
  uint64_t table_id_{500001};
  int64_t rowkey_count_{DEFAULT_ROWKEY_COUNT};
  const ObObjType *col_types_{DEFAULT_COL_TYPES};
  int64_t col_count_{DEFAULT_COL_COUNT};
  TestDagConfig dag_config_;

  DDLPipelineSimpleParam()
  {
    dag_config_.target_table_id_ = table_id_;
    dag_config_.ddl_thread_count_ = 1;
  }
};

// 构建基础表 schema
inline int build_basic_table_schema(const DDLPipelineSimpleParam &param, ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == param.tenant_id_)
      || OB_UNLIKELY(param.col_count_ <= 0)
      || OB_ISNULL(param.col_types_)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    TestTableSchemaBuilder builder;
    builder.set_tenant_id(param.tenant_id_)
           .set_table_id(param.table_id_)
           .add_columns(param.col_types_, param.col_count_, param.rowkey_count_);
    ret = builder.build(table_schema);
  }
  return ret;
}

// 创建 tablet + 初始化 dag / tablet context / storage schema
inline int create_tablet_and_dag_env(
    const DDLPipelineSimpleParam &param,
    ObArenaAllocator &allocator,
    observer::ObTableLoadDag &dag,
    ObTableSchema &table_schema,
    ObStorageSchema *&storage_schema,
    ObDDLTabletContext *&tablet_context,
    ObTabletHandle *tablet_handle = nullptr)
{
  int ret = OB_SUCCESS;
  storage_schema = nullptr;
  tablet_context = nullptr;

  // 构建表 schema
  if (OB_FAIL(build_basic_table_schema(param, table_schema))) {
    STORAGE_LOG(WARN, "fail to build table schema", K(ret));
  }

  // 初始化 dag
  TestDagConfig dag_cfg = param.dag_config_;
  dag_cfg.target_table_id_ = param.table_id_;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(TestDagBuilder::init_dag(dag, dag_cfg, allocator))) {
      STORAGE_LOG(WARN, "fail to init dag", K(ret));
    }
  }

  // storage schema
  if (OB_SUCC(ret)) {
    storage_schema = OB_NEWx(ObStorageSchema, &allocator);
    if (OB_ISNULL(storage_schema)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to alloc storage schema", K(ret));
    } else if (OB_FAIL(storage_schema->init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
      STORAGE_LOG(WARN, "fail to init storage schema", K(ret));
    }
  }

  // tablet context
  if (OB_SUCC(ret)) {
    if (OB_FAIL(TestDagBuilder::create_tablet_context(
            dag, param.ls_id_, param.tablet_id_, storage_schema, allocator, tablet_context))) {
      STORAGE_LOG(WARN, "fail to create tablet context", K(ret));
    }
  }

  // 生成 ddl table schema
  if (OB_SUCC(ret)) {
    if (OB_FAIL(TestDagBuilder::init_ddl_table_schema(dag, table_schema, storage_schema, param.rowkey_count_))) {
      STORAGE_LOG(WARN, "fail to init ddl table schema", K(ret));
    }
  }

  // 创建 tablet
  if (OB_SUCC(ret)) {
    ObTabletHandle local_handle;
    ObTabletHandle *target_handle = (nullptr == tablet_handle) ? &local_handle : tablet_handle;
    if (OB_FAIL(TestTabletFactory::create_tablet(
            param.ls_id_, param.tablet_id_, table_schema, allocator, *target_handle))) {
      STORAGE_LOG(WARN, "fail to create tablet", K(ret), K(param.tablet_id_));
    }
  }

  // 初始化 tablet context
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_context->init(param.ls_id_,
                                     param.tablet_id_,
                                     dag_cfg.ddl_thread_count_,
                                     dag_cfg.snapshot_version_,
                                     dag.direct_load_type_,
                                     dag.ddl_table_schema_))) {
      STORAGE_LOG(WARN, "fail to init tablet context", K(ret));
    }
  }

  return ret;
}

// 获取 LS 句柄（若 LS 已存在则直接返回）
inline int get_ls_handle(const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    ret = ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD);
  }
  return ret;
}

// 同步添加任务并执行 dag
inline int add_task_and_run(observer::ObTableLoadDag &dag, ObITask &task)
{
  int ret = OB_SUCCESS;
  dag.dag_status_ = ObIDag::DAG_STATUS_NODE_RUNNING;
  if (OB_FAIL(dag.add_task(task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret));
  } else if (OB_FAIL(dag.process())) {
    STORAGE_LOG(WARN, "fail to process dag", K(ret));
  }
  return ret;
}

} // namespace unittest
} // namespace oceanbase

#undef USING_LOG_PREFIX

#endif // OCEANBASE_UNITTEST_DDL_PIPELINE_SIMPLE_HELPER_H_
