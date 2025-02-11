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
#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_index_table_builder.h"
#include "observer/table_load/ob_table_load_data_table_builder.h"
#include "observer/table_load/ob_table_load_row_projector.h"
#include "observer/table_load/ob_table_load_lob_table_builder.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_data_table_ctx.h"
#include "storage/direct_load/ob_direct_load_insert_lob_table_ctx.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "src/observer/table_load/ob_table_load_error_row_handler.h"

namespace oceanbase
{
namespace observer
{
using namespace table;
using namespace share;
using namespace storage;

ObTableLoadStoreTableCtx::ObTableLoadStoreTableCtx(ObTableLoadStoreCtx *store_ctx)
  : store_ctx_(store_ctx),
    table_id_(OB_INVALID_ID),
    schema_(nullptr),
    insert_table_ctx_(nullptr),
    allocator_("TLD_STCtx"),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreTableCtx::~ObTableLoadStoreTableCtx()
{
  if (OB_NOT_NULL(schema_)) {
    schema_->~ObTableLoadSchema();
    allocator_.free(schema_);
    schema_ = NULL;
  }
  if (OB_NOT_NULL(insert_table_ctx_)) {
    insert_table_ctx_->~ObDirectLoadInsertTableContext();
    allocator_.free(insert_table_ctx_);
    insert_table_ctx_ = NULL;
  }
}

int ObTableLoadStoreTableCtx::inner_init(const uint64_t table_id)
{
  int ret = OB_SUCCESS;
  table_id_ = table_id;
  // init schema_
  if (OB_ISNULL(schema_ = OB_NEWx(ObTableLoadSchema, (&allocator_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadSchema", KR(ret));
  } else if (OB_FAIL(schema_->init(store_ctx_->ctx_->param_.tenant_id_, table_id_))) {
    LOG_WARN("fail to init schema", KR(ret));
  }
  return ret;
}

//////////////////////// table builder ////////////////////////

#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_INIT_IMPL(classType, builderType, name, table_store) \
  int classType::init_build_##name##_table()                                                     \
  {                                                                                              \
    int ret = OB_SUCCESS;                                                                        \
    if (IS_NOT_INIT) {                                                                           \
      ret = OB_NOT_INIT;                                                                         \
      LOG_WARN(#classType " not init", KR(ret), KP(this));                                       \
    } else if (OB_UNLIKELY(name##_table_builder_map_.created())) {                               \
      ret = OB_ERR_UNEXPECTED;                                                                   \
      LOG_WARN("unexpected " #name " hashmap created", KR(ret));                                 \
    } else if (OB_FAIL(                                                                          \
                 name##_table_builder_map_.create(1024, "TLD_TBMap", "TLD_TBMap", MTL_ID()))) {  \
      LOG_WARN("fail to create hashmap", KR(ret));                                               \
    }                                                                                            \
    return ret;                                                                                  \
  }

#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_GET_BUILDER_IMPL(classType, builderType, name,        \
                                                             table_store)                         \
  int classType::get_##name##_table_builder(builderType *&table_builder)                          \
  {                                                                                               \
    int ret = OB_SUCCESS;                                                                         \
    table_builder = nullptr;                                                                      \
    if (IS_NOT_INIT) {                                                                            \
      ret = OB_NOT_INIT;                                                                          \
      LOG_WARN(#classType " not init", KR(ret), KP(this));                                        \
    } else if (OB_UNLIKELY(!name##_table_builder_map_.created())) {                               \
      ret = OB_ERR_UNEXPECTED;                                                                    \
      LOG_WARN("unexpected " #name " hashmap not created", KR(ret));                              \
    } else {                                                                                      \
      const int64_t part_id = get_tid_cache();                                                    \
      if (OB_FAIL(name##_table_builder_map_.get_refactored(part_id, table_builder))) {            \
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {                                              \
          LOG_WARN("fail to get table builder", KR(ret), K(get_tid_cache()));                     \
        } else {                                                                                  \
          ret = OB_SUCCESS;                                                                       \
          if (OB_FAIL(acquire_table_builder(table_builder, name##_table_builder_safe_allocator_,  \
                                            get_##name##_table_data_desc()))) {                   \
            LOG_WARN("fail to acquire " #name " table builder", KR(ret));                         \
          } else if (OB_ISNULL(table_builder)) {                                                  \
            ret = OB_ERR_UNEXPECTED;                                                              \
            LOG_WARN("unexpected table builder is null", KR(ret));                                \
          } else if (OB_FAIL(name##_table_builder_map_.set_refactored(part_id, table_builder))) { \
            LOG_WARN("fail to set hashmap", KR(ret), K(part_id));                                 \
          }                                                                                       \
          if (OB_FAIL(ret)) {                                                                     \
            if (nullptr != table_builder) {                                                       \
              table_builder->~builderType();                                                      \
              name##_table_builder_safe_allocator_.free(table_builder);                           \
              table_builder = nullptr;                                                            \
            }                                                                                     \
          }                                                                                       \
        }                                                                                         \
      }                                                                                           \
    }                                                                                             \
    return ret;                                                                                   \
  }

#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_CLOSE_IMPL(classType, builderType, name, table_store) \
  int classType ::close_build_##name##_table()                                                    \
  {                                                                                               \
    int ret = OB_SUCCESS;                                                                         \
    if (IS_NOT_INIT) {                                                                            \
      ret = OB_NOT_INIT;                                                                          \
      LOG_WARN(#classType " not init", KR(ret), KP(this));                                        \
    } else if (OB_UNLIKELY(!name##_table_builder_map_.created())) {                               \
      ret = OB_ERR_UNEXPECTED;                                                                    \
      LOG_WARN("unexpected " #name " hashmap not created", KR(ret));                              \
    } else {                                                                                      \
      table_store.set_table_data_desc(get_##name##_table_data_desc());                            \
      table_store.set_external_table();                                                           \
      FOREACH_X(it, name##_table_builder_map_, OB_SUCC(ret))                                      \
      {                                                                                           \
        builderType *table_builder = it->second;                                                  \
        ObDirectLoadTableHandleArray table_handle_array;                                          \
        if (OB_FAIL(table_builder->close())) {                                                    \
          LOG_WARN("fail to close table", KR(ret));                                               \
        } else if (OB_FAIL(                                                                       \
                     table_builder->get_tables(table_handle_array, store_ctx_->table_mgr_))) {    \
          LOG_WARN("fail to get tables", KR(ret));                                                \
        } else if (OB_FAIL(table_store.add_tables(table_handle_array))) {                         \
          LOG_WARN("fail to add tables", KR(ret));                                                \
        }                                                                                         \
      }                                                                                           \
      if (OB_SUCC(ret)) {                                                                         \
        clear_##name##_table_builder();                                                           \
      }                                                                                           \
    }                                                                                             \
    return ret;                                                                                   \
  }

#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_CLEAR_IMPL(classType, builderType, name, table_store) \
  void classType::clear_##name##_table_builder()                                                  \
  {                                                                                               \
    if (name##_table_builder_map_.created()) {                                                    \
      FOREACH(it, name##_table_builder_map_)                                                      \
      {                                                                                           \
        builderType *table_builder = it->second;                                                  \
        table_builder->~builderType();                                                            \
        name##_table_builder_allocator_.free(table_builder);                                      \
      }                                                                                           \
      name##_table_builder_map_.destroy();                                                        \
      name##_table_builder_allocator_.reset();                                                    \
    }                                                                                             \
  }

#define DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(classType, builderType, name, table_store)        \
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_INIT_IMPL(classType, builderType, name, table_store);        \
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_GET_BUILDER_IMPL(classType, builderType, name, table_store); \
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_CLOSE_IMPL(classType, builderType, name, table_store);       \
  DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_CLEAR_IMPL(classType, builderType, name, table_store);

/**
 * ObTableLoadStoreDataTableCtx
 */

ObTableLoadStoreDataTableCtx::ObTableLoadStoreDataTableCtx(ObTableLoadStoreCtx *store_ctx)
  : ObTableLoadStoreTableCtx(store_ctx),
    delete_table_builder_allocator_("TLD_DeTB"),
    delete_table_builder_safe_allocator_(delete_table_builder_allocator_),
    ack_table_builder_allocator_("TLD_AckTB"),
    ack_table_builder_safe_allocator_(ack_table_builder_allocator_),
    lob_table_ctx_(nullptr),
    project_(nullptr)
{
  delete_table_builder_allocator_.set_tenant_id(MTL_ID());
  ack_table_builder_allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreDataTableCtx::~ObTableLoadStoreDataTableCtx()
{
  clear_delete_table_builder();
  clear_ack_table_builder();
  if (nullptr != lob_table_ctx_) {
    lob_table_ctx_->~ObTableLoadStoreLobTableCtx();
    store_ctx_->allocator_.free(lob_table_ctx_);
    lob_table_ctx_ = nullptr;
  }
  if (nullptr != project_) {
    project_->~ObTableLoadRowProjector();
    allocator_.free(project_);
    project_ = nullptr;
  }
}

int ObTableLoadStoreDataTableCtx::init(
  const uint64_t table_id,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreDataTableCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || partition_id_array.empty() ||
                         target_partition_id_array.empty() ||
                         partition_id_array.count() != target_partition_id_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id), K(partition_id_array),
             K(target_partition_id_array));
  } else if (OB_FAIL(inner_init(table_id))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(init_data_project())) {
    LOG_WARN("fail to init data project", KR(ret));
  } else if (OB_FAIL(insert_table_store_.init())) {
    LOG_WARN("fail to init table store", KR(ret));
  } else if (OB_FAIL(delete_table_store_.init())) {
    LOG_WARN("fail to init table store", KR(ret));
  } else if (OB_FAIL(ack_table_store_.init())) {
    LOG_WARN("fail to init table store", KR(ret));
  } else if (OB_FAIL(init_ls_partition_ids(partition_id_array, target_partition_id_array))) {
    LOG_WARN("fail to init ls partition ids", KR(ret));
  } else if (!schema_->lob_column_idxs_.empty()) {
    if (OB_ISNULL(lob_table_ctx_ =
                    OB_NEWx(ObTableLoadStoreLobTableCtx, &allocator_, store_ctx_, this))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadStoreLobTableCtx", KR(ret));
    } else if (OB_FAIL(lob_table_ctx_->init(schema_->lob_meta_table_id_, partition_id_array,
                                            target_partition_id_array))) {
      LOG_WARN("fail to init lob table ctx", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadStoreDataTableCtx::init_data_project()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *data_table_schema = nullptr;
  const share::schema::ObTableSchema *index_table_schema = nullptr;
  if (OB_FAIL(
        ObTableLoadSchema::get_schema_guard(store_ctx_->ctx_->param_.tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(
               schema_guard, store_ctx_->ctx_->param_.tenant_id_,
               store_ctx_->ctx_->ddl_param_.dest_table_id_, data_table_schema))) {
    LOG_WARN("fail to get table shema of main table", KR(ret));
  } else {
    const ObIArray<ObAuxTableMetaInfo> &simple_index_infos =
      data_table_schema->get_simple_index_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
      const ObAuxTableMetaInfo &index_table_info = simple_index_infos.at(i);
      const share::schema::ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(ObTableLoadSchema::get_table_schema(
            schema_guard, MTL_ID(), index_table_info.table_id_, index_table_schema))) {
        LOG_WARN("fail to get table shema of index table", KR(ret), K(index_table_info.table_id_));
      } else if (index_table_schema->is_unique_index()) {
        if (OB_ISNULL(project_ = OB_NEWx(ObTableLoadUniqueIndexToMainRowkeyProjector, (&allocator_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadUniqueIndexToMainRowkeyProjector", KR(ret));
        } else if (OB_FAIL(project_->init(index_table_schema, data_table_schema))) {
          LOG_WARN("fail to init ObTableLoadUniqueIndexToMainRowkeyProjector", KR(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObTableLoadStoreDataTableCtx::init_ls_partition_ids(
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids[i];
    if (OB_FAIL(ls_partition_ids_.push_back(ls_partition_id))) {
      LOG_WARN("fail to push back ", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = target_ls_partition_ids[i];
    if (OB_FAIL(target_ls_partition_ids_.push_back(ls_partition_id))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

//////////////////////// insert_table_ctx ////////////////////////

int ObTableLoadStoreDataTableCtx::init_insert_table_ctx(const ObDirectLoadTransParam &trans_param,
                                                        bool online_opt_stat_gather,
                                                        bool is_insert_lob)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreDataTableCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr != insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx not null", KR(ret));
  } else {
    ObDirectLoadInsertTableParam insert_table_param;
    // 目标表table_id, 目前只用于填充统计信息收集结果
    insert_table_param.table_id_ = store_ctx_->ctx_->ddl_param_.dest_table_id_;
    insert_table_param.schema_version_ = store_ctx_->ctx_->ddl_param_.schema_version_;
    insert_table_param.snapshot_version_ = store_ctx_->ctx_->ddl_param_.snapshot_version_;
    insert_table_param.ddl_task_id_ = store_ctx_->ctx_->ddl_param_.task_id_;
    insert_table_param.data_version_ = store_ctx_->ctx_->ddl_param_.data_version_;
    insert_table_param.parallel_ = store_ctx_->thread_cnt_;
    // 全量在快速堆表路径需要为存量数据预留parallel
    insert_table_param.reserved_parallel_ =
      store_ctx_->write_ctx_.is_fast_heap_table_ ? store_ctx_->thread_cnt_ : 0;
    insert_table_param.rowkey_column_count_ = schema_->rowkey_column_count_;
    insert_table_param.column_count_ = schema_->store_column_count_;
    insert_table_param.lob_inrow_threshold_ = schema_->lob_inrow_threshold_;
    insert_table_param.is_partitioned_table_ = schema_->is_partitioned_table_;
    insert_table_param.is_table_without_pk_ = schema_->is_table_without_pk_;
    insert_table_param.is_table_with_hidden_pk_column_ = schema_->is_table_with_hidden_pk_column_;
    insert_table_param.online_opt_stat_gather_ = online_opt_stat_gather && store_ctx_->ctx_->param_.online_opt_stat_gather_;
    insert_table_param.reuse_pk_ = !ObDirectLoadInsertMode::is_overwrite_mode(store_ctx_->ctx_->param_.insert_mode_);
    insert_table_param.is_insert_lob_ = is_insert_lob;
    insert_table_param.is_incremental_ =
      ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_);
    insert_table_param.trans_param_ = trans_param;
    insert_table_param.datum_utils_ = &(schema_->datum_utils_);
    insert_table_param.col_descs_ = &(schema_->column_descs_);
    insert_table_param.cmp_funcs_ = &(schema_->cmp_funcs_);
    insert_table_param.lob_column_idxs_ = &(schema_->lob_column_idxs_);
    insert_table_param.online_sample_percent_ = store_ctx_->ctx_->param_.online_sample_percent_;
    insert_table_param.is_no_logging_ = store_ctx_->ctx_->ddl_param_.is_no_logging_;
    insert_table_param.max_batch_size_ = store_ctx_->ctx_->param_.batch_size_;
    // new ObDirectLoadInsertDataTableContext
    ObDirectLoadInsertDataTableContext *insert_data_table_ctx = nullptr;
    if (OB_ISNULL(insert_table_ctx_ = insert_data_table_ctx =
                    OB_NEWx(ObDirectLoadInsertDataTableContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertDataTableContext", KR(ret));
    } else if (OB_FAIL(insert_data_table_ctx->init(insert_table_param, ls_partition_ids_,
                                                   target_ls_partition_ids_))) {
      LOG_WARN("fail to init insert table ctx", KR(ret));
    }
    // init lob insert_table_ctx_
    else if (nullptr != lob_table_ctx_ &&
             OB_FAIL(lob_table_ctx_->init_insert_table_ctx(
               trans_param, false /*online_opt_stat_gather*/, false /*is_insert_lob*/))) {
      LOG_WARN("fail to init lob insert table ctx", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreDataTableCtx::close_insert_table_ctx()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreDataTableCtx not init", KR(ret), KP(this));
  } else if (OB_ISNULL(insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx is null", KR(ret));
  } else {
    if (nullptr != lob_table_ctx_ && OB_FAIL(lob_table_ctx_->close_insert_table_ctx())) {
      LOG_WARN("fail to close lob insert table ctx", KR(ret));
    } else {
      insert_table_ctx_->~ObDirectLoadInsertTableContext();
      allocator_.free(insert_table_ctx_);
      insert_table_ctx_ = nullptr;
    }
  }
  return ret;
}

//////////////////////// table builder ////////////////////////

ObDirectLoadTableDataDesc ObTableLoadStoreDataTableCtx::get_delete_table_data_desc()
{
  ObDirectLoadTableDataDesc table_data_desc = store_ctx_->basic_table_data_desc_;
  table_data_desc.rowkey_column_num_ = schema_->rowkey_column_count_;
  table_data_desc.column_count_ = schema_->rowkey_column_count_;
  table_data_desc.row_flag_.reset();
  return table_data_desc;
}

ObDirectLoadTableDataDesc ObTableLoadStoreDataTableCtx::get_ack_table_data_desc()
{
  ObDirectLoadTableDataDesc table_data_desc = store_ctx_->basic_table_data_desc_;
  table_data_desc.rowkey_column_num_ = schema_->rowkey_column_count_;
  table_data_desc.column_count_ = schema_->rowkey_column_count_;
  table_data_desc.row_flag_.reset();
  return table_data_desc;
}

int ObTableLoadStoreDataTableCtx::acquire_table_builder(ObTableLoadDataTableBuilder *&table_builder,
                                                        ObIAllocator &allocator,
                                                        ObDirectLoadTableDataDesc table_data_desc)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  ObTableLoadDataTableBuildParam builder_param;
  builder_param.datum_utils_ = &schema_->datum_utils_;
  builder_param.table_data_desc_ = table_data_desc;
  builder_param.project_ = project_;
  builder_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  ObTableLoadDataTableBuilder *new_builder = nullptr;
  if (OB_ISNULL(new_builder = OB_NEWx(ObTableLoadDataTableBuilder, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadIndexTableBuilder", KR(ret));
  } else if (OB_FAIL(new_builder->init(builder_param))) {
    LOG_WARN("fail to init new_builder", KR(ret));
  } else {
    table_builder = new_builder;
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      new_builder->~ObTableLoadDataTableBuilder();
      allocator.free(new_builder);
      new_builder = nullptr;
    }
  }
  return ret;
}

DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(ObTableLoadStoreDataTableCtx, ObTableLoadDataTableBuilder,
                                         delete, delete_table_store_);
DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(ObTableLoadStoreDataTableCtx, ObTableLoadDataTableBuilder,
                                         ack, ack_table_store_);

/**
 * ObTableLoadStoreLobTableCtx
 */

ObTableLoadStoreLobTableCtx::ObTableLoadStoreLobTableCtx(
  ObTableLoadStoreCtx *store_ctx, ObTableLoadStoreDataTableCtx *data_table_ctx)
  : ObTableLoadStoreTableCtx(store_ctx),
    delete_table_builder_allocator_("TLD_LobTB"),
    delete_table_builder_safe_allocator_(delete_table_builder_allocator_),
    data_table_ctx_(data_table_ctx)
{
  delete_table_builder_allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreLobTableCtx::~ObTableLoadStoreLobTableCtx()
{
  clear_delete_table_builder();
}

int ObTableLoadStoreLobTableCtx::init(
  const uint64_t table_id,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreLobTableCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || partition_id_array.empty() ||
                         target_partition_id_array.empty() ||
                         partition_id_array.count() != target_partition_id_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id), K(partition_id_array),
             K(target_partition_id_array));
  } else if (OB_FAIL(inner_init(table_id))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(delete_table_store_.init())) {
    LOG_WARN("fail to init delete table store", KR(ret));
  } else if (OB_FAIL(tablet_id_map_.create(1024, ObMemAttr(MTL_ID(), "TLD_TbltIDMap")))) {
    LOG_WARN("fail to create hashmap", KR(ret));
  } else if (OB_FAIL(init_ls_partition_ids(partition_id_array, target_partition_id_array))) {
    LOG_WARN("fail to init ls partition ids", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadStoreLobTableCtx::init_ls_partition_ids(
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletBindingMdsUserData ddl_data;
  ObTableLoadLSIdAndPartitionId lob_ls_partition_id;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids[i];
    const ObLSID &ls_id = ls_partition_id.ls_id_;
    const ObTabletID &tablet_id = ls_partition_id.part_tablet_id_.tablet_id_;
    if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle,
                                                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet handle failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(SCN::max_scn(),
                                                                                    ddl_data))) {
      LOG_WARN("get ddl data failed", K(ret));
    } else {
      lob_ls_partition_id.ls_id_ = ls_id;
      lob_ls_partition_id.part_tablet_id_.partition_id_ =
        ls_partition_id.part_tablet_id_.partition_id_;
      lob_ls_partition_id.part_tablet_id_.tablet_id_ = ddl_data.lob_meta_tablet_id_;
      if (OB_FAIL(ls_partition_ids_.push_back(lob_ls_partition_id))) {
        LOG_WARN("fail to push back ", KR(ret));
      }
      // set map
      else if (OB_FAIL(tablet_id_map_.set_refactored(tablet_id, ddl_data.lob_meta_tablet_id_))) {
        LOG_WARN("fail to set tablet id", KR(ret), K(tablet_id));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = target_ls_partition_ids[i];
    const ObLSID &ls_id = ls_partition_id.ls_id_;
    const ObTabletID &tablet_id = ls_partition_id.part_tablet_id_.tablet_id_;
    if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get log stream", K(ret));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tablet_handle,
                                                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet handle failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(SCN::max_scn(),
                                                                                    ddl_data))) {
      LOG_WARN("get ddl data failed", K(ret));
    } else {
      lob_ls_partition_id.ls_id_ = ls_id;
      lob_ls_partition_id.part_tablet_id_.partition_id_ =
        ls_partition_id.part_tablet_id_.partition_id_;
      lob_ls_partition_id.part_tablet_id_.tablet_id_ = ddl_data.lob_meta_tablet_id_;
      if (OB_FAIL(target_ls_partition_ids_.push_back(lob_ls_partition_id))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreLobTableCtx::get_tablet_id(const ObTabletID &data_tablet_id,
                                               ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreLobTableCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!data_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(data_tablet_id));
  } else if (OB_FAIL(tablet_id_map_.get_refactored(data_tablet_id, tablet_id))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
      LOG_WARN("fail to get hashmap", KR(ret), K(data_tablet_id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tablet id not exist", KR(ret), K(data_tablet_id));
    }
  }
  return ret;
}

//////////////////////// insert_table_ctx ////////////////////////

int ObTableLoadStoreLobTableCtx::init_insert_table_ctx(const ObDirectLoadTransParam &trans_param,
                                                       bool online_opt_stat_gather,
                                                       bool is_insert_lob)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreLobTableCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr != insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx not null", KR(ret));
  } else {
    ObDirectLoadInsertTableParam insert_table_param;
    // TODO 目标表table_id, lob依附于数据表并不创建真正的ddl对象, 先填数据表的table_id
    insert_table_param.table_id_ = store_ctx_->ctx_->ddl_param_.dest_table_id_;
    insert_table_param.schema_version_ = store_ctx_->ctx_->ddl_param_.schema_version_;
    insert_table_param.snapshot_version_ = store_ctx_->ctx_->ddl_param_.snapshot_version_;
    insert_table_param.ddl_task_id_ = store_ctx_->ctx_->ddl_param_.task_id_;
    insert_table_param.data_version_ = store_ctx_->ctx_->ddl_param_.data_version_;
    insert_table_param.parallel_ = store_ctx_->thread_cnt_;
    // 增量需要为del_lob预留parallel
    insert_table_param.reserved_parallel_ =
      ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_) ? store_ctx_->thread_cnt_
                                                                           : 0;
    insert_table_param.rowkey_column_count_ = schema_->rowkey_column_count_;
    insert_table_param.column_count_ = schema_->store_column_count_;
    insert_table_param.lob_inrow_threshold_ = schema_->lob_inrow_threshold_;
    insert_table_param.is_partitioned_table_ = schema_->is_partitioned_table_;
    insert_table_param.is_table_without_pk_ = schema_->is_table_without_pk_;
    insert_table_param.is_table_with_hidden_pk_column_ = schema_->is_table_with_hidden_pk_column_;
    insert_table_param.online_opt_stat_gather_ = online_opt_stat_gather;
    insert_table_param.is_insert_lob_ = is_insert_lob;
    insert_table_param.reuse_pk_ = true;
    insert_table_param.is_incremental_ =
      ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_);
    insert_table_param.trans_param_ = trans_param;
    insert_table_param.datum_utils_ = &(schema_->datum_utils_);
    insert_table_param.col_descs_ = &(schema_->column_descs_);
    insert_table_param.cmp_funcs_ = &(schema_->cmp_funcs_);
    insert_table_param.lob_column_idxs_ = &(schema_->lob_column_idxs_);
    insert_table_param.online_sample_percent_ = store_ctx_->ctx_->param_.online_sample_percent_;
    insert_table_param.is_no_logging_ = store_ctx_->ctx_->ddl_param_.is_no_logging_;
    insert_table_param.max_batch_size_ = store_ctx_->ctx_->param_.batch_size_;
    insert_table_param.is_index_table_ = schema_->is_index_table_;
    // new ObDirectLoadInsertLobTableContext
    ObDirectLoadInsertLobTableContext *insert_lob_table_ctx = nullptr;
    if (OB_ISNULL(data_table_ctx_->insert_table_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data table not init", KR(ret));
    } else if (OB_ISNULL(insert_table_ctx_ = insert_lob_table_ctx =
                           OB_NEWx(ObDirectLoadInsertLobTableContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertLobTableContext", KR(ret));
    } else if (OB_FAIL(insert_lob_table_ctx->init(insert_table_param,
                                                  static_cast<ObDirectLoadInsertDataTableContext *>(
                                                    data_table_ctx_->insert_table_ctx_),
                                                  ls_partition_ids_, target_ls_partition_ids_,
                                                  data_table_ctx_->ls_partition_ids_))) {
      LOG_WARN("fail to init insert table ctx", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreLobTableCtx::close_insert_table_ctx()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreLobTableCtx not init", KR(ret), KP(this));
  } else if (OB_ISNULL(insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx is null", KR(ret));
  } else {
    insert_table_ctx_->~ObDirectLoadInsertTableContext();
    allocator_.free(insert_table_ctx_);
    insert_table_ctx_ = nullptr;
  }
  return ret;
}

//////////////////////// table builder ////////////////////////

ObDirectLoadTableDataDesc ObTableLoadStoreLobTableCtx::get_delete_table_data_desc()
{
  ObDirectLoadTableDataDesc table_data_desc = store_ctx_->basic_table_data_desc_;
  table_data_desc.rowkey_column_num_ = 1;
  table_data_desc.column_count_ = 1;
  table_data_desc.row_flag_.lob_id_only_ = true;
  return table_data_desc;
}

int ObTableLoadStoreLobTableCtx::acquire_table_builder(ObTableLoadLobTableBuilder *&table_builder,
                                                       ObIAllocator &allocator,
                                                       ObDirectLoadTableDataDesc table_data_desc)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  ObTableLoadLobTableBuildParam builder_param;
  builder_param.lob_table_ctx_ = this;
  builder_param.lob_column_idxs_ = &data_table_ctx_->schema_->lob_column_idxs_;
  builder_param.table_data_desc_ = table_data_desc;
  builder_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  if (OB_ISNULL(table_builder = OB_NEWx(ObTableLoadLobTableBuilder, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadLobTableBuilder", KR(ret));
  } else if (OB_FAIL(table_builder->init(builder_param))) {
    LOG_WARN("fail to init lob table builder", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      table_builder->~ObTableLoadLobTableBuilder();
      allocator.free(table_builder);
      table_builder = nullptr;
    }
  }
  return ret;
}

DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(ObTableLoadStoreLobTableCtx, ObTableLoadLobTableBuilder,
                                         delete, delete_table_store_);
/**
 * ObTableLoadStoreIndexTableCtx
 */

ObTableLoadStoreIndexTableCtx::ObTableLoadStoreIndexTableCtx(ObTableLoadStoreCtx *store_ctx)
  : ObTableLoadStoreTableCtx(store_ctx),
    insert_table_builder_allocator_("TLD_IndexTB"),
    insert_table_builder_safe_allocator_(insert_table_builder_allocator_),
    delete_table_builder_allocator_("TLD_IndexTB"),
    delete_table_builder_safe_allocator_(delete_table_builder_allocator_),
    project_(nullptr)
{
  insert_table_builder_allocator_.set_tenant_id(MTL_ID());
  delete_table_builder_allocator_.set_tenant_id(MTL_ID());
}

ObTableLoadStoreIndexTableCtx::~ObTableLoadStoreIndexTableCtx()
{
  clear_insert_table_builder();
  clear_delete_table_builder();
  if (OB_NOT_NULL(project_)) {
    project_->~ObTableLoadRowProjector();
    allocator_.free(project_);
    project_ = nullptr;
  }
}

int ObTableLoadStoreIndexTableCtx::init(
  const uint64_t table_id,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &partition_id_array,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_partition_id_array)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadStoreIndexTableCtx init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_INVALID_ID == table_id || partition_id_array.empty() ||
                         target_partition_id_array.empty() ||
                         partition_id_array.count() != target_partition_id_array.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(table_id), K(partition_id_array),
             K(target_partition_id_array));
  } else if (OB_FAIL(inner_init(table_id))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(insert_table_store_.init())) {
    LOG_WARN("fail to init insert table store", KR(ret));
  } else if (OB_FAIL(delete_table_store_.init())) {
    LOG_WARN("fail to init delete table store", KR(ret));
  } else if (OB_FAIL(init_index_projector())) {
    LOG_WARN("fail to init index projector", KR(ret));
  } else if (OB_FAIL(init_ls_partition_ids(partition_id_array, target_partition_id_array))) {
    LOG_WARN("fail to init ls partition ids", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadStoreIndexTableCtx::init_index_projector()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = store_ctx_->ctx_->param_.tenant_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *index_table_schema = nullptr;
  if (OB_FAIL(ObTableLoadSchema::get_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(
               schema_guard, tenant_id, store_ctx_->ctx_->param_.table_id_, data_table_schema))) {
    LOG_WARN("fail to get table shema of main table", KR(ret));
  } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(schema_guard, tenant_id, table_id_,
                                                         index_table_schema))) {
    LOG_WARN("fail to get table shema of index table", KR(ret));
  } else {
    if (index_table_schema->is_unique_index()) {
      if (OB_ISNULL(project_ = OB_NEWx(ObTableLoadMainToUniqueIndexProjector, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadIndexTableProjector", KR(ret));
      } else if (OB_FAIL(project_->init(data_table_schema, index_table_schema))) {
        LOG_WARN("fail to init ObTableLoadIndexTableProjector", KR(ret));
      }
    } else {
      if (OB_ISNULL(project_ = OB_NEWx(ObTableLoadMainToIndexProjector, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ObTableLoadIndexTableProjector", KR(ret));
      } else if (OB_FAIL(project_->init(data_table_schema, index_table_schema))) {
        LOG_WARN("fail to init ObTableLoadIndexTableProjector", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadStoreIndexTableCtx::init_ls_partition_ids(
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &ls_partition_ids,
  const ObTableLoadArray<ObTableLoadLSIdAndPartitionId> &target_ls_partition_ids)
{
  int ret = OB_SUCCESS;
  ObTableLoadLSIdAndPartitionId index_ls_partition_id;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = ls_partition_ids[i];
    const ObTabletID &data_tablet_id = ls_partition_id.part_tablet_id_.tablet_id_;
    index_ls_partition_id.ls_id_ = ls_partition_id.ls_id_;
    if (OB_FAIL(project_->get_dest_tablet_id_and_part_id_by_src_tablet_id(
          data_tablet_id, index_ls_partition_id.part_tablet_id_.tablet_id_,
          index_ls_partition_id.part_tablet_id_.partition_id_))) {
      LOG_WARN("fail to get index_tablet_id", KR(ret), K(data_tablet_id));
    } else if (OB_FAIL(ls_partition_ids_.push_back(index_ls_partition_id))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < target_ls_partition_ids.count(); ++i) {
    const ObTableLoadLSIdAndPartitionId &ls_partition_id = target_ls_partition_ids[i];
    const ObTabletID &data_tablet_id = ls_partition_id.part_tablet_id_.tablet_id_;
    index_ls_partition_id.ls_id_ = ls_partition_id.ls_id_;
    if (OB_FAIL(project_->get_dest_tablet_id_and_part_id_by_src_tablet_id(
          data_tablet_id, index_ls_partition_id.part_tablet_id_.tablet_id_,
          index_ls_partition_id.part_tablet_id_.partition_id_))) {
      LOG_WARN("fail to get index_tablet_id", KR(ret), K(data_tablet_id));
    } else if (OB_FAIL(target_ls_partition_ids_.push_back(index_ls_partition_id))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

//////////////////////// insert_table_ctx ////////////////////////

int ObTableLoadStoreIndexTableCtx::init_insert_table_ctx(const ObDirectLoadTransParam &trans_param,
                                                         bool online_opt_stat_gather,
                                                         bool is_insert_lob)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreIndexTableCtx not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr != insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx not null", KR(ret));
  } else {
    ObDirectLoadInsertTableParam insert_table_param;
    // TODO 目标表table_id, 索引只在增量场景, 目标表和原表是同一个
    insert_table_param.table_id_ = table_id_;
    insert_table_param.schema_version_ = store_ctx_->ctx_->ddl_param_.schema_version_;
    insert_table_param.snapshot_version_ = store_ctx_->ctx_->ddl_param_.snapshot_version_;
    insert_table_param.ddl_task_id_ = store_ctx_->ctx_->ddl_param_.task_id_;
    insert_table_param.data_version_ = store_ctx_->ctx_->ddl_param_.data_version_;
    insert_table_param.parallel_ = store_ctx_->thread_cnt_;
    insert_table_param.reserved_parallel_ = 0;
    insert_table_param.rowkey_column_count_ = schema_->rowkey_column_count_;
    insert_table_param.column_count_ = schema_->store_column_count_;
    insert_table_param.lob_inrow_threshold_ = schema_->lob_inrow_threshold_;
    insert_table_param.is_partitioned_table_ = schema_->is_partitioned_table_;
    insert_table_param.is_table_without_pk_ = schema_->is_table_without_pk_;
    insert_table_param.is_table_with_hidden_pk_column_ = schema_->is_table_with_hidden_pk_column_;
    insert_table_param.online_opt_stat_gather_ = online_opt_stat_gather;
    insert_table_param.is_insert_lob_ = is_insert_lob;
    insert_table_param.reuse_pk_ = true;
    insert_table_param.is_incremental_ =
      ObDirectLoadMethod::is_incremental(store_ctx_->ctx_->param_.method_);
    insert_table_param.trans_param_ = trans_param;
    insert_table_param.datum_utils_ = &(schema_->datum_utils_);
    insert_table_param.col_descs_ = &(schema_->column_descs_);
    insert_table_param.cmp_funcs_ = &(schema_->cmp_funcs_);
    insert_table_param.lob_column_idxs_ = &(schema_->lob_column_idxs_);
    insert_table_param.online_sample_percent_ = store_ctx_->ctx_->param_.online_sample_percent_;
    insert_table_param.is_no_logging_ = store_ctx_->ctx_->ddl_param_.is_no_logging_;
    insert_table_param.max_batch_size_ = store_ctx_->ctx_->param_.batch_size_;
    // new ObDirectLoadInsertDataTableContext
    ObDirectLoadInsertDataTableContext *insert_data_table_ctx = nullptr;
    if (OB_ISNULL(insert_table_ctx_ = insert_data_table_ctx =
                    OB_NEWx(ObDirectLoadInsertDataTableContext, (&allocator_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDirectLoadInsertDataTableContext", KR(ret));
    } else if (OB_FAIL(insert_data_table_ctx->init(insert_table_param, ls_partition_ids_,
                                                   target_ls_partition_ids_))) {
      LOG_WARN("fail to init insert table ctx", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadStoreIndexTableCtx::close_insert_table_ctx()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadStoreIndexTableCtx not init", KR(ret), KP(this));
  } else if (OB_ISNULL(insert_table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected insert table ctx is null", KR(ret));
  } else {
    insert_table_ctx_->~ObDirectLoadInsertTableContext();
    allocator_.free(insert_table_ctx_);
    insert_table_ctx_ = nullptr;
  }
  return ret;
}

//////////////////////// table builder ////////////////////////

ObDirectLoadTableDataDesc ObTableLoadStoreIndexTableCtx::get_insert_table_data_desc()
{
  ObDirectLoadTableDataDesc table_data_desc = store_ctx_->basic_table_data_desc_;
  table_data_desc.rowkey_column_num_ = schema_->rowkey_column_count_;
  table_data_desc.column_count_ = schema_->store_column_count_;
  return table_data_desc;
}

ObDirectLoadTableDataDesc ObTableLoadStoreIndexTableCtx::get_delete_table_data_desc()
{
  ObDirectLoadTableDataDesc table_data_desc = store_ctx_->basic_table_data_desc_;
  table_data_desc.rowkey_column_num_ = schema_->rowkey_column_count_;
  table_data_desc.column_count_ = schema_->store_column_count_;
  return table_data_desc;
}

int ObTableLoadStoreIndexTableCtx::acquire_table_builder(
  ObTableLoadIndexTableBuilder *&table_builder,
  ObIAllocator &allocator, ObDirectLoadTableDataDesc table_data_desc)
{
  int ret = OB_SUCCESS;
  table_builder = nullptr;
  ObTableLoadIndexTableBuildParam builder_param;
  builder_param.rowkey_column_count_ = schema_->rowkey_column_count_;
  builder_param.datum_utils_ = &schema_->datum_utils_;
  builder_param.table_data_desc_ = table_data_desc;
  builder_param.project_ = project_;
  builder_param.file_mgr_ = store_ctx_->tmp_file_mgr_;
  if (OB_ISNULL(table_builder = OB_NEWx(ObTableLoadIndexTableBuilder, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadIndexTableBuilder", KR(ret));
  } else if (OB_FAIL(table_builder->init(builder_param))) {
    LOG_WARN("fail to init index table builder", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_builder) {
      table_builder->~ObTableLoadIndexTableBuilder();
      allocator.free(table_builder);
      table_builder = nullptr;
    }
  }
  return ret;
}

DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(ObTableLoadStoreIndexTableCtx,
                                         ObTableLoadIndexTableBuilder, insert, insert_table_store_);
DEFINE_TABLE_LOAD_STORE_TABLE_BUILD_IMPL(ObTableLoadStoreIndexTableCtx,
                                         ObTableLoadIndexTableBuilder, delete, delete_table_store_);

} // namespace observer
} // namespace oceanbase
