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

#define USING_LOG_PREFIX STORAGE
#include "ob_partition_split_task.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_ms_row_iterator.h"
#include "storage/ob_sstable_row_iterator.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_pg_storage.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "observer/ob_inner_sql_connection.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::omt;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace storage {

ObSSTableScheduleSplitParam::ObSSTableScheduleSplitParam()
    : is_major_split_(false), merge_version_(), src_pkey_(), dest_pkey_(), index_id_(OB_INVALID_ID)
{}

bool ObSSTableScheduleSplitParam::is_valid() const
{
  return src_pkey_.is_valid() && dest_pkey_.is_valid() && index_id_ != OB_INVALID_ID &&
         (!is_major_split_ || merge_version_.is_valid());
}

ObSSTableSplitCtx::ObSSTableSplitCtx()
    : param_(),
      report_(NULL),
      tables_handle_(),
      base_sstable_version_(0),
      concurrent_count_(0),
      partition_guard_(),
      checksum_method_(0),
      schema_version_(-1),
      split_schema_version_(-1),
      table_schema_(NULL),
      schema_guard_(),
      split_table_schema_(NULL),
      split_schema_guard_(),
      split_handle_(),
      remain_handle_(),
      split_cnt_(0),
      column_cnt_(0),
      mem_ctx_factory_(NULL),
      is_range_opt_(false)
{}

bool ObSSTableSplitCtx::is_valid() const
{
  return param_.is_valid() && NULL != report_ && !tables_handle_.empty() &&
         ((OB_NOT_NULL(split_handle_.get_table()) && param_.is_major_split_) || !param_.is_major_split_) &&
         split_schema_version_ >= 0 && schema_version_ >= 0 && NULL != table_schema_ && column_cnt_ > 0 &&
         split_cnt_ > 1 && concurrent_count_ > 0;
}

ObSSTableSplitDag::ObSSTableSplitDag()
    : ObIDag(DAG_TYPE_SSTABLE_SPLIT, DAG_PRIO_SSTABLE_SPLIT),
      is_inited_(false),
      compat_mode_(ObWorker::CompatMode::INVALID),
      ctx_()
{}

ObSSTableSplitDag::~ObSSTableSplitDag()
{}

bool ObSSTableSplitDag::operator==(const ObIDag& other) const
{
  bool is_same = true;
  if (this == &other) {
    // same
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObSSTableScheduleSplitParam& param = static_cast<const ObSSTableSplitDag&>(other).ctx_.param_;
    if (param.is_major_split_ != ctx_.param_.is_major_split_ || param.src_pkey_ != ctx_.param_.src_pkey_ ||
        param.index_id_ != ctx_.param_.index_id_) {
      is_same = false;
    }
  }

  return is_same;
}

int64_t ObSSTableSplitDag::hash() const
{
  int64_t hash_value = 0;

  hash_value = common::murmurhash(&ctx_.param_.is_major_split_, sizeof(ctx_.param_.is_major_split_), hash_value);
  hash_value = common::murmurhash(&ctx_.param_.src_pkey_, sizeof(ctx_.param_.src_pkey_), hash_value);
  hash_value = common::murmurhash(&ctx_.param_.index_id_, sizeof(ctx_.param_.index_id_), hash_value);
  return hash_value;
}

int ObSSTableSplitDag::init(const ObSSTableScheduleSplitParam& param, memtable::ObIMemtableCtxFactory* mem_ctx_factory,
    ObIPartitionReport* report)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(mem_ctx_factory)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(mem_ctx_factory));
  } else if (OB_ISNULL(report)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(report));
  } else if (OB_FAIL(get_compat_mode_with_table_id(param.dest_pkey_.table_id_, compat_mode_))) {
    STORAGE_LOG(WARN, "failed to get compat mode", K(ret), K(param));
  } else {
    ctx_.param_ = param;
    ctx_.mem_ctx_factory_ = mem_ctx_factory;
    ctx_.report_ = report;
    is_inited_ = true;
  }
  return ret;
}

int ObSSTableSplitDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "split sstable task: dest_key=%s src_key=%s",
                 to_cstring(ctx_.param_.dest_pkey_),
                 to_cstring(ctx_.param_.src_pkey_)))) {
    STORAGE_LOG(WARN, "failed to fill comment", K(ret), K(ctx_));
  }
  return ret;
}

ObSSTableSplitPrepareTask::ObSSTableSplitPrepareTask()
    : ObITask(ObITaskType::TASK_TYPE_SPLIT_PREPARE_TASK), split_dag_(NULL), is_inited_(false)
{}

int ObSSTableSplitPrepareTask::init()
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "dag must not null", K(ret));
  } else {
    split_dag_ = static_cast<ObSSTableSplitDag*>(dag_);
    if (OB_UNLIKELY(!split_dag_->get_ctx().param_.is_valid())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "ctx.param_ not valid", K(ret), K(split_dag_->get_ctx().param_));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObSSTableSplitPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObPGPartitionGuard pg_partition_guard;
  ObPartitionStorage* storage = NULL;
  ObSSTableSplitCtx* ctx = NULL;
  bool need_split = false;
  bool need_minor_split = false;
  bool has_lob_column = false;
  common::ObVersion split_version;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "ctx must not null", K(ret));
  } else if (OB_UNLIKELY(!ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    STORAGE_LOG(INFO, "Merge has been paused", K(ret), K(*ctx));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(ctx->param_.dest_pkey_, ctx->partition_guard_))) {
  } else if (OB_ISNULL(partition = ctx->partition_guard_.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The partition must not null", K(ret), K(*ctx));
  } else if (OB_FAIL(partition->get_pg_storage().get_pg_partition(ctx->param_.dest_pkey_, pg_partition_guard))) {
    STORAGE_LOG(WARN, "get pg partition error", K(ret), "pkey", ctx->param_.dest_pkey_);
  } else if (OB_ISNULL(pg_partition_guard.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "get pg partition error", K(ret), "pkey", ctx->param_.dest_pkey_);
  } else if (OB_ISNULL(
                 storage = static_cast<ObPartitionStorage*>(pg_partition_guard.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The partition storage must not NULL", K(ret), K(*ctx));
  } else if (OB_FAIL(storage->get_partition_store().check_need_split(
                 ctx->param_.index_id_, split_version, need_split, need_minor_split))) {
    STORAGE_LOG(WARN, "Failed to get split table ids", K(ret), K(*ctx));
  } else if (!need_split) {
    // no need to split, get_split_tables and adding to dag is not atomic
    ret = OB_EAGAIN;
    STORAGE_LOG(INFO, "no need to split", K(need_split), K(need_minor_split), K(*ctx));
  } else if (OB_FAIL(build_split_ctx(*storage, *ctx))) {
    STORAGE_LOG(WARN, "Failed to build split ctx", K(ret), K(*ctx));
  } else if (OB_ISNULL(ctx->table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The table schema must not null", K(ret), K(*ctx));
  } else if (OB_FAIL(ctx->table_schema_->has_lob_column(has_lob_column, true))) {
    STORAGE_LOG(WARN, "Failed to check lob column in table schema", K(ret));
  } else if (OB_FAIL(ctx->table_schema_->get_store_column_count(ctx->column_cnt_))) {
    STORAGE_LOG(WARN, "failed to get store column count", K(ret));
  } else if (OB_FAIL(generate_split_task(has_lob_column))) {
    STORAGE_LOG(WARN, "failed to generate split task", K(ret), K(*ctx));
  }
  return ret;
}

int ObSSTableSplitPrepareTask::generate_split_task(const bool has_lob_column)
{
  int ret = OB_SUCCESS;
  ObSSTableSplitTask* split_task = NULL;
  ObSSTableSplitCtx* ctx = NULL;
  int64_t concurrent_index = 0;
  storage::ObIPartitionGroupGuard dest_pg_guard;

  if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "ctx must not null", K(ret));
  } else if (OB_FAIL(split_dag_->alloc_task(split_task))) {
    STORAGE_LOG(WARN, "Fail to allocator memory, ", K(ret), K(*ctx));
  } else if (OB_FAIL(split_task->init(concurrent_index, has_lob_column))) {
    STORAGE_LOG(WARN, "Fail to init split task", K(ret), K(*ctx));
  } else {
    const ObPartitionSplitInfo& split_info = ctx->partition_guard_.get_partition_group()->get_split_info();
    if (ctx->split_cnt_ <= 0) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "split cnt should be greater than 0", K(ret), K(*ctx));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ctx->split_cnt_; i++) {
        const common::ObPartitionKey& dest_pgkey = split_info.get_dest_partitions().at(i);
        ObSSTableSplitFinishTask* split_finish_task = NULL;
        common::ObPartitionKey dest_pkey;

        if (OB_FAIL(ObPartitionService::get_instance().get_partition(dest_pgkey, dest_pg_guard))) {
          if (OB_PARTITION_NOT_EXIST != ret) {
            STORAGE_LOG(WARN, "Fail to get partition", K(ret), K(*ctx), K(dest_pgkey));
          } else {
            // the partiton may have been miagrated out
            // generate a empty task with invalid pkey
            ret = OB_SUCCESS;
            dest_pkey = dest_pgkey;
          }
        } else if (OB_ISNULL(dest_pg_guard.get_partition_group())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "The partition must not null", K(ret), K(*ctx));
        } else if (OB_FAIL(dest_pg_guard.get_partition_group()->get_pg_storage().get_pkey_for_table(
                       ctx->param_.dest_pkey_.get_table_id(), dest_pkey))) {
          STORAGE_LOG(WARN, "get dest pkey failed", K(ret), "table id", ctx->param_.src_pkey_.get_table_id());
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(split_dag_->alloc_task(split_finish_task))) {
            STORAGE_LOG(WARN, "Fail to alloc task", K(ret));
          } else if (OB_FAIL(split_finish_task->init(dest_pkey, has_lob_column))) {
            STORAGE_LOG(WARN, "Fail to init split table finish task, ", K(ret));
          } else if (OB_FAIL(split_task->add_child(*split_finish_task))) {
            STORAGE_LOG(WARN, "Fail to add child, ", K(ret));
          } else if (OB_FAIL(split_dag_->add_task(*split_finish_task))) {
            STORAGE_LOG(WARN, "Fail to add task, ", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(split_dag_->add_task(*split_task))) {
          STORAGE_LOG(WARN, "Fail to add task, ", K(ret), K(*ctx));
        }
      }
    }
  }

  return ret;
}

int ObSSTableSplitPrepareTask::get_schemas_to_split(storage::ObSSTableSplitCtx& ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t index_id = ctx.param_.index_id_;
  int64_t& schema_version = ctx.schema_version_;
  ObSchemaGetterGuard& schema_guard = ctx.schema_guard_;
  const ObTableSchema*& table_schema = ctx.table_schema_;

  int64_t& split_schema_version = ctx.split_schema_version_;
  ObSchemaGetterGuard& split_schema_guard = ctx.split_schema_guard_;
  const ObTableSchema*& split_table_schema = ctx.split_table_schema_;

  ObITable* table = NULL;

  const uint64_t tenant_id = is_inner_table(index_id) ? OB_SYS_TENANT_ID : extract_tenant_id(index_id);
  const bool check_formal = extract_pure_id(index_id) > OB_MAX_CORE_TABLE_ID;  // Avoid cyclic dependencies

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition storage is not initialized", K(ret));
  } else if (!is_valid_id(index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(index_id));
  } else if (ctx.tables_handle_.get_tables().count() < 1) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "table count must not zero", K(ret), K(ctx));
  } else if (ctx.param_.is_major_split_) {
    if (OB_ISNULL(table = ctx.tables_handle_.get_table(0))) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "failed to get first table", K(ret), K(ctx));
    }
  } else if (!ctx.param_.is_major_split_) {
    if (OB_ISNULL(table = ctx.tables_handle_.get_last_table())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "failed to get last table", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ctx.partition_guard_.get_partition_group())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "partition should not be NULL", K(ret));
    } else {
      split_schema_version = ctx.partition_guard_.get_partition_group()->get_split_info().get_schema_version();
      if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id, split_schema_guard, split_schema_version, OB_INVALID_VERSION /*sys_schema_version*/))) {
        STORAGE_LOG(WARN, "Fail to get schema mgr by version, ", K(ret), K(ctx));
      } else if (check_formal && OB_FAIL(split_schema_guard.check_formal_guard())) {
        STORAGE_LOG(WARN, "schema_guard is not formal", K(ret), K(tenant_id));
      } else if (OB_FAIL(split_schema_guard.get_table_schema(ctx.param_.index_id_, split_table_schema))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TABLE_IS_DELETED;
          STORAGE_LOG(INFO, "table is deleted", K(index_id), K(schema_version));
        } else {
          STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table->get_frozen_schema_version(schema_version))) {
      STORAGE_LOG(WARN, "failed to get frozen schema version, ", K(ret));
    } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                   tenant_id, schema_guard, schema_version, OB_INVALID_VERSION /*sys_schema_version*/))) {
      STORAGE_LOG(WARN, "Fail to get schema, ", K(schema_version), K(ret));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      STORAGE_LOG(WARN, "schema_guard is not formal", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(index_id, table_schema))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLE_IS_DELETED;
        STORAGE_LOG(INFO, "table is deleted", K(index_id), K(schema_version));
      } else {
        STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(index_id));
      }
    } else if (NULL == table_schema) {
      ret = OB_SCHEMA_ERROR;
      STORAGE_LOG(WARN, "table_schema must not null", K(ret), K(ctx), K(index_id), K(schema_version));
    } else {
      STORAGE_LOG(INFO, "table_schema is, ", K(*table_schema));
    }
  }

  return ret;
}

int ObSSTableSplitPrepareTask::cal_split_param(storage::ObSSTableSplitCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObSSTable* base_sstable = NULL;
  ctx.checksum_method_ = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition split prepare task is not initialized", K(ret));
  } else if (ctx.param_.is_major_split_) {
    if (ctx.tables_handle_.get_tables().count() < 1) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "table count must not zero", K(ret), K(ctx));
    } else if (OB_FAIL(ctx.tables_handle_.get_first_sstable(base_sstable))) {
      STORAGE_LOG(WARN, "failed to get first sstable", K(ret), K(ctx));
    } else if (OB_ISNULL(base_sstable)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "sstable must not null", K(ret));
    } else {
      const ObSSTableMeta& meta = base_sstable->get_meta();
      ctx.checksum_method_ = meta.checksum_method_;
    }
  }

  return ret;
}

int ObSSTableSplitPrepareTask::build_split_sstable_handle(storage::ObSSTableSplitCtx& split_ctx)
{
  int ret = OB_SUCCESS;
  ObSSTable* sstable = nullptr;
  if (OB_FAIL(split_ctx.tables_handle_.get_first_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "sstable should not be NULL", K(ret));
  } else {
    if (OB_FAIL(split_ctx.split_handle_.set_table(sstable))) {
      STORAGE_LOG(WARN, "failed to set table", K(ret), K(split_ctx));
    }
  }
  return ret;
}

int ObSSTableSplitPrepareTask::build_split_ctx(ObPartitionStorage& storage, storage::ObSSTableSplitCtx& ctx)
{
  int ret = OB_SUCCESS;
  bool is_range_opt = false;
  bool has_lob_column = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSTableSplitPrepareTask has not been inited", K(ret), K(ctx));
  } else if (!ctx.param_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx));
  } else if (OB_FAIL(storage.get_partition_store().get_split_tables(
                 ctx.param_.is_major_split_, ctx.param_.index_id_, ctx.tables_handle_))) {
    LOG_WARN("failed to get split tables", K(ret), K(ctx));
  } else if (OB_FAIL(get_schemas_to_split(ctx))) {
    STORAGE_LOG(WARN, "failed to get schemas to split, ", K(ret), K(ctx));
  } else if (OB_FAIL(ctx.table_schema_->has_lob_column(has_lob_column, true))) {
    STORAGE_LOG(WARN, "Failed to check lob column in table schema", K(ret));
  } else if (OB_FAIL(cal_split_param(ctx))) {
    STORAGE_LOG(WARN, "failed to cal_split_param", K(ret), K(ctx));
  } else if (OB_FAIL(ctx.split_table_schema_->is_partition_key_match_rowkey_prefix(is_range_opt))) {
    STORAGE_LOG(WARN, "Fail to check rowkey prefix", K(ret), K(ctx));
  } else if (is_range_opt && !has_lob_column) {
    // we cannot parallelize the split task for the partition of which rowkey contains partition key
    ctx.concurrent_count_ = 1;
  } else if (ctx.param_.is_major_split_ && OB_FAIL(build_split_sstable_handle(ctx))) {
    STORAGE_LOG(WARN, "failed to split sstable", K(ret), K(ctx));
  } else {
    ObTablesHandle tables_handle;
    const ObMergeType merge_type = ctx.param_.is_major_split_ ? MAJOR_MERGE : MINOR_MERGE;
    if (ctx.param_.is_major_split_ && OB_FAIL(tables_handle.add_table(ctx.split_handle_.get_table()))) {
      STORAGE_LOG(WARN, "fail to add table", K(ret));
    } else if (OB_FAIL(
                   storage.get_concurrent_cnt(tables_handle, *ctx.table_schema_, merge_type, ctx.concurrent_count_))) {
      LOG_WARN("failed to get_concurrent_cnt", K(ret), K(ctx));
    }
  }
  if (OB_SUCC(ret)) {
    ctx.is_range_opt_ = (is_range_opt && !has_lob_column);
    const ObPartitionSplitInfo& split_info = ctx.partition_guard_.get_partition_group()->get_split_info();
    ctx.split_cnt_ = split_info.get_dest_partitions().count();
    LOG_INFO("succeed to build split ctx", K(ctx));
  }
  return ret;
}

ObLobSplitHelper::ObLobSplitHelper()
    : lob_writer_(),
      lob_reader_(),
      lob_block_id_map_(),
      allocator_(ObModIds::OB_SSTABLE_LOB_BLOCK_ID_MAP),
      status_(STATUS_INVALID)
{}

ObLobSplitHelper::~ObLobSplitHelper()
{
  reset();
}

void ObLobSplitHelper::reset()
{
  lob_writer_.reset();
  lob_reader_.reset();
  lob_block_id_map_.destroy();
  allocator_.reset();
  status_ = STATUS_INVALID;
}

int ObLobSplitHelper::init(const ObDataStoreDesc& data_store_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobSplitHelper init twice", K(ret));
  } else if (OB_FAIL(lob_writer_.init(0, data_store_desc, NULL))) {
    STORAGE_LOG(WARN, "Failed to init lob merge writer", K(data_store_desc), K(ret));
  } else {
    status_ = STATUS_WRITE;
  }

  return ret;
}

int ObLobSplitHelper::switch_for_read()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobSplitHelper not inited", K_(status), K(ret));
  } else if (status_ != STATUS_WRITE) {
    STORAGE_LOG(WARN, "ObLobSplitHelper switch for read twice", K_(status));
  } else if (OB_FAIL(build_lob_block_id_map())) {
    STORAGE_LOG(WARN, "Failed to build lob block id map", K(ret));
  } else if (status_ == STATUS_EMPTY) {
    // TODO(): add a tempory sstable
    //} else if (OB_FAIL(lob_reader_.init(true, lob_block_id_map_))) {
    //  STORAGE_LOG(WARN, "Failed to init lob reader", K(ret));
  } else {
    status_ = STATUS_READ;
  }
  return ret;
}

int ObLobSplitHelper::overflow_lob_objs(const ObStoreRow* row, const ObStoreRow*& result_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(status_ != STATUS_WRITE)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobSplitHelper not inited", K_(status), K(ret));
  } else if (OB_FAIL(lob_writer_.overflow_lob_objs(*row, result_row))) {
    STORAGE_LOG(WARN, "Failed to overflow lob objs in store row", K(ret));
  }

  return ret;
}

int ObLobSplitHelper::read_lob_columns(ObStoreRow* store_row)
{
  int ret = OB_SUCCESS;

  if (status_ == STATUS_EMPTY) {
    // skip empty helper
  } else if (status_ != STATUS_READ) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobSplitHelper not ready for read", K_(status), K(ret));
  } else if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid row to read lob columns", K(ret));
  } else {
    lob_reader_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < store_row->row_val_.count_; ++i) {
      ObObj& obj = store_row->row_val_.cells_[i];
      if (ob_is_text_tc(obj.get_type())) {
        if (obj.is_lob_outrow()) {
          if (OB_FAIL(lob_reader_.read_lob_data(obj, obj))) {
            STORAGE_LOG(WARN, "Failed to read lob obj", K(obj.get_scale()), K(obj.get_meta()), K(obj.val_len_), K(ret));
          } else {
            STORAGE_LOG(
                DEBUG, "[LOB] Succ to load lob obj", K(obj.get_scale()), K(obj.get_meta()), K(obj.val_len_), K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObLobSplitHelper::build_lob_block_id_map()
{
  int ret = OB_SUCCESS;

  if (lob_writer_.get_macro_block_write_ctx().get_macro_block_count() == 0) {
    status_ = STATUS_EMPTY;
  } else if (OB_FAIL(lob_block_id_map_.create(
                 lob_writer_.get_macro_block_write_ctx().get_macro_block_count(), &allocator_))) {
    STORAGE_LOG(WARN, "Failed to create lob_block_id_map", K(ret));
  } else {
    status_ = STATUS_READ;
  }

  return ret;
}

ObSSTableSplitTask::ObSSTableSplitTask()
    : ObITask(ObITaskType::TASK_TYPE_SPLIT_TASK),
      split_dag_(),
      allocator_(ObModIds::OB_PARTITION_SPLIT),
      is_major_split_(false),
      split_cnt_(0),
      param_(),
      context_(),
      store_ctx_(),
      range_(),
      block_cache_ws_(),
      row_with_part_id_(NULL),
      row_without_part_id_(NULL),
      partition_index_(),
      writer_(),
      has_lob_column_(false),
      desc_(),
      desc_generator_(),
      multi_version_row_info_(NULL),
      idx_(0),
      macro_block_iter_(),
      exec_ctx_(),
      session_(),
      table_location_(),
      link_schema_guard_(),
      is_inited_(false)
{}

ObSSTableSplitTask::~ObSSTableSplitTask()
{
  multi_version_row_info_ = NULL;
  if (nullptr != split_dag_ && nullptr != store_ctx_.mem_ctx_) {
    ObSSTableSplitCtx& ctx = split_dag_->get_ctx();
    if (nullptr != ctx.mem_ctx_factory_) {
      ctx.mem_ctx_factory_->free(store_ctx_.mem_ctx_);
      store_ctx_.mem_ctx_ = nullptr;
    }
  }
}

int ObSSTableSplitTask::init(const int64_t idx, const bool has_lob_column)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "dag must not null", K(ret));
  } else {
    split_dag_ = static_cast<ObSSTableSplitDag*>(dag_);
    idx_ = idx;
    has_lob_column_ = has_lob_column;
    is_major_split_ = split_dag_->get_ctx().param_.is_major_split_;
    is_inited_ = true;
  }

  return ret;
}

int ObSSTableSplitTask::generate_next_task(ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  ObSSTableSplitTask* split_task = NULL;
  next_task = NULL;
  ObSSTableSplitCtx* ctx = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init, ", K(ret));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "ctx must not null", K(ret));
  } else if (idx_ == (ctx->concurrent_count_ - 1)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(split_dag_->alloc_task(split_task))) {
    STORAGE_LOG(WARN, "Fail to allocator memory, ", K(ret), K(*ctx));
  } else if (OB_FAIL(split_task->init(idx_ + 1, has_lob_column_))) {
    STORAGE_LOG(WARN, "Fail to init split task", K(ret), K(*ctx));
  } else {
    next_task = split_task;
  }

  return ret;
}

int ObSSTableSplitTask::process()
{
  int ret = OB_SUCCESS;
  ObSSTableSplitCtx* ctx = NULL;
  ObIStoreRowIterator* row_iter = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSSTableSplitTask not init", K(ret));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "ctx must not null", K(ret));
  } else if (OB_UNLIKELY(!ObPartitionScheduler::get_instance().could_merge_start())) {
    ret = OB_CANCELED;
    STORAGE_LOG(INFO, "Merge has been paused", K(ret), K(*ctx));
  } else if (OB_FAIL(init_table_scan_param(*ctx))) {
    STORAGE_LOG(WARN, "fail to init table scan param", K(ret), K(*ctx));
  } else if (OB_FAIL(init_table_split_param(*ctx))) {
    STORAGE_LOG(WARN, "Fail to init table split param", K(ret));
  } else if (ctx->is_range_opt_) {
    if (OB_FAIL(split_range_by_range(*ctx))) {
      STORAGE_LOG(WARN, "failed to split range by range", K(ret), K(*ctx));
    }
  } else {
    if (OB_FAIL(split_row_by_row(*ctx, row_iter))) {
      STORAGE_LOG(WARN, "failed to split row by row", K(ret), K(*ctx));
    }
  }

  if (NULL != row_iter) {
    row_iter->~ObIStoreRowIterator();
  }
  return ret;
}

int ObSSTableSplitTask::split_range_by_range(ObSSTableSplitCtx& split_ctx)
{
  int ret = OB_SUCCESS;
  ObTablesHandle& handle = split_ctx.tables_handle_;
  ObSEArray<ObSSTable*, 2> sstables;
  ObSSTable* sstable = NULL;

  if (OB_FAIL(handle.get_all_sstables(sstables))) {
    STORAGE_LOG(WARN, "fail to get all sstables", K(ret), K(split_ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstables.count(); i++) {
      idx_ = i;
      sstable = sstables.at(i);
      if (OB_FAIL(sstable->scan_macro_block(macro_block_iter_))) {
        STORAGE_LOG(WARN, "Fail to get macro_block_iter", K(ret), K(i), K(split_ctx));
      } else if (OB_FAIL(split_sstable(split_ctx, sstable, macro_block_iter_))) {
        STORAGE_LOG(WARN, "fail to split sstable", K(ret), K(i), K(split_ctx));
      }
    }
  }
  return ret;
}

int ObSSTableSplitTask::split_row_by_row(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(build_row_iterator(split_ctx, row_iter))) {
    STORAGE_LOG(WARN, "Fail to build row iterator, ", K(ret));
  } else if (OB_FAIL(split_sstable(split_ctx, row_iter))) {
    STORAGE_LOG(WARN, "Fail to split sstable, ", K(ret));
  }
  return ret;
}

int ObSSTableSplitTask::build_row_iterator(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator*& row_iter)
{
  int ret = OB_SUCCESS;
  if (split_ctx.param_.is_major_split_) {
    ObStoreRowIterator* iter = NULL;
    ObSSTable* sstable = NULL;
    if (OB_FAIL(split_ctx.split_handle_.get_sstable(sstable))) {
      STORAGE_LOG(WARN, "failed to get sstable", K(ret), K(split_ctx));
    } else if (OB_ISNULL(sstable)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "sstable should not be NULL", K(ret));
    } else if (OB_FAIL(sstable->get_range(idx_, split_ctx.concurrent_count_, allocator_, range_))) {
      STORAGE_LOG(WARN, "failed to get range", K(ret), K(split_ctx));
    } else if (OB_FAIL(
                   sstable->scan(param_.iter_param_, context_, *const_cast<common::ObExtStoreRange*>(&range_), iter))) {
      STORAGE_LOG(WARN, "failed to scan sstable", K(ret), K(split_ctx));
    } else {
      row_iter = iter;
    }
  } else {
    void* buf = NULL;
    ObMSRowIterator* iter = NULL;
    ObVersionRange version_range;
    range_.get_range().set_whole_range();
    version_range.base_version_ = 0;
    version_range.multi_version_start_ = common::ObVersionRange::MAX_VERSION - 2;
    version_range.snapshot_version_ = common::ObVersionRange::MAX_VERSION - 2;

    if (split_ctx.tables_handle_.get_count() <= 0) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "count of tables should be greater than 0", K(ret));
    } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMSRowIterator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to allocate memory", K(sizeof(ObMSRowIterator)));
    } else if (OB_FAIL(range_.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
      STORAGE_LOG(WARN, "Failed to transform range to collation free and range cutoff", K_(range), K(ret));
    } else {
      iter = new (buf) ObMSRowIterator();
      if (OB_FAIL(iter->init(split_ctx.tables_handle_,
              *split_ctx.table_schema_,
              range_,
              version_range,
              split_ctx.mem_ctx_factory_,
              split_ctx.param_.src_pkey_))) {
        STORAGE_LOG(WARN, "failed to init minor split iterator", K(ret));
      } else {
        row_iter = iter;
      }
    }
  }

  return ret;
}

int ObSSTableSplitTask::overflow_if_needed(
    const ObStoreRow* base_row, ObLobSplitHelper* lob_helper, const ObStoreRow*& target_row)
{
  int ret = OB_SUCCESS;
  if (has_lob_column_) {
    if (OB_ISNULL(lob_helper)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpecte null lob split helper", K(ret), KP(lob_helper));
    } else if (OB_FAIL(lob_helper->overflow_lob_objs(base_row, target_row))) {
      STORAGE_LOG(WARN, "Failed to overflow lob objs", K(ret));
    }
  } else {
    target_row = base_row;
  }
  return ret;
}

int ObSSTableSplitTask::split_sstable(
    ObSSTableSplitCtx& split_ctx, ObSSTable* sstable, storage::ObMacroBlockIterator& macro_block_iter)
{
  int ret = OB_SUCCESS;
  ObTablesHandle handle;
  const ObStoreRow* base_row = NULL;
  int64_t row_partition_id = 0;
  int64_t partition_id = -1;
  ObLobSplitHelper* lob_helper = NULL;
  void* buf = nullptr;

  if (OB_FAIL(handle.add_table(sstable))) {
    STORAGE_LOG(WARN, "fail to add sstable", K(ret));
  } else if (has_lob_column_) {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobSplitHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for lob split helper", K(ret));
    } else if (FALSE_IT(lob_helper = new (buf) ObLobSplitHelper())) {

    } else if (OB_FAIL(lob_helper->init(desc_))) {
      STORAGE_LOG(WARN, "Failed to init lob helper for split", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    storage::ObMacroBlockDesc curr_block_desc;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_block_iter.get_next_macro_block(curr_block_desc))) {
        if (OB_LIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("Fail to get next macro block, ", K(ret));
        }
      } else {
        bool all_part = false;
        common::ObExtStoreRange macro_range;
        ObNewRange range;
        ObSEArray<int64_t, 2> partition_ids;

        curr_block_desc.range_.to_new_range(range);
        macro_range.get_range() = curr_block_desc.range_;

        if (OB_FAIL(table_location_.calc_partition_ids_by_range(
                exec_ctx_, &split_ctx.split_schema_guard_, &range, partition_ids, all_part, NULL, NULL))) {
          STORAGE_LOG(WARN, "failed to calculate partition id by row", K(ret), K(*base_row));
        } else if (0 == partition_ids.count() && !all_part) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "unexpected error", K(ret), K(range));
        } else if (1 == partition_ids.count()) {
          const int64_t range_partition_id = partition_ids.at(0);
          if (range_partition_id != partition_id) {
            if (-1 != partition_id) {
              if (OB_FAIL(fill_macro_blocks(partition_id))) {
                STORAGE_LOG(WARN, "Fail to fill macro blocks, ", K(ret));
              } else {
                STORAGE_LOG(INFO, "Success to fill macro block, ", K(partition_id));
              }
            }

            if (OB_SUCC(ret)) {
              ObMacroDataSeq macro_start_seq(0);
              desc_.partition_id_ = range_partition_id;
              macro_start_seq.set_split_status();
              if (OB_FAIL(macro_start_seq.set_parallel_degree(0))) {
                STORAGE_LOG(WARN, "Failed to set parallel degree", K_(idx), K(ret));
              } else if (OB_FAIL(writer_.open(desc_, macro_start_seq))) {
                STORAGE_LOG(WARN, "Fail to open macro block writer, ", K(ret));
              } else {
                partition_id = range_partition_id;
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(writer_.append_macro_block(curr_block_desc.macro_block_ctx_))) {
              STORAGE_LOG(WARN, "failed to append macro block", K(ret));
            }
          }
        } else {
          void* buf = NULL;
          ObIStoreRowIterator* row_iter = NULL;

          if (OB_FAIL(macro_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_))) {
            STORAGE_LOG(WARN, "fail to get collation free range", K(ret));
          } else {

            ObVersionRange version_range;
            version_range.base_version_ = 0;
            version_range.multi_version_start_ = common::ObVersionRange::MAX_VERSION - 2;
            version_range.snapshot_version_ = common::ObVersionRange::MAX_VERSION - 2;

            if (is_major_split_) {
              ObStoreRowIterator* iter = NULL;
              if (OB_FAIL(sstable->scan(param_.iter_param_, context_, macro_range, iter))) {
                STORAGE_LOG(WARN, "failed to scan sstable", K(ret), K(split_ctx));
              } else {
                row_iter = iter;
              }
            } else {
              if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMSRowIterator)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                STORAGE_LOG(WARN, "failed to allocate memory", K(sizeof(ObMSRowIterator)));
              } else {
                ObMSRowIterator* ms_iter = NULL;
                ms_iter = new (buf) ObMSRowIterator();
                if (OB_FAIL(ms_iter->init(handle,
                        *split_ctx.table_schema_,
                        macro_range,
                        version_range,
                        split_ctx.mem_ctx_factory_,
                        split_ctx.param_.src_pkey_))) {
                  STORAGE_LOG(WARN, "failed to init minor split iterator", K(ret));
                } else {
                  row_iter = ms_iter;
                }
              }
            }

            while (OB_SUCC(ret)) {
              const ObStoreRow* target_row = nullptr;
              if (OB_FAIL(row_iter->get_next_row(base_row))) {
                if (OB_ITER_END != ret) {
                  STORAGE_LOG(WARN, "Fail to get next row, ", K(ret));
                } else {
                  ret = OB_SUCCESS;
                  break;
                }
              } else if (OB_FAIL(overflow_if_needed(base_row, lob_helper, target_row))) {
                STORAGE_LOG(WARN, "failed to overflow base row", K(ret));
              } else if (OB_FAIL(table_location_.calculate_partition_id_by_row(
                             exec_ctx_, &split_ctx.split_schema_guard_, target_row->row_val_, row_partition_id))) {
                STORAGE_LOG(WARN, "failed to calculate partition id by row", K(ret), K(*target_row));
              } else if (row_partition_id != partition_id) {
                if (-1 != partition_id) {
                  if (OB_FAIL(fill_macro_blocks(partition_id))) {
                    STORAGE_LOG(WARN, "Fail to fill macro blocks, ", K(ret));
                  } else {
                    STORAGE_LOG(INFO, "Success to fill macro block, ", K(partition_id));
                  }
                }

                if (OB_SUCC(ret)) {
                  ObMacroDataSeq macro_start_seq(0);
                  desc_.partition_id_ = row_partition_id;
                  macro_start_seq.set_split_status();
                  if (OB_FAIL(macro_start_seq.set_parallel_degree(0))) {
                    STORAGE_LOG(WARN, "Failed to set parallel degree", K_(idx), K(ret));
                  } else if (OB_FAIL(writer_.open(desc_, macro_start_seq))) {
                    STORAGE_LOG(WARN, "Fail to open macro block writer, ", K(ret));
                  } else {
                    partition_id = row_partition_id;
                  }
                }
              }

              if (OB_SUCC(ret)) {
                if (OB_FAIL(writer_.append_row(*target_row))) {
                  STORAGE_LOG(WARN, "failed to append row", K(ret), K(*target_row));
                }
              }
            }

            if (NULL != row_iter) {
              row_iter->~ObIStoreRowIterator();
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(terminate_split_iteration(partition_id))) {
        STORAGE_LOG(WARN, "failed to terminate split iteration", K(ret), K(partition_id));
      } else {
        const ObPartitionSplitInfo& split_info = split_ctx.partition_guard_.get_partition_group()->get_split_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < split_ctx.split_cnt_; ++i) {
          if (OB_FAIL(partition_index_.set_refactored(split_info.get_dest_partitions().at(i).get_partition_id(), i))) {
            STORAGE_LOG(WARN, "Fail to set partition index, ", K(ret));
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(lob_helper)) {
    lob_helper->~ObLobSplitHelper();
    allocator_.free(lob_helper);
    lob_helper = nullptr;
  }

  return ret;
}

int ObSSTableSplitTask::split_sstable(ObSSTableSplitCtx& split_ctx, ObIStoreRowIterator* row_iter)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* base_row = NULL;
  const ObStoreRow* split_row = NULL;
  int64_t partition_id = 0;
  int64_t row_partition_id = 0;
  ObExternalSort<ObStoreRow, ObSplitRowComparer> sorter;
  ObSplitRowComparer comparer(ret);
  ObLobSplitHelper* lob_helper = NULL;
  void* buf = nullptr;

  if (OB_FAIL(malloc_store_row(allocator_, desc_.row_column_count_ + 1, row_with_part_id_))) {
    STORAGE_LOG(WARN, "Fail to malloc store row with part id, ", K(ret));
  } else if (OB_FAIL(malloc_store_row(allocator_, desc_.row_column_count_, row_without_part_id_))) {
    STORAGE_LOG(WARN, "Fail to malloc store row without part id, ", K(ret));
  } else if (OB_FAIL(sorter.init(DEFAULT_SPLIT_SORT_MEMORY_LIMIT,
                 ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER,
                 0,
                 split_ctx.table_schema_->get_tenant_id(),
                 &comparer))) {
    STORAGE_LOG(WARN, "Fail to init external sort, ", K(ret));
  } else if (has_lob_column_) {
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObLobSplitHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Failed to alloc memory for lob split helper", K(ret));
    } else if (FALSE_IT(lob_helper = new (buf) ObLobSplitHelper())) {

    } else if (OB_FAIL(lob_helper->init(desc_))) {
      STORAGE_LOG(WARN, "Failed to init lob helper for split", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    comparer.column_cnt_ = desc_.rowkey_column_count_ + 1;  // part_id + rowkey

    // iterate rows
    while (OB_SUCC(ret)) {
      const ObStoreRow* target_row = nullptr;
      if (OB_FAIL(row_iter->get_next_row(base_row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next row, ", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(overflow_if_needed(base_row, lob_helper, target_row))) {
        STORAGE_LOG(WARN, "failed to overflow base row", K(ret));
      } else if (OB_FAIL(table_location_.calculate_partition_id_by_row(
                     exec_ctx_, &split_ctx.split_schema_guard_, target_row->row_val_, partition_id))) {
        STORAGE_LOG(WARN, "failed to calculate partition id by row", K(ret), K(*target_row));
      } else if (OB_FAIL(convert_row_with_part_id(target_row, partition_id, row_with_part_id_))) {
        STORAGE_LOG(WARN, "Fail to convert row with part id, ", K(ret), K(partition_id));
      } else if (OB_FAIL(sorter.add_item(*row_with_part_id_))) {
        STORAGE_LOG(WARN, "Fail to add row to sorter, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sorter.do_sort(true))) {
        STORAGE_LOG(WARN, "Fail to sort split rows, ", K(ret));
      } else if (has_lob_column_) {
        if (OB_ISNULL(lob_helper)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpecte null lob split helper", K(ret), KP(lob_helper));
        } else if (OB_FAIL(lob_helper->switch_for_read())) {
          STORAGE_LOG(WARN, "Failed to switch lob helper for read", K(ret));
        }
      }
    }

    // append row to the partitions
    partition_id = -1;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sorter.get_next_item(split_row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next sort row, ", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(split_row->row_val_.cells_[0].get_int(row_partition_id))) {
        STORAGE_LOG(WARN, "Fail to get split row partition id, ", K(ret));
      } else {
        share::dag_yield();
        if (partition_id != row_partition_id) {
          // partition has changed
          if (-1 != partition_id) {
            if (OB_FAIL(fill_macro_blocks(partition_id))) {
              STORAGE_LOG(WARN, "Fail to fill macro blocks, ", K(ret));
            } else {
              STORAGE_LOG(INFO, "Success to fill macro block, ", K(partition_id));
            }
          }

          if (OB_SUCC(ret)) {
            ObMacroDataSeq macro_start_seq(0);
            desc_.partition_id_ = row_partition_id;
            macro_start_seq.set_split_status();
            if (OB_FAIL(macro_start_seq.set_parallel_degree(idx_))) {
              STORAGE_LOG(WARN, "Failed to set parallel degree", K_(idx), K(ret));
            } else if (OB_FAIL(writer_.open(desc_, macro_start_seq))) {
              STORAGE_LOG(WARN, "Fail to open macro block writer, ", K(ret));
            } else {
              partition_id = row_partition_id;
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(convert_row_without_part_id(split_row, row_without_part_id_))) {
            STORAGE_LOG(WARN, "Fail to convert row without part id, ", K(ret), K(row_partition_id));
          } else if (has_lob_column_) {
            if (OB_ISNULL(lob_helper)) {
              ret = OB_ERR_UNEXPECTED;
              STORAGE_LOG(WARN, "Unexpecte null lob split helper", K(ret), KP(lob_helper));
            } else if (OB_FAIL(lob_helper->read_lob_columns(row_without_part_id_))) {
              STORAGE_LOG(WARN, "Failed to read lob columns", K(ret));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(writer_.append_row(*row_without_part_id_))) {
            STORAGE_LOG(WARN, "Fail to append row, ", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(terminate_split_iteration(partition_id))) {
        STORAGE_LOG(WARN, "failed to terminate split iteration", K(ret), K(partition_id));
      }
    }

    if (NULL != row_with_part_id_) {
      free_store_row(allocator_, row_with_part_id_);
    }

    if (NULL != row_without_part_id_) {
      free_store_row(allocator_, row_without_part_id_);
    }
    if (OB_NOT_NULL(lob_helper)) {
      lob_helper->~ObLobSplitHelper();
      allocator_.free(lob_helper);
      lob_helper = nullptr;
    }
    sorter.clean_up();
  }

  return ret;
}

int ObSSTableSplitTask::fill_macro_blocks(const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  int64_t child_idx = 0;

  if (partition_id < 0) {
    // do nothing
  } else if (OB_FAIL(writer_.close())) {
    STORAGE_LOG(WARN, "Fail to close macro block writer, ", K(ret));
  } else if (OB_FAIL(partition_index_.get_refactored(partition_id, child_idx))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "Fail to get child idx, ", K(ret), K(partition_id));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(partition_index_.erase_refactored(partition_id))) {
    STORAGE_LOG(WARN, "Fail to erase partition id, ", K(ret), K(partition_id));
  } else if (OB_FAIL(fill_finish_task(
                 child_idx, writer_.get_macro_block_write_ctx(), writer_.get_lob_macro_block_write_ctx()))) {
    STORAGE_LOG(WARN, "Fail to fill finish task, ", K(child_idx));
  } else {
    STORAGE_LOG(INFO, "Success to fill partition, ", K(partition_id), K(child_idx));
  }
  return ret;
}

int ObSSTableSplitTask::fill_finish_task(const int64_t child_idx, blocksstable::ObMacroBlocksWriteCtx& macro_blocks,
    blocksstable::ObMacroBlocksWriteCtx& lob_macro_blocks)
{
  int ret = OB_SUCCESS;
  ObITask* child_task = NULL;
  ObSSTableSplitFinishTask* split_finish_task = NULL;
  if (OB_FAIL(get_child_tasks().at(child_idx, child_task))) {
    STORAGE_LOG(WARN, "Fail to get child task, ", K(ret), K(child_idx));
  } else if (NULL == (split_finish_task = static_cast<ObSSTableSplitFinishTask*>(child_task))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The split finish task is NULL, ", K(ret), K(child_idx));
  } else if (OB_FAIL(split_finish_task->add_macro_blocks(idx_, macro_blocks, lob_macro_blocks))) {
    STORAGE_LOG(WARN, "Fail to add macro blocks, ", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      STORAGE_LOG(INFO,
          "Success to fill macro block, ",
          K(child_idx),
          "macro_block_count",
          macro_blocks.get_macro_block_count(),
          "lob_macro_blocks",
          lob_macro_blocks.get_macro_block_count());
    }
  }
  return ret;
}

int ObSSTableSplitTask::convert_row_with_part_id(const ObStoreRow* in_row, const int64_t part_id, ObStoreRow* out_row)
{
  int ret = OB_SUCCESS;
  out_row->flag_ = in_row->flag_;
  out_row->dml_ = in_row->dml_;
  out_row->row_type_flag_ = in_row->row_type_flag_;
  out_row->row_val_.cells_[0].set_int(part_id);

  for (int64_t i = 0; i < in_row->row_val_.count_; ++i) {
    out_row->row_val_.cells_[i + 1] = in_row->row_val_.cells_[i];
  }
  return ret;
}

int ObSSTableSplitTask::convert_row_without_part_id(const ObStoreRow* in_row, ObStoreRow* out_row)
{
  int ret = OB_SUCCESS;
  out_row->flag_ = in_row->flag_;
  out_row->dml_ = in_row->dml_;
  out_row->row_type_flag_ = in_row->row_type_flag_;

  for (int64_t i = 0; i < in_row->row_val_.count_; ++i) {
    out_row->row_val_.cells_[i] = in_row->row_val_.cells_[i + 1];
  }
  return ret;
}

int ObSSTableSplitTask::init_table_scan_param(ObSSTableSplitCtx& split_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t INNER_ARRAY_BLOCK_SIZE = 2048;
  ObArray<share::schema::ObColDesc, ObIAllocator&> column_ids(INNER_ARRAY_BLOCK_SIZE, allocator_);

  common::ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = common::ObVersionRange::MAX_VERSION - 2;

  ObQueryFlag query_flag(ObQueryFlag::Forward,
      true,  /*is daily merge scan*/
      true,  /*is read multiple macro block*/
      true,  /*sys task scan, read one macro block in single io*/
      false, /*is full row scan?*/
      false,
      false);

  if (OB_FAIL(split_ctx.table_schema_->get_column_ids(column_ids, true))) {
    STORAGE_LOG(WARN, "invalid argument", K(split_ctx));
  } else if (OB_FAIL(param_.init(split_ctx.table_schema_->get_table_id(),
                 split_ctx.table_schema_->get_schema_version(),
                 split_ctx.table_schema_->get_rowkey_column_num(),
                 column_ids))) {
    STORAGE_LOG(WARN, "invalid argument", K(split_ctx));
  } else if (OB_FAIL(block_cache_ws_.init(split_ctx.table_schema_->get_tenant_id()))) {
    STORAGE_LOG(WARN, "invalid argument", K(split_ctx));
  } else if (OB_ISNULL(split_ctx.mem_ctx_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("memtable ctx factory is null", K(ret));
  } else if (OB_ISNULL(store_ctx_.mem_ctx_ = split_ctx.mem_ctx_factory_->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memtable ctx failed", K(ret));
  } else if (OB_FAIL(store_ctx_.init_trans_ctx_mgr(split_ctx.param_.src_pkey_))) {
    STORAGE_LOG(WARN, "failed to init_trans_ctx_mgr", K(ret), K(split_ctx));
  } else if (OB_FAIL(
                 context_.init(query_flag, store_ctx_, allocator_, allocator_, block_cache_ws_, trans_version_range))) {
    STORAGE_LOG(WARN, "invalid argument", K(split_ctx));
  } else if (!is_major_split_ && OB_FAIL(desc_generator_.init(split_ctx.table_schema_))) {
    STORAGE_LOG(INFO, "Fail to init descriptor generator", K(ret), K(split_ctx));
  } else if (!is_major_split_ && OB_FAIL(desc_generator_.generate_multi_version_row_info(multi_version_row_info_))) {
    STORAGE_LOG(INFO, "Fail to generate multi version row info", K(ret), K(split_ctx));
  }

  return ret;
}

int ObSSTableSplitTask::init_table_split_param(ObSSTableSplitCtx& split_ctx)
{
  int ret = OB_SUCCESS;

  const bool print_info_log = false;
  const bool is_sys_tenant = true;
  link_schema_guard_.set_schema_guard(&split_ctx.split_schema_guard_);
  session_.set_inner_session();
  exec_ctx_.set_my_session(&session_);

  if (OB_FAIL(partition_index_.create(split_ctx.split_cnt_, ObModIds::OB_PARTITION_SPLIT))) {
    STORAGE_LOG(WARN, "Fail to create hash table for partition index, ", K(ret));
  } else if (OB_FAIL(session_.init(observer::ObInnerSQLConnection::INNER_SQL_SESS_VERSION,
                 observer::ObInnerSQLConnection::INNER_SQL_SESS_ID,
                 observer::ObInnerSQLConnection::INNER_SQL_PROXY_SESS_ID,
                 &allocator_))) {
    STORAGE_LOG(WARN, "init session failed", K(ret));
  } else if (OB_FAIL(session_.load_default_sys_variable(print_info_log, is_sys_tenant))) {
    STORAGE_LOG(WARN, "session load default system variable failed", K(ret));
  } else if (OB_FAIL(table_location_.init_table_location_with_rowkey(
                 link_schema_guard_, split_ctx.param_.index_id_, session_))) {
    STORAGE_LOG(WARN, "init table location with rowkey failed", K(ret), K(split_ctx.param_.index_id_));
  } else if (OB_FAIL(desc_.init(*split_ctx.table_schema_,
                 split_ctx.param_.merge_version_,
                 multi_version_row_info_,
                 0,
                 split_ctx.param_.is_major_split_ ? storage::MAJOR_MERGE : storage::MINOR_MERGE,
                 split_ctx.param_.is_major_split_,
                 split_ctx.param_.is_major_split_,
                 split_ctx.param_.dest_pg_key_,
                 split_ctx.partition_guard_.get_partition_group()->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "Fail to init desc", K(ret), KP(split_ctx.table_schema_), K(split_ctx.param_.merge_version_));
  } else {
    desc_.need_prebuild_bloomfilter_ = false;
    const ObPartitionSplitInfo& split_info = split_ctx.partition_guard_.get_partition_group()->get_split_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < split_ctx.split_cnt_; ++i) {
      if (OB_FAIL(partition_index_.set_refactored(split_info.get_dest_partitions().at(i).get_partition_id(), i))) {
        STORAGE_LOG(WARN, "Fail to set partition index, ", K(ret));
      }
    }
  }
  return ret;
}

int ObSSTableSplitTask::terminate_split_iteration(const int64_t partition_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_macro_blocks(partition_id))) {
    STORAGE_LOG(WARN, "Fail to fill macro blocks, ", K(ret));
  } else {
    STORAGE_LOG(INFO, "Success to fill macro block, ", K(partition_id));
    ObSEArray<int64_t, 2> partition_ids;
    hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode>::iterator map_iter;
    ObMacroBlocksWriteCtx write_ctx;
    ObMacroBlocksWriteCtx lob_write_ctx;

    for (map_iter = partition_index_.begin(); OB_SUCC(ret) && map_iter != partition_index_.end(); ++map_iter) {
      write_ctx.reset();
      lob_write_ctx.reset();
      if (!write_ctx.file_handle_.is_valid() && OB_FAIL(write_ctx.file_handle_.assign(desc_.file_handle_))) {
        LOG_WARN("failed to assign file handle", K(ret), K(desc_.file_handle_));
      } else if (OB_FAIL(lob_write_ctx.file_handle_.assign(desc_.file_handle_))) {
        LOG_WARN("failed to assign file handle", K(ret), K(desc_.file_handle_));
      } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, write_ctx.file_ctx_))) {
        LOG_WARN("failed to init write ctx", K(ret));
      } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, lob_write_ctx.file_ctx_))) {
        LOG_WARN("failed to init write ctx", K(ret));
      } else if (OB_FAIL(fill_finish_task(map_iter->second, write_ctx, lob_write_ctx))) {
        STORAGE_LOG(WARN, "Fail to fill finis task, ", K(ret), "child_idx", map_iter->second);
      } else if (OB_FAIL(partition_ids.push_back(map_iter->first))) {
        STORAGE_LOG(WARN, "Fail to push back partition_id", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); i++) {
      if (OB_FAIL(partition_index_.erase_refactored(partition_ids.at(i)))) {
        STORAGE_LOG(WARN, "Fail to erase partition id, ", K(ret), K(partition_id));
      }
    }
  }
  return ret;
}

ObSSTableSplitFinishTask::ObSSTableSplitFinishTask()
    : ObITask(ObITaskType::TASK_TYPE_SPLIT_FINISH_TASK),
      split_dag_(NULL),
      split_context_(),
      allocator_(ObModIds::OB_PARTITION_SPLIT),
      dest_pkey_(),
      is_major_split_(false),
      is_inited_(false)
{}

ObSSTableSplitFinishTask::~ObSSTableSplitFinishTask()
{}

int ObSSTableSplitFinishTask::init(const common::ObPartitionKey& dest_pkey, const bool has_lob_column)
{
  int ret = OB_SUCCESS;
  ObSSTableSplitCtx* ctx = NULL;
  int64_t concurrent_cnt = 0;
  split_dag_ = static_cast<ObSSTableSplitDag*>(dag_);
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "dag must not null", K(ret));
  } else if (!dest_pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument, ", K(ret), K(dest_pkey));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "ctx must not null", K(ret));
  } else {
    if (ctx->is_range_opt_) {
      concurrent_cnt = ctx->tables_handle_.get_count();
    } else {
      concurrent_cnt = ctx->concurrent_count_;
    }
    if (OB_FAIL(split_context_.init(concurrent_cnt, has_lob_column, NULL /*column stats*/, false /*merge_memtable*/))) {
      STORAGE_LOG(WARN, "failed to init split context", K(ret));
    } else {
      dest_pkey_ = dest_pkey;
      is_major_split_ = ctx->param_.is_major_split_;
      is_inited_ = true;
    }
  }

  return ret;
}

int ObSSTableSplitFinishTask::add_macro_blocks(
    int64_t idx, blocksstable::ObMacroBlocksWriteCtx& blocks_ctx, blocksstable::ObMacroBlocksWriteCtx& lob_blocks_ctx)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeInfo sstable_merge_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "split finish task is not initialized", K(ret));
  } else if (OB_FAIL(split_context_.add_macro_blocks(idx, &blocks_ctx, &lob_blocks_ctx, sstable_merge_info))) {
    STORAGE_LOG(WARN, "failed to add macro blocks", K(ret), K(idx));
  }
  return ret;
}

int ObSSTableSplitFinishTask::process()
{
  int ret = OB_SUCCESS;
  ObSSTableSplitCtx* ctx = NULL;
  if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "ctx must not null", K(ret));
  } else {
    if (ctx->is_range_opt_) {
      if (OB_FAIL(process_range_split())) {
        STORAGE_LOG(WARN, "fail to process range split", K(ret));
      }
    } else {
      if (OB_FAIL(process_row_split())) {
        STORAGE_LOG(WARN, "fail to process row split", K(ret));
      }
    }
  }

  return ret;
}

int ObSSTableSplitFinishTask::process_row_split()
{
  int ret = OB_SUCCESS;
  ObSSTableSplitCtx* ctx = NULL;

  storage::ObIPartitionGroupGuard dest_pg_guard;
  storage::ObIPartitionGroup* dest_pg = NULL;

  ObTablesHandle tables_handle;
  ObTableHandle table_handle;
  ObITable* first_source_table = NULL;
  ObITable* last_source_table = NULL;

  ObITable::TableKey table_key;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "split finish task is not initialized", K(ret));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "ctx must not null", K(ret), K(*ctx));
  } else if (OB_ISNULL(first_source_table = ctx->tables_handle_.get_table(0))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "first source table should not be NULL", K(ret), K(*ctx));
  } else if (OB_ISNULL(last_source_table = ctx->tables_handle_.get_last_table())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "last source table should not be NULL", K(ret), K(*ctx));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(dest_pkey_, dest_pg_guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "Fail to get partition", K(ret), K(*ctx), K(dest_pkey_));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(dest_pg = dest_pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The partition must not null", K(ret), K(*ctx));
  } else {
    ObCreateSSTableParamWithTable table_param;
    table_key = last_source_table->get_key();
    table_key.pkey_ = dest_pkey_;
    if (ctx->param_.is_major_split_) {
      table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
      table_param.logical_data_version_ = table_key.version_;
    } else {
      table_key.trans_version_range_.base_version_ = first_source_table->get_key().trans_version_range_.base_version_;
      table_key.table_type_ = ObITable::TableType::MULTI_VERSION_MINOR_SSTABLE;
      table_param.logical_data_version_ = table_key.trans_version_range_.snapshot_version_;
    }

    table_param.table_key_ = table_key;
    table_param.schema_ = ctx->table_schema_;
    table_param.schema_version_ = ctx->schema_version_;
    table_param.checksum_method_ = ctx->checksum_method_;

    if (OB_FAIL(split_context_.create_sstable(table_param, dest_pg_guard, table_handle))) {
      STORAGE_LOG(WARN, "failed to fill sstable", K(ret), K(*ctx));
    } else if (OB_FAIL(tables_handle.add_table(table_handle))) {
      STORAGE_LOG(WARN, "failed to get sstable", K(ret), K(*ctx));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dest_pg->get_pg_storage().update_split_table_store(
              dest_pkey_, table_key.table_id_, ctx->param_.is_major_split_, tables_handle))) {
        STORAGE_LOG(WARN, "Failed to update split table store", K(ret), K(dest_pkey_), K(*ctx));
      } else {
        STORAGE_LOG(INFO, "succ to update table store after split", K(dest_pkey_), K(table_key));
      }
    }
  }
  return ret;
}

int ObSSTableSplitFinishTask::process_range_split()
{
  int ret = OB_SUCCESS;
  ObSSTableSplitCtx* ctx = NULL;

  storage::ObIPartitionGroupGuard dest_pg_guard;
  storage::ObIPartitionGroup* dest_pg = NULL;

  ObTablesHandle tables_handle;
  ObSEArray<ObCreateSSTableParamWithTable, 2> table_params;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "split finish task is not initialized", K(ret));
  } else if (OB_ISNULL(ctx = &split_dag_->get_ctx())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "ctx must not null", K(ret), K(*ctx));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(dest_pkey_, dest_pg_guard))) {
    if (OB_PARTITION_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "Fail to get partition", K(ret), K(*ctx), K(dest_pkey_));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(dest_pg = dest_pg_guard.get_partition_group())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "The partition must not null", K(ret), K(*ctx));
  } else {
    ObTablesHandle& handle = ctx->tables_handle_;

    for (int64_t i = 0; OB_SUCC(ret) && i < handle.get_count(); i++) {
      ObCreateSSTableParamWithTable table_param;

      ObITable::TableKey table_key;
      table_key = handle.get_table(i)->get_key();
      table_key.pkey_ = dest_pkey_;

      table_param.table_key_ = table_key;
      if (ctx->param_.is_major_split_) {
        table_param.logical_data_version_ = table_key.version_;
      } else {
        table_param.logical_data_version_ = table_key.trans_version_range_.snapshot_version_;
      }
      table_param.schema_ = ctx->table_schema_;
      table_param.schema_version_ = ctx->schema_version_;
      table_param.checksum_method_ = ctx->checksum_method_;

      if (OB_FAIL(table_params.push_back(table_param))) {
        STORAGE_LOG(WARN, "failed to push back table param", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(split_context_.create_sstables(table_params, dest_pg_guard, tables_handle))) {
        STORAGE_LOG(WARN, "failed to fill sstable", K(ret), K(*ctx));
      } else if (OB_FAIL(dest_pg->get_pg_storage().update_split_table_store(
                     dest_pkey_, ctx->param_.dest_pkey_.get_table_id(), ctx->param_.is_major_split_, tables_handle))) {
        STORAGE_LOG(WARN, "Failed to update split table store", K(ret), K(dest_pkey_), K(*ctx));
      } else {
        STORAGE_LOG(INFO, "succ to update table store after split", K(dest_pkey_));
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
