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
#include "ob_migrate_logic_row_writer.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ob_multi_version_col_desc_generate.h"
#include "observer/omt/ob_multi_tenant.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace share::schema;
namespace storage {
ObMigrateLogicRowWriter::ObMigrateLogicRowWriter() : is_inited_(false), iterator_(NULL), data_checksum_(0), pg_key_()
{}

int ObMigrateLogicRowWriter::init(
    ObILogicRowIterator* iterator, const common::ObPGKey& pg_key, const blocksstable::ObStorageFileHandle& file_handle)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("writer should not be init twice", K(ret));
  } else if (OB_ISNULL(iterator) || !pg_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("logic row reader should not be NULL", K(ret), KP(iterator), K(pg_key));
  } else if (OB_FAIL(file_handle_.assign(file_handle))) {
    LOG_WARN("fail to assign file handle", K(ret), KP(iterator), K(file_handle), K(pg_key));
  } else {
    iterator_ = iterator;
    pg_key_ = pg_key;
    is_inited_ = true;
  }
  return ret;
}

int ObMigrateLogicRowWriter::process(
    ObMacroBlocksWriteCtx& macro_block_write_ctx, ObMacroBlocksWriteCtx& lob_block_write_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t data_version = 0;
  const bool is_major_merge = false;
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  ObMacroDataSeq macro_start_seq(0);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema* table_schema = NULL;
  const ObTenantSchema* tenant_schema = NULL;
  const obrpc::ObFetchLogicRowArg* arg = NULL;
  const ObStoreRow* store_row = NULL;
  ObMultiVersionColDescGenerate multi_version_col_desc_gen;
  ObDataStoreDesc desc;
  ObMacroBlockWriter block_writer;
  bool has_lob_column = false;
  uint64_t table_id = OB_INVALID_ID;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  bool check_formal = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate logic row writer do not init", K(ret));
  } else if (NULL == (arg = iterator_->get_fetch_logic_row_arg())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arguments invalid", K(ret), KP(iterator_), KP(arg));
  } else if (!arg->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invaild", K(ret), KP(arg), K(*arg));
  } else {
    table_id = arg->table_key_.table_id_;
    tenant_id = is_inner_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
    check_formal = extract_pure_id(table_id) > OB_MAX_CORE_TABLE_ID;  // Avoid the problem of circular dependencies
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 tenant_id, schema_guard, arg->schema_version_, OB_INVALID_VERSION))) {
    LOG_WARN("fail to get schema guard", K(ret), K(arg->schema_version_));
  } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(arg->table_key_.table_id_, table_schema)) ||
             OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("fail to get table schema", K(ret), KP(table_schema), K(arg->schema_version_));
  } else if (OB_FAIL(schema_guard.get_tenant_info(extract_tenant_id(arg->table_key_.table_id_), tenant_schema)) ||
             OB_ISNULL(tenant_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("Fail to get tenant schema",
        K(ret),
        K(arg->schema_version_),
        "tenant_id",
        extract_tenant_id(arg->table_key_.table_id_));
  } else if (OB_FAIL(table_schema->has_lob_column(has_lob_column, true))) {
    LOG_WARN("Failed to check lob column in table schema", K(ret));
  } else if (OB_FAIL(multi_version_col_desc_gen.init(table_schema))) {
    LOG_WARN("fail to init multi version col desc generate", K(ret));
  } else if (OB_FAIL(multi_version_col_desc_gen.generate_multi_version_row_info(multi_version_row_info))) {
    LOG_WARN("fail to get multi version row info", K(ret), KP(multi_version_row_info));
  } else if (OB_FAIL(desc.init(*table_schema,
                 data_version,
                 multi_version_row_info,
                 arg->table_key_.pkey_.get_partition_id(),
                 is_major_merge ? MAJOR_MERGE : MINOR_MERGE,
                 false /*need calc new column checksum*/,
                 false /*need store column checksum in micro*/,
                 pg_key_,
                 file_handle_))) {
    LOG_WARN("fail to init data store desc", K(ret), K(data_version), KP(multi_version_row_info));
  } else if (OB_FAIL(block_writer.open(desc, macro_start_seq))) {
    LOG_WARN("fail to init block writer", K(ret), K(desc));
  } else {
    const uint64_t tenant_id = arg->table_key_.get_tenant_id();
    while (OB_SUCC(ret)) {
      share::dag_yield();
      if (!GCTX.omt_->has_tenant(tenant_id)) {
        ret = OB_TENANT_NOT_EXIST;
        STORAGE_LOG(WARN, "tenant not exists, stop logic migrate", K(ret), K(tenant_id));
      } else if (OB_FAIL(iterator_->get_next_row(store_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch next row", K(ret));
        }
      } else if (OB_ISNULL(store_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("store row should not be null here", K(ret));
      } else if (common::ObActionFlag::OP_ROW_DOES_NOT_EXIST == store_row->flag_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR(
            "Unexpected not exist row in logical migrate writer", K(ret), K(*store_row), K(tenant_id), K(table_id));
      } else if (OB_FAIL(block_writer.append_row(*store_row))) {
        LOG_WARN("fail to append row to macro block", K(ret), K(*store_row));
      }
    }

    if (OB_ITER_END == ret) {
      if (OB_FAIL(block_writer.close())) {
        LOG_WARN("fail to close block writer", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(macro_block_write_ctx.set(block_writer.get_macro_block_write_ctx()))) {
        LOG_WARN("Failed to set macro block write ctx", K(ret), K(*store_row));
      } else if (has_lob_column && OB_FAIL(lob_block_write_ctx.set(block_writer.get_lob_macro_block_write_ctx()))) {
        LOG_WARN("Failed to set macro block write ctx", K(ret), K(*store_row));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

void ObMigrateLogicRowWriter::reset()
{
  is_inited_ = false;
  iterator_ = NULL;
}

}  // end namespace storage
}  // end namespace oceanbase
