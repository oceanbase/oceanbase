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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_table_scan_create_domain_index.h"
#include "ob_table_scan_with_checksum.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObTableScanCreateDomainIndex::ObTableScanCreateDomainIndex(common::ObIAllocator& allocator)
    : ObTableScan(allocator),
      words_(),
      word_index_(0),
      domain_index_col_pos_(-1),
      orig_obj_type_(),
      create_index_id_(OB_INVALID_ID)
{}

ObTableScanCreateDomainIndex::~ObTableScanCreateDomainIndex()
{}

void ObTableScanCreateDomainIndex::set_create_index_table_id(const uint64_t table_id)
{
  create_index_id_ = table_id;
}

uint64_t ObTableScanCreateDomainIndex::get_create_index_table_id() const
{
  return create_index_id_;
}

int ObTableScanCreateDomainIndex::inner_open(ObExecContext& ctx) const
{
  int ret = OB_NOT_SUPPORTED;
  if (OB_FAIL(ret)) {
    LOG_WARN("global domain index is not supported", K(ret));
  } else if (OB_FAIL(ObTableScan::inner_open(ctx))) {
    LOG_WARN("fail to do ObTableScan inner open", K(ret));
  } else {
    ObTableScanCtx* scan_ctx = GET_PHY_OPERATOR_CTX(ObTableScanCtx, ctx, get_id());
    if (OB_ISNULL(scan_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, scan ctx must not be NULL", K(ret));
    } else {
      ObArray<int32_t> col_ids;
      const ObTableParam& table_param = *scan_ctx->scan_param_.table_param_;
      /*if (OB_FAIL(ObTableScanWithChecksum::get_output_col_ids(table_param, col_ids))) {
        LOG_WARN("fail to get output col ids", K(ret));
      } else */
      if (OB_FAIL(
              get_domain_index_col_pos(ctx, col_ids, scan_ctx->scan_param_.schema_version_, domain_index_col_pos_))) {
        LOG_WARN("fail to get domain index col pos", K(ret), K(create_index_id_));
      }
    }
  }
  return ret;
}

int ObTableScanCreateDomainIndex::get_domain_index_col_pos(
    ObExecContext& ctx, const ObIArray<int32_t>& col_ids, const int64_t schema_version, int64_t& domain_index_pos) const
{
  int ret = OB_SUCCESS;
  uint64_t domain_column_id = OB_INVALID_ID;
  ObSQLSessionInfo* my_session = NULL;
  domain_index_pos = -1;
  if (OB_INVALID_ID == create_index_id_) {
    ret = OB_ERR_SYS;
    LOG_WARN("invalid create index table id", K(ret), K(create_index_id_));
  } else if (OB_UNLIKELY(col_ids.count() <= 0 || OB_INVALID_VERSION == schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(col_ids.count()), K(schema_version));
  } else if (OB_ISNULL(my_session = GET_MY_SESSION(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema* index_schema = NULL;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
            my_session->get_effective_tenant_id(), schema_guard, schema_version))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(create_index_id_, index_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(create_index_id_));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_INFO("index has been deleted while creating index", K(ret), K(create_index_id_));
    } else if (OB_FAIL(index_schema->get_index_info().get_fulltext_column(domain_column_id))) {
      STORAGE_LOG(WARN, "failed to get domain column id", K(ret), K(index_schema->get_index_info()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && -1 == domain_index_pos && i < col_ids.count(); ++i) {
        if (domain_column_id == col_ids.at(i)) {
          domain_index_pos = i;
        }
      }
      if (-1 == domain_index_pos) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index has not domain index column", K(ret), K(create_index_id_));
      }
    }
  }
  return ret;
}

int ObTableScanCreateDomainIndex::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (word_index_ == words_.count()) {
    if (OB_FAIL(ObTableScan::inner_get_next_row(ctx, row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to inner get next row", K(ret));
      }
    } else {
      ObObj orig_obj = row->cells_[domain_index_col_pos_];
      ObString orig_string = orig_obj.get_string();
      words_.reuse();
      word_index_ = 0;
      if (OB_FAIL(split_on(orig_string, ',', words_))) {
        LOG_WARN("fail to split string", K(ret));
      } else {
        orig_obj_type_ = orig_obj.get_type();
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (word_index_ >= 0 && word_index_ < words_.count()) {
      row->cells_[domain_index_col_pos_].set_string(orig_obj_type_, words_.at(word_index_));
      ++word_index_;
    }
  }
  return ret;
}
