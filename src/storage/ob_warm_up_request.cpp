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

#include "ob_warm_up_request.h"
#include "ob_table_scan_iterator.h"
#include "common/ob_range.h"
#include "lib/stat/ob_diagnose_info.h"
#include "storage/ob_partition_service.h"

namespace oceanbase {
using namespace common;
using namespace share::schema;
using namespace memtable;
using namespace blocksstable;
namespace storage {
/**
 * ------------------------------------------------------ObIWarmUpRequest------------------------------------------------------
 */
ObIWarmUpRequest::ObIWarmUpRequest(ObIAllocator& allocator)
    : is_inited_(false), pkey_(), table_id_(0), column_ids_(512, allocator), allocator_(allocator)
{}

ObIWarmUpRequest::~ObIWarmUpRequest()
{}

int ObIWarmUpRequest::assign(const common::ObPartitionKey& pkey, const uint64_t table_id,
    const common::ObIArray<share::schema::ObColDesc>& column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObIWarmUpRequest has been inited, ", K(ret));
  } else if (OB_FAIL(column_ids_.assign(column_ids))) {
    STORAGE_LOG(WARN, "Fail to assign column ids, ", K(ret));
  } else {
    pkey_ = pkey;
    table_id_ = table_id;
  }
  return ret;
}

int ObIWarmUpRequest::prepare_store_ctx(memtable::ObIMemtableCtxFactory* memctx_factory, ObStoreCtx* store_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == memctx_factory) || OB_UNLIKELY(NULL == store_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", KP(memctx_factory), KP(store_ctx));
  } else if (OB_ISNULL(store_ctx->mem_ctx_ = memctx_factory->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "cannot allocate memory context for scan context, ", K(ret));
  } else if (OB_FAIL(store_ctx->mem_ctx_->trans_begin())) {
    STORAGE_LOG(WARN, "fail to begin transaction.", K(ret));
  } else if (OB_FAIL(
                 store_ctx->mem_ctx_->sub_trans_begin(WARM_UP_READ_SNAPSHOT_VERSION, WARM_UP_READ_SNAPSHOT_VERSION))) {
    STORAGE_LOG(WARN, "fail to begin sub transaction.", K(ret));
  }
  return ret;
}

void ObIWarmUpRequest::revert_store_ctx(memtable::ObIMemtableCtxFactory* memctx_factory, ObStoreCtx* store_ctx) const
{
  if (NULL != memctx_factory && NULL != store_ctx && NULL != store_ctx->mem_ctx_) {
    store_ctx->mem_ctx_->trans_end(true, 0);
    store_ctx->mem_ctx_->trans_clear();
    memctx_factory->free(store_ctx->mem_ctx_);
    store_ctx->mem_ctx_ = NULL;
  }
}

OB_SERIALIZE_MEMBER(ObIWarmUpRequest, pkey_, table_id_, column_ids_);

/**
 * -----------------------------------------------------ObIWarmUpReadRequest---------------------------------------------------
 */
ObIWarmUpReadRequest::ObIWarmUpReadRequest(ObIAllocator& allocator)
    : ObIWarmUpRequest(allocator),
      schema_version_(0),
      rowkey_cnt_(0),
      out_cols_project_(256, allocator),
      out_cols_param_(256, allocator),
      query_flag_()
{}

ObIWarmUpReadRequest::~ObIWarmUpReadRequest()
{}

int ObIWarmUpReadRequest::assign(const ObTableAccessParam& param, const ObTableAccessContext& context)
{
  int ret = OB_SUCCESS;
  if (NULL == param.iter_param_.out_cols_project_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "The projector is NULL in param, ", K(param), K(ret));
  } else if (OB_FAIL(ObIWarmUpRequest::assign(
                 context.pkey_, param.iter_param_.table_id_, param.out_col_desc_param_.get_col_descs()))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(out_cols_project_.assign(*param.iter_param_.out_cols_project_))) {
    STORAGE_LOG(WARN, "Fail to assign out cols project, ", K(ret));
  } else {
    if (NULL != param.out_cols_param_) {
      const ObColumnParam* src_col_param = NULL;
      ObColumnParam* dest_col_param = NULL;
      void* tmp = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.out_cols_param_->count(); ++i) {
        src_col_param = param.out_cols_param_->at(i);
        if (NULL == (tmp = allocator_.alloc(sizeof(ObColumnParam)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
        } else {
          dest_col_param = new (tmp) ObColumnParam(allocator_);
          if (OB_FAIL(out_cols_param_.push_back(dest_col_param))) {
            STORAGE_LOG(WARN, "Fail to push back dest col param, ", K(ret));
          } else if (OB_FAIL(dest_col_param->set_orig_default_value(src_col_param->get_orig_default_value()))) {
            STORAGE_LOG(WARN, "Fail to set original default value, ", K(ret));
          } else if (OB_FAIL(dest_col_param->set_cur_default_value(src_col_param->get_orig_default_value()))) {
            STORAGE_LOG(WARN, "Fail to set cur default ,value", K(ret));
          } else {
            dest_col_param->set_column_id(src_col_param->get_column_id());
            dest_col_param->set_meta_type(src_col_param->get_meta_type());
            dest_col_param->set_accuracy(src_col_param->get_accuracy());
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      schema_version_ = param.iter_param_.schema_version_;
      rowkey_cnt_ = param.iter_param_.rowkey_cnt_;
      query_flag_ = context.query_flag_;
    }
  }
  return ret;
}

int ObIWarmUpReadRequest::prepare(ObTableAccessParam& param, ObTableAccessContext& context,
    common::ObArenaAllocator& allocator, ObBlockCacheWorkingSet& block_cache_ws, ObStoreCtx& store_ctx) const
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(table_id_);
  ObQueryFlag query_flag = query_flag_;
  ObVersionRange trans_version_range;
  trans_version_range.base_version_ = 0;
  trans_version_range.multi_version_start_ = 0;
  trans_version_range.snapshot_version_ = WARM_UP_READ_SNAPSHOT_VERSION;
  query_flag.prewarm_ = 1;
  ObPartitionKey pg_key;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObIWarmUpReadRequest has not been inited, ", K(ret));
  } else if (OB_FAIL(block_cache_ws.init(tenant_id))) {
    STORAGE_LOG(WARN, "block_cache_ws init failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(param.out_col_desc_param_.init())) {
    STORAGE_LOG(WARN, "Fail to init param, ", K(ret));
  } else if (OB_FAIL(param.out_col_desc_param_.assign(column_ids_))) {
    STORAGE_LOG(WARN, "Fail to assign param, ", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(get_pkey(), pg_key))) {
    STORAGE_LOG(WARN, "failed to get_pg_key", K(ret), K_(pkey));
  } else if (OB_FAIL(store_ctx.init_trans_ctx_mgr(pg_key))) {
    STORAGE_LOG(WARN, "failed to init_trans_ctx_mgr", K(ret), K(pg_key));
  } else if (OB_FAIL(context.init(query_flag, store_ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
    STORAGE_LOG(WARN, "failed to init context", K(ret));
  } else {
    param.iter_param_.table_id_ = table_id_;
    param.iter_param_.schema_version_ = schema_version_;
    param.iter_param_.rowkey_cnt_ = rowkey_cnt_;
    param.iter_param_.out_cols_project_ = &out_cols_project_;
    param.iter_param_.out_cols_ = &column_ids_;
    param.out_cols_param_ = &out_cols_param_;

    context.pkey_ = pkey_;
    context.timeout_ = ObTimeUtility::current_time() + DEFAULT_WARM_UP_REQUEST_TIMEOUT_US;
  }
  return ret;
}

OB_DEF_SERIALIZE(ObIWarmUpReadRequest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObWarmUpExistRequest has not been inited, ", K(ret));
  } else if (OB_FAIL(ObIWarmUpRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, schema_version_))) {
    STORAGE_LOG(WARN, "Fail to serialize schema version, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, rowkey_cnt_))) {
    STORAGE_LOG(WARN, "Fail to serialize rowkey cnt, ", K(ret));
  } else if (OB_FAIL(query_flag_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize query flag, ", K(ret));
  } else if (OB_FAIL(out_cols_project_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize out cols project, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, out_cols_param_.count()))) {
    STORAGE_LOG(WARN, "Fail to serialize out cols param, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_param_.count(); ++i) {
      if (OB_FAIL(out_cols_param_.at(i)->serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Fail to serialize out cols param, ", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObIWarmUpReadRequest)
{
  int ret = OB_SUCCESS;
  int64_t out_cols_param_cnt = 0;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObWarmUpExistRequest has been inited, ", K(ret));
  } else if (OB_FAIL(ObIWarmUpRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &schema_version_))) {
    STORAGE_LOG(WARN, "Fail to deserialize schema version, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &rowkey_cnt_))) {
    STORAGE_LOG(WARN, "Fail to deserialize rowkey cnt, ", K(ret));
  } else if (OB_FAIL(query_flag_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize query flag, ", K(ret));
  } else if (OB_FAIL(out_cols_project_.deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize out cols project, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &out_cols_param_cnt))) {
    STORAGE_LOG(WARN, "Fail to deserialize out cols param count, ", K(ret));
  } else {
    void* tmp = NULL;
    ObColumnParam* param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < out_cols_param_cnt; ++i) {
      if (NULL == (tmp = allocator_.alloc(sizeof(ObColumnParam)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
      } else {
        param = new (tmp) ObColumnParam(allocator_);
        if (OB_FAIL(param->deserialize(buf, data_len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize param, ", K(ret));
        } else if (OB_FAIL(out_cols_param_.push_back(param))) {
          STORAGE_LOG(WARN, "Fail to put column param to array, ", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIWarmUpReadRequest)
{
  int64_t size = 0;
  size += ObIWarmUpRequest::get_serialize_size();
  size += serialization::encoded_length_vi64(schema_version_);
  size += serialization::encoded_length_vi64(rowkey_cnt_);
  size += query_flag_.get_serialize_size();
  size += out_cols_project_.get_serialize_size();
  size += serialization::encoded_length_vi64(out_cols_param_.count());
  for (int64_t i = 0; i < out_cols_param_.count(); ++i) {
    size += out_cols_param_.at(i)->get_serialize_size();
  }
  return size;
}

/**
 * ------------------------------------------------------ObWarmUpExistRequest---------------------------------------------------
 */
ObWarmUpExistRequest::ObWarmUpExistRequest(ObIAllocator& allocator) : ObIWarmUpRequest(allocator), rowkey_()
{}

ObWarmUpExistRequest::~ObWarmUpExistRequest()
{}

int ObWarmUpExistRequest::assign(const common::ObPartitionKey& pkey, const int64_t table_id,
    const common::ObStoreRowkey& rowkey, const common::ObIArray<share::schema::ObColDesc>& column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpRequest::assign(pkey, table_id, column_ids))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(rowkey.deep_copy(rowkey_, allocator_))) {
    STORAGE_LOG(WARN, "Fail to deep copy rowkey, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObWarmUpRequestType::ObWarmUpRequestEnum ObWarmUpExistRequest::get_request_type() const
{
  return ObWarmUpRequestType::EXIST_WARM_REQUEST;
}

int ObWarmUpExistRequest::warm_up(
    memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObWarmUpGetRequest has not been inited, ", K(ret));
  } else if (OB_FAIL(prepare_store_ctx(memctx_factory, &store_ctx))) {
    STORAGE_LOG(WARN, "Fail to prepare store ctx, ", K(ret));
  } else {
    ObITable* store = NULL;
    bool is_exist = false;
    bool has_found = false;
    for (int64_t i = stores.count() - 1; OB_SUCC(ret) && i >= 0 && !has_found; --i) {

      if (NULL != (store = stores.at(i)) && store->is_sstable()) {
        if (OB_FAIL(store->exist(store_ctx, table_id_, rowkey_, column_ids_, is_exist, has_found))) {
          STORAGE_LOG(WARN, "Fail to check rowkey exist, ", K(ret));
        }
      }
    }
  }

  EVENT_INC(WARM_UP_REQUEST_ROWKEY_CHECK_COUNT);
  revert_store_ctx(memctx_factory, &store_ctx);
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpExistRequest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObWarmUpExistRequest has not been inited, ", K(ret));
  } else if (OB_FAIL(ObIWarmUpRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(rowkey_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize rowkey, ", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpExistRequest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObWarmUpExistRequest has been inited, ", K(ret));
  } else if (OB_FAIL(ObIWarmUpRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(rowkey_.deserialize(allocator_, buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deseriallize rowkey, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpExistRequest)
{
  int64_t size = 0;
  size += ObIWarmUpRequest::get_serialize_size();
  size += rowkey_.get_serialize_size();
  return size;
}

/**
 * ------------------------------------------------------ObWarmUpGetRequest---------------------------------------------------
 */
ObWarmUpGetRequest::ObWarmUpGetRequest(ObIAllocator& allocator) : ObIWarmUpReadRequest(allocator), rowkey_()
{}

ObWarmUpGetRequest::~ObWarmUpGetRequest()
{}

int ObWarmUpGetRequest::assign(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObExtStoreRowkey& rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::assign(param, ctx))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(rowkey.deep_copy(rowkey_, allocator_))) {
    STORAGE_LOG(WARN, "Fail to deep copy rowkey, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

ObWarmUpRequestType::ObWarmUpRequestEnum ObWarmUpGetRequest::get_request_type() const
{
  return ObWarmUpRequestType::GET_WARM_REQUEST;
}

int ObWarmUpGetRequest::warm_up(
    memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTableAccessParam param;
  ObTableAccessContext context;
  ObTablesHandle tables_handle;
  ObGetTableParam get_table_param;
  ObBlockCacheWorkingSet block_cache_ws;
  get_table_param.tables_handle_ = &tables_handle;
  ObArenaAllocator allocator(ObModIds::OB_WARM_UP_REQUEST);
  ObSingleMerge merge;
  ObStoreRow* row = NULL;
  if (stores.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(tables_handle.add_tables(stores))) {
    STORAGE_LOG(WARN, "fail to add tables", K(ret));
  } else if (OB_FAIL(prepare_store_ctx(memctx_factory, &store_ctx))) {
    STORAGE_LOG(WARN, "Fail to prepare store ctx, ", K(ret));
  } else if (OB_FAIL(prepare(param, context, allocator, block_cache_ws, store_ctx))) {
    STORAGE_LOG(WARN, "Fail to prepare ObWarmUpGetRequest, ", K(ret));
  } else if (OB_FAIL(merge.init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "Fail to init single merge, ", K(ret));
  } else if (OB_FAIL(merge.open(rowkey_))) {
    STORAGE_LOG(WARN, "Fail to open single get merge, ", K(ret));
  } else if (OB_FAIL(merge.get_next_row(row))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next row, ", K(ret));
    }
  }

  EVENT_INC(WARM_UP_REQUEST_GET_COUNT);
  revert_store_ctx(memctx_factory, &store_ctx);
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpGetRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpRequest, ", K(ret));
  } else if (OB_FAIL(rowkey_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize rowkey, ", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpGetRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(rowkey_.deserialize(allocator_, buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize rowkey, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpGetRequest)
{
  int64_t size = 0;
  size += ObIWarmUpReadRequest::get_serialize_size();
  size += rowkey_.get_serialize_size();
  return size;
}

/**
 * ----------------------------------------------------ObWarmUpMultiGetRequest---------------------------------------------------
 */
ObWarmUpMultiGetRequest::ObWarmUpMultiGetRequest(ObIAllocator& allocator)
    : ObIWarmUpReadRequest(allocator), rowkeys_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
{}

ObWarmUpMultiGetRequest::~ObWarmUpMultiGetRequest()
{}

ObWarmUpRequestType::ObWarmUpRequestEnum ObWarmUpMultiGetRequest::get_request_type() const
{
  return ObWarmUpRequestType::MULTI_GET_WARM_REQUEST;
}

int ObWarmUpMultiGetRequest::assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
    const common::ObIArray<common::ObExtStoreRowkey>& rowkeys)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::assign(param, ctx))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpReadRequest, ", K(ret));
  } else {
    ObExtStoreRowkey extkey;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      if (OB_FAIL(rowkeys.at(i).deep_copy(extkey, allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy ext key, ", K(ret));
      } else if (OB_FAIL(rowkeys_.push_back(extkey))) {
        STORAGE_LOG(WARN, "Fail to push extkey to rowkeys array, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObWarmUpMultiGetRequest::warm_up(
    memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTableAccessParam param;
  ObTableAccessContext context;
  ObTablesHandle tables_handle;
  ObGetTableParam get_table_param;
  ObBlockCacheWorkingSet block_cache_ws;
  get_table_param.tables_handle_ = &tables_handle;
  ObArenaAllocator allocator(ObModIds::OB_WARM_UP_REQUEST);
  ObMultipleGetMerge merge;

  if (stores.count() <= 0) {
    // do nothing
  } else {
    if (OB_FAIL(prepare_store_ctx(memctx_factory, &store_ctx))) {
      STORAGE_LOG(WARN, "Fail to prepare store ctx, ", K(ret));
    } else if (OB_FAIL(prepare(param, context, allocator, block_cache_ws, store_ctx))) {
      STORAGE_LOG(WARN, "Fail to prepare ObWarmUpMultiGetRequest, ", K(ret));
    } else if (OB_FAIL(merge.init(param, context, get_table_param))) {
      STORAGE_LOG(WARN, "Fail to init single merge, ", K(ret));
    } else if (OB_FAIL(merge.open(rowkeys_))) {
      STORAGE_LOG(WARN, "Fail to open single get merge, ", K(ret));
    } else {
      // ignore ret
      ObStoreRow* row = NULL;
      if (OB_FAIL(merge.get_next_row(row))) {
        ret = OB_SUCCESS;
      }
    }
  }

  EVENT_INC(WARM_UP_REQUEST_MULTI_GET_COUNT);
  revert_store_ctx(memctx_factory, &store_ctx);
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpMultiGetRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, rowkeys_.count()))) {
    STORAGE_LOG(WARN, "Fail to serialize rowkey count, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys_.count(); ++i) {
      if (OB_FAIL(rowkeys_.at(i).serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Fail to serialize rowkeys, ", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpMultiGetRequest)
{
  int ret = OB_SUCCESS;
  int64_t rowkeys_cnt = 0;
  ObExtStoreRowkey rowkey;
  if (OB_FAIL(ObIWarmUpReadRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &rowkeys_cnt))) {
    STORAGE_LOG(WARN, "Fail to deserialize rowkeys count, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkeys_cnt; ++i) {
      rowkey.reset();
      if (OB_FAIL(rowkey.deserialize(allocator_, buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Fail to deserialize rowkey, ", K(ret));
      } else if (OB_FAIL(rowkeys_.push_back(rowkey))) {
        STORAGE_LOG(WARN, "Fail to push rowkey to array, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpMultiGetRequest)
{
  int64_t size = 0;
  size += ObIWarmUpReadRequest::get_serialize_size();
  size += serialization::encoded_length_vi64(rowkeys_.count());
  for (int64_t i = 0; i < rowkeys_.count(); ++i) {
    size += rowkeys_.at(i).get_serialize_size();
  }
  return size;
}

/**
 * ----------------------------------------------------ObWarmUpScanRequest---------------------------------------------------
 */
ObWarmUpScanRequest::ObWarmUpScanRequest(ObIAllocator& allocator) : ObIWarmUpReadRequest(allocator), range_()
{}

ObWarmUpScanRequest::~ObWarmUpScanRequest()
{}

ObWarmUpRequestType::ObWarmUpRequestEnum ObWarmUpScanRequest::get_request_type() const
{
  return ObWarmUpRequestType::SCAN_WARM_REQUEST;
}

int ObWarmUpScanRequest::assign(
    const ObTableAccessParam& param, const ObTableAccessContext& ctx, const common::ObExtStoreRange& range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::assign(param, ctx))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(range.deep_copy(range_, allocator_))) {
    STORAGE_LOG(WARN, "Fail to deep copy range, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObWarmUpScanRequest::warm_up(
    memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTableAccessParam param;
  ObTableAccessContext context;
  ObTablesHandle tables_handle;
  ObGetTableParam get_table_param;
  ObBlockCacheWorkingSet block_cache_ws;
  get_table_param.tables_handle_ = &tables_handle;
  ObArenaAllocator allocator(ObModIds::OB_WARM_UP_REQUEST);
  ObMultipleScanMerge merge;

  if (stores.count() <= 0) {
    // do nothing
  } else {
    if (OB_FAIL(tables_handle.add_tables(stores))) {
      STORAGE_LOG(WARN, "fail to add tables", K(ret));
    } else if (OB_FAIL(prepare_store_ctx(memctx_factory, &store_ctx))) {
      STORAGE_LOG(WARN, "Fail to prepare store ctx, ", K(ret));
    } else if (OB_FAIL(prepare(param, context, allocator, block_cache_ws, store_ctx))) {
      STORAGE_LOG(WARN, "Fail to prepare ObWarmUpScanRequest, ", K(ret));
    } else if (OB_FAIL(merge.init(param, context, get_table_param))) {
      STORAGE_LOG(WARN, "Fail to init single merge, ", K(ret));
    } else if (OB_FAIL(merge.open(range_))) {
      STORAGE_LOG(WARN, "Fail to open single get merge, ", K(ret));
    } else {
      // ignore ret
      ObStoreRow* row = NULL;
      if (OB_FAIL(merge.get_next_row(row))) {
        ret = OB_SUCCESS;
      }
    }
  }

  EVENT_INC(WARM_UP_REQUEST_SCAN_COUNT);
  revert_store_ctx(memctx_factory, &store_ctx);
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpScanRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(range_.serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize range, ", K(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpScanRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(range_.deserialize(allocator_, buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize range, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpScanRequest)
{
  int64_t size = 0;
  size += ObIWarmUpReadRequest::get_serialize_size();
  size += range_.get_serialize_size();
  return size;
}

/**
 * ----------------------------------------------------ObWarmUpMultiScanRequest---------------------------------------------------
 */
ObWarmUpMultiScanRequest::ObWarmUpMultiScanRequest(ObIAllocator& allocator)
    : ObIWarmUpReadRequest(allocator), ranges_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator)
{}

ObWarmUpMultiScanRequest::~ObWarmUpMultiScanRequest()
{}

ObWarmUpRequestType::ObWarmUpRequestEnum ObWarmUpMultiScanRequest::get_request_type() const
{
  return ObWarmUpRequestType::MULTI_SCAN_WARM_REQUEST;
}

int ObWarmUpMultiScanRequest::assign(const ObTableAccessParam& param, const ObTableAccessContext& ctx,
    const common::ObIArray<common::ObExtStoreRange>& ranges)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::assign(param, ctx))) {
    STORAGE_LOG(WARN, "Fail to assign ObIWarmUpReadRequest, ", K(ret));
  } else {
    ObExtStoreRange range;
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      if (OB_FAIL(ranges.at(i).deep_copy(range, allocator_))) {
        STORAGE_LOG(WARN, "Fail to deep copy ext range, ", K(ret));
      } else if (OB_FAIL(ranges_.push_back(range))) {
        STORAGE_LOG(WARN, "Fail to push range to ranges array, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObWarmUpMultiScanRequest::warm_up(
    memtable::ObIMemtableCtxFactory* memctx_factory, const common::ObIArray<ObITable*>& stores) const
{
  int ret = OB_SUCCESS;
  ObStoreCtx store_ctx;
  ObTableAccessParam param;
  ObTableAccessContext context;
  ObTablesHandle tables_handle;
  ObGetTableParam get_table_param;
  ObBlockCacheWorkingSet block_cache_ws;
  get_table_param.tables_handle_ = &tables_handle;
  ObArenaAllocator allocator(ObModIds::OB_WARM_UP_REQUEST);
  ObMultipleMultiScanMerge merge;

  if (stores.count() <= 0) {
    // do nothing
  } else if (OB_FAIL(tables_handle.add_tables(stores))) {
    STORAGE_LOG(WARN, "fail to add tables", K(ret));
  } else if (OB_FAIL(prepare_store_ctx(memctx_factory, &store_ctx))) {
    STORAGE_LOG(WARN, "Fail to prepare store ctx, ", K(ret));
  } else if (OB_FAIL(prepare(param, context, allocator, block_cache_ws, store_ctx))) {
    STORAGE_LOG(WARN, "Fail to prepare ObWarmUpMultiScanRequest, ", K(ret));
  } else if (OB_FAIL(merge.init(param, context, get_table_param))) {
    STORAGE_LOG(WARN, "Fail to init single merge, ", K(ret));
  } else if (OB_FAIL(merge.open(ranges_))) {
    STORAGE_LOG(WARN, "Fail to open single get merge, ", K(ret));
  } else {
    // ignore ret
    ObStoreRow* row = NULL;
    if (OB_FAIL(merge.get_next_row(row))) {
      ret = OB_SUCCESS;
    }
  }

  EVENT_INC(WARM_UP_REQUEST_MULTI_SCAN_COUNT);
  revert_store_ctx(memctx_factory, &store_ctx);
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpMultiScanRequest)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIWarmUpReadRequest::serialize(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "Fail to serialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, ranges_.count()))) {
    STORAGE_LOG(WARN, "Fail to serialize ranges count, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges_.count(); ++i) {
      if (OB_FAIL(ranges_.at(i).serialize(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "Fail to serialize ranges, ", K(ret));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpMultiScanRequest)
{
  int ret = OB_SUCCESS;
  int64_t ranges_cnt = 0;
  ObExtStoreRange range;
  if (OB_FAIL(ObIWarmUpReadRequest::deserialize(buf, data_len, pos))) {
    STORAGE_LOG(WARN, "Fail to deserialize ObIWarmUpReadRequest, ", K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &ranges_cnt))) {
    STORAGE_LOG(WARN, "Fail to deserialize ranges count, ", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges_cnt; ++i) {
      range.reset();
      if (OB_FAIL(range.deserialize(allocator_, buf, data_len, pos))) {
        STORAGE_LOG(WARN, "Fail to deserialize range, ", K(ret));
      } else if (OB_FAIL(ranges_.push_back(range))) {
        STORAGE_LOG(WARN, "Fail to push range to array, ", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpMultiScanRequest)
{
  int64_t size = 0;
  size += ObIWarmUpReadRequest::get_serialize_size();
  size += serialization::encoded_length_vi64(ranges_.count());
  for (int64_t i = 0; i < ranges_.count(); ++i) {
    size += ranges_.at(i).get_serialize_size();
  }
  return size;
}

/**
 * ----------------------------------------------------ObWarmUpRequestWrapper---------------------------------------------------
 */
ObWarmUpRequestWrapper::ObWarmUpRequestWrapper() : allocator_(ObModIds::OB_WARM_UP_REQUEST), request_list_(allocator_)
{}

ObWarmUpRequestWrapper::~ObWarmUpRequestWrapper()
{}

int ObWarmUpRequestWrapper::add_request(const ObIWarmUpRequest& request)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(request_list_.push_back(&request))) {
    STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObWarmUpRequestWrapper)
{
  int ret = OB_SUCCESS;
  const ObIWarmUpRequest* request = NULL;
  OB_UNIS_ENCODE(request_list_.size());
  for (ObWarmUpRequestList::const_iterator iter = request_list_.begin(); OB_SUCC(ret) && iter != request_list_.end();
       ++iter) {
    if (NULL == (request = *iter)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "The WarmUp request is NULL, ", K(ret));
    } else if (OB_FAIL(
                   serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(request->get_request_type())))) {
      STORAGE_LOG(WARN, "Fail to serialize warm up type, ", K(ret));
    } else if (OB_FAIL(request->serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "Fail to serialize warm up request, ", K(ret));
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObWarmUpRequestWrapper)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  int64_t request_type = 0;
  void* tmp = NULL;
  ObIWarmUpRequest* request = NULL;
  request_list_.reset();
  allocator_.reuse();
  OB_UNIS_DECODEx(count);
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &request_type))) {
      STORAGE_LOG(WARN, "Fail to decode request type, ", K(ret));
    } else {
      switch (request_type) {
        case ObWarmUpRequestType::EXIST_WARM_REQUEST: {
          if (NULL == (tmp = allocator_.alloc(sizeof(ObWarmUpExistRequest)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            request = new (tmp) ObWarmUpExistRequest(allocator_);
          }
          break;
        }
        case ObWarmUpRequestType::GET_WARM_REQUEST: {
          if (NULL == (tmp = allocator_.alloc(sizeof(ObWarmUpGetRequest)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            request = new (tmp) ObWarmUpGetRequest(allocator_);
          }
          break;
        }
        case ObWarmUpRequestType::MULTI_GET_WARM_REQUEST: {
          if (NULL == (tmp = allocator_.alloc(sizeof(ObWarmUpMultiGetRequest)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            request = new (tmp) ObWarmUpMultiGetRequest(allocator_);
          }
          break;
        }
        case ObWarmUpRequestType::SCAN_WARM_REQUEST: {
          if (NULL == (tmp = allocator_.alloc(sizeof(ObWarmUpScanRequest)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            request = new (tmp) ObWarmUpScanRequest(allocator_);
          }
          break;
        }
        case ObWarmUpRequestType::MULTI_SCAN_WARM_REQUEST: {
          if (NULL == (tmp = allocator_.alloc(sizeof(ObWarmUpMultiScanRequest)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret));
          } else {
            request = new (tmp) ObWarmUpMultiScanRequest(allocator_);
          }
          break;
        }
        default:
          ret = OB_NOT_SUPPORTED;
          break;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(request->deserialize(buf, data_len, pos))) {
          STORAGE_LOG(WARN, "Fail to deserialize warm up request, ", K(ret));
        } else if (OB_FAIL(request_list_.push_back(request))) {
          STORAGE_LOG(WARN, "Fail to push request to list, ", K(ret));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObWarmUpRequestWrapper)
{
  int64_t len = 0;
  const ObIWarmUpRequest* request = NULL;
  int64_t count = request_list_.size();
  OB_UNIS_ADD_LEN(count);
  for (ObWarmUpRequestList::const_iterator iter = request_list_.begin(); iter != request_list_.end(); ++iter) {
    if (NULL != (request = *iter)) {
      len += serialization::encoded_length_vi64(static_cast<int64_t>(request->get_request_type()));
      len += request->get_serialize_size();
    }
  }
  return len;
}
}  // namespace storage

namespace obrpc {
OB_SERIALIZE_MEMBER(ObWarmUpRequestArg, wrapper_);
}  // namespace obrpc
}  // namespace oceanbase
