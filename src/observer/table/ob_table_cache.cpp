/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_cache.h"
namespace oceanbase
{

namespace table
{

int ObTableApiCacheKey::deep_copy(common::ObIAllocator &allocator, const ObILibCacheKey &other)
{
  int ret = OB_SUCCESS;
  const ObTableApiCacheKey &table_key = static_cast<const ObTableApiCacheKey&>(other);
  table_id_ = table_key.table_id_;
  index_table_id_ = table_key.index_table_id_;
  schema_version_ = table_key.schema_version_;
  is_ttl_table_ = table_key.is_ttl_table_;
  operation_type_ = table_key.operation_type_;
  namespace_ = table_key.namespace_;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_key.op_column_ids_.count(); i++) {
    if(OB_FAIL(op_column_ids_.push_back(table_key.op_column_ids_.at(i)))) {
      LOG_WARN("fail to push back column id ", K(ret));
    }
  }
  return ret;
}

uint64_t ObTableApiCacheKey::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&table_id_, sizeof(table_id_), hash_val);
  hash_val = murmurhash(&index_table_id_, sizeof(index_table_id_), hash_val);
  hash_val = murmurhash(&schema_version_, sizeof(schema_version_), hash_val);
  hash_val = murmurhash(&is_ttl_table_, sizeof(is_ttl_table_), hash_val);
  hash_val = murmurhash(&operation_type_, sizeof(operation_type_), hash_val);
  for (int64_t i = 0; i < op_column_ids_.count(); i++) {
    hash_val = murmurhash(&(op_column_ids_.at(i)), sizeof(uint64_t), hash_val);
  }
  return hash_val;
}

bool ObTableApiCacheKey::is_equal(const ObILibCacheKey &other) const
{
  const ObTableApiCacheKey &table_key = static_cast<const ObTableApiCacheKey&>(other);
  bool cmp_ret = table_id_ == table_key.table_id_ &&
                 index_table_id_ == table_key.index_table_id_ &&
                 schema_version_ == table_key.schema_version_ &&
                 is_ttl_table_ == table_key.is_ttl_table_ &&
                 operation_type_ == table_key.operation_type_ &&
                 namespace_ == table_key.namespace_ &&
                 op_column_ids_.count() == table_key.op_column_ids_.count();
  for (int64_t i = 0; (cmp_ret == true) && i < op_column_ids_.count(); i++) {
    if (op_column_ids_.at(i) != table_key.op_column_ids_.at(i)) {
      cmp_ret = false;
    }
  }
  return cmp_ret;
}

void ObTableApiCacheKey::reset()
{
  table_id_ = common::OB_INVALID_ID;
  index_table_id_ = common::OB_INVALID_ID;
  schema_version_ = -1;
  is_ttl_table_ = false;
  operation_type_ = ObTableOperationType::Type::INVALID;
  namespace_ = ObLibCacheNameSpace::NS_TABLEAPI;
  op_column_ids_.reset();
}

int ObTableApiCacheNode::inner_get_cache_obj(ObILibCacheCtx &ctx,
                                          ObILibCacheKey *key,
                                          ObILibCacheObject *&cache_obj)
{
  UNUSED(ctx);
  UNUSED(key);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj_)) {
    ret = OB_SQL_PC_NOT_EXIST;
    LOG_WARN("fail to get cache obj", K(ret));
  } else {
    cache_obj = cache_obj_;
  }
  return ret;
}

int ObTableApiCacheNode::inner_add_cache_obj(ObILibCacheCtx &ctx,
                                             ObILibCacheKey *key,
                                             ObILibCacheObject *cache_obj)
{
  UNUSED(ctx);
  UNUSED(key);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cache obj is null", K(ret));
  } else {
    cache_obj_ = cache_obj;
  }
  return ret;
}

int ObTableApiCacheGuard::init(ObTableCtx *tb_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tb_ctx->get_tenant_id();
  if (MTL_ID() != tenant_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unmatched tenant_id", K(MTL_ID()), K(tenant_id), K(lbt()));
  } else if (OB_ISNULL(lib_cache_ = MTL(ObPlanCache*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get plan cache", K(ret));
  } else if (OB_FAIL(create_cache_key(tb_ctx))) {
    LOG_WARN("fail to create cache key", K(ret));
  } else if (OB_FAIL(get_or_create_cache_obj())) {
    LOG_WARN("fail to get or generate cache obj", K(ret), K(is_use_cache_));
  }
  return ret;
}

int ObTableApiCacheGuard::create_cache_key(ObTableCtx *tb_ctx)
{
  int ret = OB_SUCCESS;
  cache_key_.table_id_ = tb_ctx->get_table_id();
  cache_key_.index_table_id_ = tb_ctx->get_index_table_id();
  cache_key_.schema_version_ = tb_ctx->get_table_schema()->get_schema_version();
  cache_key_.is_ttl_table_ = tb_ctx->is_ttl_table();
  ObTableOperationType::Type operation_type = tb_ctx->get_opertion_type();
  cache_key_.operation_type_ = operation_type;
  if (operation_type == ObTableOperationType::Type::UPDATE
      || operation_type == ObTableOperationType::Type::INSERT_OR_UPDATE
      || operation_type == ObTableOperationType::Type::INCREMENT
      || operation_type == ObTableOperationType::Type::APPEND) {
    const ObIArray<ObTableAssignment> &assigns = tb_ctx->get_assignments();
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      const ObTableAssignment &tmp_assign = assigns.at(i);
      if (OB_ISNULL(tmp_assign.column_item_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is null", K(ret), K(tmp_assign));
      } else if (OB_FAIL(cache_key_.op_column_ids_.push_back(tmp_assign.column_item_->column_id_))) {
        LOG_WARN("fail to add assign column id", K(ret), K(i));
      }
    }
  } else if (tb_ctx->is_get() || tb_ctx->is_scan()) {
    const ObIArray<uint64_t> &select_col_ids = tb_ctx->get_select_col_ids();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_col_ids.count(); i++) {
      if (OB_FAIL(cache_key_.op_column_ids_.push_back(select_col_ids.at(i)))) {
        LOG_WARN("fail to add select column id", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTableApiCacheGuard::get_or_create_cache_obj()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lib_cache_->get_cache_obj(cache_ctx_, &cache_key_, cache_guard_))) {
    if (ret == OB_SQL_PC_NOT_EXIST) {
      is_use_cache_ = false;
      if (OB_FAIL(ObCacheObjectFactory::alloc(cache_guard_,
                                              ObLibCacheNameSpace::NS_TABLEAPI,
                                              lib_cache_->get_tenant_id()))) {
        LOG_WARN("fail to alloc new cache obj", K(ret));
      }
    } else {
      LOG_WARN("fail to get cache obj", K(ret), K(cache_key_));
    }
  } else {
    is_use_cache_ = true;
  }
  return ret;
}

int ObTableApiCacheGuard::get_expr_info(ObTableCtx *tb_ctx, ObExprFrameInfo *&expr_frame_info)
{
  int ret = OB_SUCCESS;
  ObTableApiCacheObj *cache_obj = static_cast<ObTableApiCacheObj *>(cache_guard_.get_cache_obj());
  if (OB_ISNULL(cache_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get cache obj", K(ret), K(is_use_cache_));
  } else {
    ObIAllocator &allocator = cache_obj->get_allocator();
    expr_frame_info = cache_obj->get_expr_frame_info();
    if (!is_use_cache_ && OB_FAIL(ObTableExprCgService::generate_exprs(*tb_ctx,
                                                                       allocator,
                                                                       *expr_frame_info))) {
      LOG_WARN("fail to generate expr", K(ret));
    } else if (!is_use_cache_ && OB_FAIL(tb_ctx->classify_scan_exprs())) {
      LOG_WARN("fail to classify scan exprs", K(ret));
    } else {
      expr_frame_info = cache_obj->get_expr_frame_info();
    }
  }
  return ret;
}

void ObTableApiCacheGuard::reset()
{
  int ret = cache_guard_.force_early_release(lib_cache_);
  if (OB_FAIL(ret)) {
    LOG_WARN("fail to release cache guard", K(ret));
  }
  cache_key_.reset();
  is_use_cache_ = false;
  lib_cache_ = nullptr;
}

int ObTableApiCacheGuard::append_column_ids(const ObITableEntity *entity,
                                            const ObTableSchema *table_schema,
                                            common::ObArray<uint64_t> &op_column_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObString> properties_names;
  if (OB_FAIL(entity->get_properties_names(properties_names))) {
    LOG_WARN("fail to get columns name", K(ret));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; i < properties_names.count(); i++) {
      if (OB_ISNULL(col_schema = table_schema->get_column_schema(properties_names.at(i)))) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("fail to get column schema", K(ret), K(i), K(properties_names.at(i)));
      } else if (OB_FAIL(op_column_ids.push_back(col_schema->get_column_id()))) {
        LOG_WARN("fail to push back column id", K(ret), K(i), K(col_schema->get_column_id()));
      }
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase
