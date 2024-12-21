/**
 * Copyright (c) 2024 OceanBase
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
#include "ob_redis_context.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::rpc;

///////////////////////////////////////////////////////////////////////////////////
bool ObRedisCtx::valid() const
{
  bool valid = true;

  if (OB_ISNULL(trans_param_) || OB_ISNULL(entity_factory_) || OB_ISNULL(credential_)) {
    valid = false;
    int ret = OB_ERR_UNEXPECTED;
    LOG_WARN("redis ctx is not valid", KP_(trans_param), KP_(entity_factory), KP_(credential));
  }
  return valid;
}

int ObRedisSingleCtx::decode_request()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(request_.decode())) {
    LOG_WARN("fail to decode request", K(ret));
  }

  return ret;
}

int ObRedisCtx::init_table_info(
    ObRowkey &rowkey,
    ObSchemaGetterGuard &schema_guard,
    ObTableApiSessGuard &sess_guard,
    const ObString &table_name,
    const ObIArray<ObString> &keys,
    ObRedisTableInfo *&tb_info,
    bool &is_in_same_ls,
    ObLSID &last_ls_id)
{
  int ret = OB_SUCCESS;
  const ObSimpleTableSchemaV2 *simple_table_schema = nullptr;
  ObKvSchemaCacheGuard *schema_cache_guard = nullptr;
  ObTabletID tablet_id;

  if (!rowkey.is_valid() || rowkey.length() < 2) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rowkey", K(ret), K(rowkey));
  } else if (OB_ISNULL(tb_info = OB_NEWx(ObRedisTableInfo, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObRedisTableInfo", K(ret));
  } else if (OB_ISNULL(schema_cache_guard = OB_NEWx(ObKvSchemaCacheGuard, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObKvSchemaCacheGuard", K(ret));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(credential_->tenant_id_,
                                                          credential_->database_id_,
                                                          table_name,
                                                          false, /* is_index */
                                                          simple_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(credential_->tenant_id_),
              K(credential_->database_id_), K(table_name));
  } else if (OB_ISNULL(simple_table_schema) || simple_table_schema->get_table_id() == OB_INVALID_ID) {
    ret = OB_ERR_UNKNOWN_TABLE;
    ObString db("");
    LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE, table_name.length(), table_name.ptr(), db.length(), db.ptr());
    LOG_WARN("table not exist", K(ret), K(credential_->tenant_id_), K(credential_->database_id_), K(table_name));
  } else if (OB_FAIL(schema_cache_guard->init(credential_->tenant_id_,
                                              simple_table_schema->get_table_id(),
                                              simple_table_schema->get_schema_version(),
                                              schema_guard))) {
    LOG_WARN("fail to init schema cache guard", K(ret));
  } else if (OB_FAIL(tb_info->tablet_ids_.reserve(keys.count()))) {
    LOG_WARN("fail to reserve space", K(ret), K(keys.count()));
  }

  ObTabletID cur_tablet_id;
  is_in_same_ls = false;
  ObArenaAllocator alloc("RedisCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  for (int i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    // partition by <db, key>
    rowkey.get_obj_ptr()[ObRedisUtil::COL_IDX_RKEY].set_varbinary(keys.at(i));
    cur_tablet_id.reset();
    if (OB_FAIL(get_tablet_id(rowkey, *simple_table_schema, sess_guard, schema_guard, cur_tablet_id))) {
      LOG_WARN("fail to get tablet id and ls id", K(ret));
    } else if (OB_FAIL(tb_info->tablet_ids_.push_back(cur_tablet_id))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(cur_tablet_id));
    } else {
      // get ls
      bool is_cache_hit = false;
      int64_t expire_renew_time = 0; // not refresh ls location cache
      ObLSID ls_id;
      if (OB_FAIL(GCTX.location_service_->get(MTL_ID(), cur_tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
        LOG_WARN("fail to get ls id", K(ret), K(is_cache_hit));
      } else if (!last_ls_id.is_valid()) {
        is_in_same_ls = true;
      } else if (is_in_same_ls && last_ls_id != ls_id) {
        is_in_same_ls = false;
      }
      if (OB_SUCC(ret)) {
        last_ls_id = ls_id;
      }
    }
  }


  if (OB_SUCC(ret)) {
    tb_info->schema_cache_guard_ = schema_cache_guard;
    tb_info->simple_schema_ = simple_table_schema;
    tb_info->table_name_ = table_name;
  }

  return ret;
}

// rowkey is 'partition by (rowkey)'
int ObRedisCtx::get_tablet_id(const ObRowkey &rowkey,
                              const ObSimpleTableSchemaV2 &simple_table_schema,
                              ObTableApiSessGuard &sess_guard,
                              ObSchemaGetterGuard &schema_guard,
                              ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRowkey, 1> rowkeys;
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObObjectID, 1> part_ids;
  SMART_VAR(sql::ObTableLocation, location_calc) {
    const uint64_t tenant_id = MTL_ID();
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("fail to push back rowkey", K(ret), K(rowkey));
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(sess_guard.get_sess_info(),
                                                                      schema_guard,
                                                                      simple_table_schema.get_table_id(),
                                                                      rowkeys,
                                                                      tablet_ids,
                                                                      part_ids))) {
      LOG_WARN("fail to calc partition id", K(ret));
    } else if (1 != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("should have one tablet", K(ret), K(tablet_ids));
    } else {
      tablet_id = tablet_ids.at(0);
    }
  }
  return ret;
}

int ObRedisCtx::init_cmd_ctx(int db, const ObString &table_name, const ObIArray<ObString> &keys)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *ret_ctx = nullptr;
  ObSchemaGetterGuard *schema_guard = redis_guard_.schema_guard_;
  ObTableApiSessGuard *sess_guard = redis_guard_.sess_guard_;
  ObArenaAllocator tmp_allocator;
  ObObj *obj_ptr = nullptr;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(credential_) || OB_ISNULL(schema_guard) || OB_ISNULL(sess_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service, credential, schema guard or sess guard",
      K(ret),
      K(GCTX.schema_service_),
      K(credential_),
      K(schema_guard),
      K(sess_guard));
  } else if (OB_ISNULL(ret_ctx = OB_NEWx(ObRedisCmdCtx, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObRedisCmdCtx", K(ret));
  } else if (OB_ISNULL(
      obj_ptr = static_cast<ObObj *>(tmp_allocator.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else if (OB_FAIL(reset_objects(obj_ptr, 2))) {
    LOG_WARN("fail to init object", K(ret));
  } else {
    obj_ptr[0].set_int(db);
    ObRowkey rowkey(obj_ptr, 2);
    ObRedisTableInfo *tb_info = nullptr;
    bool is_in_same_ls = false;
    ObLSID last_ls_id(ObLSID::INVALID_LS_ID);
    if (OB_FAIL(init_table_info(rowkey, *schema_guard, *sess_guard, table_name, keys, tb_info, is_in_same_ls, last_ls_id))) {
      LOG_WARN("fail to init table info", K(ret), K(rowkey));
    } else if (OB_FAIL(ret_ctx->add_info(tb_info))) {
      LOG_WARN("fail to add model info", K(ret), K(tb_info));
    } else {
      ret_ctx->set_in_same_ls(is_in_same_ls);
      cmd_ctx_ = ret_ctx;
    }
  }
  return ret;
}

// for generic command
//  args: <key1, key2, ...>
//  cur_rowkey: <db, key>
int ObRedisCtx::init_cmd_ctx(ObRowkey &cur_rowkey, const ObIArray<ObString> &keys)
{
  int ret = OB_SUCCESS;
  ObRedisCmdCtx *ret_ctx = nullptr;
  ObSchemaGetterGuard *schema_guard = redis_guard_.schema_guard_;
  ObTableApiSessGuard *sess_guard = redis_guard_.sess_guard_;
  if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(credential_) || OB_ISNULL(schema_guard) || OB_ISNULL(sess_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid schema service, credential, schema guard or sess guard",
      K(ret),
      K(GCTX.schema_service_),
      K(credential_),
      K(schema_guard),
      K(sess_guard));
  } else if (OB_ISNULL(ret_ctx = OB_NEWx(ObRedisCmdCtx, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObRedisCmdCtx", K(ret));
  }

  bool is_in_same_ls = true;
  ObLSID last_ls_id(ObLSID::INVALID_LS_ID);
  for (int i = ObRedisModel::STRING; OB_SUCC(ret) && i < ObRedisModel::INVALID; ++i) {
    ObRedisTableInfo *tb_info = nullptr;
    bool cur_is_in_same_ls = false;
    ObString table_name;
    if (OB_FAIL(ObRedisHelper::get_table_name_by_model(static_cast<ObRedisModel>(i), table_name))) {
      LOG_WARN("fail to get table name by model", K(ret), K(static_cast<ObRedisModel>(i)));
    } else if (OB_FAIL(init_table_info(cur_rowkey, *schema_guard, *sess_guard, table_name, keys, tb_info, cur_is_in_same_ls, last_ls_id))) {
      LOG_WARN("fail to init table info", K(ret), K(i));
    } else {
      if (!cur_is_in_same_ls) {
        is_in_same_ls = false;
      }
      if (OB_FAIL(ret_ctx->add_info(tb_info))) {
        LOG_WARN("fail to add model info", K(ret), K(tb_info));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (!ret_ctx->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("init ret ctx failed", K(ret));
    } else {
      ret_ctx->set_in_same_ls(is_in_same_ls);
      cmd_ctx_ = ret_ctx;
    }
  }
  return ret;
}

// may return null
int ObRedisCtx::try_get_table_info(ObRedisTableInfo *&tb_info)
{
  int ret = OB_SUCCESS;
  tb_info = nullptr;
  if (OB_ISNULL(cmd_ctx_) || cur_table_idx_ == -1) {
    // do nothing
  } else if (OB_FAIL(cmd_ctx_->get_info(cur_table_idx_, tb_info))) {
    LOG_WARN("fail to get info", K(ret), K(cur_table_idx_));
  }
  return ret;
}

int ObRedisCtx::reset_objects(common::ObObj *objs, int64_t obj_cnt)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(objs)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null objs", K(ret), K(obj_cnt));
  } else if (obj_cnt == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("obj_cnt is zero", K(ret));
  } else {
    ObObj *ptr = objs;
    for (int64_t i = 0; i < obj_cnt; i++) {
      ptr = new (ptr) ObObj();
      ptr++;
    }
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////////

int ObRedisCmdCtx::add_info(ObRedisTableInfo *tb_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tb_infos_.push_back(tb_info))) {
    LOG_WARN("fail to push back", K(ret), K(tb_info));
  }
  return ret;
}

int ObRedisCmdCtx::get_info(int64_t idx, ObRedisTableInfo *&tb_info)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || idx >= tb_infos_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to get info", K(ret), K(idx), K(is_valid()), K(tb_infos_.count()));
  } else {
    tb_info = tb_infos_.at(idx);
    if (OB_ISNULL(tb_info)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("invalid null tb_info", K(ret), K(idx));
    }
  }
  return ret;
}
