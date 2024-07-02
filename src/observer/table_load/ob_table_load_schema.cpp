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

#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/lob/ob_lob_meta.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace table;
using namespace blocksstable;
using namespace sql;
using namespace storage;

int ObTableLoadSchema::get_schema_guard(uint64_t tenant_id, ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLoadSchema::get_table_schema(uint64_t tenant_id, uint64_t database_id,
                                        const ObString &table_name,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  if (OB_FAIL(get_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, database_id, table_name, false,
                                                   table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(database_id), K(table_name));
  }
  return ret;
}

int ObTableLoadSchema::get_table_id(uint64_t tenant_id, uint64_t database_id,
                                    const ObString &table_name, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_table_schema(tenant_id, database_id, table_name, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(database_id), K(table_name));
  } else {
    table_id = table_schema->get_table_id();
  }
  return ret;
}

int ObTableLoadSchema::get_table_schema(uint64_t tenant_id, uint64_t table_id,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  bool get_table_schema_succ = false;
  const int64_t MAX_RETRY_COUNT = 10;
  for (int64_t i = 0; OB_SUCC(ret) && (!get_table_schema_succ) && (i < MAX_RETRY_COUNT); ++i) {
    if (OB_FAIL(get_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      const int64_t RESERVED_TIME_US = 600 * 1000; // 600 ms
      const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
      const int64_t idle_time_us = 200 * 1000 * (i + 1);
      if (timeout_remain_us - idle_time_us > RESERVED_TIME_US) {
        LOG_WARN("fail to get table schema, will retry", KR(ret), K(i), K(tenant_id), K(table_id),
                 K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
        USLEEP(idle_time_us);
        ret = OB_SUCCESS;
      } else {
        ret = OB_TIMEOUT;
        LOG_WARN("fail to get table schema, will not retry cuz timeout_remain is not enough",
                 KR(ret), K(i), K(tenant_id), K(table_id), K(timeout_remain_us), K(idle_time_us),
                 K(RESERVED_TIME_US));
      }
    } else {
      get_table_schema_succ = true;
    }
  }
  if (OB_SUCC(ret) && !get_table_schema_succ) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTableLoadSchema::get_user_column_schemas(const ObTableSchema *table_schema,
                                               ObIArray<const ObColumnSchemaV2 *> &column_schemas)
{
  int ret = OB_SUCCESS;
  column_schemas.reset();
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    ObColumnIterByPrevNextID iter(*table_schema);
    const ObColumnSchemaV2 *column_schema = NULL;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.next(column_schema))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to iterate all table columns", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is null", KR(ret));
      } else if (column_schema->is_hidden()) {
        // 不显示隐藏主键列
      } else if (column_schema->is_unused()) {
        // 不显示快速删除列
      } else if (column_schema->is_shadow_column()) {
        // 不显示shadow列
      } else if (OB_FAIL(column_schemas.push_back(column_schema))) {
        LOG_WARN("fail to push back column schema", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::get_user_column_schemas(ObSchemaGetterGuard &schema_guard,
                                               uint64_t tenant_id,
                                               uint64_t table_id,
                                               ObIArray<const ObColumnSchemaV2 *> &column_schemas)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else {
    ret = get_user_column_schemas(table_schema, column_schemas);
  }
  return ret;
}

int ObTableLoadSchema::get_user_column_ids(ObSchemaGetterGuard &schema_guard,
                                           uint64_t tenant_id,
                                           uint64_t table_id,
                                           ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  ObArray<const ObColumnSchemaV2 *> column_schemas;
  if (OB_FAIL(get_user_column_schemas(schema_guard, tenant_id, table_id, column_schemas))) {
    LOG_WARN("fail to get user column schemas", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_schemas.count(); ++i) {
    const ObColumnSchemaV2 *column_schema = column_schemas.at(i);
    if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("fail to push back column id", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadSchema::get_user_column_count(ObSchemaGetterGuard &schema_guard,
                                             uint64_t tenant_id,
                                             uint64_t table_id,
                                             int64_t &column_count)
{
  int ret = OB_SUCCESS;
  column_count = 0;
  ObArray<const ObColumnSchemaV2 *> column_schemas;
  if (OB_FAIL(get_user_column_schemas(schema_guard, tenant_id, table_id, column_schemas))) {
    LOG_WARN("fail to get user column schemas", KR(ret));
  } else {
    column_count = column_schemas.count();
  }
  return ret;
}

int ObTableLoadSchema::get_column_ids(ObSchemaGetterGuard &schema_guard,
                                      uint64_t tenant_id,
                                      uint64_t table_id,
                                      ObIArray<uint64_t> &column_ids,
                                      bool contain_hidden_pk_column)
{
  int ret = OB_SUCCESS;
  column_ids.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_id));
  } else {
    const ObTableSchema *table_schema = nullptr;
    ObArray<ObColDesc> column_descs;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
      STORAGE_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (ObColumnSchemaV2::is_hidden_pk_column_id(col_schema->get_column_id()) &&
                 !contain_hidden_pk_column) {
      } else if (OB_FAIL(column_ids.push_back(col_schema->get_column_id()))) {
        LOG_WARN("failed to push back column id", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::check_has_udt_column(const ObTableSchema *table_schema, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
       OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
    ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid column schema", K(column_schema));
    } else if (column_schema->get_udt_set_id() > 0) {
      bret = true;
      break;
    }
  }
  return ret;
}

int ObTableLoadSchema::get_tenant_optimizer_gather_stats_on_load(const uint64_t tenant_id,
                                                                 bool &value)
{
  int ret = OB_SUCCESS;
  value = false;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult *result = nullptr;
    // TODO(suzhi.yt) 这里为啥是带zone纬度的? 如果查询结果中有多个zone的, 选哪个作为返回值呢?
    if (OB_FAIL(sql.assign_fmt(
          "SELECT value FROM %s WHERE tenant_id = %ld and (zone, name, schema_version) in (select "
          "zone, name, max(schema_version) FROM %s group by zone, name) and name = '%s'",
          OB_ALL_SYS_VARIABLE_HISTORY_TNAME,
          ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
          OB_ALL_SYS_VARIABLE_HISTORY_TNAME, OB_SV__OPTIMIZER_GATHER_STATS_ON_LOAD))) {
      LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get sql result", KR(ret), K(sql), K(tenant_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("fail to get next row", KR(ret), K(tenant_id));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          ObString data;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", data);
          if (0 == strcmp(data.ptr(), "1")) {
            value = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::check_has_invisible_column(const ObTableSchema *table_schema, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
       OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
    ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid column schema", K(column_schema));
    } else if (column_schema->is_invisible_column()) {
      bret = true;
      break;
    }
  }
  return ret;
}

int ObTableLoadSchema::check_has_unused_column(const ObTableSchema *table_schema, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
       OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
    ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid column schema", K(column_schema));
    } else if (column_schema->is_unused()) {
      bret = true;
      break;
    }
  }
  return ret;
}

int ObTableLoadSchema::check_has_lob_column(const ObTableSchema *table_schema, bool &bret)
{
  int ret = OB_SUCCESS;
  bret = false;
  for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
       OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
    ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid column schema", K(column_schema));
    } else if (column_schema->get_meta_type().is_lob_storage()) {
      bret = true;
      break;
    }
  }
  return ret;
}

int ObTableLoadSchema::get_table_compressor_type(uint64_t tenant_id, uint64_t table_id,
                                                 ObCompressorType &compressor_type)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else {
    compressor_type = table_schema->get_compressor_type();
  }
  return ret;
}

ObTableLoadSchema::ObTableLoadSchema()
  : allocator_("TLD_Schema"),
    is_partitioned_table_(false),
    is_heap_table_(false),
    is_column_store_(false),
    has_autoinc_column_(false),
    has_identity_column_(false),
    rowkey_column_count_(0),
    store_column_count_(0),
    collation_type_(CS_TYPE_INVALID),
    part_level_(PARTITION_LEVEL_ZERO),
    schema_version_(0),
    lob_meta_table_id_(OB_INVALID_ID),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  lob_column_idxs_.set_block_allocator(ModulePageAllocator(allocator_));
  column_descs_.set_block_allocator(ModulePageAllocator(allocator_));
  multi_version_column_descs_.set_block_allocator(ModulePageAllocator(allocator_));
  lob_meta_column_descs_.set_block_allocator(ModulePageAllocator(allocator_));
}

ObTableLoadSchema::~ObTableLoadSchema()
{
  reset();
}

void ObTableLoadSchema::reset()
{
  table_name_.reset();
  is_partitioned_table_ = false;
  is_heap_table_ = false;
  is_column_store_ = false;
  has_autoinc_column_ = false;
  has_identity_column_ = false;
  rowkey_column_count_ = 0;
  store_column_count_ = 0;
  collation_type_ = CS_TYPE_INVALID;
  part_level_ = PARTITION_LEVEL_ZERO;
  schema_version_ = 0;
  lob_meta_table_id_ = OB_INVALID_ID;
  lob_column_idxs_.reset();
  column_descs_.reset();
  multi_version_column_descs_.reset();
  datum_utils_.reset();
  lob_meta_column_descs_.reset();
  lob_meta_datum_utils_.reset();
  cmp_funcs_.reset();
  partition_ids_.reset();
  allocator_.reset();
  is_inited_ = false;
}

int ObTableLoadSchema::init(uint64_t tenant_id, uint64_t table_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadSchema init twice", KR(ret));
  } else {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(get_table_schema(tenant_id, table_id, schema_guard, table_schema))) {
      LOG_WARN("fail to get database and table schema", KR(ret), K(tenant_id));
    } else if (OB_FAIL(init_table_schema(table_schema))) {
      LOG_WARN("fail to init table schema", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObTableLoadSchema::init_table_schema(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    is_partitioned_table_ = table_schema->is_partitioned_table();
    is_heap_table_ = table_schema->is_heap_table();
    is_column_store_ = (table_schema->get_column_group_count() > 1) ? true :false;
    has_autoinc_column_ = (table_schema->get_autoinc_column_id() != 0);
    rowkey_column_count_ = table_schema->get_rowkey_column_num();
    collation_type_ = table_schema->get_collation_type();
    part_level_ = table_schema->get_part_level();
    schema_version_ = table_schema->get_schema_version();
    if (table_schema->has_lob_aux_table()) {
      lob_meta_table_id_ = table_schema->get_aux_lob_meta_tid();
    }
    if (OB_FAIL(ObTableLoadUtils::deep_copy(table_schema->get_table_name_str(), table_name_,
                                            allocator_))) {
      LOG_WARN("fail to deep copy table name", KR(ret));
    } else if (OB_FAIL(table_schema->get_store_column_count(store_column_count_))) {
      LOG_WARN("fail to get store column count", KR(ret));
    } else if (OB_FAIL(table_schema->get_column_ids(column_descs_, false))) {
      LOG_WARN("fail to get column descs", KR(ret));
    } else if (OB_FAIL(prepare_col_desc(table_schema, column_descs_))) {
      LOG_WARN("fail to prepare column descs", KR(ret));
    } else if (OB_FAIL(table_schema->get_multi_version_column_descs(multi_version_column_descs_))) {
      LOG_WARN("fail to get multi version column descs", KR(ret));
    } else if (OB_FAIL(update_decimal_int_precision(table_schema, multi_version_column_descs_))){
      LOG_WARN("update decimal int precision failed", K(ret));
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs_, rowkey_column_count_,
                                         lib::is_oracle_mode(), allocator_))) {
      LOG_WARN("fail to init datum utils", KR(ret));
    } else if (OB_FAIL(init_lob_storage(column_descs_))) {
      LOG_WARN("fail to check lob storage", KR(ret));
    } else if (OB_FAIL(gen_lob_meta_datum_utils())) {
      LOG_WARN("fail to gen lob meta datum utils", KR(ret));
    } else if (OB_FAIL(init_cmp_funcs(column_descs_, lib::is_oracle_mode()))) {
      LOG_WARN("fail to init cmp funcs", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObArray<ObTabletID> tablet_ids;
      ObArray<uint64_t> part_ids;
      tablet_ids.set_tenant_id(MTL_ID());
      part_ids.set_tenant_id(MTL_ID());
      if (OB_FAIL(table_schema->get_all_tablet_and_object_ids(tablet_ids, part_ids))) {
        LOG_WARN("fail to get all tablet ids", KR(ret));
      } else if (OB_FAIL(partition_ids_.create(part_ids.count(), allocator_))) {
        LOG_WARN("fail to create array", KR(ret));
      } else {
        for (int64_t i = 0; i < part_ids.count(); ++i) {
          partition_ids_[i].partition_id_ = part_ids.at(i);
          partition_ids_[i].tablet_id_ = tablet_ids.at(i);
        }
      }
      for (ObTableSchema::const_column_iterator iter = table_schema->column_begin();
          OB_SUCC(ret) && iter != table_schema->column_end(); ++iter) {
        ObColumnSchemaV2 *column_schema = *iter;
        if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid column schema", K(column_schema));
        } else {
          uint64_t column_id = column_schema->get_column_id();
          if (column_schema->is_identity_column() && column_id != OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
            has_identity_column_ = true;
            break;
          }
        }
      }//end for
    }
  }
  return ret;
}

int ObTableLoadSchema::init_lob_storage(common::ObIArray<share::schema::ObColDesc> &column_descs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_descs.count(); ++i) {
    const ObColDesc &col_desc = column_descs.at(i);
    if (col_desc.col_type_.is_lob_storage()) {
      column_descs.at(i).col_type_.set_has_lob_header();
      if (OB_FAIL(lob_column_idxs_.push_back(i))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  LOG_INFO("ObTableLoadSchema::init_lob_storage", K(lob_column_idxs_));

  return ret;
}

int ObTableLoadSchema::init_cmp_funcs(const ObIArray<ObColDesc> &col_descs,
                                      const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const bool is_null_last = is_oracle_mode;
  ObCmpFunc cmp_func;
  if (OB_FAIL(cmp_funcs_.init(col_descs.count(), allocator_))) {
    LOG_WARN("fail to init cmp funcs array", KR(ret), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    const ObColDesc &col_desc = col_descs.at(i);
    const bool has_lob_header = is_lob_storage(col_desc.col_type_.get_type());
    ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
      col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type(),
      col_desc.col_type_.get_scale(), is_oracle_mode, has_lob_header);
    if (OB_UNLIKELY(nullptr == basic_funcs || nullptr == basic_funcs->null_last_cmp_ ||
                    nullptr == basic_funcs->murmur_hash_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("Unexpected null basic funcs", KR(ret), K(col_desc));
    } else {
      cmp_func.cmp_func_ =
        is_null_last ? basic_funcs->null_last_cmp_ : basic_funcs->null_first_cmp_;
      if (OB_FAIL(cmp_funcs_.push_back(ObStorageDatumCmpFunc(cmp_func)))) {
        LOG_WARN("Failed to push back cmp func", KR(ret), K(i), K(col_desc));
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::update_decimal_int_precision(
  const share::schema::ObTableSchema *table_schema,
  common::ObIArray<share::schema::ObColDesc> &cols_desc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null table schema");
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int i = 0; i < cols_desc.count(); i++) {
      if (OB_ISNULL(col_schema = table_schema->get_column_schema(cols_desc.at(i).col_id_))) {
        // do nothing
      } else if (col_schema->is_decimal_int()) {
        cols_desc.at(i).col_type_.set_stored_precision(col_schema->get_accuracy().get_precision());
        cols_desc.at(i).col_type_.set_scale(col_schema->get_accuracy().get_scale());
      }
    } // end for
  }
  return ret;
}

int ObTableLoadSchema::prepare_col_desc(const ObTableSchema *table_schema, common::ObIArray<share::schema::ObColDesc> &col_descs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
      ObColDesc &col_desc = col_descs.at(i);
      const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid column schema", K(column_schema));
      } else {
        col_desc.col_type_.set_scale(column_schema->get_data_scale());
        if (col_desc.col_type_.is_lob_storage() && (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0)) {
          col_desc.col_type_.set_has_lob_header();
        }
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::gen_lob_meta_datum_utils()
{
  int ret = OB_SUCCESS;
  ObColDesc col_desc1;
  col_desc1.col_id_ = ObLobMetaUtil::LOB_ID_COL_ID;
  col_desc1.col_type_.set_varbinary();
  col_desc1.col_order_ = ObOrderType::ASC;
  ObColDesc col_desc2;
  col_desc2.col_id_ = ObLobMetaUtil::SEQ_ID_COL_ID;
  col_desc2.col_type_.set_varbinary();
  col_desc2.col_order_ = ObOrderType::ASC;
  if (OB_FAIL(lob_meta_column_descs_.push_back(col_desc1))) {
    LOG_WARN("fail to push back col_desc", KR(ret));
  } else if (OB_FAIL(lob_meta_column_descs_.push_back(col_desc2))) {
    LOG_WARN("fail to push back col_desc", KR(ret));
  } else if (OB_FAIL(lob_meta_datum_utils_.init(lob_meta_column_descs_, ObLobMetaUtil::LOB_META_SCHEMA_ROWKEY_COL_CNT, false, allocator_))) {
    LOG_WARN("fail to init", KR(ret));
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
