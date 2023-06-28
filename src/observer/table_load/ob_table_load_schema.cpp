// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_multi_version_schema_service.h"

namespace oceanbase
{
namespace observer
{
using namespace common;
using namespace share::schema;
using namespace table;
using namespace blocksstable;
using namespace sql;

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

int ObTableLoadSchema::get_table_schema(uint64_t tenant_id, uint64_t table_id,
                                        ObSchemaGetterGuard &schema_guard,
                                        const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = nullptr;
  if (OB_FAIL(get_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObTableLoadSchema::get_column_names(const ObTableSchema *table_schema, ObIAllocator &allocator,
                                        ObTableLoadArray<ObString> &column_names)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadSchema not init", KR(ret));
  } else {
    ObSEArray<ObString, 64> column_name_array;
    ObColumnIterByPrevNextID iter(*table_schema);
    const ObColumnSchemaV2 *column_schema = NULL;
    while (OB_SUCC(ret) && OB_SUCC(iter.next(column_schema))) {
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The column is null", K(ret));
      } else if (!column_schema->is_hidden()
                 && !column_schema->is_invisible_column()) {
        if (column_schema->is_virtual_generated_column()) {
        } else if (OB_FAIL(column_name_array.push_back(column_schema->get_column_name_str()))) {
          LOG_WARN("failed to push back item", K(ret));
        }
      }
    }
    if (ret != OB_ITER_END) {
      LOG_WARN("Failed to iterate all table columns. iter quit. ", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_UNLIKELY(column_name_array.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty column desc", KR(ret));
      } else if (OB_FAIL(column_names.create(column_name_array.count(), allocator))) {
        LOG_WARN("fail to create", KR(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < column_name_array.count(); ++i) {
        const ObString &column_name = column_name_array.at(i);
        if (OB_FAIL(ob_write_string(allocator, column_name, column_names[i]))) {
          LOG_WARN("fail to write string", KR(ret), K(i), K(column_name));
        }
      }
    }
  }
  return ret;
}

ObTableLoadSchema::ObTableLoadSchema()
  : allocator_("TLD_Schema"),
    is_partitioned_table_(false),
    is_heap_table_(false),
    has_autoinc_column_(false),
    has_identity_column_(false),
    rowkey_column_count_(0),
    store_column_count_(0),
    collation_type_(CS_TYPE_INVALID),
    schema_version_(0),
    is_inited_(false)
{
  column_descs_.set_block_allocator(ModulePageAllocator(allocator_));
  multi_version_column_descs_.set_block_allocator(ModulePageAllocator(allocator_));
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
  has_autoinc_column_ = false;
  has_identity_column_ = false;
  rowkey_column_count_ = 0;
  store_column_count_ = 0;
  collation_type_ = CS_TYPE_INVALID;
  schema_version_ = 0;
  column_descs_.reset();
  multi_version_column_descs_.reset();
  datum_utils_.reset();
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
    allocator_.set_tenant_id(tenant_id);
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
    has_autoinc_column_ = (table_schema->get_autoinc_column_id() != 0);
    rowkey_column_count_ = table_schema->get_rowkey_column_num();
    collation_type_ = table_schema->get_collation_type();
    schema_version_ = table_schema->get_schema_version();
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
    } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs_, rowkey_column_count_,
                                         lib::is_oracle_mode(), allocator_))) {
      LOG_WARN("fail to init datum utils", KR(ret));
    } else if (OB_FAIL(init_cmp_funcs(column_descs_, lib::is_oracle_mode()))) {
      LOG_WARN("fail to init cmp funcs", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObArray<ObTabletID> tablet_ids;
      ObArray<uint64_t> part_ids;
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
      }
    }
  }
  return ret;
}

int ObTableLoadSchema::init_cmp_funcs(const ObArray<ObColDesc> &col_descs,
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

}  // namespace observer
}  // namespace oceanbase
