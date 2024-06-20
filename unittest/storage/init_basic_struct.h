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

#ifndef OCEANBASE_STORAGE_INIT_BASIC_STRUCT_H_
#define OCEANBASE_STORAGE_INIT_BASIC_STRUCT_H_
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_tenant_info_proxy.h"
#include "logservice/palf/palf_base_info.h"
#include "share/scn.h"
namespace oceanbase
{
namespace storage
{

int __attribute__((weak))  build_test_schema(share::schema::ObTableSchema &table_schema, uint64_t table_id, const char* table_name)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 column;
  table_schema.reset();
  table_schema.set_table_name(table_name);
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_schema_version(1000);

  column.set_table_id(table_id);
  column.set_column_id(16);
  column.set_column_name("a");
  column.set_data_type(ObIntType);
  column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  column.set_rowkey_position(1);
  table_schema.set_max_used_column_id(1);
  if (OB_FAIL(table_schema.add_column(column))) {
   STORAGE_LOG(WARN, "failed to add column", KR(ret), K(column));
  }
  return ret;
}

int __attribute__((weak))  build_test_schema(share::schema::ObTableSchema &table_schema, uint64_t table_id)
{
  return build_test_schema(table_schema, table_id, "test_merge");
}

int __attribute__((weak)) gen_create_ls_arg(const int64_t tenant_id,
    const share::ObLSID &ls_id,
    obrpc::ObCreateLSArg &arg)
{
  int ret = OB_SUCCESS;
  ObReplicaType replica_type = REPLICA_TYPE_FULL;
  ObReplicaProperty property;
  share::ObAllTenantInfo tenant_info;
  const share::SCN create_scn = share::SCN::base_scn();
  arg.reset();
  lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
  palf::PalfBaseInfo palf_base_info;
  if (OB_FAIL(tenant_info.init(tenant_id, share::PRIMARY_TENANT_ROLE))) {
    STORAGE_LOG(WARN, "failed to init tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(arg.init(tenant_id, ls_id, replica_type, property, tenant_info, create_scn, compat_mode, false, palf_base_info))) {
   STORAGE_LOG(WARN, "failed to init arg", KR(ret), K(tenant_id), K(ls_id), K(tenant_info), K(create_scn), K(compat_mode), K(palf_base_info));
  }
  return ret;
}

int __attribute__((weak)) gen_create_tablet_arg(const int64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    obrpc::ObBatchCreateTabletArg &arg,
    const int64_t count = 1,
    share::schema::ObTableSchema *out_table_schema = nullptr)
{
  int ret = OB_SUCCESS;
  obrpc::ObCreateTabletInfo tablet_info;
  ObArray<common::ObTabletID> index_tablet_ids;
  ObArray<int64_t> index_tablet_schema_idxs;
  uint64_t table_id = 12345;
  arg.reset();
  share::schema::ObTableSchema table_schema_obj;
  share::schema::ObTableSchema *table_schema_ptr = nullptr;
  if (out_table_schema != nullptr) {
    table_schema_ptr = out_table_schema;
  } else {
    table_schema_ptr = &table_schema_obj;
  }
  share::schema::ObTableSchema &table_schema = *table_schema_ptr;
  if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    STORAGE_LOG(WARN, "failed to build test table schema", KR(ret), K(table_id));
  }

  for(int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    ObTabletID tablet_id_insert(tablet_id.id() + i);
    if (OB_FAIL(index_tablet_ids.push_back(tablet_id_insert))) {
      STORAGE_LOG(WARN, "failed to push back tablet id", KR(ret), K(tablet_id_insert));
    } else if (OB_FAIL(index_tablet_schema_idxs.push_back(0))) {
      STORAGE_LOG(WARN, "failed to push back index id", KR(ret));
    }
  }


  if (FAILEDx(tablet_info.init(index_tablet_ids,
          tablet_id,
          index_tablet_schema_idxs,
          lib::Worker::CompatMode::MYSQL,
          false))) {
    STORAGE_LOG(WARN, "failed to init tablet info", KR(ret), K(index_tablet_ids),
        K(tablet_id), K(index_tablet_schema_idxs));
  } else if (OB_FAIL(arg.init_create_tablet(ls_id, share::SCN::min_scn(), false/*need_check_tablet_cnt*/))) {
    STORAGE_LOG(WARN, "failed to init create tablet", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(arg.table_schemas_.push_back(table_schema))) {
    STORAGE_LOG(WARN, "failed to push back table schema", KR(ret), K(table_schema));
  } else if (OB_FAIL(arg.tablets_.push_back(tablet_info))) {
    STORAGE_LOG(WARN, "failed to push back tablet info", KR(ret), K(tablet_info));
  }
  return ret;
}
}//end namespace storage
}//end namespace oceanbase

#endif //OCEANBASE_STORAGE_INIT_BASIC_STRUCT_H_
