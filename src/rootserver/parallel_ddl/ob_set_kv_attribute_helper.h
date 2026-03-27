/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_ROOTSERVER_OB_KV_ATTRIBUTE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_KV_ATTRIBUTE_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "lib/hash/ob_hashmap.h"
#include "share/table/ob_ttl_util.h"
#include "share/table/ob_table_ddl_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace rootserver
{
class ObSetKvAttributeHelper : public ObDDLHelper
{
private:
  static const char* ALTER_KV_ATTRIBUTE_FORMAT_STR;
public:
  ObSetKvAttributeHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const obrpc::ObHTableDDLArg &arg,
    obrpc::ObParallelDDLRes &res);
  virtual ~ObSetKvAttributeHelper();
private:
  virtual int check_inner_stat_() override;
  virtual int lock_objects_() override;
  int check_database_legitimacy_();
  virtual int generate_schemas_() override;
  virtual int calc_schema_version_cnt_() override;
  virtual int operate_schemas_() override;
  int lock_tablegroup_by_name_();
  int check_tablegroup_name_();
  int lock_databases_by_obj_name_();
  int lock_objects_by_id_();
  int lock_objects_by_name_();
  int lock_for_common_ddl_();
  int check_table_legitimacy_();
  int check_and_modify_kv_attr_(ObKVAttr &kv_attr, bool is_table_disable);
  virtual int init_();
  virtual int construct_and_adjust_result_(int &return_ret) override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  int construct_ddl_stmt_(const ObString &table_name,
                         const ObString &kv_attr_str,
                         ObString &ddl_stmt_str);
  const table::ObSetKvAttributeParam& get_params_() const { return static_cast<table::ObSetKvAttributeParam&>(*arg_.ddl_param_); }
private:
  const obrpc::ObHTableDDLArg &arg_;
  obrpc::ObParallelDDLRes &res_;
  uint64_t database_id_;
  common::ObSArray<ObString> table_names_;
  common::ObSArray<uint64_t> table_ids_;
  common::ObSArray<const ObTableSchema *> origin_table_schemas_;
  common::ObSArray<ObTableSchema *> new_table_schemas_;
  common::ObSArray<ObString> ddl_stmt_strs_;
  uint64_t tablegroup_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetKvAttributeHelper);

};

} // end namespace rootserver
} // end namespace oceanbase

#endif//OCEANBASE_ROOTSERVER_OB_KV_ATTRIBUTE_HELPER_H_
