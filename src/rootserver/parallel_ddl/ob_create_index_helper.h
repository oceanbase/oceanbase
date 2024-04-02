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
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_INDEX_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_INDEX_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "rootserver/ob_index_builder.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
class ObCreateIndexArg;
class ObAlterTableRes;
}
namespace rootserver
{
class ObCreateIndexHelper : public ObDDLHelper
{
public:
  ObCreateIndexHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    rootserver::ObDDLService &ddl_service,
    const obrpc::ObCreateIndexArg &arg,
    obrpc::ObAlterTableRes &res);
  virtual ~ObCreateIndexHelper();
  virtual int execute() override;
private:
  int lock_objects_();
  int lock_database_by_obj_name_();
  int lock_objects_by_name_();
  int check_database_legitimacy_();
  int lock_objects_by_id_();
  int check_table_legitimacy_();
  int generate_index_schema_();
  int calc_schema_version_cnt_();
  int create_index_();
  int is_local_generate_schema_(bool &is_local_generate);
  int create_table_();
  int create_tablets_();
  int add_index_name_to_cache_();
  int check_fk_related_table_ddl_(const share::schema::ObTableSchema &data_table_schema,
                                  const share::ObDDLType &ddl_type);
private:
  const obrpc::ObCreateIndexArg &arg_;
  obrpc::ObCreateIndexArg *new_arg_;
  obrpc::ObAlterTableRes &res_;
  uint64_t database_id_;
  const ObTableSchema *orig_data_table_schema_;
  ObTableSchema* new_data_table_schema_;
  ObArray<ObTableSchema> index_schemas_;
  ObSEArray<ObColumnSchemaV2*, 1> gen_columns_;
  ObIndexBuilder index_builder_;
  ObDDLTaskRecord task_record_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateIndexHelper);
};
}
}

#endif//OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_HELPER_H_