/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVICE_RECOVER_TABLE_TASK_GENERATOR_H
#define OCEANBASE_ROOTSERVICE_RECOVER_TABLE_TASK_GENERATOR_H

#include "lib/ob_define.h"
#include "share/restore/ob_import_table_struct.h"
namespace oceanbase
{

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
class ObSimpleTableSchemaV2;
}
}

namespace common
{
class ObMySQLProxy;
class ObString;
}

namespace obrpc
{
class ObRecoverTableArg;
class ObPhysicalRestoreTenantArg;
}

namespace rootserver
{

class ObImportTableTaskGenerator final
{
public:
  ObImportTableTaskGenerator()
    : is_inited_(false), schema_service_(nullptr), sql_proxy_(nullptr) {}
  ~ObImportTableTaskGenerator() {}

  int init(
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy);

  int gen_import_task(
      share::ObImportTableJob &import_job,
      common::ObIArray<share::ObImportTableTask> &import_tasks);


private:
  int gen_db_import_tasks_(
      share::ObImportTableJob &import_job,
      common::ObIArray<share::ObImportTableTask> &import_tasks);
  int gen_one_db_import_tasks_(
      share::ObImportTableJob &import_job,
      const share::ObImportDatabaseItem &db_item,
      common::ObIArray<share::ObImportTableTask> &import_tasks);
  int gen_table_import_tasks_(
      share::ObImportTableJob &import_job,
      common::ObIArray<share::ObImportTableTask> &import_tasks);
  int gen_table_import_task_(
      share::ObImportTableJob &import_job,
      const share::ObImportTableItem &item,
      share::ObImportTableTask &import_task);
  int fill_import_task_from_import_db_(
      share::ObImportTableJob &import_job,
      share::schema::ObSchemaGetterGuard &guard,
      const share::ObImportDatabaseItem &db_item,
      const share::ObImportTableItem &table_item,
      const share::schema::ObTableSchema &table_schema,
      share::ObImportTableTask &import_task);
  int fill_import_task_from_import_table_(
      share::ObImportTableJob &import_job,
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObTableSchema &table_schema,
      const share::ObImportTableItem &table_item,
      share::ObImportTableTask &import_task);
  int fill_import_task_(
      share::ObImportTableJob &import_job,
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObTableSchema &table_schema,
      const share::ObImportTableItem &table_item,
      const share::ObImportTableItem &remap_table_item,
      share::ObImportTableTask &import_task);
  int check_src_table_schema_(
      share::ObImportTableJob &import_job,
      const share::schema::ObTableSchema &table_schema,
      const share::ObImportTableItem &table_item);
  int fill_common_para_(
    const share::ObImportTableJob &import_job,
    const share::schema::ObTableSchema &table_schema,
    share::ObImportTableTask &task);
  int fill_tablespace_(
    const share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &task);
  int fill_tablegroup_(
    const share::ObImportTableJob &import_job,
    share::schema::ObSchemaGetterGuard &guard,
    const share::schema::ObTableSchema &table_schema,
    const share::ObImportTableItem &table_item,
    share::ObImportTableTask &task);
  int check_target_schema_(
      share::ObImportTableJob &import_job,
      const share::ObImportTableTask &task);
private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy                       *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObImportTableTaskGenerator);
};

}
}

#endif