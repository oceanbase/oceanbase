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

#ifndef OCEANBASE_ROOTSERVICE_RECOVER_TABLE_INITIATOR_H
#define OCEANBASE_ROOTSERVICE_RECOVER_TABLE_INITIATOR_H

#include "lib/ob_define.h"
#include "share/restore/ob_import_table_struct.h"
#include "rootserver/restore/ob_import_table_task_generator.h"
namespace oceanbase
{

namespace share
{
struct ObPhysicalRestoreJob;
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
class ObRecoverTableInitiator final
{
public:
  ObRecoverTableInitiator()
    : is_inited_(false), schema_service_(nullptr), sql_proxy_(nullptr) {}
  ~ObRecoverTableInitiator() {}

  int init(share::schema::ObMultiVersionSchemaService *schema_service, common::ObMySQLProxy *sql_proxy);
  int initiate_recover_table(const obrpc::ObRecoverTableArg &arg);
  int is_recover_job_exist(const uint64_t tenant_id, bool &is_exist) const;

private:
  int start_recover_table_(const obrpc::ObRecoverTableArg &arg);
  int cancel_recover_table_(const obrpc::ObRecoverTableArg &arg);
  int check_before_initiate_(const obrpc::ObRecoverTableArg &arg);
  int fill_aux_tenant_name_(share::ObRecoverTableJob &job);
  int fill_aux_tenant_restore_info_(
      const obrpc::ObRecoverTableArg &arg,
      share::ObRecoverTableJob &job,
      share::ObPhysicalRestoreJob &physical_restore_job);
  int fill_recover_table_arg_(
      const obrpc::ObRecoverTableArg &arg, share::ObRecoverTableJob &job);
  int insert_sys_job_(share::ObRecoverTableJob &job, share::ObPhysicalRestoreJob &physical_restore_job);
  int fill_recover_database(const share::ObImportArg &import_arg, share::ObImportTableArg &import_table_arg);
  int fill_recover_table(const share::ObImportArg &import_arg, share::ObImportTableArg &import_table_arg);
  int fill_recover_partition(const share::ObImportArg &import_arg, share::ObImportTableArg &import_table_arg);
  int fill_remap_database(const share::ObImportArg &import_arg,
      const share::ObImportTableArg &import_table_arg,
      share::ObImportRemapArg &import_remap_arg);
  int fill_remap_table(const share::ObImportArg &import_arg,
      const share::ObImportTableArg &import_table_arg,
      share::ObImportRemapArg &import_remap_arg);
  int fill_remap_partition(const share::ObImportArg &import_arg,
      const share::ObImportTableArg &import_table_arg,
      share::ObImportRemapArg &import_remap_arg);
  int fill_remap_tablespace(const share::ObImportArg &import_arg,
      share::ObImportRemapArg &import_remap_arg);
  int fill_remap_tablegroup(const share::ObImportArg &import_arg,
      share::ObImportRemapArg &import_remap_arg);

private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy                       *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObRecoverTableInitiator);
};

}
}

#endif