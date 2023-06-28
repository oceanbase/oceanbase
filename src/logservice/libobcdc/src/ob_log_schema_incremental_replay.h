/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_LIBOBCDC_LOG_SCHEMA_INCREMENTAL_REPALYER_H_
#define OCEANBASE_LIBOBCDC_LOG_SCHEMA_INCREMENTAL_REPALYER_H_

#include "lib/utility/ob_print_utils.h"         // TO_STRING_KV
#include "lib/utility/ob_macro_utils.h"         // DISALLOW_COPY_AND_ASSIGN
#include "ob_log_meta_data_struct.h"            // ObDictTenantInfo
#include "ob_log_part_trans_task.h"             // PartTransTask

namespace oceanbase
{
namespace libobcdc
{
class ObLogSchemaIncReplay
{
public:
  ObLogSchemaIncReplay();
  ~ObLogSchemaIncReplay();

public:
  // ObLogSchemaIncReplay init function
  // An incremental schema refresh can be triggered at two points:
  // 1. When CDCConnector is started in data dictionary refresh mode, in order to obtain accurate MetaData information of startup time,
  //    it is necessary to obtain the corresponding baseline dictionary data and incremental data incremental data will be replayed.
  // 2. During the running of CDCConnector, DDL transaction will be replayed.
  //
  // @param [in]   is_start_progress Mark whether it is a start process
  //
  // @retval OB_SUCCESS          Init success
  // @retval other_err_code      Unexpected error
  int init(const bool is_start_progress);
  void destroy();
  int replay(
      const PartTransTask &part_trans_task,
      const ObIArray<const datadict::ObDictTenantMeta*> &tenant_metas,
      const ObIArray<const datadict::ObDictDatabaseMeta*> &database_metas,
      const ObIArray<const datadict::ObDictTableMeta*> &table_metas,
      ObDictTenantInfo &tenant_info);

  TO_STRING_KV(
      K_(is_inited),
      K_(is_start_progress));

private:
  // Replays all the incremental tenant metas information
  int replay_tenant_metas_(
      const transaction::ObTransID &trans_id,
      const ObIArray<const datadict::ObDictTenantMeta*> &tenant_metas,
      ObDictTenantInfo &tenant_info);
  // Replays all the incremental database metas information
  int replay_database_metas_(
      const transaction::ObTransID &trans_id,
      const ObIArray<const datadict::ObDictDatabaseMeta*> &database_metas,
      ObDictTenantInfo &tenant_info);
  // Replays all the incremental table metas information
  int replay_table_metas_(
      const transaction::ObTransID &trans_id,
      const ObIArray<const datadict::ObDictTableMeta*> &table_metas,
      ObDictTenantInfo &tenant_info);

private:
  bool is_inited_;
  bool is_start_progress_;

  DISALLOW_COPY_AND_ASSIGN(ObLogSchemaIncReplay);
};

} // namespace libobcdc
} // namespace oceanbase

#endif
