/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_
#define OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_
#include "storage/ddl/ob_ddl_lock.h"
#include "src/storage/fts/dict/ob_dic_loader.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace storage
{
class ObDicLock : public ObDDLLock
{
public:
  static int lock_dic_tables_out_trans(
      const uint64_t tenant_id,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner,
      ObMySQLTransaction &trans,
      const common::ObIArray<uint64_t> &dict_table_ids);
  static int unlock_dict_tables(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &dict_table_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner,
      ObMySQLTransaction &trans);
  static int lock_dic_tables_in_trans(
      const int64_t tenant_id,
      const ObTenantDicLoader &dic_loader,
      const transaction::tablelock::ObTableLockMode lock_mode,
      ObMySQLTransaction &trans);
  static int lock_dic_tables_in_trans(
      const uint64_t tenant_id,
      const common::ObIArray<uint64_t> &dict_table_ids,
      const transaction::tablelock::ObTableLockMode lock_mode,
      ObMySQLTransaction &trans);
private:
  static constexpr int64_t DEFAULT_TIMEOUT = 0;
};
} //end storage
} // end oceanbase



#endif //OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_