/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_
#define OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_
#include "storage/ddl/ob_ddl_lock.h"
#include "src/storage/fts/dict/ob_dic_loader.h"

namespace oceanbase
{
namespace storage
{
class ObDicLock : public ObDDLLock
{
public:
  static int lock_dic_tables_out_trans(
      const uint64_t tenant_id,
      const ObTenantDicLoader &dic_loader,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID &lock_owner);
  static int unlock_dic_tables(
      const uint64_t tenant_id,
      const ObTenantDicLoader &dic_loader,
      const transaction::tablelock::ObTableLockMode lock_mode,
      const transaction::tablelock::ObTableLockOwnerID lock_owner,
      ObMySQLTransaction &trans);
  static int lock_dic_tables_in_trans(
      const int64_t tenant_id,
      const ObTenantDicLoader &dic_loader,
      const transaction::tablelock::ObTableLockMode lock_mode,
      ObMySQLTransaction &trans);
private:
  static constexpr int64_t DEFAULT_TIMEOUT = 0;
};
} //end storage
} // end oceanbase



#endif //OCEANBASE_STORAGE_DICT_OB_DIC_LOCK_H_