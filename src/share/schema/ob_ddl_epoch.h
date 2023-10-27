/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_DDL_EPOCH_H
#define OCEANBASE_DDL_EPOCH_H

#include "lib/container/ob_se_array.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{

namespace common
{
class ObMySQLProxy;
}

namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;

struct ObDDLEpoch
{
  uint64_t tenant_id_;
  int64_t ddl_epoch_;
  TO_STRING_KV(K_(tenant_id), K_(ddl_epoch));
};


// use ddl_epoch to promise only ddl_service master execute ddl trans
// promote ddl_epoch when ddl_service init
// compare ddl_epoch in ddl trans
class ObDDLEpochMgr
{
public:
  ObDDLEpochMgr()
    : inited_(false),
      sql_proxy_(NULL),
      schema_service_(NULL),
      ddl_epoch_stat_(),
      lock_(),
      lock_for_promote_(),
      mutex_for_promote_() {}
  int init(ObMySQLProxy *sql_proxy, share::schema::ObMultiVersionSchemaService *schema_service);
  // 获取本地ddl_epoch
  int get_ddl_epoch(uint64_t tenant_id, int64_t &ddl_epoch);
  // 推高内部表ddl_epoch
  int promote_ddl_epoch(const uint64_t tenant_id, int64_t wait_us, int64_t &ddl_epoch_ret);
  int remove_ddl_epoch(const uint64_t tenant_id);
  int remove_all_ddl_epoch();
  int check_and_lock_ddl_epoch(
      common::ObMySQLTransaction &trans,
      const uint64_t tenant_id,
      const int64_t ddl_epoch_local);
private:
  int promote_ddl_epoch_inner_(const uint64_t tenant_id, int64_t &new_ddl_epoch);
  int update_ddl_epoch_(uint64_t tenant_id, const int64_t ddl_epoch);
private:
  bool inited_;
  ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  ObSEArray<ObDDLEpoch, 10> ddl_epoch_stat_;
  common::SpinRWLock lock_; // protect ddl_epoch_stat_
  common::SpinRWLock lock_for_promote_; // promise only one thread to promote
  lib::ObMutex mutex_for_promote_; // to make promote ddl epoch serially to avoid conflict
};


} // end schema
} // end share
} // end oceanbase


#endif
