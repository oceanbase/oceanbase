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

#ifndef OCEANBASE_ROOTSERVER_OB_SEQUENCE_MIGRATOR_H_
#define OCEANBASE_ROOTSERVER_OB_SEQUENCE_MIGRATOR_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace rootserver {
class ObSequenceMigrator {
public:
  ObSequenceMigrator();
  virtual ~ObSequenceMigrator();

  int init(common::ObMySQLProxy& sql_proxy, share::schema::ObMultiVersionSchemaService& schema_service);
  inline bool is_inited() const
  {
    return inited_;
  }

  // step 1. call before end upgrade
  int create_tables();
  // step 2. call after end upgrade
  int run();

  void start();
  int stop();

private:
  int check_stop();
  int set_migrate_mark();

  int migrate();
  int do_migrate(const uint64_t tenant_id);

private:
  bool inited_;
  bool stopped_;
  bool migrate_;
  common::SpinRWLock rwlock_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSequenceMigrator);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_SEQUENCE_MIGRATOR_H_
