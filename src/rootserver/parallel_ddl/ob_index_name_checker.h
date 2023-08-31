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
#ifndef OCEANBASE_ROOTSERVER_OB_INDEX_NAME_CHECKER_H_
#define OCEANBASE_ROOTSERVER_OB_INDEX_NAME_CHECKER_H_

#include "lib/hash/ob_pointer_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

/*
 * ObIndexNameCache will be protected by mutex
 *
 * Any failure when call related function will reset cache immediately for correctness.
 */
class ObIndexNameCache
{
public:
  ObIndexNameCache() = delete;
  ObIndexNameCache(const uint64_t tenant_id,
                   common::ObMySQLProxy &sql_proxy);
  ~ObIndexNameCache() {}

  void reset_cache();
  int check_index_name_exist(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const ObString &index_name,
                             bool &is_exist);
  int add_index_name(const share::schema::ObTableSchema &index_schema);
private:
  void inner_reset_cache_();
  int try_load_cache_();
private:
  lib::ObMutex mutex_;
  uint64_t tenant_id_;
  common::ObMySQLProxy &sql_proxy_;
  common::ObArenaAllocator allocator_;
  ObIndexNameMap cache_;
  bool loaded_;
  DISALLOW_COPY_AND_ASSIGN(ObIndexNameCache);
};

/*
 * design doc: ob/rootservice/ffa2062ce3n1tvi0#SLpFs
 *
 * usage: for parallel ddl to check if index name is duplicated in oracle mode
 *
 * notice:
 * 1. ObIndexNameChecker should be used in ObDDLSQLTransaction only.
 * 2. nothing will be done for mysql tenant.
 */
class ObIndexNameChecker
{
public:
  ObIndexNameChecker();
  ~ObIndexNameChecker();

  int init(common::ObMySQLProxy &sql_proxy);
  void destroy();

  // release memory when RS changed.
  void reset_all_cache();

  // will be called in the following cases:
  // 1. before non parallel ddl commit.
  // 2. after non parallel ddl commit failed.
  int reset_cache(const uint64_t tenant_id);

  // lock object by original index name first before call check_index_name_exist().
  int check_index_name_exist(const uint64_t tenant_id,
                             const uint64_t database_id,
                             const ObString &index_name,
                             bool &is_exist);

  // call add_index_name() before parallel ddl trans commit.
  int add_index_name(const share::schema::ObTableSchema &index_schema);
private:
  int check_tenant_can_be_skipped_(const uint64_t tenant_id, bool &can_skip);
  int try_init_index_name_cache_map_(const uint64_t tenant_id);
private:
  common::SpinRWLock rwlock_;
  common::ObArenaAllocator allocator_;
  // index_name_cache_map_ won't be erased()
  common::hash::ObHashMap<uint64_t, ObIndexNameCache*, common::hash::ReadWriteDefendMode> index_name_cache_map_;
  common::ObMySQLProxy *sql_proxy_;
  bool inited_;
};

} // end schema
} // end share
} // end oceanbase

#endif//OCEANBASE_ROOTSERVER_OB_INDEX_NAME_CHECKER_H_
