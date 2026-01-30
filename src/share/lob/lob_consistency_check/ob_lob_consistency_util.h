/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_LOB_LOB_CONSISTENCY_UTIL_H_
#define OCEANBASE_SHARE_LOB_LOB_CONSISTENCY_UTIL_H_

#include "share/ob_rpc_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/hash/ob_hashset.h"
#include "share/table/ob_table_ttl_common.h"
#include "observer/table/ttl/ob_tenant_tablet_ttl_mgr.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
}

namespace share {

enum ObLobCheckType {
  LOB_CHECK_TYPE,
  LOB_REPAIR_TYPE,
  LOB_INVALID_TYPE,
};

enum ObLobInconsistencyType {
  LOB_MISS_TYPE,
  LOB_MISMATCH_LEN_TYPE,
  LOB_ORPHAN_TYPE,
};

class ObLobConsistencyUtil {
public:
  ObLobConsistencyUtil()
  {
  }
  ~ObLobConsistencyUtil()
  {
  }

public:
  const static uint64_t LOB_CHECK_TENANT_TASK_TABLE_ID = -3;
  const static uint64_t LOB_REPAIR_TENANT_TASK_TABLE_ID = -4;
  const static uint64_t LOB_CHECK_TENANT_TASK_TABLET_ID = -3;
  const static uint64_t LOB_REPAIR_TENANT_TASK_TABLET_ID = -4;
  const static uint64_t LOB_CHECK_BATCH_LS_SIZE = 1024;
  const static int64_t LOB_EXCEPTION_CLEAN_BATCH_SIZE = 50; // Batch size for cleaning orphaned exception records
  const static int64_t LOB_EXCEPTION_CLEAN_INTERVAL = 24 * 3600 * 1000000L; // Clean once per day (24 hours in microseconds)
  const static common::ObTabletID LOB_CHECK_TABLET_ID;

public:
  static bool is_lob_cmd(int32_t cmd_code)
  {
    return is_lob_check_cmd(cmd_code) || is_lob_repair_cmd(cmd_code);
  }
  static bool is_lob_check_cmd(int32_t cmd_code)
  {
    return cmd_code >= 10 && cmd_code < obrpc::ObTTLRequestArg::LOB_CHECK_INVALID_TYPE;
  }
  static bool is_lob_repair_cmd(int32_t cmd_code)
  {
    return cmd_code >= 20 && cmd_code < obrpc::ObTTLRequestArg::LOB_REPAIR_INVALID_TYPE;
  }

  static int query_exception_tablets(uint64_t tenant_id,
                                     uint64_t ls_id,
                                     uint64_t table_id,
                                     ObLobInconsistencyType inconsistency_type,
                                     common::ObMySQLTransaction &trans,
                                     common::ObIAllocator &allocator,
                                     common::ObJsonNode *&tablets_json);
  static int insert_or_merge_exception_tablets(uint64_t tenant_id,
                                               uint64_t ls_id,
                                               uint64_t table_id,
                                               common::ObJsonNode *new_tablets_json,
                                               ObLobInconsistencyType inconsistency_type,
                                               common::ObMySQLTransaction &trans);

  static int remove_exception_tablets(uint64_t tenant_id,
                                      uint64_t ls_id,
                                      uint64_t table_id,
                                      common::ObJsonNode *tablets_to_remove,
                                      common::ObMySQLTransaction &trans,
                                      ObLobInconsistencyType inconsistency_type = LOB_ORPHAN_TYPE);

  static int delete_exception_record_if_empty(uint64_t tenant_id,
                                              uint64_t ls_id,
                                              uint64_t table_id,
                                              ObISQLClient &trans,
                                              int32_t inconsistency_type);

  // 获取有 LOB 表的 LS 数量
  static int check_all_ls_finished(uint64_t tenant_id, bool is_repair_task, const ObString &row_key, uint64_t task_id, bool &all_finished);

  static int merge_ctx_exception_tablets(ObArenaAllocator &allocator,
                                         ObJsonNode *&exception_table_tablets,
                                         table::ObTTLTaskInfo &info);

  // Clean orphaned exception records (tables/tablets/ls that no longer exist)
  // Only cleans LOB_MISS_TYPE and LOB_MISMATCH_LEN_TYPE records without LOB_ORPHAN_TYPE
  static int clean_invalid_exception_records(uint64_t tenant_id,
                                              common::ObMySQLProxy &sql_proxy,
                                              uint64_t &last_processed_ls_id,
                                              uint64_t &last_processed_table_id,
                                              int64_t batch_size,
                                              int64_t &processed_count);

  static int handle_lob_task_info(table::ObTTLTaskCtx *&ctx, table::ObTTLTaskInfo &task_info);
private:
  // Helper: convert json array to tablet id set
  static int json_array_to_tablet_set(common::ObJsonNode *json_array, common::hash::ObHashSet<uint64_t> &tablet_set);

  // Helper: convert tablet id set to json binary string
  static int tablet_set_to_json_string(const common::hash::ObHashSet<uint64_t> &tablet_set,
                                       common::ObIAllocator &allocator,
                                       common::ObString &json_str);

  // Helper: check valid tablets for a table
  static int filter_valid_tablets(const common::ObString &tablet_ids_str,
                                  const common::ObArray<common::ObTabletID> &all_tablet_ids,
                                  common::ObIAllocator &allocator,
                                  common::hash::ObHashSet<uint64_t> &valid_tablet_set,
                                  bool &all_valid,
                                  bool &all_invalid);

  // Helper: update or delete exception record
  static int update_or_delete_exception_record(uint64_t tenant_id,
                                               uint64_t ls_id,
                                               uint64_t table_id,
                                               int32_t inconsistency_type,
                                               const common::ObString &json_str,
                                               bool should_delete,
                                               ObMySQLTransaction &trans);

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobConsistencyUtil);
};

struct PendingOp {
  PendingOp() : ls_id_(0), table_id_(0), inconsistency_type_(0), should_delete_(false), json_str_() {}
  uint64_t ls_id_;
  uint64_t table_id_;
  int32_t inconsistency_type_;
  bool should_delete_;
  ObString json_str_;  // deep copied if update

  TO_STRING_KV(K_(ls_id), K_(table_id), K_(inconsistency_type), K_(should_delete), K_(json_str));
};

}  // namespace share
}  // namespace oceanbase

#endif