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

#ifndef __OB_RS_RESTORE_META_H__
#define __OB_RS_RESTORE_META_H__

#include "share/ob_rpc_struct.h"
#include "share/restore/ob_restore_base_reader.h"
#include "share/restore/ob_restore_args.h"
#include "observer/ob_restore_ctx.h"
#include "observer/ob_inner_sql_connection.h"
#include "rootserver/restore/ob_restore_sql_modifier_impl.h"
#include "rootserver/restore/ob_restore_info.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {
namespace share {

class ObRestoreArgs;
namespace restore {
class ObRestoreSchemaReader;
class ObRestoreResourceReader;
}  // namespace restore
}  // namespace share

namespace rootserver {

class ObSystemAdminCtx;
class ObRestoreMeta {
private:
  struct TablegroupIdPair {
    TablegroupIdPair() : tablegroup_id_(common::OB_INVALID_ID), backup_tablegroup_id_(common::OB_INVALID_ID)
    {}
    TablegroupIdPair(uint64_t tid, uint64_t b_tid) : tablegroup_id_(tid), backup_tablegroup_id_(b_tid)
    {}
    bool is_valid() const
    {
      return common::OB_INVALID_ID != tablegroup_id_ && common::OB_INVALID_ID != backup_tablegroup_id_;
    }
    uint64_t tablegroup_id_;
    uint64_t backup_tablegroup_id_;  // tablegroup_id in backup meta data
    TO_STRING_KV(K_(tablegroup_id), K_(backup_tablegroup_id));
  };

  struct TableIdPair {
    TableIdPair() : table_id_(common::OB_INVALID_ID), backup_table_id_(common::OB_INVALID_ID)
    {}
    TableIdPair(uint64_t tid, uint64_t b_tid) : table_id_(tid), backup_table_id_(b_tid)
    {}
    bool is_valid() const
    {
      return common::OB_INVALID_ID != table_id_ && common::OB_INVALID_ID != backup_table_id_;
    }
    uint64_t table_id_;
    uint64_t backup_table_id_;  // table_id in backup meta data
    TO_STRING_KV(K_(table_id), K_(backup_table_id));
  };

  struct IndexIdPair {
    IndexIdPair() : table_id_(common::OB_INVALID_ID), index_id_(0), backup_index_id_(common::OB_INVALID_ID)
    {}
    IndexIdPair(uint64_t tid, uint64_t idx_tid, uint64_t b_tid)
        : table_id_(tid), index_id_(idx_tid), backup_index_id_(b_tid)
    {}
    bool is_valid() const
    {
      return common::OB_INVALID_ID != table_id_ && common::OB_INVALID_ID != index_id_ &&
             common::OB_INVALID_ID != backup_index_id_;
    }
    uint64_t table_id_;
    uint64_t index_id_;
    uint64_t backup_index_id_;
    TO_STRING_KV(K_(table_id), K_(index_id), K_(backup_index_id));
  };

public:
  explicit ObRestoreMeta(observer::ObRestoreCtx& restore_ctx, RestoreJob& job_info, const volatile bool& is_stop);
  ~ObRestoreMeta();
  int execute();

private:
  /* functions */
  int check_stop()
  {
    return is_stop_ ? common::OB_CANCELED : common::OB_SUCCESS;
  }
  int init();
  int init_sql_modifier();
  int init_connection();
  int restore_resource();
  int restore_schema();
  int restore_partitions(const uint64_t tenant_id);
  int restore_all_recyclebin(
      const uint64_t tenant_id, const common::ObIArray<share::schema::ObRecycleObject>& recycle_object);
  int clear_all_recyclebin(const uint64_t tenant_id);
  int fill_recycle_objects(common::ObIAllocator& allocator, const uint64_t tenant_id,
      const common::ObIArray<share::schema::ObRecycleObject>& orig_objects,
      ObIArray<const share::schema::ObRecycleObject*>& new_objects);
  int generate_task();
  int get_created_tablegroup_id(uint64_t tenant_id, const common::ObString& tg_name, uint64_t& created_tablegroup_id);
  int get_created_table_id(
      uint64_t tenant_id, const common::ObString& db_name, const common::ObString& tb_name, uint64_t& created_table_id);
  int get_created_index_schema(uint64_t tenant_id, const common::ObString& db_name, const common::ObString& index_name,
      const share::schema::ObSimpleTableSchemaV2*& index);
  int get_backup_tablegroup_id(const uint64_t tablegroup_id, uint64_t& backup_tablegroup_id);
  int get_backup_table_id(const uint64_t table_id, uint64_t& backup_table_id);
  int get_backup_index_id(const uint64_t table_id, common::ObIArray<share::ObSchemaIdPair>& index_id_pairs);
  /* variables */
  bool inited_;
  const volatile bool& is_stop_;
  observer::ObRestoreCtx& restore_ctx_;
  hash::ObHashSet<uint64_t> dropped_index_ids_;
  RestoreJob& job_info_;
  share::ObRestoreArgs restore_args_;
  share::restore::ObRestoreBaseReader oss_reader_;
  observer::ObInnerSQLConnection conn_;
  observer::ObInnerSQLConnection oracle_conn_;
  rootserver::ObRestoreSQLModifierImpl sql_modifier_;
  common::ObArray<TablegroupIdPair> tablegroup_id_pairs_;
  common::ObArray<TableIdPair> table_id_pairs_;
  common::ObArray<IndexIdPair> index_id_pairs_;
  DISALLOW_COPY_AND_ASSIGN(ObRestoreMeta);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif /* __OB_RS_RESTORE_META_H__ */
//// end of header file
