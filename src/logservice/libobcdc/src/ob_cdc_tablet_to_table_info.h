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
 *
 * Tenant TabletIDToTableIDInfo
 */

#ifndef OCEANBASE_LIBOBCDC_TABLET_TO_TABLE_INFO_H__
#define OCEANBASE_LIBOBCDC_TABLET_TO_TABLE_INFO_H__

#include "common/ob_tablet_id.h"                  // common::ObTabletID
#include "lib/hash/ob_linear_hash_map.h"          // ObLinkHashMap
#include "share/schema/ob_schema_struct.h"        // ObTableType
#include "storage/tx/ob_multi_data_source.h"      // ObTxBufferNode
#include "rootserver/ob_tablet_creator.h"         // ObBatchCreateTabletArg
#include "rootserver/ob_tablet_drop.h"            // ObBatchRemoveTabletArg

#include "logservice/common_util/ob_log_ls_define.h"

namespace oceanbase
{
namespace libobcdc
{
// TabletChangeOp Type
enum TabletChangeCmd
{
  CMD_UNKNOWN = 0,
  CMD_CREATE,
  CMD_DELETE,
  CMD_TRANSFER,
  CMD_MAX
};

// CDCTableInfo, which record table_id and table_type.
// parser get table_id of tablet_id from TabletToTableMap, and will parse user-table, thus store
// table_type of table_id in TabletToTableMap.
struct ObCDCTableInfo
{
  ObCDCTableInfo () { reset(); }
  ~ObCDCTableInfo() { reset(); }
  inline void reset()
  {
    table_id_ = OB_INVALID_ID;
    table_type_ = share::schema::ObTableType::MAX_TABLE_TYPE;
  }

  inline void reset(const uint64_t table_id, const share::schema::ObTableType &table_type)
  {
    table_id_ = table_id;
    table_type_ = table_type;
  }

  inline bool is_valid() const
  {
    return table_id_ > 0 && (OB_INVALID_ID != table_id_)
        && table_type_ < share::schema::ObTableType::MAX_TABLE_TYPE;
  }

  inline uint64_t get_table_id() const { return table_id_; }
  inline const share::schema::ObTableType &get_table_type() const { return table_type_; }
  inline bool is_index_table() const { return share::schema::is_index_table(table_type_); }

  bool operator==(const ObCDCTableInfo &that) const {
    return table_id_ == that.table_id_ && table_type_ == that.table_type_;
  }

  bool operator!=(const ObCDCTableInfo &that) const {
    return !(*this == that);
  }

  TO_STRING_KV(K_(table_id), K_(table_type), "table_type", ob_table_type_str(table_type_));

  uint64_t table_id_;
  share::schema::ObTableType table_type_;
};

// data used for CreateTablet
class CreateTabletOp
{
public:
  CreateTabletOp() { reset(); }
  ~CreateTabletOp() { reset(); }
  void reset();
  void reset(const common::ObTabletID &tablet_id, const ObCDCTableInfo &table_info);
public:
  bool is_valid() const;
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ObCDCTableInfo &get_table_info() const { return table_info_; }
  uint64_t get_table_id() const { return table_info_.get_table_id(); }
public:
  TO_STRING_KV(K_(tablet_id), K_(table_info));
private:
  common::ObTabletID tablet_id_;
  ObCDCTableInfo table_info_;
};

// data used for DeleteTablet
class DeleteTabletOp
{
public:
  DeleteTabletOp() { reset(); }
  ~DeleteTabletOp() { reset(); }
  void reset();
  void reset(const common::ObTabletID &tablet_id);
public:
  bool is_valid() const;
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
public:
  TO_STRING_KV(K_(tablet_id));
private:
  common::ObTabletID tablet_id_;
};

// struct store the TabletChangeOp that get from LS_MEMTABLE mulit_data_source trans
class ObCDCTabletChangeInfo
{
public:
  ObCDCTabletChangeInfo();
  ~ObCDCTabletChangeInfo() { reset(); }
  void reset();
  void reset(const TabletChangeCmd cmd);
  // deserialize from LS_MEMBER_TABLE multi_data_source_node
  int parse_from_multi_data_source_buf(
      const logservice::TenantLSID &tls_id,
      const transaction::ObTxBufferNode &multi_data_source_node);
public:
  inline bool is_valid() const
  {
    return (TabletChangeCmd::CMD_CREATE == cmd_ && 0 >= delete_tablet_op_arr_.count()) // create_tablet_op_arr_ may empty after filter tablet.
        || (TabletChangeCmd::CMD_DELETE == cmd_ && 0 >= create_tablet_op_arr_.count()); // delete_tablet_op_arri_ may empty.
  }
  inline bool is_create_tablet_op() const { return TabletChangeCmd::CMD_CREATE == cmd_; }
  inline bool is_delete_tablet_op() const { return TabletChangeCmd::CMD_DELETE == cmd_; }
  const ObArray<CreateTabletOp> &get_create_tablet_op_arr() const { return create_tablet_op_arr_; }
  const ObArray<DeleteTabletOp> &get_delete_tablet_op_arr() const { return delete_tablet_op_arr_; }
public:
  void print_detail_for_debug() const;
  TO_STRING_KV(K_(cmd),
      "create_tablet_cnt", create_tablet_op_arr_.count(),
      "delete_tablet_cnt", delete_tablet_op_arr_.count());
private:
  int parse_create_tablet_op_(
      const logservice::TenantLSID &tls_id,
      const obrpc::ObBatchCreateTabletArg &create_tablet_arg);
  int parse_remove_tablet_op_(
      const logservice::TenantLSID &tls_id,
      const obrpc::ObBatchRemoveTabletArg &remove_tablet_arg);
  int push_create_tablet_op_(const CreateTabletOp &create_tablet_op);
  int push_delete_tablet_op_(const DeleteTabletOp &delete_tablet_op);
private:
  TabletChangeCmd cmd_;
  ObArray<CreateTabletOp> create_tablet_op_arr_;
  ObArray<DeleteTabletOp> delete_tablet_op_arr_;
};

typedef common::ObLinearHashMap<common::ObTabletID, ObCDCTableInfo> TabletToTableMap; // Map of TabletID->TableID

class TabletToTableInfo
{
public:
  TabletToTableInfo();
  ~TabletToTableInfo() { destroy(); }
public:
  int init(const uint64_t tenant_id);
  void destroy();
  uint64_t count() const { return tablet_to_table_map_.count(); }
public:
  /// get table_info of specified tablet_id
  ///
  /// @param [in]   tablet_id     tablet_id to query
  /// @param [out]  table_info    table_info(query result)
  ///
  /// @retval OB_SUCCESS          get table_info of tablet_id success
  /// @retval OB_INVALID_ARGUMENT tablet_id is not valid
  /// @retval OB_ENTRY_NOT_EXIST  tablet_id is not in keyset of tablet_to_table_map_
  /// @retval other ERROR         unexpected error(invoker should check schema_version is valid and reasonable)
  int get_table_info_of_tablet(
      const common::ObTabletID &tablet_id,
      ObCDCTableInfo &table_info) const;

  /// insert a pair of tablet_id->table_info
  ///
  /// @param [in] tablet_id       tablet_id to insert
  /// @param [in] table_info      table_info of tablet_id
  ///
  /// @retval OB_SUCCESS          insert success
  /// @retval OB_INVALID_ARGUMENT tablet_id is invalid
  /// @retval OB_ENTRY_EXIST      the tablet_id is already in tablet_to_table_map_
  /// @retval other ERROR         unexpected error.
  int insert_tablet_table_info(const common::ObTabletID &tablet_id, const ObCDCTableInfo &table_info);

  /// remove tablet_id->table_info pair for specified tablet_id
  ///
  /// @param [in]   tablet_id     tablet_id to remove
  ///
  /// @retval OB_SUCCESS          remove success
  /// @retval other ERROR         remove fail
  int remove_tablet_table_info(const common::ObTabletID &tablet_id);
  // TODO: need support Tablet Transfer(wait OBServer imply)
public:
  TO_STRING_KV(K_(tenant_id), K_(is_inited), "tablet_to_table_count", tablet_to_table_map_.count());
private:
  bool              is_inited_;
  uint64_t          tenant_id_;
  TabletToTableMap  tablet_to_table_map_;
};

} // namespace libobcdc
} // namespace oceanbase
#endif
