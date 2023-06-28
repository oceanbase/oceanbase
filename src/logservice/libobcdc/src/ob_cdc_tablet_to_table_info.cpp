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

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_cdc_tablet_to_table_info.h"

#include "share/ob_errno.h"

namespace oceanbase
{
namespace libobcdc
{

void CreateTabletOp::reset()
{
  tablet_id_.reset();
  table_info_.reset();
}

void CreateTabletOp::reset(const common::ObTabletID &tablet_id, const ObCDCTableInfo &table_info)
{
  tablet_id_ = tablet_id;
  table_info_ = table_info;
}

bool CreateTabletOp::is_valid() const
{
  return tablet_id_.is_valid() && table_info_.is_valid();
}

void DeleteTabletOp::reset()
{
  tablet_id_.reset();
}

void DeleteTabletOp::reset(const common::ObTabletID &tablet_id)
{
  tablet_id_ = tablet_id;
}

bool DeleteTabletOp::is_valid() const
{
  return tablet_id_.is_valid();
}

ObCDCTabletChangeInfo::ObCDCTabletChangeInfo() :
    cmd_(TabletChangeCmd::CMD_UNKNOWN), create_tablet_op_arr_(), delete_tablet_op_arr_()
{}

void ObCDCTabletChangeInfo::reset()
{
  cmd_ = TabletChangeCmd::CMD_MAX;
  create_tablet_op_arr_.reset();
  delete_tablet_op_arr_.reset();
}

void ObCDCTabletChangeInfo::reset(const TabletChangeCmd cmd)
{
  reset();
  cmd_ = cmd;
}

int ObCDCTabletChangeInfo::parse_from_multi_data_source_buf(
    const logservice::TenantLSID &tls_id,
    const transaction::ObTxBufferNode &multi_data_source_node)
{
  int ret = OB_SUCCESS;
  const char *buf = multi_data_source_node.get_data_buf().ptr();
  const int64_t buf_len = multi_data_source_node.get_data_size();
  const transaction::ObTxDataSourceType &mds_type = multi_data_source_node.get_data_source_type();
  int64_t pos = 0;

  if (OB_ISNULL(buf) || OB_UNLIKELY(0 >= buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid buf to deserialize", KR(ret), K(tls_id), K(buf_len));
  } else {
    switch (mds_type) {
      case transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS:
      {
        obrpc::ObBatchCreateTabletArg create_tablet_arg;
        if (OB_FAIL(create_tablet_arg.deserialize(buf, buf_len, pos))) {
          LOG_ERROR("deserialize create_tablet_arg failed", KR(ret), K(tls_id), K(multi_data_source_node),
              K(create_tablet_arg), K(buf_len), K(pos));
        } else if (OB_FAIL(parse_create_tablet_op_(tls_id, create_tablet_arg))) {
          LOG_ERROR("parse_create_tablet_op_ failed", KR(ret), K(tls_id), K(multi_data_source_node),
              K(create_tablet_arg), KPC(this));
        }
        break;
      }
      case transaction::ObTxDataSourceType::DELETE_TABLET_NEW_MDS:
      {
        obrpc::ObBatchRemoveTabletArg remove_tablet_arg;

        if (OB_FAIL(remove_tablet_arg.deserialize(buf, buf_len, pos))) {
          LOG_ERROR("deserialize remove_tablet_arg failed", KR(ret), K(tls_id), K(multi_data_source_node),
              K(remove_tablet_arg), K(buf_len), K(pos));
        } else if (OB_FAIL(parse_remove_tablet_op_(tls_id, remove_tablet_arg))) {
          LOG_ERROR("parse_remove_tablet_op_ failed", KR(ret), K(tls_id), K(multi_data_source_node),
              K(remove_tablet_arg), KPC(this));
        }
        break;
      }
      default:
      {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("not support tablet_change_op", KR(ret), K(tls_id), K(multi_data_source_node));
      }
    }
  }

  return ret;
}

int ObCDCTabletChangeInfo::parse_create_tablet_op_(
    const logservice::TenantLSID &tls_id,
    const obrpc::ObBatchCreateTabletArg &create_tablet_arg)
{
  int ret = OB_SUCCESS;
  cmd_ = TabletChangeCmd::CMD_CREATE;

  if (OB_UNLIKELY(! create_tablet_arg.is_valid())
      || OB_UNLIKELY(tls_id.get_ls_id() != create_tablet_arg.id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObBatchCreateTabletArg is invalid", KR(ret), K(tls_id), K(create_tablet_arg));
  } else {
    const common::ObSArray<share::schema::ObTableSchema> &table_schemas = create_tablet_arg.table_schemas_;
    const common::ObSArray<obrpc::ObCreateTabletInfo> &tablets_info = create_tablet_arg.tablets_;

    for (int64_t create_tablet_idx = 0; OB_SUCC(ret) && create_tablet_idx < tablets_info.count(); create_tablet_idx++) {
      const obrpc::ObCreateTabletInfo &ob_create_tablet_info = tablets_info.at(create_tablet_idx);
      const common::ObSArray<common::ObTabletID> &tablet_ids = ob_create_tablet_info.tablet_ids_;
      const common::ObSArray<int64_t> &tb_schema_index_arr = ob_create_tablet_info.table_schema_index_;

      for (int64_t tablet_id_idx = 0; OB_SUCC(ret) && tablet_id_idx < tablet_ids.count(); tablet_id_idx++) {
        const common::ObTabletID &tablet_id = tablet_ids.at(tablet_id_idx);
        const int64_t table_schema_idx = tb_schema_index_arr.at(tablet_id_idx);
        if (OB_UNLIKELY(0 > table_schema_idx || table_schemas.count() <= table_schema_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid table_schema_index", KR(ret), K(tls_id), K(ob_create_tablet_info), K(table_schemas), K(table_schema_idx));
        } else {
          const ObTableSchema &table_schema = table_schemas.at(table_schema_idx);
          const uint64_t table_id = table_schema.get_table_id();
          const share::schema::ObTableType table_type = table_schema.get_table_type();
          ObCDCTableInfo table_info;
          table_info.reset(table_id, table_type);
          CreateTabletOp create_tablet_op;
          create_tablet_op.reset(tablet_id, table_info);

          if (OB_FAIL(push_create_tablet_op_(create_tablet_op))) {
            LOG_ERROR("push_create_tablet_op failed", KR(ret), K(tls_id), K(create_tablet_op), K(create_tablet_idx), K(tablet_id_idx),
                K(table_schema_idx), K(ob_create_tablet_info), K(tablet_ids), K(tb_schema_index_arr));
          } else {
            LOG_DEBUG("[CREATE_TABLET_INFO]", K(tls_id), K(create_tablet_op), K(create_tablet_idx), K(tablet_id_idx));
          }
        }
      }
    }
  }

  return ret;
}

int ObCDCTabletChangeInfo::parse_remove_tablet_op_(
    const logservice::TenantLSID &tls_id,
    const obrpc::ObBatchRemoveTabletArg &remove_tablet_arg)
{
  int ret = OB_SUCCESS;
  cmd_ = TabletChangeCmd::CMD_DELETE;

  if (OB_UNLIKELY(! remove_tablet_arg.is_valid())
      || OB_UNLIKELY(tls_id.get_ls_id() != remove_tablet_arg.id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObBatchRemoveTabletArg is invalid", KR(ret), K(tls_id), K(remove_tablet_arg));
  } else {
    const common::ObSArray<common::ObTabletID> &tablet_ids = remove_tablet_arg.tablet_ids_;

    for (int64_t tablet_id_idx = 0; OB_SUCC(ret) && tablet_id_idx < tablet_ids.count(); tablet_id_idx++) {
      const common::ObTabletID &tablet_id = tablet_ids.at(tablet_id_idx);
      DeleteTabletOp delete_tablet_op;
      delete_tablet_op.reset(tablet_id);

      if (OB_FAIL(push_delete_tablet_op_(delete_tablet_op))) {
        LOG_ERROR("push_delete_tablet_op failed", KR(ret), K(tls_id), K(delete_tablet_op), KPC(this));
      } else {
        LOG_DEBUG("[DELETE_TABLET_INFO]", K(tls_id), K(delete_tablet_op));
      }
    }
  }

  return ret;
}

int ObCDCTabletChangeInfo::push_create_tablet_op_(const CreateTabletOp &create_tablet_op)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!create_tablet_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arg for push_create_tablet_op", KR(ret), K(create_tablet_op));
  } else if (OB_FAIL(create_tablet_op_arr_.push_back(create_tablet_op))) {
    LOG_ERROR("push_create_tablet_op into create_tablet_op_arr_ failed",
        KR(ret), K(create_tablet_op), K_(create_tablet_op_arr));
  } else {
    // success
  }

  return ret;
}

int ObCDCTabletChangeInfo::push_delete_tablet_op_(const DeleteTabletOp &delete_tablet_op)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!delete_tablet_op.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arg for push_delete_tablet_op", KR(ret), K(delete_tablet_op));
  } else if (OB_FAIL(delete_tablet_op_arr_.push_back(delete_tablet_op))) {
    LOG_ERROR("push_delete_tablet_op into delete_tablet_op_arr_ failed",
        KR(ret), K(delete_tablet_op), K_(delete_tablet_op_arr));
  } else {
    // success
  }

  return ret;
}

void ObCDCTabletChangeInfo::print_detail_for_debug() const
{
  if (is_create_tablet_op()) {
    LOG_DEBUG("tablet_change_info", "create_cnt", create_tablet_op_arr_.count(), K_(create_tablet_op_arr));
  } else if (is_delete_tablet_op()) {
    LOG_DEBUG("tablet_change_info", "delete_cnt", delete_tablet_op_arr_.count(),  K_(delete_tablet_op_arr));
  } else {
    LOG_DEBUG("tablet_change_info: None");
  }
}

TabletToTableInfo::TabletToTableInfo() :
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    tablet_to_table_map_()
{}

int TabletToTableInfo::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("TabletToTableInfo inited twice", KR(ret), K(tenant_id), K_(is_inited));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments to init TabletToTableInfo", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tablet_to_table_map_.init("TabletToTable", tenant_id))) {
    LOG_ERROR("init tablet_to_table_map_ fail", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
    LOG_INFO("init TabletIDToTableIDInfo success", K_(is_inited), K_(tenant_id));
  }

  return ret;
}

void TabletToTableInfo::destroy()
{
  if (IS_INIT) {
    tablet_to_table_map_.destroy();
    tenant_id_ = OB_INVALID_TENANT_ID;
    is_inited_ = false;
  }
}

int TabletToTableInfo::get_table_info_of_tablet(
    const common::ObTabletID &tablet_id,
    ObCDCTableInfo &table_info) const
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TabletIDToTableIDInfo is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tablet_id", KR(ret), K_(tenant_id), K(tablet_id), KPC(this));
  } else if (OB_FAIL(tablet_to_table_map_.get(tablet_id, table_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("failed to find table_info of tablet_id", KR(ret), K_(tenant_id), K(tablet_id));
    }
  } else if (OB_UNLIKELY(!table_info.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("find invalid table_info for tablet_id", KR(ret), K_(tenant_id), K(tablet_id), K(table_info));
  } else {
    // success
  }

  return ret;
}

int TabletToTableInfo::insert_tablet_table_info(const common::ObTabletID &tablet_id, const ObCDCTableInfo &table_info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TabletIDToTableIDInfo is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_UNLIKELY(! table_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments", KR(ret), K_(tenant_id), K(tablet_id), K(table_info));
  } else if (OB_FAIL(tablet_to_table_map_.insert(tablet_id, table_info))) {
    if (OB_ENTRY_EXIST == ret) {
      ObCDCTableInfo dup_table_info;
      if (OB_FAIL(tablet_to_table_map_.get(tablet_id, dup_table_info))) {
        LOG_ERROR("tablet_to_table_map failed to get dup_table_info while the same tablet id is found",
            K(tablet_id), K(table_info));
      } else if (dup_table_info == table_info) {
        ret = OB_SUCCESS;
        LOG_INFO("found duplicate tablet while insert_tablet_table_info, ignore when table_info is same",
            K_(tenant_id), K(tablet_id), K(table_info));
      } else {
        LOG_ERROR("found duplicate tablet while insert_tablet_table_info", KR(ret),
            K_(tenant_id), K(tablet_id), K(table_info), K(dup_table_info));
      }
    } else {
      LOG_ERROR("tablet_to_table_map_ insert tablet failed", KR(ret), K_(tenant_id), K(tablet_id), K(table_info));
    }
  } else {
    LOG_DEBUG("[TABLET_ID_TO_TABLE_ID][INSERT]", K_(tenant_id), "tablet_id", tablet_id.id(), K(table_info));
  }

  return ret;
}

int TabletToTableInfo::remove_tablet_table_info(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObCDCTableInfo table_info;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("TabletIDToTableIDInfo is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K_(tenant_id), K(tablet_id));
  } else if (OB_FAIL(tablet_to_table_map_.erase(tablet_id, table_info))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_DEBUG("tablet_id to remove is not exist in tablet_to_table_map_", KR(ret), K_(tenant_id), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("tablet_to_table_map_ remove tablet failed", KR(ret), K_(tenant_id), K(tablet_id));
    }
  } else {
    LOG_DEBUG("[TABLET_ID_TO_TABLE_ID][DELETE]", K_(tenant_id), "tablet_id", tablet_id.id(), K(table_info));
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
