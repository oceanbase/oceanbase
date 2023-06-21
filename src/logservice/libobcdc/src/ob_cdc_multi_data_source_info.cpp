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
 * MultiDataSourceNode and MultiDataSourceInfo
 */

#define USING_LOG_PREFIX OBLOG_PARSER

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

#include "ob_cdc_multi_data_source_info.h"
#include "ob_log_schema_getter.h"

namespace oceanbase
{
using namespace datadict;
namespace libobcdc
{

MultiDataSourceNode::MultiDataSourceNode() :
    lsn_(),
    tx_buf_node_()
{}

int MultiDataSourceNode::init(
    const palf::LSN &lsn,
    const transaction::ObTxDataSourceType &type,
    const char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(0 >= buf_size)
      || OB_UNLIKELY(! lsn.is_valid())
      || OB_UNLIKELY(transaction::ObTxDataSourceType::UNKNOWN >= type
      || transaction::ObTxDataSourceType::MAX_TYPE <= type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(lsn), K(type), K(buf), K(buf_size));
  } else {
    lsn_ = lsn;
    common::ObString data;
    data.assign_ptr(reinterpret_cast<const char *>(buf), buf_size);

    if (OB_FAIL(tx_buf_node_.init(type, data, share::SCN(), nullptr))) {
      LOG_ERROR("init tx_buf_node failed", KR(ret), K(lsn), K(type), K(data), K(buf_size));
    }
  }

  return ret;
}

void MultiDataSourceNode::reset()
{
  lsn_.reset();
  tx_buf_node_.reset();
}

MultiDataSourceInfo::MultiDataSourceInfo() :
    ls_attr_arr_(),
    tablet_change_info_arr_(),
    has_ddl_trans_op_(false),
    dict_tenant_metas_(),
    dict_database_metas_(),
    dict_table_metas_()
{}

void MultiDataSourceInfo::reset()
{
  ls_attr_arr_.reset();
  tablet_change_info_arr_.reset();
  has_ddl_trans_op_ = false;
  dict_tenant_metas_.reset();
  dict_database_metas_.reset();
  dict_table_metas_.reset();
}

int MultiDataSourceInfo::push_back_ls_table_op(const share::ObLSAttr &ls_attr)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! ls_attr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls_attr in multi_data_source_log", KR(ret), K(ls_attr), KPC(this));
  } else if (OB_FAIL(ls_attr_arr_.push_back(ls_attr))) {
    LOG_ERROR("push_back ls_attr into multi_data_source_info failed", KR(ret), K(ls_attr), KPC(this));
  }

  return ret;
}

int MultiDataSourceInfo::push_back_tablet_change_info(const ObCDCTabletChangeInfo &tablet_change_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! tablet_change_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tablet_change_info));
  } else if (OB_FAIL(tablet_change_info_arr_.push_back(tablet_change_info))) {
    LOG_ERROR("push_back tablet_change_info failed", KR(ret), K(tablet_change_info), KPC(this));
  }

  return ret;
}

int64_t MultiDataSourceInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    if (has_ls_table_op()) {
      (void)common::databuff_printf(buf, buf_len, pos, "{ls_table_op: %s", to_cstring(ls_attr_arr_));
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, "has_ls_table_op: false");
    }

    (void)common::databuff_printf(buf, buf_len, pos, ", is_ddl_trans: %d", has_ddl_trans_op_);
    if (has_tablet_change_op()) {
      (void)common::databuff_printf(buf, buf_len, pos, ", tablet_change_info: %s}", to_cstring(tablet_change_info_arr_));
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, ", tablet_change_info: None}");
    }
  }

  return pos;
}

int MultiDataSourceInfo::get_new_tenant_scehma_info(
    const uint64_t tenant_id,
    TenantSchemaInfo &tenant_schema_info)
{
  int ret = OB_SUCCESS;
  tenant_schema_info.reset();
  bool found = false;
  const int64_t tenant_meta_cnt = dict_tenant_metas_.count();

  if (OB_UNLIKELY(tenant_meta_cnt > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("expect at most one tenant_dict in multi_data_source_info", KR(ret),
        K(tenant_meta_cnt), K_(dict_tenant_metas));
  } else if (0 == tenant_meta_cnt) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    const ObDictTenantMeta *tenant_meta = dict_tenant_metas_[0];

    if (OB_ISNULL(tenant_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid dict_tenant_meta", KR(ret), K(tenant_id));
    } else {
      tenant_schema_info.reset(
          tenant_id,
          tenant_meta->get_schema_version(),
          tenant_meta->get_tenant_name(),
          tenant_meta->is_restore());
    }
  }

  return ret;
}

int MultiDataSourceInfo::get_new_database_scehma_info(
    const uint64_t tenant_id,
    const uint64_t database_id,
    DBSchemaInfo &db_schema_info)
{
  int ret = OB_SUCCESS;
  bool found = false;
  db_schema_info.reset();
  const int64_t db_meta_cnt = dict_database_metas_.count();

  for (int i = 0; OB_SUCC(ret) && ! found && i < db_meta_cnt; i++) {
    const ObDictDatabaseMeta *db_meta = dict_database_metas_[i];

    if (OB_ISNULL(db_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid database_meta", KR(ret), K(tenant_id), K(database_id));
    } else if (db_meta->get_database_id() == database_id) {
      db_schema_info.reset(
          db_meta->get_database_id(),
          db_meta->get_schema_version(),
          db_meta->get_database_name());
      found = true;
    }
  }

  if (OB_SUCC(ret) && ! found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int MultiDataSourceInfo::get_new_table_meta(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const datadict::ObDictTableMeta *&table_meta)
{
  int ret = OB_SUCCESS;
  bool found = false;
  table_meta = nullptr;
  const int64_t tb_meta_cnt = dict_table_metas_.count();

  for (int i = 0; OB_SUCC(ret) &&! found && i < tb_meta_cnt; i++) {
    const ObDictTableMeta *tb_meta = dict_table_metas_[i];

    if (OB_ISNULL(tb_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid table_meta", KR(ret), K(tenant_id), K(table_id));
    } else if (tb_meta->get_table_id() == table_id) {
      table_meta = tb_meta;
      found = true;
    }
  }

  if (OB_SUCC(ret) && ! found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase
