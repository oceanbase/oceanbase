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

#define USING_LOG_PREFIX SHARE
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_backup_config.h"
#include "share/ob_cluster_version.h"
#include "share/ob_tenant_info_proxy.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace sqlclient;

/**
 * ------------------------------ObArchiveDestParaItem---------------------
 */
ObArchiveDestParaItem::ObArchiveDestParaItem(ObInnerKVItemValue *value)
  : ObInnerKVItem(value), dest_no_(-1)
{

}

int ObArchiveDestParaItem::set_dest_no(const int64_t dest_no)
{
  int ret = OB_SUCCESS;
  if (0 > dest_no) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest no", K(ret), K(dest_no));
  } else {
    dest_no_ = dest_no;
  }
  return ret;
}

int64_t ObArchiveDestParaItem::get_dest_no() const
{
  return dest_no_;
}

bool ObArchiveDestParaItem::is_pkey_valid() const
{
  return !name_.is_empty() && dest_no_ >= 0;
}

int ObArchiveDestParaItem::fill_pkey_dml(share::ObDMLSqlSplicer &dml) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_NO, dest_no_))) {
    LOG_WARN("failed to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column("name", name_.ptr()))) {
    LOG_WARN("failed to add column", K(ret));
  }

  return ret;
}

// Parse one full item from sql result, the result has full columns.
int ObArchiveDestParaItem::parse_from(sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char name_str[OB_INNER_TABLE_DEFAULT_KEY_LENTH] = {0};

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, dest_no_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, "name", name_str, OB_INNER_TABLE_DEFAULT_KEY_LENTH, real_length);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(set_kv_name(name_str))) {
    LOG_WARN("failed to set name", K(ret), K(name_str));
  } else if (OB_FAIL(value_->parse_value_from(result))) {
    LOG_WARN("failed to parse value", K(ret), K(name_str));
  }

  return ret;
}



/**
 * ------------------------------ObArchivePersistHelper---------------------
 */
ObArchivePersistHelper::ObArchivePersistHelper()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID)
{

}

uint64_t ObArchivePersistHelper::get_exec_tenant_id() const
{
  return gen_meta_tenant_id(tenant_id_);
}

int ObArchivePersistHelper::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObArchivePersistHelper init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObArchivePersistHelper::get_archive_mode(
    common::ObISQLClient &proxy, ObArchiveMode &mode) const
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  const bool for_update = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(tenant_id_, &proxy, for_update, tenant_info))) {
    LOG_WARN("failed to get tenant info", K(ret), K_(tenant_id));
  } else {
    mode = tenant_info.get_log_mode();
  }
  return ret;
}

int ObArchivePersistHelper::open_archive_mode(common::ObISQLClient &proxy) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_log_mode(tenant_id_, &proxy, NOARCHIVE_MODE, ARCHIVE_MODE))) {
    LOG_WARN("failed to open archive mode", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::close_archive_mode(common::ObISQLClient &proxy) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(ObAllTenantInfoProxy::update_tenant_log_mode(tenant_id_, &proxy, ARCHIVE_MODE, NOARCHIVE_MODE))) {
    LOG_WARN("failed to close archive mode", K(ret), K_(tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::lock_archive_dest(
    common::ObISQLClient &trans, 
    const int64_t dest_no,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupPathString path;
  if (OB_FAIL(get_archive_dest(trans, true /* need_lock */, dest_no, path))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_exist = false;
    } else {
      LOG_WARN("failed to get archive dest", K(ret), K(dest_no));
    }
  } else {
    is_exist = true;
  }

  return ret;
}


int ObArchivePersistHelper::lock_backup_archive_dest(
    common::ObISQLClient &trans,
    bool &is_exist) const
{
  int ret = OB_SUCCESS;
  ObBackupPathString path;
  if (OB_FAIL(get_backup_archive_dest(trans, true /* need_lock */, path))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      is_exist = false;
    } else {
      LOG_WARN("failed to get backup archive dest", K(ret));
    }
  } else {
    is_exist = true;
  }
  return ret;
}

int ObArchivePersistHelper::get_archive_dest(
    common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no,
    ObBackupPathString &path) const
{
  int ret = OB_SUCCESS;
  common::ObSqlString value;
  const common::ObString str(OB_STR_PATH);
  ObLogArchiveDestAtrr dest_attr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(get_string_value(proxy, dest_no, need_lock, str, value))) {
    LOG_WARN("fail to get string value", K(ret));
  } else if (OB_FAIL(path.assign(value.ptr()))) {
    LOG_WARN("fail to assign string value", K(ret), K(value));
  }
  return ret;
}

int ObArchivePersistHelper::get_backup_archive_dest(
    common::ObISQLClient &proxy,
    const bool need_lock,
    ObBackupPathString &path) const
{
  int ret = OB_SUCCESS;
  common::ObSqlString value;
  common::ObSqlString name;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(name.assign_fmt("%s%s", OB_STR_BACKUP_ARCHIVE_PREFIX, OB_STR_PATH))) {
    LOG_WARN("fail to assign backup archive path key", K(ret));
  } else if (OB_FAIL(get_string_value(proxy, OB_BACKUP_ARCHIVE_DEST_NO_OFFSET, need_lock, name.string(), value))) {
    LOG_WARN("fail to get backup archive path value", K(ret));
  } else if (OB_FAIL(path.assign(value.ptr()))) {
    LOG_WARN("fail to assign backup archive path value", K(ret), K(value));
  }
  return ret;
}


int ObArchivePersistHelper::get_dest_id(
    common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no,
    int64_t &dest_id) const
{
  int ret = OB_SUCCESS;

  ObInnerKVTableOperator kv_table_operator;
  ObInnerKVItemIntValue kv_value;
  ObArchiveDestParaItem item(&kv_value);
  ObInnerKVItemTenantIdWrapper item_with_tenant_id(&item);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(kv_table_operator.init(OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, *this))) {
    LOG_WARN("failed to init kv parameter table operator", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(item.set_dest_no(dest_no))) {
    LOG_WARN("failed to set dest no", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(item.set_kv_name(OB_STR_DEST_ID))) {
    LOG_WARN("failed to set kv name", K(ret));
  } else if (OB_FAIL(item_with_tenant_id.set_tenant_id(tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(kv_table_operator.get_item(proxy, need_lock, item_with_tenant_id))) {
    LOG_WARN("failed to get item", K(ret), K(need_lock), K(item_with_tenant_id));
  } else {
    dest_id = kv_value.get_value();
  }

  return ret;
}


int ObArchivePersistHelper::get_piece_switch_interval(
    common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no,
    int64_t &piece_switch_interval) const
{
  int ret = OB_SUCCESS;
  common::ObSqlString value;
  const common::ObString str(OB_STR_PIECE_SWITCH_INTERVAL);
  ObLogArchiveDestAtrr dest_attr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(get_string_value(proxy, dest_no, need_lock, str, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not exist, use default, 1d.
      piece_switch_interval = OB_DEFAULT_PIECE_SWITCH_INTERVAL;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get string value", K(ret));
    }
  } else if (OB_FAIL(dest_attr.set_piece_switch_interval(value.ptr()))) {
    LOG_WARN("fail to set piece switch interval", K(ret), K(value));
  } else {
    piece_switch_interval = dest_attr.piece_switch_interval_;
  }
  return ret;
}

int ObArchivePersistHelper::get_binding(common::ObISQLClient &proxy, const bool need_lock, const int64_t dest_no,
      ObLogArchiveDestAtrr::Binding &binding) const
{
  int ret = OB_SUCCESS;
  common::ObSqlString value;
  const common::ObString str(OB_STR_BINDING);
  ObLogArchiveDestAtrr dest_attr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(get_string_value(proxy, dest_no, need_lock, str, value))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not exist, use default, OPTIONAL.
      binding = ObLogArchiveDestAtrr::Binding::OPTIONAL;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get string value", K(ret));
    }
  } else if (OB_FAIL(dest_attr.set_binding(value.ptr()))) {
    LOG_WARN("fail to set binding", K(ret), K(value));
  } else {
    binding = dest_attr.binding_;
  }
  return ret;
}

int ObArchivePersistHelper::get_dest_state(
    common::ObISQLClient &proxy,
    const bool need_lock,
    const int64_t dest_no,
    ObLogArchiveDestState &state) const
{
  int ret = OB_SUCCESS;
  common::ObSqlString value;
  const common::ObString str(OB_STR_STATE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(get_string_value(proxy, dest_no, need_lock, str, value))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get string value", K(ret));
    }
  } else if (OB_FAIL(state.set_state(value.ptr()))) {
    LOG_WARN("fail to set dest state", K(ret), K(value));
  }
  return ret;
}

int ObArchivePersistHelper::set_kv_item(common::ObISQLClient &proxy,
    const int64_t dest_no, const common::ObSqlString &name,
    const common::ObSqlString &value) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerKVTableOperator kv_table_operator;
  ObInnerKVItemStringValue kv_value;
  ObArchiveDestParaItem item(&kv_value);
  ObInnerKVItemTenantIdWrapper item_with_tenant_id(&item);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, name and value must not be empty", K(ret), K(name), K(value));
  } else if (OB_FAIL(kv_table_operator.init(OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, *this))) {
    LOG_WARN("failed to init kv parameter table operator", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(item.set_dest_no(dest_no))) {
    LOG_WARN("failed to set dest no", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(kv_value.set_value(value.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(tenant_id_), K(value));
  } else if (OB_FAIL(item_with_tenant_id.set_tenant_id(tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(item_with_tenant_id.set_kv_name(name.ptr()))) {
    LOG_WARN("failed to set kv name", K(ret), K(name));
  } else if (OB_FAIL(kv_table_operator.insert_or_update_item(proxy, item_with_tenant_id, affected_rows))) {
    LOG_WARN("failed to set backup_dest", K(ret), K(item_with_tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::del_dest(common::ObISQLClient &proxy, const int64_t dest_no)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = -1;
  if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, tenant_id_))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_DEST_NO, dest_no))) {
    LOG_WARN("fail to add column", K(ret));
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, sql))) {
    LOG_WARN("failed to splice delete sql", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("del dest succeed", K(dest_no), K(get_exec_tenant_id()));
  }
  return ret;
}

int ObArchivePersistHelper::set_dest_state(common::ObISQLClient &proxy, const int64_t dest_no,
    const ObLogArchiveDestState &state)
{
  int ret = OB_SUCCESS;
  common::ObSqlString name;
  common::ObSqlString value;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (!state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid state", K(ret), K(dest_no), K(state));
  } else if (OB_FAIL(name.assign(OB_STR_STATE))) {
    LOG_WARN("failed to assign str", K(ret));
  } else if (OB_FAIL(value.assign(state.get_str()))) {
    LOG_WARN("failed to assign state", K(ret), K(dest_no), K(state));
  } else if (OB_FAIL(set_kv_item(proxy, dest_no, name, value))) {
    LOG_WARN("failed to set state", K(ret), K(dest_no), K(state));
  }

  return ret;
}

int ObArchivePersistHelper::get_string_value(common::ObISQLClient &proxy,
    const int64_t dest_no, const bool need_lock,
    const common::ObString &name, common::ObSqlString &value) const
{
  int ret = OB_SUCCESS;
  ObInnerKVTableOperator kv_table_operator;
  ObInnerKVItemStringValue kv_value;
  ObArchiveDestParaItem item(&kv_value);
  ObInnerKVItemTenantIdWrapper item_with_tenant_id(&item);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(kv_table_operator.init(OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, *this))) {
    LOG_WARN("failed to init kv parameter table operator", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(item.set_dest_no(dest_no))) {
    LOG_WARN("failed to set dest no", K(ret), K(tenant_id_), K(dest_no));
  } else if (OB_FAIL(item_with_tenant_id.set_tenant_id(tenant_id_))) {
    LOG_WARN("failed to set tenant id", K(ret), K(tenant_id_));
  } else if (OB_FAIL(item_with_tenant_id.set_kv_name(name.ptr()))) {
    LOG_WARN("failed to set kv name", K(ret));
  } else if (OB_FAIL(kv_table_operator.get_item(proxy, need_lock, item_with_tenant_id))) {
    LOG_WARN("failed to get item", K(ret), K(need_lock), K(item_with_tenant_id));
  } else if (OB_FAIL(value.assign(kv_value.get_value()))) {
    LOG_WARN("failed to assign value", K(ret), K(need_lock), K(item_with_tenant_id));
  }

  return ret;
}

int ObArchivePersistHelper::get_valid_dest_pairs(common::ObISQLClient &proxy,
    common::ObIArray<std::pair<int64_t, int64_t>> &pair_array) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt("select dest_no, value from %s where %s=%lu and name='%s' order by %s asc",
        OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, OB_STR_DEST_NO))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_dest_pair_result_(*result, pair_array))) {
        LOG_WARN("failed to parse dest pair result", K(ret));
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::get_valid_dest_pairs(common::ObISQLClient &proxy,
    common::ObIArray<std::pair<int64_t, ObBackupPathString>> &pair_array) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt("select dest_no, value from %s where %s=%lu and name='%s' order by %s asc",
        OB_ALL_LOG_ARCHIVE_DEST_PARAMETER_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_PATH, OB_STR_DEST_NO))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_dest_pair_result_(*result, pair_array))) {
        LOG_WARN("failed to parse dest pair result", K(ret));
      }
    }
  }

  return ret;
}


int ObArchivePersistHelper::get_round(common::ObISQLClient &proxy, const int64_t dest_no,
    const bool need_lock, ObTenantArchiveRoundAttr &round) const
{
  int ret = OB_SUCCESS;

  ObInnerTableOperator round_table_operator;
  ObTenantArchiveRoundAttr::Key key = {tenant_id_, dest_no};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(round_table_operator.get_row(proxy, need_lock, key, round))) {
    LOG_WARN("failed to get round", K(ret));
  }

  return ret;
}

int ObArchivePersistHelper::get_round_by_dest_id(common::ObISQLClient &proxy, const int64_t dest_id,
      const bool need_lock, ObTenantArchiveRoundAttr &round) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive table operator not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select * from %s where %s=%ld",
    OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, OB_STR_DEST_ID, dest_id))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    common::ObArray<ObTenantArchiveRoundAttr> rounds;
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_round_result_(*result, rounds))) {
        LOG_WARN("failed to parse result", K(ret), K(sql));
      } else if (rounds.size() > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("more than 2 rounds with same dest id", K(ret), K(sql), K(rounds));
      } else if (rounds.size() == 0) {
        // no round match the dest id
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(round.deep_copy_from(rounds.at(0)))) {
        LOG_WARN("failed to deep copy round", K(ret), K(rounds));
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::get_round_stopping_ts(
    common::ObISQLClient &proxy, const int64_t dest_no, int64_t &ts) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive table operator not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select time_to_usec(gmt_modified) as stopping_ts from %s where tenant_id=%ld and dest_no=%ld and status='STOPPING'",
                     OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME,
                     tenant_id_,
                     dest_no))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          LOG_WARN("failed to get next", K(ret));
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "stopping_ts", ts, int64_t);
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::del_round(common::ObISQLClient &proxy, const int64_t dest_no) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator round_table_operator;
  ObTenantArchiveRoundAttr::Key key = {tenant_id_, dest_no};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(round_table_operator.delete_row(proxy, key, affected_rows))) {
    LOG_WARN("failed to delete round", K(ret), K(key));
  }

  return ret;
}

int ObArchivePersistHelper::start_new_round(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &new_round) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator round_table_operator;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (!new_round.state_.is_prepare()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round state", K(ret), K(new_round));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(round_table_operator.insert_or_update_row(proxy, new_round, affected_rows))) {
    LOG_WARN("failed to start new round", K(ret), K(new_round));
  }

  return ret;
}

int ObArchivePersistHelper::stop_round(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &old_round,
    const ObTenantArchiveRoundAttr &new_round) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (!new_round.state_.is_stop()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid round state", K(ret), K(new_round));
  } else if (OB_FAIL(switch_round_state_to(proxy, old_round, new_round))) {
    LOG_WARN("failed to stop round", K(ret), K(old_round), K(new_round));
  }

  return ret;
}

int ObArchivePersistHelper::switch_round_state_to(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &round,
    const ObArchiveRoundState &new_state) const
{
  int ret = OB_SUCCESS;
  ObSqlString assignments;
  ObSqlString predicates;
  ObInnerTableOperator round_table_operator;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(predicates.assign_fmt("%s = '%s' and %s = %ld and %s = %lu",
                                           OB_STR_STATUS,
                                           round.state_.to_status_str(),
                                           OB_STR_ROUND_ID,
                                           round.round_id_,
                                           OB_STR_CHECKPOINT_SCN,
                                           round.checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to assign predicates", K(ret), K(round));
  } else if (OB_FAIL(assignments.assign_fmt("%s='%s'", OB_STR_STATUS, new_state.to_status_str()))) {
    LOG_WARN("failed to assign assignments", K(ret), K(new_state));
  } else if (OB_FAIL(round_table_operator.compare_and_swap(proxy, round, assignments.ptr(), predicates.ptr(), affected_rows))) {
    LOG_WARN("failed to switch round state", K(ret), K(round), K(new_state));
  }

  return ret;
}

int ObArchivePersistHelper::switch_round_state_to(common::ObISQLClient &proxy, const ObTenantArchiveRoundAttr &old_round,
    const ObTenantArchiveRoundAttr &new_round) const
{
  int ret = OB_SUCCESS;
  ObSqlString condition;
  ObSqlString assignments;
  ObInnerTableOperator round_table_operator;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(condition.assign_fmt("%s = '%s' and %s = %ld and %s = %lu",
                                          OB_STR_STATUS,
                                          old_round.state_.to_status_str(),
                                          OB_STR_ROUND_ID,
                                          old_round.round_id_,
                                          OB_STR_CHECKPOINT_SCN,
                                          old_round.checkpoint_scn_.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to assign condition", K(ret), K(old_round));
  } else if (OB_FAIL(new_round.build_assignments(assignments))) {
    LOG_WARN("failed to build assignments", K(ret), K(new_round));
  } else if (OB_FAIL(round_table_operator.compare_and_swap(proxy, old_round, assignments.ptr(), condition.ptr(), affected_rows))) {
    LOG_WARN("failed to switch round state", K(ret), K(old_round), K(new_round));
  } else if (0 == affected_rows) {
    ret = OB_EAGAIN;
    LOG_WARN("round attr has changed, may be leader switched.", K(ret), K(old_round), K(new_round));
  }

  return ret;
}

int ObArchivePersistHelper::get_all_active_rounds(common::ObISQLClient &proxy,
    common::ObIArray<ObTenantArchiveRoundAttr> &rounds) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant archive table operator not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select * from %s", OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_round_result_(*result, rounds))) {
        LOG_WARN("failed to parse result", K(ret), K(sql));
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::get_his_round(common::ObISQLClient &proxy, const int64_t dest_no,
    const int64_t round_id, ObTenantArchiveHisRoundAttr &his_round) const
{
  int ret = OB_SUCCESS;

  ObInnerTableOperator his_round_table_operator;
  ObTenantArchiveHisRoundAttr::Key key = {tenant_id_, dest_no, round_id};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(his_round_table_operator.init(OB_ALL_LOG_ARCHIVE_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init round progress history table", K(ret));
  } else if (OB_FAIL(his_round_table_operator.get_row(proxy, false, key, his_round))) {
    LOG_WARN("failed to get his round", K(ret));
  }

  return ret;
}

int ObArchivePersistHelper::insert_his_round(common::ObISQLClient &proxy, const ObTenantArchiveHisRoundAttr &his_round) const
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObInnerTableOperator his_round_table_operator;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(his_round_table_operator.init(OB_ALL_LOG_ARCHIVE_HISTORY_TNAME, *this))) {
    LOG_WARN("failed to init round progress history table", K(ret));
  } else if (OB_FAIL(his_round_table_operator.insert_row(proxy, his_round, affected_rows))) {
    LOG_WARN("failed to get round", K(ret), K(his_round));
  }

  return ret;
}

int ObArchivePersistHelper::is_all_piece_in_round_deleted(
    common::ObISQLClient &proxy,
    const int64_t round_id,
    const bool check_backup_file,
    bool &is_piece_all_deleted) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  is_piece_all_deleted = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select count(1) as cnt from %s where %s=%lu and %s=%ld and %s!='%s'",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_,
          OB_STR_ROUND_ID, round_id, check_backup_file ? OB_STR_BACKUP_FILE_STATUS : OB_STR_FILE_STATUS, OB_STR_DELETED))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      int64_t count = 0;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to get next", K(ret), K_(tenant_id), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
        is_piece_all_deleted = (count == 0);
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_piece(common::ObISQLClient &proxy, const int64_t dest_id,
    const int64_t round_id, const int64_t piece_id, const bool need_lock, ObTenantArchivePieceAttr &piece) const
{
  int ret = OB_SUCCESS;

  ObInnerTableOperator piece_table_operator;
  ObTenantArchivePieceAttr::Key key = {tenant_id_, dest_id, round_id, piece_id};
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(piece_table_operator.init(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, *this))) {
    LOG_WARN("failed to init piece files table", K(ret));
  } else if (OB_FAIL(piece_table_operator.get_row(proxy, need_lock, key, piece))) {
    LOG_WARN("failed to get piece", K(ret), K(key));
  }

  return ret;
}

int ObArchivePersistHelper::get_piece(common::ObISQLClient &proxy, const int64_t dest_id,
      const int64_t piece_id, const bool need_lock, ObTenantArchivePieceAttr &piece) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select * from %s where %s=%lu and %s=%ld and %s=%ld",
    OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_PIECE_ID, piece_id))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no row exist", K(ret), K_(tenant_id), K(dest_id), K(piece_id));
        } else {
          LOG_WARN("failed to get next", K(ret), K_(tenant_id), K(sql));
        }
      } else if (OB_FAIL(piece.parse_from(*result))) {
        LOG_WARN("failed to parse piece", K(ret));
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::get_piece(common::ObISQLClient &proxy, const int64_t piece_id,
          const bool need_lock, ObTenantArchivePieceAttr &piece) const {
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("select * from %s where %s=%lu and %s=%ld",
    OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_PIECE_ID, piece_id))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("no row exist", K(ret), K_(tenant_id), K(piece_id));
        } else {
          LOG_WARN("failed to get next", K(ret), K_(tenant_id), K(sql));
        }
      } else if (OB_FAIL(piece.parse_from(*result))) {
        LOG_WARN("failed to parse piece", K(ret));
      } else if (OB_SUCC(result->next())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get more than one piece for specific piece_id, unexpected", K(ret), K(sql), K(piece_id));
      } else if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next result", K(ret), K(sql));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_pieces(
    common::ObISQLClient &proxy,
    const int64_t dest_id,
    common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),  K(dest_id));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld and %s!='%s'",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_FILE_STATUS, OB_STR_DELETED))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
        LOG_WARN("failed to parse result", K(ret));
      } else {
        LOG_INFO("success get piece", K(sql), K(piece_list));
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::build_available_piece_predicate_(common::ObSqlString &predicate) const
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
    LOG_WARN("failed to get min data version", K(ret), K_(tenant_id));
  } else if (data_version < ENABLE_BACKUP_ARCHIVE_VERSION) {
    // 'backup_file_status' column does not exist before ENABLE_BACKUP_ARCHIVE_VERSION,
    if (OB_FAIL(predicate.assign_fmt("file_status != '%s'", OB_STR_DELETED))) {
      LOG_WARN("failed to assign predicate", K(ret));
    }
  } else if (OB_FAIL(predicate.assign_fmt("(file_status != '%s' or backup_file_status = '%s')",
      OB_STR_DELETED, ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_AVAILABLE)))) {
    // A piece deleted on log archive dest maybe still available on BACKUP_ARCHIVE_DEST, let it pass.
    LOG_WARN("failed to assign predicate", K(ret));
  }
  return ret;
}

int ObArchivePersistHelper::get_frozen_pieces(
    common::ObISQLClient &proxy,
    const int64_t dest_id,
    const int64_t upper_piece_id,
    common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicate;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),  K(dest_id));
  } else if (OB_FAIL(build_available_piece_predicate_(predicate))) {
    LOG_WARN("failed to build available piece predicate", K(ret));
  } else {
    ObArchivePieceStatus frozen = ObArchivePieceStatus::frozen();
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      // Filter unavailable pieces in sql to avoid pulling the whole archive history back.
      // A deleted log archive dest piece maybe available on BACKUP_ARCHIVE_DEST, the version
      // gated predicate lets it pass, and avoids referring to the new 'backup_file_status'
      // column in sql during upgrade.
      if (OB_FAIL(sql.assign_fmt("select * from %s where tenant_id=%lu and dest_id=%ld and status='%s' and %s and piece_id<%ld order by piece_id asc",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, tenant_id_, dest_id, frozen.to_status_str(), predicate.ptr(), upper_piece_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
        LOG_WARN("failed to parse result", K(ret));
      } else {
        LOG_INFO("success get piece", K(sql), K(piece_list));
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_candidate_obsolete_backup_pieces(common::ObISQLClient &proxy, const SCN &end_scn,
    const char *backup_dest_str, ObIArray<ObTenantArchivePieceAttr> &pieces) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_ISNULL(backup_dest_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup_dest_str", K(ret), K(backup_dest_str));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s<=%lu and %s='%s' and %s!='%s'",
      OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_END_SCN,
      end_scn.get_val_for_inner_table_field(), OB_STR_PATH, backup_dest_str, OB_STR_FILE_STATUS, OB_STR_DELETED))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, pieces))) {
        LOG_WARN("failed to do parse pieces", K(ret));
      } else {
        FLOG_INFO("success get candidate obsolete pieces", K(sql), K(pieces));
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_candidate_obsolete_backup_archive_pieces(
    common::ObISQLClient &proxy,
    const SCN &end_scn,
    const int64_t src_dest_id,
    common::ObIArray<ObTenantArchivePieceAttr> &pieces) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (!end_scn.is_valid() || src_dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(end_scn), K(src_dest_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld and %s<=%lu and %s!='%s'",
      OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, src_dest_id,
      OB_STR_END_SCN, end_scn.get_val_for_inner_table_field(), OB_STR_BACKUP_FILE_STATUS, OB_STR_DELETED))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, pieces))) {
        LOG_WARN("failed to parse pieces", K(ret));
      } else {
        FLOG_INFO("success get candidate obsolete backup archive pieces", K(sql), K(pieces));
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::insert_or_update_piece(common::ObISQLClient &proxy, const ObTenantArchivePieceAttr &piece) const
{
  int ret = OB_SUCCESS;
   int64_t affected_rows = 0;
  ObInnerTableOperator piece_table_operator;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(piece_table_operator.init(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, *this))) {
    LOG_WARN("failed to init piece files table", K(ret));
  } else if (OB_FAIL(piece_table_operator.insert_or_update_row(proxy, piece, affected_rows))) {
    LOG_WARN("failed to insert or update piece", K(ret), K(piece));
  }

  return ret;
}

int ObArchivePersistHelper::batch_update_pieces(common::ObISQLClient &proxy, const common::ObIArray<ObTenantArchivePieceAttr> &pieces_array) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < pieces_array.count(); i++) {
    const ObTenantArchivePieceAttr &piece = pieces_array.at(i);
    if (OB_FAIL(insert_or_update_piece(proxy, piece))) {
      LOG_WARN("insert or update piece failed", K(ret), K(piece), K(pieces_array));
    }
  }

  return ret;
}

int ObArchivePersistHelper::mark_new_piece_file_status(common::ObISQLClient &proxy, const int64_t dest_id,
    const int64_t round_id, const int64_t piece_id, const ObBackupFileStatus::STATUS new_status) const
{
  int ret = OB_SUCCESS;
  ObInnerTableOperator piece_table_operator;
  ObTenantArchivePieceAttr::Key key = {tenant_id_, dest_id, round_id, piece_id};
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(piece_table_operator.init(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, *this))) {
    LOG_WARN("failed to init piece files table", K(ret));
  } else if (OB_FAIL(piece_table_operator.update_string_column(proxy, key, OB_STR_FILE_STATUS, ObBackupFileStatus::get_str(new_status), affected_rows))) {
    LOG_WARN("failed to mark new piece file status", K(ret), K(key), K(new_status));
  }

  return ret;
}

int ObArchivePersistHelper::mark_piece_backup_file_status(
    common::ObISQLClient &proxy,
    const int64_t dest_id,
    const int64_t round_id,
    const int64_t piece_id,
    const ObBackupFileStatus::STATUS old_status,
    const ObBackupFileStatus::STATUS new_status) const
{
  int ret = OB_SUCCESS;
  ObSqlString assignments;
  ObSqlString predicates;
  ObInnerTableOperator piece_table_operator;
  ObTenantArchivePieceAttr::Key key = {tenant_id_, dest_id, round_id, piece_id};
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0 || round_id <= 0 || piece_id <= 0
          || !ObBackupFileStatus::is_valid(old_status) || !ObBackupFileStatus::is_valid(new_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_id), K(round_id), K(piece_id), K(old_status), K(new_status));
  } else if (old_status == new_status) {
    // do nothing
  } else if (OB_FAIL(ObBackupFileStatus::check_can_change_status(old_status, new_status, true/*for_archive_backup*/))) {
    LOG_WARN("can not change piece backup file status", K(ret), K(old_status), K(new_status));
  } else if (OB_FAIL(piece_table_operator.init(OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, *this))) {
    LOG_WARN("failed to init piece files table", K(ret));
  } else if (OB_FAIL(assignments.assign_fmt("%s='%s'", OB_STR_BACKUP_FILE_STATUS, ObBackupFileStatus::get_str(new_status)))) {
    LOG_WARN("failed to build assignments", K(ret), K(new_status));
  } else if (OB_FAIL(predicates.assign_fmt("%s='%s'", OB_STR_BACKUP_FILE_STATUS, ObBackupFileStatus::get_str(old_status)))) {
    LOG_WARN("failed to build predicates", K(ret), K(old_status));
  } else if (OB_FAIL(piece_table_operator.compare_and_swap(proxy, key, assignments.ptr(), predicates.ptr(), affected_rows))) {
    LOG_WARN("failed to mark piece backup file status", K(ret), K(key), K(old_status), K(new_status));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key), K(old_status), K(new_status));
  }
  return ret;
}

int ObArchivePersistHelper::reset_all_pieces_backup_file_status(common::ObISQLClient &proxy) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const char *incomplete_str = ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_INCOMPLETE);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.assign_fmt("update %s set backup_file_status = '%s' where tenant_id = %lu and backup_file_status != '%s'",
      OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, incomplete_str, tenant_id_, incomplete_str))) {
    LOG_WARN("failed to assign sql", K(ret), K_(tenant_id));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    LOG_INFO("succeed to reset all pieces backup file status", K_(tenant_id), K(affected_rows), K(sql));
  }

  return ret;
}

int ObArchivePersistHelper::get_unbackuped_frozen_pieces(
    common::ObISQLClient &proxy,
    const int64_t dest_id,
    common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_id));
  } else {
    ObArchivePieceStatus frozen = ObArchivePieceStatus::frozen();
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      // filter "DELETE" AND "DELETED" pieces
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld and %s='%s' and %s='%s' and %s='%s' "
                                 "order by %s asc, %s asc",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id,
          OB_STR_STATUS, frozen.to_status_str(), OB_STR_FILE_STATUS, ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_AVAILABLE),
          OB_STR_BACKUP_FILE_STATUS, ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_INCOMPLETE),
          OB_STR_ROUND_ID, OB_STR_PIECE_ID))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
        LOG_WARN("failed to parse result", K(ret), K(sql));
      }
    }
  }
  return ret;
}


int ObArchivePersistHelper::get_backed_up_frozen_pieces(
    common::ObISQLClient &proxy,
    const int64_t dest_id,
    common::ObIArray<ObTenantArchivePieceAttr> &piece_list) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_id));
  } else {
    ObArchivePieceStatus frozen = ObArchivePieceStatus::frozen();
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld and %s='%s' and %s='%s' and %s='%s' "
                                 "order by %s asc, %s asc",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id,
          OB_STR_STATUS, frozen.to_status_str(), OB_STR_FILE_STATUS, ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_AVAILABLE),
          OB_STR_BACKUP_FILE_STATUS, ObBackupFileStatus::get_str(ObBackupFileStatus::BACKUP_FILE_AVAILABLE),
          OB_STR_ROUND_ID, OB_STR_PIECE_ID))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
        LOG_WARN("failed to parse result", K(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_latest_ls_archive_progress(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
    const ObLSID &id, ObLSArchivePersistInfo &info, bool &record_exist) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.append_fmt("select * from %s where %s=%lu and %s=%ld and %s=%ld and %s=%ld order by %s desc limit 1",
        OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME, OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_ROUND_ID, round_id,
        OB_STR_LS_ID, id.id(), OB_STR_PIECE_ID))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          record_exist = false;
        } else {
          LOG_WARN("failed to get next", K(ret), K_(tenant_id), K(sql));
        }
      } else if (OB_FAIL(info.parse_from(*result))) {
        LOG_WARN("failed to parse ls archive piece", K(ret));
      } else {
        record_exist = true;
      }
    }
  }

  return ret;
}

int ObArchivePersistHelper::insert_ls_archive_progress(common::ObISQLClient &proxy,
    const ObLSArchivePersistInfo &info, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;

  ObInnerTableOperator ls_table_operator;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(ls_table_operator.init(OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init ls archive table", K(ret));
  } else if (OB_FAIL(ls_table_operator.insert_or_update_row(proxy, info, affected_rows))) {
    LOG_WARN("failed to insert or update ls archive", K(ret), K(info));
  }

  return ret;
}

int ObArchivePersistHelper::set_ls_archive_stop(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
    const ObLSID &id, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArchiveRoundState stop = ObArchiveRoundState::stop();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("update %s set %s='%s' where %s=%lu and %s=%ld and %s=%ld and %s=%ld", OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
    OB_STR_STATUS, stop.to_status_str(), OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_ROUND_ID, round_id, OB_STR_LS_ID, id.id()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K_(tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::set_ls_archive_suspend(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
    const ObLSID &id, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArchiveRoundState suspend = ObArchiveRoundState::suspend();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("update %s set %s='%s' where %s=%lu and %s=%ld and %s=%ld and %s=%ld", OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
    OB_STR_STATUS, suspend.to_status_str(), OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_ROUND_ID, round_id, OB_STR_LS_ID, id.id()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K_(tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::set_ls_archive_doing(common::ObISQLClient &proxy, const int64_t dest_id, const int64_t round_id,
    const ObLSID &id, int64_t &affected_rows) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObArchiveRoundState doing = ObArchiveRoundState::doing();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(sql.append_fmt("update %s set %s='%s' where %s=%lu and %s=%ld and %s=%ld and %s=%ld", OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
    OB_STR_STATUS, doing.to_status_str(), OB_STR_TENANT_ID, tenant_id_, OB_STR_DEST_ID, dest_id, OB_STR_ROUND_ID, round_id, OB_STR_LS_ID, id.id()))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(proxy.write(get_exec_tenant_id(), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K_(tenant_id));
  }
  return ret;
}

int ObArchivePersistHelper::get_dest_round_summary(common::ObISQLClient &proxy, const int64_t dest_id,
    const int64_t round_id, const int64_t since_piece_id, ObDestRoundSummary &summary) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString ls_piece_cnames;
  ObLSArchivePersistInfo ls_piece;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(ls_piece.build_column_names(ls_piece_cnames))) {
    LOG_WARN("failed to build ls piece column names", K(ret));
  } else if (OB_FAIL(sql.append_fmt("SELECT T1.ls_id AS ls_id_bak, T2.tenant_id AS tenant_id, dest_id, round_id, T2.ls_id AS ls_id, piece_id,"
    "incarnation, min_lsn, start_scn, max_lsn, checkpoint_scn, file_id, file_offset, input_bytes, output_bytes, T2.status AS status FROM %s AS T1", OB_ALL_LS_STATUS_TNAME))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" FULL OUTER JOIN (SELECT %s FROM %s WHERE tenant_id=%lu AND dest_id=%ld AND round_id=%ld AND piece_id>=%ld) AS T2",
    ls_piece_cnames.ptr(), OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME, tenant_id_, dest_id, round_id, since_piece_id))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" ON T1.ls_id=T2.ls_id ORDER BY ls_id, piece_id ASC"))) {
    LOG_WARN("failed to append sql", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_dest_round_summary_result_(*result, summary))) {
        LOG_WARN("failed to parse result", K(ret), K(sql));
      } else {
        // If all log streams have not started archive, then the following info need to be corrected.
        summary.tenant_id_ = tenant_id_;
        summary.dest_id_ = dest_id;
        summary.round_id_ = round_id;
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_piece_by_scn(common::ObISQLClient &proxy, const int64_t dest_id,
    const share::SCN &scn, ObTenantArchivePieceAttr &piece) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicate;
  ObArray<ObTenantArchivePieceAttr> piece_list;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),  K(dest_id));
  } else if (OB_FAIL(build_available_piece_predicate_(predicate))) {
    LOG_WARN("failed to build available piece predicate", K(ret));
  }

  if (OB_SUCC(ret)) {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("select * from %s where tenant_id=%lu and dest_id=%ld and %s and checkpoint_scn>=%ld order by piece_id asc limit 1",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, tenant_id_, dest_id, predicate.ptr(), scn.get_val_for_inner_table_field()))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
        LOG_WARN("failed to parse result", K(ret));
      } else if (piece_list.count() > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece list should not greater than 1", K(ret), K(scn));
      } else if (piece_list.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("no piece exist", K(ret), K(dest_id), K(sql), K(scn));
      } else {
        piece = piece_list.at(0);
      }
    }
  }
  return ret;
}

int ObArchivePersistHelper::get_pieces_by_range(common::ObISQLClient &proxy, const int64_t dest_id,
    const int64_t start_piece_id, const int64_t end_piece_id, ObIArray<ObTenantArchivePieceAttr> &pieces) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),  K(dest_id));
  } else {
    ObArchivePieceStatus frozen = ObArchivePieceStatus::frozen();
    ObArray<ObTenantArchivePieceAttr> tmp_piece_list;
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      ObMySQLResult *result = NULL;
      // Do not filter deleted pieces in sql, a deleted log archive dest piece maybe available on BACKUP_ARCHIVE_DEST.
      // Filtering after parsing also avoids referring to the new 'backup_file_status' column in sql during upgrade.
      if (OB_FAIL(sql.assign_fmt("select * from %s where tenant_id=%lu and dest_id=%ld and piece_id>=%ld and piece_id<=%ld order by piece_id asc",
          OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, tenant_id_, dest_id, start_piece_id, end_piece_id))) {
        LOG_WARN("failed to append fmt", K(ret));
      } else if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_piece_result_(*result, tmp_piece_list))) {
        LOG_WARN("failed to parse result", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_piece_list.count(); ++i) {
          const ObTenantArchivePieceAttr &piece = tmp_piece_list.at(i);
          if (piece.file_status_ == ObBackupFileStatus::BACKUP_FILE_DELETED &&
              piece.backup_file_status_ != ObBackupFileStatus::BACKUP_FILE_AVAILABLE) {
            // Filter the piece which is not available on both archive dest.
          } else if (OB_FAIL(pieces.push_back(piece))) {
            LOG_WARN("failed to push back piece", K(ret), K(piece));
          }
        }
        if (OB_SUCC(ret)) {
          LOG_INFO("success get piece", K(sql));
        }
      }
    }
  }
  return ret;
}


int ObArchivePersistHelper::parse_round_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObTenantArchiveRoundAttr> &rounds) const
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObTenantArchiveRoundAttr round;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(round.parse_from(result))) {
      LOG_WARN("failed to parse round result", K(ret));
    } else if (OB_FAIL(rounds.push_back(round))) {
      LOG_WARN("failed to push back round", K(ret));
    }
  }

  return ret;
}

int ObArchivePersistHelper::parse_piece_result_(sqlclient::ObMySQLResult &result, common::ObIArray<ObTenantArchivePieceAttr> &pieces) const
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObTenantArchivePieceAttr piece;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(piece.parse_from(result))) {
      LOG_WARN("failed to parse piece result", K(ret));
    } else if (OB_FAIL(pieces.push_back(piece))) {
      LOG_WARN("failed to push back piece", K(ret));
    }
  }

  return ret;
}


int ObArchivePersistHelper::parse_dest_round_summary_result_(sqlclient::ObMySQLResult &result, ObDestRoundSummary &summary) const
{
  int ret = OB_SUCCESS;
  ObLSDestRoundSummary ls_dest_round_summary;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    ObArchiveLSPieceSummary piece;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_ls_archive_piece_summary_result_(result, piece))) {
      LOG_WARN("failed to parse result", K(ret));
    } else if (ls_dest_round_summary.ls_id_ != piece.ls_id_ && ls_dest_round_summary.is_valid()) {
      if (OB_FAIL(summary.add_ls_dest_round_summary(ls_dest_round_summary))) {
        LOG_WARN("failed to push back ls_dest_round_summary", K(ret));
      } else {
        ls_dest_round_summary.reset();
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls_dest_round_summary.add_one_piece(piece))) {
      LOG_WARN("failed to add one piece", K(ret));
    }
  }

  // push backup last log stream
  if (OB_FAIL(ret)) {
  } else if (!ls_dest_round_summary.is_valid()) {
  } else if (OB_FAIL(summary.add_ls_dest_round_summary(ls_dest_round_summary))) {
    LOG_WARN("failed to push back ls_dest_round_summary", K(ret));
  }

  return ret;
}

int ObArchivePersistHelper::do_parse_ls_archive_piece_summary_result_(sqlclient::ObMySQLResult &result, ObArchiveLSPieceSummary &piece) const
{
  int ret = OB_SUCCESS;
  int64_t ls_id = 0;
  int64_t ls_id_bak = 0;
  int64_t real_length = 0;
  uint64_t start_scn = 0;
  uint64_t checkpoint_scn = 0;
  char status_str[OB_DEFAULT_STATUS_LENTH] = "";

  EXTRACT_INT_FIELD_MYSQL(result, "ls_id", ls_id, int64_t);
  if (OB_SUCC(ret)) {
    // log stream is in table __all_ls_log_archive_progress.
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, piece.tenant_id_, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, piece.dest_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, piece.round_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_ID, piece.piece_id_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INCARNATION, piece.incarnation_, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MIN_LSN, piece.min_lsn_, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_START_SCN, start_scn, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_MAX_LSN, piece.max_lsn_, uint64_t);
    EXTRACT_UINT_FIELD_MYSQL(result, OB_STR_CHECKPOINT_SCN, checkpoint_scn, uint64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_STATUS, status_str, OB_DEFAULT_STATUS_LENTH, real_length);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_INPUT_BYTES, piece.input_bytes_, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, OB_STR_OUTPUT_BYTES, piece.output_bytes_, int64_t);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(piece.state_.set_status(status_str))) {
      LOG_WARN("failed to set status", K(ret), K(status_str));
    } else if (OB_FAIL(piece.start_scn_.convert_for_inner_table_field(start_scn))) {
      LOG_WARN("failed to set start scn", K(ret), K(start_scn));
    } else if (OB_FAIL(piece.checkpoint_scn_.convert_for_inner_table_field(checkpoint_scn))) {
      LOG_WARN("failed to set checkpoint scn", K(ret), K(checkpoint_scn));
    } else {
      piece.ls_id_ = ObLSID(ls_id);
      piece.is_archiving_ = true;

      // but not exist in table __all_ls_status, must be deleted.
      EXTRACT_INT_FIELD_MYSQL(result, "ls_id_bak", ls_id_bak, int64_t);
      if (OB_SUCC(ret)) {
        piece.is_deleted_ = false;
      } else if (OB_ERR_NULL_VALUE == ret) {
        piece.is_deleted_ = true;
        ret = OB_SUCCESS;
      }
    }
  } else if (OB_ERR_NULL_VALUE == ret) {
    ret = OB_SUCCESS;
    // log stream is not in table __all_ls_log_archive_progress, but exist in __all_ls_status,
    // must has not been archived.
    EXTRACT_INT_FIELD_MYSQL(result, "ls_id_bak", ls_id_bak, int64_t);
    if (OB_FAIL(ret)) {
    } else {
      piece.tenant_id_ = tenant_id_;
      piece.ls_id_ = ObLSID(ls_id_bak);
      piece.is_archiving_ = false;
      piece.is_deleted_ = false;
      piece.dest_id_ = 0;
      piece.round_id_ = 0;
      piece.piece_id_ = 0;
      piece.incarnation_ = 0;
      piece.state_.set_invalid();
      piece.start_scn_ = SCN::min_scn();
      piece.checkpoint_scn_ = SCN::min_scn();
      piece.min_lsn_ = 0;
      piece.max_lsn_ = 0;
      piece.input_bytes_ = 0;
      piece.output_bytes_ = 0;
      LOG_INFO("encounter a log stream not started.", K(ret), K(piece));
    }
  }

  return ret;
}

int ObArchivePersistHelper::parse_dest_pair_result_(sqlclient::ObMySQLResult &result, common::ObIArray<std::pair<int64_t, int64_t>> &pair_array) const
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    std::pair<int64_t, int64_t> pair;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_dest_pair_(result, pair))) {
      LOG_WARN("failed to parse dest pair", K(ret));
    } else if (OB_FAIL(pair_array.push_back(pair))) {
      LOG_WARN("failed to push back dest pair", K(ret));
    }
  }

  return ret;
}

int ObArchivePersistHelper::parse_dest_pair_result_(sqlclient::ObMySQLResult &result, common::ObIArray<std::pair<int64_t, ObBackupPathString>> &pair_array) const
{
  int ret = OB_SUCCESS;
  // traverse each returned row
  while (OB_SUCC(ret)) {
    std::pair<int64_t, ObBackupPathString> pair;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(do_parse_dest_pair_(result, pair))) {
      LOG_WARN("failed to parse dest pair", K(ret));
    } else if (OB_FAIL(pair_array.push_back(pair))) {
      LOG_WARN("failed to push back dest pair", K(ret));
    }
  }

  return ret;
}

int ObArchivePersistHelper::do_parse_dest_pair_(sqlclient::ObMySQLResult &result, std::pair<int64_t, int64_t> &pair) const
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char value[OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = {0};

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, pair.first, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value, OB_INNER_TABLE_DEFAULT_VALUE_LENTH, real_length);
  if (OB_SUCC(ret) && OB_FAIL(ob_atoll(value, pair.second))) {
    LOG_WARN("atoll failed", K(ret), K(value));
  }

  return ret;
}

int ObArchivePersistHelper::do_parse_dest_pair_(sqlclient::ObMySQLResult &result, std::pair<int64_t, ObBackupPathString> &pair) const
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;

  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_NO, pair.first, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, "value", pair.second.ptr(), pair.second.capacity(), real_length);

  return ret;
}


int ObArchivePersistHelper::clean_round_comment(common::ObISQLClient &proxy, const int64_t dest_no) const
{
  int ret = OB_SUCCESS;

  ObInnerTableOperator round_table_operator;
  ObTenantArchiveRoundAttr::Key key = {tenant_id_, dest_no};
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (OB_FAIL(round_table_operator.init(OB_ALL_LOG_ARCHIVE_PROGRESS_TNAME, *this))) {
    LOG_WARN("failed to init round progress table", K(ret));
  } else if (OB_FAIL(round_table_operator.update_string_column(proxy, key, "comment"/* column name */, "", affected_rows))) {
    LOG_WARN("failed to clean round comment", K(ret), K(key));
  }

  return ret;
}

// Get the first piece which start scn is not greater than start_scn
// and the next piece which checkpoint scn is not less than end_scn
// Note: the two pieces are continuous if they has same round_id, otherwise not continuous.
int ObArchivePersistHelper::check_piece_continuity_between_two_scn(
    common::ObISQLClient &proxy, const int64_t dest_id,
    const share::SCN &start_scn, const share::SCN &end_scn, bool &is_continuous) const
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString predicate;
  ObArray<ObTenantArchivePieceAttr> piece_list;
  ObTenantArchivePieceAttr floor_piece;
  ObTenantArchivePieceAttr ceil_piece;
  bool floor_piece_found = false;
  bool ceil_piece_found = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObArchivePersistHelper not init", K(ret));
  } else if (dest_id <= 0 || !start_scn.is_valid() || !end_scn.is_valid() || start_scn >= end_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(build_available_piece_predicate_(predicate))) {
    LOG_WARN("failed to build available piece predicate", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.assign_fmt( // Get the latest piece whose start_scn <= start_scn
        "select * from %s where tenant_id = %lu and dest_id = %ld and %s and start_scn <= %ld order by piece_id desc limit 1",
        OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, tenant_id_, dest_id, predicate.ptr(), start_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("failed to assign sql format for floor piece", K(ret));
    } else {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
          LOG_WARN("failed to execute sql for floor piece", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null for floor piece", K(ret), K(sql));
        } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
          LOG_WARN("failed to parse floor piece result", K(ret));
        } else if (piece_list.count() == 1) {
          floor_piece = piece_list.at(0);
          floor_piece_found = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    piece_list.reset();
    sql.reset();
    if (OB_FAIL(sql.assign_fmt( // Get the first piece whose checkpoint_scn >= end_scn
        "select * from %s where tenant_id = %lu and dest_id = %ld and %s and checkpoint_scn >= %ld order by piece_id limit 1",
        OB_ALL_LOG_ARCHIVE_PIECE_FILES_TNAME, tenant_id_, dest_id, predicate.ptr(), end_scn.get_val_for_inner_table_field()))) {
      LOG_WARN("failed to assign sql format for ceil piece", K(ret));
    } else {
      HEAP_VAR(ObMySQLProxy::ReadResult, res) {
        ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy.read(res, get_exec_tenant_id(), sql.ptr()))) {
          LOG_WARN("failed to execute sql for ceil piece", K(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null for ceil piece", K(ret), K(sql));
        } else if (OB_FAIL(parse_piece_result_(*result, piece_list))) {
          LOG_WARN("failed to parse ceil piece result", K(ret));
        } else if (piece_list.count() == 1) {
          ceil_piece = piece_list.at(0);
          ceil_piece_found = true;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (floor_piece_found && ceil_piece_found && floor_piece.key_.round_id_ == ceil_piece.key_.round_id_) {
      is_continuous = true;
      LOG_INFO("success to find 2 pieces in the same round", K(is_continuous), K(floor_piece), K(ceil_piece));
    } else {
      is_continuous = false;
      LOG_INFO("fail to find 2 pieces in the same round", K(is_continuous), K(floor_piece), K(ceil_piece));
    }
  }
  return ret;
}


/*====================================== ObBackupArchivePieceTaskOperator ======================================*/
int ObBackupArchivePieceTaskOperator::insert_tasks(
    common::ObISQLClient &proxy,
    const common::ObIArray<ObBackupArchivePieceTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  if (!tasks.empty()) {
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    int64_t affected_rows = 0;
    const uint64_t tenant_id = tasks.at(0).key_.tenant_id_;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
      const ObBackupArchivePieceTaskAttr &task = tasks.at(i);
      char ip_buf[MAX_IP_ADDR_LENGTH] = "";
      char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";

      if (OB_UNLIKELY(!task.is_valid() || task.key_.tenant_id_ != tenant_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid backup archive piece task", K(ret), K(tenant_id), K(task));
      } else if (task.svr_addr_.is_valid() && !task.svr_addr_.ip_to_string(ip_buf, sizeof(ip_buf))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("failed to stringify server ip", K(ret), K(task));
      } else if (!task.task_trace_id_.is_invalid()) {
        task.task_trace_id_.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, task.key_.tenant_id_))
              || OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, task.key_.job_id_))
              || OB_FAIL(dml.add_pk_column(OB_STR_ROUND_ID, task.key_.round_id_))
              || OB_FAIL(dml.add_pk_column(OB_STR_PIECE_ID, task.key_.piece_id_))
              || OB_FAIL(dml.add_column(OB_STR_DEST_ID, task.archive_dest_id_))
              || OB_FAIL(dml.add_column(OB_STR_TASK_STATUS, task.task_status_.get_str()))
              || OB_FAIL(dml.add_column(OB_STR_SEVER_IP, task.svr_addr_.is_valid() ? ip_buf : ""))
              || OB_FAIL(dml.add_column(OB_STR_SERVER_PORT, task.svr_addr_.is_valid() ? task.svr_addr_.get_port() : 0))
              || OB_FAIL(dml.add_column(OB_STR_TASK_TRACE_ID, trace_id_str))
              || OB_FAIL(dml.add_column(OB_STR_RETRY_COUNT, task.retry_cnt_))
              || OB_FAIL(dml.add_column(OB_STR_RESULT, task.result_))) {
        LOG_WARN("failed to fill dml", K(ret), K(task));
      } else if (OB_FAIL(dml.finish_row())) {
        LOG_WARN("failed to finish row", K(ret), K(task));
      }
    }

    if (FAILEDx(dml.splice_batch_insert_sql(OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, sql))) {
      LOG_WARN("failed to splice batch insert sql", K(ret));
    } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to batch insert backup archive piece tasks", K(ret), K(sql), K(exec_tenant_id));
    } else if (OB_UNLIKELY(affected_rows != tasks.count())) { // rollback the backup archive job trans
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), "expected", tasks.count());
    }
  }
  return ret;
}

#define PARSE_PIECE_TASK_ROWS(sql_str, task_array)                                               \
do {                                                                                             \
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {                                                      \
    ObMySQLResult *result = nullptr;                                                             \
    if (FAILEDx(proxy.read(res, exec_tenant_id, (sql_str).ptr()))) {                             \
      LOG_WARN("failed to execute sql", K(ret), K(sql_str), K(exec_tenant_id));                  \
    } else if (OB_ISNULL(result = res.get_result())) {                                           \
      ret = OB_ERR_UNEXPECTED;                                                                   \
      LOG_WARN("result is null", K(ret), K(sql_str));                                            \
    }                                                                                            \
    while (OB_SUCC(ret)) {                                                                       \
      ObBackupArchivePieceTaskAttr tmp_task;                                                     \
      if (OB_FAIL(result->next())) {                                                             \
        if (OB_ITER_END == ret) {                                                                \
          ret = OB_SUCCESS;                                                                      \
        } else {                                                                                 \
          LOG_WARN("failed to get next row", K(ret));                                            \
        }                                                                                        \
        break;                                                                                   \
      } else if (OB_FAIL(ObBackupArchivePieceTaskOperator::do_parse_task_(*result, tmp_task))) { \
        LOG_WARN("failed to parse task", K(ret));                                                \
      } else if (OB_FAIL((task_array).push_back(tmp_task))) {                                    \
        LOG_WARN("failed to push task", K(ret), K(tmp_task));                                    \
      }                                                                                          \
    }                                                                                            \
  }                                                                                              \
} while (false)

int ObBackupArchivePieceTaskOperator::get_task(
    common::ObISQLClient &proxy,
    const ObBackupArchivePieceTaskAttr::Key &key,
    const bool need_lock,
    ObBackupArchivePieceTaskAttr &task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(key.tenant_id_);
  ObSEArray<ObBackupArchivePieceTaskAttr, 1> tasks;

  if (!key.is_pkey_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld and %s=%ld and %s=%ld",
      OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, OB_STR_TENANT_ID, key.tenant_id_, OB_STR_JOB_ID, key.job_id_,
      OB_STR_ROUND_ID, key.round_id_, OB_STR_PIECE_ID, key.piece_id_))) {
    LOG_WARN("failed to build sql", K(ret), K(key));
  } else if (need_lock && OB_FAIL(sql.append(" for update"))) {
    LOG_WARN("failed to append lock sql", K(ret), K(sql));
  } else {
    PARSE_PIECE_TASK_ROWS(sql, tasks);
  }

  if (OB_FAIL(ret)) {
  } else if (tasks.empty()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (OB_UNLIKELY(tasks.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected piece task count", K(ret), K(key), K(tasks.count()));
  } else {
    task = tasks.at(0);
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::get_tasks(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t job_id,
    const bool need_lock,
    common::ObIArray<ObBackupArchivePieceTaskAttr> &tasks)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (!is_user_tenant(tenant_id) || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where %s=%lu and %s=%ld order by %s, %s",
      OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_JOB_ID, job_id,
      OB_STR_ROUND_ID, OB_STR_PIECE_ID))) {
    LOG_WARN("failed to build sql", K(ret));
  } else if (need_lock && OB_FAIL(sql.append(" for update"))) {
    LOG_WARN("failed to append lock sql", K(ret), K(sql));
  } else {
    PARSE_PIECE_TASK_ROWS(sql, tasks);
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::update_task_status(
    common::ObISQLClient &proxy,
    const ObBackupArchivePieceTaskAttr::Key &key,
    const ObBackupTaskStatus &src_status,
    const ObBackupTaskStatus &dst_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(key.tenant_id_);

  if (!key.is_pkey_valid() || !src_status.is_valid() || !dst_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(src_status), K(dst_status));
  } else if (src_status.status_ == dst_status.status_) {
    // nothing to update
  } else if (OB_FAIL(sql.assign_fmt("update %s set %s='%s' where %s=%lu and %s=%ld and %s=%ld and %s=%ld and %s='%s'",
             OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, OB_STR_TASK_STATUS, dst_status.get_str(),
             OB_STR_TENANT_ID, key.tenant_id_, OB_STR_JOB_ID, key.job_id_, OB_STR_ROUND_ID, key.round_id_,
             OB_STR_PIECE_ID, key.piece_id_, OB_STR_TASK_STATUS, src_status.get_str()))) {
    LOG_WARN("failed to build sql", K(ret), K(key));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql), K(exec_tenant_id));
  } else if (affected_rows == 0) {
    ret = OB_EAGAIN;
    LOG_WARN("cas update failed due to status mismatch", K(ret), K(key), K(src_status), K(dst_status));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key), K(src_status), K(dst_status));
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::update_task_status(
    common::ObISQLClient &proxy,
    const ObBackupArchivePieceTaskAttr::Key &key,
    const ObBackupTaskStatus &src_status,
    const ObBackupTaskStatus &dst_status,
    const common::ObAddr &dst,
    const share::ObTaskId &trace_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char ip_buf[MAX_IP_ADDR_LENGTH] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  const uint64_t exec_tenant_id = gen_meta_tenant_id(key.tenant_id_);

  if (!key.is_pkey_valid() || !src_status.is_valid() || !dst_status.is_valid() || !dst.is_valid() || trace_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(src_status), K(dst_status), K(dst), K(trace_id));
  } else {
    dst.ip_to_string(ip_buf, sizeof(ip_buf));
    trace_id.to_string(trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE);
  }

  if (FAILEDx(sql.assign_fmt("update %s set %s='%s', %s='%s', %s=%d, %s='%s' where %s=%lu and %s=%ld and %s=%ld and %s=%ld and %s='%s'",
      OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, OB_STR_TASK_STATUS, dst_status.get_str(), OB_STR_SEVER_IP, ip_buf, OB_STR_SERVER_PORT, dst.get_port(), OB_STR_TASK_TRACE_ID, trace_id_str,
      OB_STR_TENANT_ID, key.tenant_id_, OB_STR_JOB_ID, key.job_id_, OB_STR_ROUND_ID, key.round_id_, OB_STR_PIECE_ID, key.piece_id_, OB_STR_TASK_STATUS, src_status.get_str()))) {
    LOG_WARN("failed to build sql", K(ret), K(key));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql), K(exec_tenant_id));
  } else if (affected_rows == 0) {
    ret = OB_EAGAIN;
    LOG_WARN("cas update failed due to status mismatch", K(ret), K(key), K(src_status), K(dst_status), K(dst));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key), K(src_status), K(dst_status), K(dst));
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::report_result(
    common::ObISQLClient &proxy,
    const ObBackupArchivePieceTaskAttr::Key &key,
    const ObBackupTaskStatus &src_status,
    const ObBackupTaskStatus &dst_status,
    const int64_t retry_cnt,
    const int result)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(key.tenant_id_);
  if (!key.is_pkey_valid() || !src_status.is_valid() || !dst_status.is_valid() || retry_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(src_status), K(dst_status), K(retry_cnt), K(result));
  } else if (OB_FAIL(dml.add_pk_column(OB_STR_TENANT_ID, key.tenant_id_))
          || OB_FAIL(dml.add_pk_column(OB_STR_JOB_ID, key.job_id_))
          || OB_FAIL(dml.add_pk_column(OB_STR_ROUND_ID, key.round_id_))
          || OB_FAIL(dml.add_pk_column(OB_STR_PIECE_ID, key.piece_id_))
          || OB_FAIL(dml.add_column(OB_STR_TASK_STATUS, dst_status.get_str()))
          || OB_FAIL(dml.add_column(OB_STR_RETRY_COUNT, retry_cnt))
          || OB_FAIL(dml.add_column(OB_STR_RESULT, result))) {
    LOG_WARN("failed to add column", K(ret), K(key));
  } else if (OB_FAIL(dml.splice_update_sql(OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret), K(key));
  } else if (OB_FAIL(sql.append_fmt(" and %s='%s'", OB_STR_TASK_STATUS, src_status.get_str()))) {
    LOG_WARN("failed to append cas guard", K(ret), K(key));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql), K(exec_tenant_id));
  } else if (affected_rows == 0) {
    ret = OB_EAGAIN;
    LOG_WARN("cas update failed due to status mismatch", K(ret), K(key), K(src_status), K(dst_status));
  } else if (affected_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(key), K(src_status), K(dst_status));
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::move_tasks_to_his(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t job_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (!is_user_tenant(tenant_id) || job_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(job_id));
  } else if (OB_FAIL(sql.assign_fmt("insert into %s select * from %s where %s=%lu and %s=%lu",
      OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_HISTORY_TNAME, OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME,
      OB_STR_TENANT_ID, tenant_id, OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else if (OB_FALSE_IT(sql.reset())) {
  } else if (OB_FAIL(sql.assign_fmt("delete from %s where %s=%lu and %s=%lu",
      OB_ALL_BACKUP_ARCHIVE_PIECE_TASK_TNAME, OB_STR_TENANT_ID, tenant_id, OB_STR_JOB_ID, job_id))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(proxy.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql), K(exec_tenant_id));
  } else {
    LOG_INFO("[BACKUP_ARCHIVE]succeed move backup archive piece tasks to history table", K(tenant_id), K(job_id));
  }
  return ret;
}

int ObBackupArchivePieceTaskOperator::do_parse_task_(
    sqlclient::ObMySQLResult &result,
    ObBackupArchivePieceTaskAttr &task)
{
  int ret = OB_SUCCESS;
  int64_t real_length = 0;
  char task_status[OB_DEFAULT_STATUS_LENTH] = "";
  char svr_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
  char trace_id_str[OB_MAX_TRACE_ID_BUFFER_SIZE] = "";
  int64_t svr_port = 0;
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_TENANT_ID, task.key_.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_JOB_ID, task.key_.job_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_ROUND_ID, task.key_.round_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_PIECE_ID, task.key_.piece_id_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_DEST_ID, task.archive_dest_id_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_STATUS, task_status, OB_DEFAULT_STATUS_LENTH, real_length);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_SEVER_IP, svr_ip, OB_MAX_SERVER_ADDR_SIZE, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_SERVER_PORT, svr_port, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(result, OB_STR_TASK_TRACE_ID, trace_id_str, OB_MAX_TRACE_ID_BUFFER_SIZE, real_length);
  EXTRACT_INT_FIELD_MYSQL(result, OB_STR_RETRY_COUNT, task.retry_cnt_, int64_t);
  EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, OB_STR_RESULT, task.result_, int, true /*skip_null_error*/, false /*skip_column_error*/, 0 /*default*/);
  if (FAILEDx(task.task_status_.set_status(task_status))) {
    LOG_WARN("failed to set task status", K(ret), K(task_status));
  } else if (0 != STRLEN(svr_ip) || 0 != svr_port) {
    if (false == task.svr_addr_.set_ip_addr(svr_ip, static_cast<int32_t>(svr_port))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to set server addr", K(ret), K(svr_ip), K(svr_port));
    }
  }

  if (OB_FAIL(ret) || 0 == strcmp(trace_id_str, "")) {
  } else if (OB_FAIL(task.task_trace_id_.set(trace_id_str))) {
    LOG_WARN("failed to set trace id", K(ret), K(trace_id_str));
  }
  return ret;
}
