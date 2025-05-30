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

#define USING_LOG_PREFIX RS


#include "ob_ddl_service.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace rootserver
{

static int get_ls_from_table(const uint64_t tenant_id, const share::schema::ObTableSchema &table_schema,
                             ObDDLSQLTransaction &trans, common::ObIArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  int64_t all_part_num = 0;
  if (-1 == (all_part_num = table_schema.get_all_part_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet num", K(table_schema), KR(ret));
  } else if (OB_FAIL(ls_ids.reserve(all_part_num))) {
    LOG_WARN("fail to reserve array", KR(ret), K(all_part_num));
  } else if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
    LOG_WARN("fail to get tablet ids", KR(ret), K(table_schema));
  } else {
    // use sys_ls if is sys tablet firstly
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (tablet_id.belong_to_sys_ls(tenant_id)) {
        if (OB_FAIL(ls_ids.push_back(share::SYS_LS))) {
          LOG_WARN("push back ls id fail", KR(ret), K(i), K(tenant_id), K(tablet_id));
        }
      }
    }
    if(OB_FAIL(ret)) {
    } else if (ls_ids.count() == 0) {
      // no sys tablet, get ls id from system table
      if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans, tenant_id, tablet_ids, ls_ids))) {
        LOG_WARN("fail to batch_get_ls", KR(ret), K(tablet_ids), K(ls_ids), K(tenant_id), K(table_schema));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ls_ids.count() != all_part_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls of tablet is not equal partition num", KR(ret), K(ls_ids.count()), K(all_part_num),
        K(tablet_ids), K(ls_ids), K(tenant_id), K(table_schema));
  }
  return ret;
}

static int check_ls_not_exist(const uint64_t tenant_id, const common::ObArray<share::ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  int64_t not_exist_ls_cnt = 0;
  for (int i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i){
    const ObLSID ls_id = ls_ids.at(i);
    ObLSExistState state;
    if (OB_FAIL(ObLocationService::check_ls_exist(tenant_id, ls_id, state))) {
      LOG_WARN("failed to check ls exist", KR(ret), K(i), K(tenant_id), K(ls_id));
    } else if (state.is_deleted()) {
      ++not_exist_ls_cnt;
      LOG_WARN("ls is deleted", KR(ret), K(i), K(tenant_id), K(ls_id), K(state));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 == not_exist_ls_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("all ls exist, unexcepted situation", KR(ret), K(tenant_id), K(not_exist_ls_cnt), K(ls_ids.count()), K(ls_ids));
    } else if (not_exist_ls_cnt != ls_ids.count()) {
      LOG_ERROR("there are some ls exist, some not exist", KR(ret), K(tenant_id), K(not_exist_ls_cnt), K(ls_ids.count()), K(ls_ids));
    } else {
      LOG_ERROR("all ls not exist", KR(ret), K(tenant_id), K(not_exist_ls_cnt), K(ls_ids.count()), K(ls_ids));
    }
  }
  return ret;
}

// Notice: this function is only used for dropping lob aux table that's main table has been dropped casued by some bugs.
int ObDDLService::force_drop_lonely_lob_aux_table(const obrpc::ObForceDropLonelyLobAuxTableArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid drop lob arg", KR(ret), K(arg));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ or sql_proxy_ is null", KR(ret), KP(schema_service_), KP(sql_proxy_));
  } else {
    ObSchemaGetterGuard schema_guard;
    ObDDLSQLTransaction trans(schema_service_);
    ObDDLOperator ddl_operator(*schema_service_, *sql_proxy_);
    int64_t refreshed_schema_version = 0;
    const ObTableSchema *lob_meta_table_schema_ptr = nullptr;
    const ObTableSchema *lob_piece_table_schema_ptr = nullptr;
    const uint64_t tenant_id = arg.get_tenant_id();
    uint64_t data_table_id = arg.get_data_table_id();
    bool exist = false;
    bool ignore_ls_not_exist_for_lob_meta = false;
    bool ignore_ls_not_exist_for_lob_piece = false;
    common::ObArray<share::ObLSID> lob_meta_ls_ids;
    common::ObArray<share::ObLSID> lob_piece_ls_ids;

    HEAP_VAR(ObTableSchema, tmp_lob_table_schema) {
      if (OB_FAIL(get_tenant_schema_guard_with_version_in_inner_table(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard with version in inner table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to get tenant schema version", KR(ret), K(tenant_id));
      } else if (OB_FAIL(trans.start(sql_proxy_, tenant_id, refreshed_schema_version))) {
        LOG_WARN("fail to start trans", KR(ret), K(tenant_id), K(refreshed_schema_version));

      // 1. check data table exist. if exist, it's not allowed to drop
      } else if (OB_FAIL(schema_guard.check_table_exist(tenant_id, data_table_id, exist))) {
        LOG_WARN("fail to check table exist", KR(ret), K(tenant_id), K(arg));
      } else if (exist) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("data table exist, so cannot drop lob table", KR(ret), K(arg));

      // 2. get and check lob meta table
      } else if (OB_FAIL(check_and_get_aux_table_schema(schema_guard, tenant_id, arg.get_aux_lob_meta_table_id(),
          data_table_id, ObTableType::AUX_LOB_META, lob_meta_table_schema_ptr))) {
        LOG_WARN("fail to get lob meta table schema", KR(ret), K(tenant_id), K(arg));
      // 3. get and check lob piece table
      } else if (OB_FAIL(check_and_get_aux_table_schema(schema_guard, tenant_id, arg.get_aux_lob_piece_table_id(),
          data_table_id, ObTableType::AUX_LOB_PIECE, lob_piece_table_schema_ptr))) {
        LOG_WARN("fail to get lob piece table schema", KR(ret), K(tenant_id), K(arg));

      // 4. drop lob meta table
      } else if (OB_FAIL(tmp_lob_table_schema.assign(*lob_meta_table_schema_ptr))) {
        LOG_WARN("fail to assign lob meta table schema", KR(ret));
      } else if (OB_FAIL(get_ls_from_table(tenant_id, tmp_lob_table_schema, trans, lob_meta_ls_ids))) {
        LOG_WARN("get ls of lob meta table fail", KR(ret), K(arg), K(tmp_lob_table_schema));
      } else if (OB_FAIL(ddl_operator.drop_table(tmp_lob_table_schema, trans, nullptr/*ddl_stmt_str*/, false/*is_truncate_table*/,
          nullptr/*drop_table_set*/, false/*is_drop_db*/, true/*delete_priv*/, true/*is_force_drop_lonely_lob_aux_table*/))) {
        LOG_WARN("fail to drop lob meta table", KR(ret), K(tmp_lob_table_schema));
        if (OB_LS_NOT_EXIST == ret || OB_LS_IS_DELETED == ret) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(check_ls_not_exist(tenant_id, lob_meta_ls_ids))) {
            LOG_WARN("check_ls_not_exist fail", KR(tmp_ret));
          } else {
            LOG_ERROR("ls not exist, ignore this when drop lob meta aux table", KR(ret), K(tenant_id), K(tmp_lob_table_schema));
            ret = OB_SUCCESS;
            ignore_ls_not_exist_for_lob_meta = true;
          }
        }
      }

      // 5. drop lob piece table
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(tmp_lob_table_schema.reset())) {
      } else if (OB_FAIL(tmp_lob_table_schema.assign(*lob_piece_table_schema_ptr))) {
        LOG_WARN("fail to assign lob piece table schema", KR(ret));
      } else if (OB_FAIL(get_ls_from_table(tenant_id, tmp_lob_table_schema, trans, lob_piece_ls_ids))) {
        LOG_WARN("get ls of lob piece table fail", KR(ret), K(arg), K(tmp_lob_table_schema));
      } else if (OB_FAIL(ddl_operator.drop_table(tmp_lob_table_schema, trans, nullptr/*ddl_stmt_str*/, false/*is_truncate_table*/,
          nullptr/*drop_table_set*/, false/*is_drop_db*/, true/*delete_priv*/, true/*is_force_drop_lonely_lob_aux_table*/))) {
        LOG_WARN("fail to drop lob piece table", KR(ret), K(tmp_lob_table_schema));
        if (OB_LS_NOT_EXIST == ret || OB_LS_IS_DELETED == ret) {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(check_ls_not_exist(tenant_id, lob_piece_ls_ids))) {
             LOG_WARN("check_ls_not_exist fail", KR(tmp_ret));
          } else {
            LOG_ERROR("ls not exist, ignore this when drop lob piece aux table", KR(ret), K(tenant_id), K(tmp_lob_table_schema));
            ret = OB_SUCCESS;
            ignore_ls_not_exist_for_lob_piece = true;
          }
        }
      }

      if (OB_SUCC(ret)) {
        if (ignore_ls_not_exist_for_lob_meta && ! ignore_ls_not_exist_for_lob_piece) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ignore_ls_not_exist_for_lob_meta but not ignore_ls_not_exist_for_lob_piece, unexpected situation", KR(ret), K(arg));
        } else if (! ignore_ls_not_exist_for_lob_meta && ignore_ls_not_exist_for_lob_piece) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("not ignore_ls_not_exist_for_lob_meta but ignore_ls_not_exist_for_lob_piece, unexpected situation", KR(ret), K(arg));
        }
      }
    }

    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
        LOG_WARN_RET(temp_ret, "trans end failed", "is_commit", OB_SUCCESS == ret, K(temp_ret));
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(publish_schema(tenant_id))) {
      LOG_WARN("publish_schema failed", KR(ret));
    }
  }
  LOG_ERROR("NOTICE: there are force_drop_lonely_lob_aux_table", KR(ret), K(arg));
  return ret;
}

int ObDDLService::check_and_get_aux_table_schema(ObSchemaGetterGuard &schema_guard, const uint64_t tenant_id, const uint64_t aux_table_id,
                                                 const uint64_t data_table_id, const ObTableType table_type, const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, aux_table_id, table_schema))) {
    LOG_WARN("failed get_table_schema", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("aux table schema is null", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type));
  } else if (table_schema->get_data_table_id() != data_table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("data table id is not match", KR(ret), K(tenant_id), K(data_table_id), K(table_type),
        K(table_schema->get_data_table_id()), KPC(table_schema));
  } else if (table_schema->get_table_type() != table_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not expected", KR(ret), K(tenant_id), K(aux_table_id), K(data_table_id), K(table_type),
        K(table_schema->get_table_type()), KPC(table_schema));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase