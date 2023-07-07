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
#define USING_LOG_PREFIX BALANCE

#include "ob_ls_all_part_builder.h"

#include "lib/container/ob_se_array.h"                        // ObSEArray
#include "share/ob_ls_id.h"                                   // ObLSID
#include "share/schema/ob_schema_getter_guard.h"              // ObSchemaGetterGuard
#include "share/schema/ob_multi_version_schema_service.h"     // ObMultiVersionSchemaService
#include "share/tablet/ob_tablet_to_ls_iterator.h"            // ObTenantTabletToLSIterator
#include "share/ob_balance_define.h"                          // need_balance_table()

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;
namespace rootserver
{

int ObLSAllPartBuilder::build(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObMultiVersionSchemaService &schema_service,
    ObISQLClient &sql_proxy,
    ObTransferPartList &part_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObLSID, 1> ls_white_list;
  ObTenantTabletToLSIterator iter;
  ObSchemaGetterGuard schema_guard;

  part_list.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tenant_id));
  } else if (OB_FAIL(get_latest_schema_guard_(tenant_id, schema_service, sql_proxy, schema_guard))) {
    LOG_WARN("get latest schema guard fail", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_white_list.push_back(ls_id))) {
    LOG_WARN("push back into LS white list fail", KR(ret), K(ls_white_list), K(ls_id));
  } else if (OB_FAIL(iter.init(sql_proxy, tenant_id, ls_white_list))) {
    LOG_WARN("init tenant tablet to LS iterator fail", KR(ret), K(tenant_id), K(ls_white_list));
  } else {
    ObTabletToLSInfo tablet;
    // iterate all tablets on this LS
    while (OB_SUCC(ret) && OB_SUCC(iter.next(tablet))) {
      ObTransferPartInfo part_info;
      bool need_skip = false;

      // build partition info for this tablet
      if (OB_FAIL(build_part_info_(tenant_id, tablet, schema_guard, part_info, need_skip))) {
        if (OB_NEED_RETRY != ret) {
          LOG_WARN("build part info fail", KR(ret), K(tenant_id), K(tablet));
        }
      } else if (! need_skip && OB_FAIL(part_list.push_back(part_info))) {
        LOG_WARN("failed to push back part_list", KR(ret), K(part_info));
      } else {
        tablet.reset();
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLSAllPartBuilder::get_latest_schema_guard_(
    const uint64_t tenant_id,
    ObMultiVersionSchemaService &schema_service,
    ObISQLClient &sql_proxy,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t version_in_inner_table = OB_INVALID_VERSION;
  int64_t schema_version_local = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status(tenant_id, OB_INVALID_TIMESTAMP, OB_INVALID_VERSION);

  // get local latest schema guard
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard fail", KR(ret), K(tenant_id));
  }
  // get latest schema version
  else if (OB_FAIL(schema_service.get_schema_version_in_inner_table(sql_proxy, schema_status,
      version_in_inner_table))) {
      LOG_WARN("fail to get latest schema version in inner table", KR(ret), K(schema_status),
          K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version_local))) {
    LOG_WARN("failed to get schema version", KR(ret), K(tenant_id));
  } else if (schema_version_local < version_in_inner_table) {
    ret = OB_NEED_RETRY;
    LOG_WARN("schema not refresh lastest, need wait", KR(ret), K(version_in_inner_table), K(schema_version_local));
  }
  return ret;
}

int ObLSAllPartBuilder::build_part_info_(
    const uint64_t tenant_id,
    ObTabletToLSInfo &tablet,
    ObSchemaGetterGuard &schema_guard,
    ObTransferPartInfo &part_info,
    bool &need_skip)
{
  int ret = OB_SUCCESS;
  const schema::ObSimpleTableSchemaV2 *table_schema = NULL;

  need_skip = false;

  if (OB_UNLIKELY(! tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet to ls info", KR(ret), K(tablet));
  } else if (OB_FAIL(schema_guard.get_simple_table_schema(tenant_id, tablet.get_table_id(), table_schema))) {
    LOG_WARN("get simple table schema fail", KR(ret), K(tenant_id), K(tablet));
  } else if (OB_ISNULL(table_schema)) {
    // schema guard may be not latest, can not find table schema, need retry
    ret = OB_NEED_RETRY;
    LOG_WARN("table schema is NULL, maybe schema guard is not latest, retry again", KR(ret), K(tablet));
  } else if (! need_balance_table(*table_schema)) {
    LOG_TRACE("[BUILD_LS_ALL_PART] ignore need not balance table", K(tablet));
    need_skip = true;
  } else {
    const ObTabletID &tablet_id = tablet.get_tablet_id();
    const uint64_t table_id = tablet.get_table_id();

    // part object id
    // for non-part table, is 0
    // for level-one part table, is partition id
    // for level-two part table, is subpartition id
    uint64_t part_object_id = OB_INVALID_ID;

    // level-one partition id
    int64_t part_id = OB_INVALID_INDEX;    // invalid value same with get_part_id_by_table()
    // level-two subpartition id
    int64_t subpart_id = OB_INVALID_INDEX; // invalid value same with get_part_id_by_table()

    // compute part object id based on table schema and tablet id
    if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
      part_object_id = 0;
    } else if (OB_FAIL(table_schema->get_part_id_by_tablet(tablet_id, part_id, subpart_id))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_WARN("tablet not exist in table schema, need retry", KR(ret), K(tablet_id), KPC(table_schema));
        ret = OB_NEED_RETRY;
      } else {
        LOG_WARN("get part id by tablet fail", KR(ret), K(tablet_id), KPC(table_schema));
      }
    } else if (PARTITION_LEVEL_ONE == table_schema->get_part_level()) {
      part_object_id = part_id;
    } else if (PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
      part_object_id = subpart_id;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition level", KR(ret), K(table_schema->get_part_level()),
          KPC(table_schema));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_info.init(table_id, part_object_id))) {
      LOG_WARN("init part info fail", KR(ret), K(table_id), K(part_object_id));
    }
  }
  return ret;
}

}
}
