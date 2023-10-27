/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_move_response.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/ob_server_struct.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase::observer;
using namespace oceanbase::common;
using namespace oceanbase::table;
////////////////////////////////////////////////////////////////

int ObTableMoveResponseSender::get_replica(const uint64_t table_id,
                                           const common::ObTabletID &tablet_id,
                                           table::ObTableMoveReplicaInfo &replica)
{
  int ret = OB_SUCCESS;
  bool is_cache_hit = false;
  int64_t expire_renew_time = INT64_MAX; //对于get接口，需要传一个最大值，表示需要拿最新的location cache，并让老的失效掉
  share::ObLSID ls_id;
  share::ObLSLocation ls_loc;
  share::ObLSReplicaLocation replica_loc;

  if (OB_FAIL(GCTX.location_service_->get(MTL_ID(), tablet_id, expire_renew_time, is_cache_hit, ls_id))) {
    LOG_WARN("fail to get partition", K(ret), K(table_id), K(tablet_id));
  } else if (OB_FAIL(GCTX.location_service_->get(GCONF.cluster_id, MTL_ID(), ls_id, expire_renew_time, is_cache_hit, ls_loc))) {
    LOG_WARN("fail get partition", K(ret), K(table_id), K(tablet_id));
  } else if (OB_FAIL(ls_loc.get_leader(replica_loc))) {
    LOG_WARN("fail to get strong leader replica", K(ret));
  } else {
    replica.server_ = replica_loc.get_server();
    replica.role_ = replica_loc.get_role();
    replica.replica_type_ = replica_loc.get_replica_type();
    replica.part_renew_time_ = ls_loc.get_renew_time();
  }

  return ret;
}

int ObTableMoveResponseSender::init(const uint64_t table_id,
                                    const common::ObTabletID &tablet_id,
                                    share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;

  ObTableMoveReplicaInfo &replica = result_.get_replica_info();
  if (OB_FAIL(get_replica(table_id, tablet_id, replica))) {
    LOG_WARN("fail to get partition info", K(ret), K(table_id), K(tablet_id));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const share::schema::ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(MTL_ID(), schema_guard))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(table_id), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("NULL ptr", K(ret), K(table_id));
    } else {
      replica.set_table_id(table_id);
      replica.set_schema_version(table_schema->get_schema_version());
      replica.set_tablet_id(tablet_id);

      // set move pcode
      response_sender_.set_pcode(obrpc::OB_TABLE_API_MOVE);
      LOG_DEBUG("move response init successfully", K(replica));
    }
  }

  return ret;
}
