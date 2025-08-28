/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_part_clip.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace table
{

int ObTablePartClipper::clip(const ObSimpleTableSchemaV2 &simple_schema,
                             ObTablePartClipType clip_type,
                             const ObIArray<ObTabletID> &src_tablet_ids,
                             ObIArray<ObTabletID> &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  bool table_cache_policy_is_hot = false;
  int64_t part_id = -1;
  int64_t subpart_id = -1;
  ObBasePartition *part = nullptr;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get data version", K(ret));
  } else if (data_version < DATA_VERSION_4_3_5_3) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support hot partition clipping less than 4_3_5_3", K(ret), K(data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "hot partition clipping less than 4_3_5_3");
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src_tablet_ids.count(); i++) {
    bool clip = true;
    const ObTabletID &tablet_id = src_tablet_ids.at(i);
    if (OB_FAIL(simple_schema.get_part_idx_by_tablet(tablet_id, part_id, subpart_id))) {
      LOG_WARN("fail to get part idx", K(ret), K(tablet_id), K(i));
    } else if (OB_FAIL(simple_schema.get_part_by_idx(part_id, subpart_id, part))) {
      LOG_WARN("fail to get part by idx", K(ret), K(tablet_id), K(part_id), K(subpart_id), K(i));
    } else if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part is null", K(ret), K(tablet_id), K(part_id), K(subpart_id), K(i));
    } else if (clip_type == ObTablePartClipType::HOT_ONLY) {
      const ObStorageCachePolicyType part_policy = part->get_part_storage_cache_policy_type();
      // If partition cache policy is NONE, then use table cache policy
      if (part_policy == ObStorageCachePolicyType::NONE_POLICY) {
        if (table_cache_policy_is_hot) {
          clip = false;
        } else {
          ObSchemaGetterGuard schema_guard;
          const ObTableSchema *table_schema = nullptr;
          const uint64_t table_id = simple_schema.get_table_id();
          ObStorageCachePolicy table_policy;
          if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid schema service", K(ret));
          } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(MTL_ID(), schema_guard))) {
            LOG_WARN("fail to get schema guard", K(ret));
          } else if (OB_FAIL(schema_guard.get_table_schema(MTL_ID(), table_id, table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(table_schema) || !table_schema->is_valid()) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("table not exist", K(ret), K(table_id));
          } else if (OB_FAIL(table_policy.load_from_string(table_schema->get_storage_cache_policy()))) {
            LOG_WARN("fail to load table storage cache policy", K(ret), K(table_schema->get_storage_cache_policy()));
          } else if (table_policy.is_hot_policy()) {
            table_cache_policy_is_hot = true;
            clip = false;
          }
        }
      } else if (part_policy == ObStorageCachePolicyType::HOT_POLICY) {
        clip = false;
      }
    } else {
      clip = false;
    }
    
    if (OB_FAIL(ret)) {
    } else if (!clip && OB_FAIL(dst_tablet_id.push_back(tablet_id))) {
      LOG_WARN("fail to push back tablet id", K(ret), K(tablet_id), K(i));
    }
    LOG_DEBUG("tablet clip", KPC(part), K(tablet_id), K(part_id), K(subpart_id), K(i));
  }

  LOG_DEBUG("clip result", K(clip_type), K(src_tablet_ids), K(dst_tablet_id));

  return ret;
}

} // end namespace table
} // end namespace oceanbase
