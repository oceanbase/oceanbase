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

#ifndef OCEANBASE_OBSERVER_OB_TABLE_PART_CLIP_H_
#define OCEANBASE_OBSERVER_OB_TABLE_PART_CLIP_H_
#include "observer/table/ob_table_cache.h"

namespace oceanbase
{

namespace table
{

enum class ObTablePartClipType
{
  NONE = 0,
  HOT_ONLY = 1
};

class ObTablePartClipper
{
public:
  ObTablePartClipper() {}
  ~ObTablePartClipper() {}
public:
  static int clip(const share::schema::ObSimpleTableSchemaV2 &simple_schema,
                  ObTablePartClipType clip_type,
                  const common::ObIArray<common::ObTabletID> &src_tablet_ids,
                  common::ObIArray<common::ObTabletID> &dst_tablet_id);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTablePartClipper);
};


} // end namespace table
} // end namespace oceanbase
#endif /* OCEANBASE_OBSERVER_OB_TABLE_PART_CLIP_H_ */
