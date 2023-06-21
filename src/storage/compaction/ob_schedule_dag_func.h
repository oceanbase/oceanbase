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

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_SCHEDULE_DAG_FUNC_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_SCHEDULE_DAG_FUNC_H_

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObMdsTableMergeDagParam;
}
struct ObDDLTableMergeDagParam;
}

namespace compaction
{
struct ObTabletMergeDagParam;

class ObScheduleDagFunc
{
public:
  static int schedule_tablet_merge_dag(
      ObTabletMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_tx_table_merge_dag(
      ObTabletMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_ddl_table_merge_dag(
      storage::ObDDLTableMergeDagParam &param,
      const bool is_emergency = false);
  static int schedule_mds_table_merge_dag(
      storage::mds::ObMdsTableMergeDagParam &param,
      const bool is_emergency = false);
};

}
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_OB_SCHEDULE_DAG_FUNC_H_ */
