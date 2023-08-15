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

#ifndef OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG
#define OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG

#include <stdint.h>
#include "share/scn.h"
#include "storage/compaction/ob_tablet_merge_task.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObMdsTableMergeDag : public compaction::ObTabletMergeDag
{
public:
  ObMdsTableMergeDag();
  virtual ~ObMdsTableMergeDag() = default;
  ObMdsTableMergeDag(const ObMdsTableMergeDag&) = delete;
  ObMdsTableMergeDag &operator=(const ObMdsTableMergeDag&) = delete;
public:
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;

  share::SCN get_flush_scn() const { return flush_scn_; }
  int64_t get_mds_construct_sequence() const { return mds_construct_sequence_; }

  INHERIT_TO_STRING_KV("ObTabletMergeDag", ObTabletMergeDag,
                       K_(is_inited),
                       K_(flush_scn),
                       KTIME_(generate_ts),
                       K_(mds_construct_sequence));
private:
  bool is_inited_;
  share::SCN flush_scn_;
  int64_t generate_ts_;
  int64_t mds_construct_sequence_;
};
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_DAG
