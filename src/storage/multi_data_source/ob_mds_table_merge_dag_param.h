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

#ifndef OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM
#define OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM

#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class ObMdsTableMergeDagParam : public share::ObIDagInitParam
{
public:
  ObMdsTableMergeDagParam();
  virtual ~ObMdsTableMergeDagParam() = default;
public:
  virtual bool is_valid() const override;
  bool operator==(const ObMdsTableMergeDagParam &other) const;

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(flush_scn), KTIME_(generate_ts));
public:
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN flush_scn_;
  int64_t generate_ts_;
};

inline bool ObMdsTableMergeDagParam::is_valid() const
{
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && !tablet_id_.is_ls_inner_tablet()
      && flush_scn_.is_valid();
}

inline bool ObMdsTableMergeDagParam::operator==(const ObMdsTableMergeDagParam &other) const
{
  return ls_id_ == other.ls_id_
      && tablet_id_ == other.tablet_id_
      && flush_scn_ == other.flush_scn_;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_TABLE_MERGE_PARAM