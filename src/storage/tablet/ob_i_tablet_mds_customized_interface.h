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

#ifndef OCEANBASE_STORAGE_TABLET_OB_I_TABLET_MDS_CUSTOMIZED_INTERFACE_H
#define OCEANBASE_STORAGE_TABLET_OB_I_TABLET_MDS_CUSTOMIZED_INTERFACE_H
#include "ob_i_tablet_mds_interface.h"

namespace oceanbase
{
namespace storage
{

// ObITabletMdsCustomizedInterface is for MDS users to customize their own wrapper needs.
// All interfaces in ObITabletMdsInterface are type independent, but some users may want read MDS with more operations.
// They want centralize their requirements into a common document, so we define ObITabletMdsCustomizedInterface here.
class ObITabletMdsCustomizedInterface : public ObITabletMdsInterface
{
public:
  // customized get_latest_committed
  int get_ddl_data(ObTabletBindingMdsUserData &ddl_data);
  int get_autoinc_seq(share::ObTabletAutoincSeq &inc_seq, ObIAllocator &allocator);

  // customized get_latest
  int get_latest_split_data(ObTabletSplitMdsUserData &data,
                            mds::MdsWriter &writer,
                            mds::TwoPhaseCommitState &trans_stat,
                            share::SCN &trans_version,
                            const int64_t read_seq = 0) const;
  int get_latest_autoinc_seq(ObTabletAutoincSeq &data,
                             ObIAllocator &allocator,
                             mds::MdsWriter &writer,
                             mds::TwoPhaseCommitState &trans_stat,
                             share::SCN &trans_version,
                             const int64_t read_seq = 0) const;

  // customized get_snapshot
  // TODO (jiahua.cjh): move interface from ob_i_tablet_mds_interface to this file
};

}
}

#endif