/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/multi_data_source/ob_mds_table_merge_dag_param.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
ObTabletMdsMiniMergeDagParam::ObTabletMdsMiniMergeDagParam()
  : ObTabletMergeDagParam(),
    flush_scn_(share::SCN::invalid_scn()),
    generate_ts_(0),
    mds_construct_sequence_(-1)
{
  // Mds dump should not access mds data to avoid potential dead lock
  // between mds table lock on ObTabletBasePointer and other mds component
  // inner locks.
  skip_get_tablet_ = true;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase