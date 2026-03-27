// Copyright (c) 2021 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_CKM_ERROR_TABLET_INFO_H_
#define OB_STORAGE_COMPACTION_CKM_ERROR_TABLET_INFO_H_
#include "common/ob_tablet_id.h"
#include "share/tablet/ob_tablet_info.h"
namespace oceanbase
{
namespace compaction
{

template<typename T>
struct ObCkmErrorStruct
{
public:
  ObCkmErrorStruct()
    : tablet_info_(),
      compaction_scn_(0)
  {}
  ObCkmErrorStruct(const T &input)
    : tablet_info_(input),
      compaction_scn_(0)
  {}
  void reset()
  {
    tablet_info_.reset();
    compaction_scn_ = 0;
  }
  TO_STRING_KV(K_(tablet_info), K_(compaction_scn));
  T tablet_info_;
  int64_t compaction_scn_;
};
typedef ObCkmErrorStruct<ObTabletID> ObCkmErrorTabletInfo;
typedef ObCkmErrorStruct<share::ObTabletLSPair> ObCkmErrorTabletLSInfo;

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_CKM_ERROR_TABLET_INFO_H_
