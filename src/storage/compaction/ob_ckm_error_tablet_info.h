//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
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
