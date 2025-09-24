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

#ifndef OCEABASE_STORAGE_TABLET_TRANSFER_INFO_
#define OCEABASE_STORAGE_TABLET_TRANSFER_INFO_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

struct ObTabletTransferInfo final
{
  OB_UNIS_VERSION(1);
public:
  static bool is_private_transfer_epoch_valid(const int64_t transfer_epoch);
public:
  ObTabletTransferInfo();
  ~ObTabletTransferInfo() = default;
  int init();
  int init(
      const share::ObLSID &ls_id,
      const share::SCN &transfer_start_scn,
      const int64_t transfer_seq,
      const share::SCN &src_reorganization_scn);
  void reset();
  bool is_valid() const;
  bool has_transfer_table() const;
  void reset_transfer_table();
  bool is_transfer_out_deleted() const;
  int get_private_transfer_epoch(int32_t &transfer_epoch) const;
  int set_private_transfer_epoch(const int32_t private_transfer_epoch);

  TO_STRING_KV(K_(ls_id), K_(transfer_start_scn), K_(transfer_seq), K_(has_transfer_table),
    K_(unused_is_transfer_out_deleted), K_(src_reorganization_scn), K_(private_transfer_epoch));
public:
  share::ObLSID ls_id_;
  share::SCN transfer_start_scn_;
  int64_t transfer_seq_;
  bool has_transfer_table_;
  bool unused_is_transfer_out_deleted_; // ATTENTION: this field is no longer used
  share::SCN src_reorganization_scn_;
  static const int64_t TRANSFER_INIT_SEQ = 0;
private:
  static const int64_t TRANSFER_INIT_LS_ID = 0;
  /**
   * @brief: transfer epoch:
   * == MacroBlockID.forth_id(20 bits), initialized at dest.
   * It's DIFFERENT with transfer_seq!!!
   * Only used for local and private data storage path.
   * See more detail:
   */
  int32_t private_transfer_epoch_;
};


}
}
#endif
