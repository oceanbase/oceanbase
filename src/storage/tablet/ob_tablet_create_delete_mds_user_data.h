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

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_MDS_USER_DATA
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_MDS_USER_DATA

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_common.h"

namespace oceanbase
{
namespace storage
{

enum class ObTabletMdsUserDataType : int64_t
{
  NONE = 0,
  //for create tablet
  CREATE_TABLET = 1,
  //for drop tablet
  REMOVE_TABLET = 2,
  //for start transfer out
  START_TRANSFER_OUT = 3,
  //for start transfer in
  START_TRANSFER_IN = 4,
  //for finish transfer out
  FINISH_TRANSFER_OUT = 5,
  // for finish transfer in
  FINISH_TRANSFER_IN = 6,
  MAX_TYPE,
};

class ObTabletCreateDeleteMdsUserData
{
  OB_UNIS_VERSION(1);
public:
  ObTabletCreateDeleteMdsUserData();
  ~ObTabletCreateDeleteMdsUserData() = default;
  ObTabletCreateDeleteMdsUserData(const ObTabletStatus::Status &status, const ObTabletMdsUserDataType type);
  ObTabletCreateDeleteMdsUserData(const ObTabletCreateDeleteMdsUserData &) = delete;
  ObTabletCreateDeleteMdsUserData &operator=(const ObTabletCreateDeleteMdsUserData &) = delete;
public:
  void reset();
  bool is_valid() const;
  int assign(const ObTabletCreateDeleteMdsUserData &other);

  ObTabletStatus get_tablet_status() const;
  share::SCN get_create_scn() const;
  void on_redo(const share::SCN &redo_scn);
  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn);
  // todo(zk250686): tablet shell
  static int set_tablet_gc_trigger(const share::ObLSID &ls_id);
  static int set_tablet_empty_shell_trigger(const share::ObLSID &ls_id);

  TO_STRING_KV(K_(tablet_status), K_(transfer_scn),
      K_(transfer_ls_id), K_(data_type),
      K_(create_commit_scn), K_(create_commit_version),
      K_(delete_commit_scn), K_(delete_commit_version), K_(transfer_out_commit_version));
private:
  void start_transfer_out_on_redo_(const share::SCN &redo_scn);
  void finish_transfer_in_on_redo_(const share::SCN &redo_scn);
  void create_tablet_on_commit_(const share::SCN &commit_version, const share::SCN &commit_scn);
  void start_transfer_in_on_commit_(const share::SCN &commit_version, const share::SCN &commit_scn);

  void delete_tablet_on_commit_(const share::SCN &commit_version, const share::SCN &commit_scn);
  void finish_transfer_out_on_commit_(const share::SCN &commit_version, const share::SCN &commit_scn);
  void start_transfer_out_on_commit_(const share::SCN &commit_version);
public:
  ObTabletStatus tablet_status_;
  share::SCN transfer_scn_;
  share::ObLSID transfer_ls_id_;
  ObTabletMdsUserDataType data_type_;

  // create_commit_scn_ remain unchanged throughout the entire tablet lifecycle
  share::SCN create_commit_scn_; // tablet's first create tx commit log scn, set this in create_tablet_on_commit_
  int64_t create_commit_version_; // create tx commit trans version
  share::SCN delete_commit_scn_; // delete tx commit log scn
  int64_t delete_commit_version_; // delete tx commit trans version
  int64_t transfer_out_commit_version_; //transfer out commit trans version
};

inline bool ObTabletCreateDeleteMdsUserData::is_valid() const
{
  return tablet_status_.is_valid()
      && data_type_ >= ObTabletMdsUserDataType::NONE
      && data_type_ < ObTabletMdsUserDataType::MAX_TYPE;
}

inline ObTabletStatus ObTabletCreateDeleteMdsUserData::get_tablet_status() const
{
  return tablet_status_;
}

inline share::SCN ObTabletCreateDeleteMdsUserData::get_create_scn() const
{
  return create_commit_scn_;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_MDS_USER_DATA
