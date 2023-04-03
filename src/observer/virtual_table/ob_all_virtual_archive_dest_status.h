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

#ifndef OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_H_
#define OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_H_

#include "share/ob_virtual_table_projector.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}

namespace observer
{
class ObVirtualArchiveDestStatus : public common::ObVirtualTableProjector
{
public:
  static const int64_t MAX_SYNC_TYPE_LENTH = 10;
  ObVirtualArchiveDestStatus();
  virtual ~ObVirtualArchiveDestStatus();
  int init(common::ObMySQLProxy *sql_proxy);
  virtual int inner_get_next_row(common::ObNewRow *&row);
  void destroy();

private:
  struct ObArchiveDestStatusInfo
  {
    ObArchiveDestStatusInfo();
    ~ObArchiveDestStatusInfo(){}
    uint64_t tenant_id_;
    int64_t dest_id_;
    share::ObBackupPathString path_;
    ObFixedLengthString<OB_DEFAULT_STATUS_LENTH> status_;
    uint64_t checkpoint_scn_;
    ObFixedLengthString<MAX_SYNC_TYPE_LENTH> synchronized_;
    share::ObBackupDefaultFixedLenString comment_;
    void reset();
    bool is_valid();
    TO_STRING_KV(K_(tenant_id), K_(dest_id), K_(status), K_(path), K_(checkpoint_scn),
      K_(synchronized),K_(comment));
  };

  typedef common::LinkHashValue<share::ObLSID> ArchiveStatValue;
  struct ObArchiveSCNValue : public ArchiveStatValue
  {
    ObArchiveSCNValue(){};
    void get(uint64_t &scn);
    int set(const uint64_t scn);
    uint64_t scn_;
  };

  typedef ObLinkHashMap<share::ObLSID, ObArchiveSCNValue> ArchiveDestMap;
  // get all tenant list
  int get_all_tenant_();
  // get all ls list
  int get_all_tenant_ls_(const uint64_t tenant_id);
  // get ls max scn and store them into ls_end_map_
  int get_ls_max_scn_(const uint64_t tenant_id); // k means ls_id, v means max_scn
  // get ls checkpoint scn and store them into ls_checkpoint_map_
  int get_ls_checkpoint_scn_(const uint64_t tenant_id, const int64_t dest_id);
  int get_full_row_(const share::schema::ObTableSchema *table,
                    const ObArchiveDestStatusInfo &dest_status,
                    ObIArray<Column> &columns);
  int get_status_info_(const uint64_t tenant_id, const int64_t dest_id, ObArchiveDestStatusInfo &dest_status_info);
  int compare_scn_map_();
  int check_if_switch_piece_(const uint64_t tenant_id, const int64_t dest_id);
  int get_log_archive_used_piece_id_(const uint64_t tenant_id, const int64_t dest_id, int64_t &piece_id);
private:
  bool is_inited_;
  bool ls_end_map_inited_;
  bool ls_checkpoint_map_inited_;
  bool is_synced_;
  common::ObMySQLProxy* sql_proxy_;
  const share::schema::ObTableSchema *table_schema_;
  ObArray<uint64_t> tenant_array_;
  ObArray<int64_t> ls_array_;
  ArchiveDestMap ls_end_map_;
  ArchiveDestMap ls_checkpoint_map_;
  DISALLOW_COPY_AND_ASSIGN(ObVirtualArchiveDestStatus);

};
}//end namespace observer
}//end namespace oceanbase
#endif //OCEANBASE_OBSERVER_OB_ALL_VIRTUAL_ARCHIVE_DEST_STATUS_H_