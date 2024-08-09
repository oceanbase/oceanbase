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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_TABLET_REORGANIZE_HELPER_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_TABLET_REORGANIZE_HELPER_H_

#include "common/ob_tablet_id.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_ls_id.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{

struct ObTabletReorganizeInfo final
{
  ObTabletReorganizeInfo();
  ~ObTabletReorganizeInfo();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(src_tablet_id), K_(dest_tablet_id));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID src_tablet_id_;
  common::ObTabletID dest_tablet_id_;
};

struct ObReorganizeTablet final
{
  ObReorganizeTablet();
  ~ObReorganizeTablet();
  void reset();
  int set(const common::ObTabletID &tablet_id);
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(children));

  common::ObTabletID tablet_id_;
  common::ObArray<ObReorganizeTablet *> children_;
  DISALLOW_COPY_AND_ASSIGN(ObReorganizeTablet);
};

class ObBackupTabletReorganizeHelper
{
public:
  static int check_tablet_has_reorganized(common::ObMySQLProxy &sql_proxy, const uint64_t tenant_id,
      const common::ObTabletID &tablet_id, share::ObLSID &ls_id, bool &reorganized);
  static int get_leaf_children(common::ObMySQLProxy &sql_proxy, const uint64_t tenant_id, const common::ObTabletID &tablet_id,
      const share::ObLSID &ls_id, common::ObIArray<ObTabletID> &tablet_list);
  static int get_leaf_children(common::ObMySQLProxy &sql_proxy, const uint64_t tenant_id, const common::ObIArray<common::ObTabletID> &tablet_list,
      const share::ObLSID &ls_id, common::ObIArray<common::ObTabletID> &descendent_list);
  static int get_ls_to_tablet_reorganize_info_map(common::ObMySQLProxy &sql_proxy,
      const uint64_t tenant_id, const common::hash::ObHashSet<share::ObLSID> &ls_id,
      common::hash::ObHashMap<share::ObLSID, common::ObArray<ObTabletReorganizeInfo>> &tablet_reorganize_ls_map);
  static int get_leaf_children_from_history(const uint64_t tenant_id, const common::ObIArray<ObTabletReorganizeInfo> &history_infos,
      const common::ObTabletID &tablet_id, common::ObIArray<common::ObTabletID> &tablet_list);

private:
  static int get_all_tablet_reorganize_history_infos_(common::ObMySQLProxy &sql_proxy, const uint64_t tenant_id,
      const share::ObLSID &ls_id, common::ObIArray<ObTabletReorganizeInfo> &history_infos);
  static int alloc_reorganize_tablet_info_(
      const common::ObTabletID &tablet_id, ObReorganizeTablet *&tablet);
  static int build_tree_from_history_(
      const common::ObIArray<ObTabletReorganizeInfo> &history_infos, ObReorganizeTablet *tablet);
  static int get_leaf_tablets_(const ObReorganizeTablet *tablet, hash::ObHashSet<ObTabletID> &hash_set);
  static void free_and_delete_node_(ObReorganizeTablet *tablet);

private:
  static int convert_set_to_array_(const common::hash::ObHashSet<ObTabletID> &tablet_set,
      common::ObIArray<ObTabletID> &tablet_list);
  static int put_array_to_set_(const common::ObIArray<common::ObTabletID> &tablet_list,
      common::hash::ObHashSet<ObTabletID> &tablet_set);

private:
  static const int64_t DEFAULT_BUCKET_SIZE = 10000;
};

}
}

#endif
