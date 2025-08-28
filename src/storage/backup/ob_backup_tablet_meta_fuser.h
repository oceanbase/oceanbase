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
 
#ifndef STORAGE_BACKUP_TABLET_META_FUSER_H_ 
#define STORAGE_BACKUP_TABLET_META_FUSER_H_

#include "storage/ob_parallel_external_sort.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/backup/ob_backup_data_struct.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/backup/ob_backup_iterator.h"

namespace oceanbase
{
namespace backup
{

struct ObBackupTabletFuseItem
{
  OB_UNIS_VERSION(1);
public:
  ObBackupTabletFuseItem();
  ~ObBackupTabletFuseItem();
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObBackupTabletFuseItem &src);
  int deep_copy(const ObBackupTabletFuseItem &src, char *buf, int64_t len, int64_t &pos);
  int assign(const ObBackupTabletFuseItem &item);
  bool is_valid() const;
  void reset();
  TO_STRING_KV(K_(tablet_id), K_(tablet_param_v1), K_(tablet_meta_index_v2), K_(has_tablet_meta_index));
  common::ObTabletID tablet_id_;
  storage::ObMigrationTabletParam tablet_param_v1_;
  ObBackupMetaIndex tablet_meta_index_v2_;
  bool has_tablet_meta_index_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletFuseItem);
};

struct ObBackupTabletMetaComparator final {
  int operator()(const common::ObTabletID &lhs, const common::ObTabletID &rhs);
};

struct ObBackupTabletMetaIteratorComparator final {
public:
  ObBackupTabletMetaIteratorComparator(int &sort_ret);
  bool operator()(const ObIBackupTabletMetaIterator *lhs, const ObIBackupTabletMetaIterator *rhs);
  int &result_code_;
};

class ObBackupTabletFuseItemComparator final {
public:
  ObBackupTabletFuseItemComparator(int &sort_ret);
  bool operator()(const ObBackupTabletFuseItem *left, const ObBackupTabletFuseItem *right);
  int &result_code_;
};

class ObBackupTabletMetaFuser final
{
  static const int64_t DEFAULT_ITER_COUNT = 2;
  typedef ObSEArray<ObIBackupTabletMetaIterator *, DEFAULT_ITER_COUNT> MERGE_ITER_ARRAY;
  typedef storage::ObExternalSort<ObBackupTabletFuseItem, ObBackupTabletFuseItemComparator> ExternalSort;
public:
  ObBackupTabletMetaFuser();
  ~ObBackupTabletMetaFuser();

  void reset();
  int init(const uint64_t tenant_id, const share::ObBackupDest &backup_tenant_dest, const share::ObBackupSetDesc &backup_set_desc,
      const share::ObLSID &ls_id, const int64_t turn_id);
  int do_fuse();
  int get_next_tablet_item(
      ObBackupTabletFuseItem &fuse_item);

private:
  int prepare_external_sort_();
  int prepare_fuse_ctx_();
  int prepare_extern_tablet_meta_iterator_();
  int prepare_tenant_meta_index_iterator_();
  int get_tenant_meta_index_retry_id_(int64_t &retry_id);
  int inner_do_fuse_();
  int get_fuse_result_(
      const MERGE_ITER_ARRAY &fuse_iters,
      ObBackupTabletFuseItem &fuse_item,
      bool &need_add);
  int fuse_tablet_when_not_exist_(
      ObIBackupTabletMetaIterator *iter,
      ObBackupTabletFuseItem &fuse_item,
      bool &need_add);
  int fuse_tablet_when_exist_(
      const ObIBackupTabletMetaIterator *iter_v1,
      const ObIBackupTabletMetaIterator *iter_v2,
      ObBackupTabletFuseItem &fuse_item);
  int feed_to_external_sort_(
      const ObBackupTabletFuseItem &item);

private:
  int get_unfinished_iters_(
      const MERGE_ITER_ARRAY &fuse_iters, MERGE_ITER_ARRAY &unfinished_iters);
  int move_iters_next_(MERGE_ITER_ARRAY &fuse_iters);
  int find_minimum_iters_(const MERGE_ITER_ARRAY &fuse_iters, MERGE_ITER_ARRAY &min_iters);
  int sort_iters_(MERGE_ITER_ARRAY &fuse_iters);
  int compare_index_iters_(
      ObIBackupTabletMetaIterator *lhs, ObIBackupTabletMetaIterator *rhs, int64_t &cmp_ret);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObBackupDest backup_dest_;
  share::ObBackupSetDesc backup_set_desc_;
  share::ObLSID ls_id_;
  int64_t turn_id_;
  ObBackupDataStore backup_data_store_;
  int result_;
  ObBackupTabletFuseItemComparator comparator_;
  MERGE_ITER_ARRAY fuse_iter_array_;
  ExternalSort external_sort_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupTabletMetaFuser);
};

}
}

#endif