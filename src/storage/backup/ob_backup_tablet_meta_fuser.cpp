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

#define USING_LOG_PREFIX STORAGE

#include "storage/backup/ob_backup_tablet_meta_fuser.h"
#include "storage/backup/ob_backup_factory.h"

namespace oceanbase
{
namespace backup
{

ObBackupTabletFuseItemComparator::ObBackupTabletFuseItemComparator(int &sort_ret)
  : result_code_(sort_ret)
{}

bool ObBackupTabletFuseItemComparator::ObBackupTabletFuseItemComparator::operator()(
   const ObBackupTabletFuseItem *left, const ObBackupTabletFuseItem *right)
{
  bool bret = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    result_code_ = OB_INVALID_DATA;
    LOG_WARN_RET(result_code_, "should not be null", K_(result_code), KP(left), KP(right));
  } else if (left->tablet_id_ < right->tablet_id_) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

int ObBackupTabletMetaComparator::operator()(const common::ObTabletID &lhs, const common::ObTabletID &rhs)
{
  int ret = OB_SUCCESS;
  if (lhs > rhs) {
    ret = -1;
  } else if (lhs < rhs) {
    ret = 1;
  } else {
    ret = 0;
  }
  return ret;
}

ObBackupTabletMetaIteratorComparator::ObBackupTabletMetaIteratorComparator(int &sort_ret)
  : result_code_(sort_ret)
{}

bool ObBackupTabletMetaIteratorComparator::operator()(
    const ObIBackupTabletMetaIterator *left, const ObIBackupTabletMetaIterator *right)
{
  bool bret = OB_SUCCESS;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    result_code_ = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(result_code_, "should not be null", K_(result_code), KP(left), KP(right));
  } else if (left->get_type() == right->get_type()) {
    result_code_ = OB_ERR_UNEXPECTED;
    LOG_WARN_RET(result_code_, "type should not be same", K_(result_code), KP(left), KP(right));
  } else if (left->get_type() < right->get_type()) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

// ObBackupTabletFuseItem

ObBackupTabletFuseItem::ObBackupTabletFuseItem()
  : tablet_id_(),
    tablet_param_v1_(),
    tablet_meta_index_v2_(),
    has_tablet_meta_index_(false)
{
}

ObBackupTabletFuseItem::~ObBackupTabletFuseItem()
{
}

int64_t ObBackupTabletFuseItem::get_deep_copy_size() const
{
  return 0;
}

int ObBackupTabletFuseItem::deep_copy(const ObBackupTabletFuseItem &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(assign(src))) {
    LOG_WARN("failed to do assign", K(ret));
  }
  return ret;
}

int ObBackupTabletFuseItem::assign(const ObBackupTabletFuseItem &src)
{
  int ret = OB_SUCCESS;
  tablet_id_ = src.tablet_id_;
  tablet_meta_index_v2_ = src.tablet_meta_index_v2_;
  has_tablet_meta_index_ = src.has_tablet_meta_index_;
  if (OB_FAIL(tablet_param_v1_.assign(src.tablet_param_v1_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

bool ObBackupTabletFuseItem::is_valid() const
{
  bool bret = false;
  bret = tablet_id_.is_valid() && tablet_param_v1_.is_valid();
  if (has_tablet_meta_index_) {
    bret = bret && tablet_meta_index_v2_.is_valid();
  }
  return bret;
}

void ObBackupTabletFuseItem::reset()
{
  tablet_id_.reset();
  tablet_param_v1_.reset();
  tablet_meta_index_v2_.reset();
  has_tablet_meta_index_ = false;
}

OB_SERIALIZE_MEMBER(ObBackupTabletFuseItem,
                    tablet_id_,
                    tablet_param_v1_,
                    tablet_meta_index_v2_,
                    has_tablet_meta_index_);

// BackupTabletMetaFuser

ObBackupTabletMetaFuser::ObBackupTabletMetaFuser()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    backup_dest_(),
    backup_set_desc_(),
    ls_id_(),
    turn_id_(),
    backup_data_store_(),
    result_(),
    comparator_(result_),
    fuse_iter_array_(),
    external_sort_()
{
}

ObBackupTabletMetaFuser::~ObBackupTabletMetaFuser()
{
  reset();
}

void ObBackupTabletMetaFuser::reset()
{
  for (int64_t i = 0; i < fuse_iter_array_.count(); ++i) {
    ObIBackupTabletMetaIterator *&iter = fuse_iter_array_.at(i);
    if (OB_NOT_NULL(iter)) {
      ObLSBackupFactory::free(iter);
    }
  }
  fuse_iter_array_.reset();
  external_sort_.clean_up();
}

int ObBackupTabletMetaFuser::init(const uint64_t tenant_id, const share::ObBackupDest &backup_dest,
    const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id, const int64_t turn_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet meta fuser init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !backup_dest.is_valid() || !backup_set_desc.is_valid()
      || !ls_id.is_valid() || turn_id <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(backup_dest), K(backup_set_desc), K(ls_id), K(turn_id));
  } else if (OB_FAIL(backup_dest_.deep_copy(backup_dest))) {
    LOG_WARN("failed to assign", K(ret), K(backup_dest));
  } else {
    tenant_id_ = tenant_id;
    backup_set_desc_ = backup_set_desc;
    ls_id_ = ls_id;
    turn_id_ = turn_id;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletMetaFuser::do_fuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fuser do not init", K(ret));
  } else if (OB_FAIL(inner_do_fuse_())) {
    LOG_WARN("failed to inner do fuse", K(ret));
  } else {
    LOG_INFO("do fuse tablet meta", K(ret));
  }
  return ret;
}

int ObBackupTabletMetaFuser::get_next_tablet_item(
    ObBackupTabletFuseItem &fuse_item)
{
  int ret = OB_SUCCESS;
  fuse_item.reset();
  const ObBackupTabletFuseItem *fuse_item_ptr = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("fuser do not init", K(ret));
  } else if (OB_FAIL(external_sort_.get_next_item(fuse_item_ptr))) {
    LOG_WARN("failed to get next item", K(ret));
  } else if (OB_FAIL(fuse_item.assign(*fuse_item_ptr))) {
    LOG_WARN("failed to do assign", K(ret), KPC(fuse_item_ptr));
  } else {
    LOG_INFO("get next tablet item", K(fuse_item));
  }
  return ret;
}

int ObBackupTabletMetaFuser::prepare_external_sort_()
{
  int ret = OB_SUCCESS;
  static const int64_t MACRO_BLOCK_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE;
  static const int64_t BUF_MEM_LIMIT = 8 * MACRO_BLOCK_SIZE;
  static const int64_t FILE_BUF_SIZE = MACRO_BLOCK_SIZE;
  static const int64_t EXPIRE_TIMESTAMP = 0;
  if (OB_FAIL(external_sort_.init(BUF_MEM_LIMIT,
                                  FILE_BUF_SIZE,
                                  EXPIRE_TIMESTAMP,
                                  tenant_id_,
                                  &comparator_))) {
    LOG_WARN("failed to init external sort", K(ret));
  } else {
    LOG_INFO("init external sort", K(ret));
  }
  return ret;
}

int ObBackupTabletMetaFuser::prepare_fuse_ctx_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_extern_tablet_meta_iterator_())) {
    LOG_WARN("failed to prepare extern tablet meta iterator", K(ret));
  } else if (OB_FAIL(prepare_tenant_meta_index_iterator_())) {
    LOG_WARN("failed to prepare tenant meta index iterator", K(ret));
  }
  return ret;
}

int ObBackupTabletMetaFuser::prepare_extern_tablet_meta_iterator_()
{
  int ret = OB_SUCCESS;
  ObExternBackupTabletMetaIterator *tmp_iter = NULL;
  if (OB_ISNULL(tmp_iter = ObLSBackupFactory::get_extern_backup_tablet_meta_iterator(tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get backup tablet meta iteratoer", K(ret));
  } else if (OB_FAIL(tmp_iter->init(backup_dest_, backup_set_desc_, ls_id_))) {
    LOG_WARN("failed to init iter", K(ret), K_(backup_dest), K_(backup_set_desc), K_(ls_id));
  } else if (OB_FAIL(fuse_iter_array_.push_back(tmp_iter))) {
    ObLSBackupFactory::free(tmp_iter);
    tmp_iter = NULL;
    LOG_WARN("failed to push back", K(ret), KP(tmp_iter));
  } else {
    tmp_iter = NULL;
    LOG_INFO("succeed to prepare extern tablet meta iterator", K(ret));
  }
  if (OB_NOT_NULL(tmp_iter)) {
    ObLSBackupFactory::free(tmp_iter);
  }
  return ret;
}

int ObBackupTabletMetaFuser::prepare_tenant_meta_index_iterator_()
{
  int ret = OB_SUCCESS;
  ObBackupTabletMetaIndexIterator *tmp_iter = NULL;
  int64_t retry_id = -1;
  if (OB_ISNULL(tmp_iter = ObLSBackupFactory::get_backup_tablet_meta_index_iterator(tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to get backup tablet meta iteratoer", K(ret));
  } else if (OB_FAIL(get_tenant_meta_index_retry_id_(retry_id))) {
    LOG_WARN("failed to get tenant meta index retry id", K(ret));
  } else if (OB_FAIL(tmp_iter->init(backup_dest_, tenant_id_, backup_set_desc_, ls_id_, turn_id_, retry_id))) {
    LOG_WARN("failed to init iter", K(ret), K_(ls_id), K_(backup_dest), K_(tenant_id), K_(backup_set_desc));
  } else if (OB_FAIL(fuse_iter_array_.push_back(tmp_iter))) {
    ObLSBackupFactory::free(tmp_iter);
    tmp_iter = NULL;
    LOG_WARN("failed to push back", K(ret), KP(tmp_iter));
  } else {
    tmp_iter = NULL;
    LOG_INFO("succeed to prepare tenant meta index iterator", K(ret));
  }
  if (OB_NOT_NULL(tmp_iter)) {
    ObLSBackupFactory::free(tmp_iter);
  }
  return ret;
}

int ObBackupTabletMetaFuser::get_tenant_meta_index_retry_id_(int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  const bool is_restore = false;
  const bool is_macro_index = false;
  const bool is_sec_meta = false;
  if (OB_FAIL(retry_id_getter.init(backup_dest_, backup_set_desc_,
      backup_data_type, turn_id_, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  } else {
    LOG_INFO("get max retry id", K_(backup_dest), K_(backup_set_desc), K_(turn_id), K(retry_id));
  }
  return ret;
}

int ObBackupTabletMetaFuser::inner_do_fuse_()
{
  int ret = OB_SUCCESS;
  int64_t round = 0;
  MERGE_ITER_ARRAY unfinished_iters;
  MERGE_ITER_ARRAY min_iters;
  ObBackupMetaIndex meta_index;
  ObBackupTabletFuseItem fuse_item;
  ObMigrationTabletParam param;
  bool need_add = true;
  if (OB_FAIL(prepare_fuse_ctx_())) {
    LOG_WARN("failed to prepare fuse ctx", K(ret));
  } else if (OB_FAIL(prepare_external_sort_())) {
    LOG_WARN("failed to prepare external sort", K(ret));
  }
  while (OB_SUCC(ret)) {
    unfinished_iters.reset();
    min_iters.reset();
    meta_index.reset();
    fuse_item.reset();
    need_add = true;
    if (OB_FAIL(get_unfinished_iters_(fuse_iter_array_, unfinished_iters))) {
      LOG_WARN("failed to get unfinished iters", K(ret), K_(ls_id), K(round), K_(fuse_iter_array));
    } else if (unfinished_iters.empty()) {
      LOG_INFO("fuse index finish", K_(ls_id), K(round), K_(fuse_iter_array));
      break;
    } else if (OB_FAIL(find_minimum_iters_(unfinished_iters, min_iters))) {
      LOG_WARN("failed to find minumum iters", K(ret), K_(ls_id), K(round), K(unfinished_iters));
    } else if (min_iters.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_INFO("should exist iters", K_(ls_id), K(round));
    } else if (OB_FAIL(sort_iters_(min_iters))) {
      LOG_WARN("failed to sort iters", K(ret), K_(ls_id), K(round), K(min_iters));
    } else if (OB_FAIL(get_fuse_result_(min_iters, fuse_item, need_add))) {
      LOG_WARN("failed to fuse iters", K(ret), K_(ls_id), K(round), K(unfinished_iters), K(min_iters));
    } else if (need_add && OB_FAIL(feed_to_external_sort_(fuse_item))) {
      LOG_WARN("failed to feed to external sort", K(ret), K_(ls_id), K(round), K(fuse_item));
    } else if (OB_FAIL(move_iters_next_(min_iters))) {
      LOG_WARN("failed to move iters next", K(ret), K_(ls_id), K(round), K(min_iters));
    } else {
      round++;
      LOG_INFO("meta index fuse round", K(round), K_(ls_id), K(min_iters), K(meta_index));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(external_sort_.do_sort(true/*final_merge*/))) {
      LOG_WARN("failed to do external sort", K(ret));
    } else {
      LOG_INFO("do sort", K_(ls_id), K(round));
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::get_unfinished_iters_(
    const MERGE_ITER_ARRAY &fuse_iters, MERGE_ITER_ARRAY &unfinished_iters)
{
  int ret = OB_SUCCESS;
  unfinished_iters.reset();
  for (int i = 0; OB_SUCC(ret) && i < fuse_iters.count(); ++i) {
    ObIBackupTabletMetaIterator *iter = fuse_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(unfinished_iters.push_back(iter))) {
      LOG_WARN("failed to push back", K(ret), K(iter));
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::move_iters_next_(MERGE_ITER_ARRAY &fuse_iters)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < fuse_iters.count(); ++i) {
    ObIBackupTabletMetaIterator *iter = fuse_iters.at(i);
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter should not be null", K(ret));
    } else if (iter->is_iter_end()) {
      continue;
    } else if (OB_FAIL(iter->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("meta index iter has reach end");
      } else {
        LOG_WARN("failed to do next", K(ret), K(iter));
      }
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::find_minimum_iters_(const MERGE_ITER_ARRAY &fuse_iters, MERGE_ITER_ARRAY &min_iters)
{
  int ret = OB_SUCCESS;
  min_iters.reset();
  int64_t cmp_ret = 0;
  ObIBackupTabletMetaIterator *iter = NULL;
  for (int64_t i = fuse_iters.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(iter = fuse_iters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null index iter", K(ret));
    } else if (iter->is_iter_end()) {
      continue;  // skip
    } else if (min_iters.empty()) {
      if (OB_FAIL(min_iters.push_back(iter))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else if (OB_FAIL(compare_index_iters_(min_iters.at(0), iter, cmp_ret))) {
      LOG_WARN("failed to compare index iters", K(ret), K(min_iters), KPC(iter));
    } else {
      if (cmp_ret < 0) {
        min_iters.reset();
      }
      if (cmp_ret <= 0) {
        if (OB_FAIL(min_iters.push_back(iter))) {
          LOG_WARN("failed to push iter to min_iters", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::sort_iters_(MERGE_ITER_ARRAY &fuse_iters)
{
  int ret = OB_SUCCESS;
  if (fuse_iters.empty()) {
    // no need sort
  } else {
    ObBackupTabletMetaIteratorComparator comp(ret);
    lib::ob_sort(fuse_iters.begin(), fuse_iters.end(), comp);
    if (OB_FAIL(ret)) {
      LOG_ERROR("failed to sort iterators", K(ret), K(fuse_iters));
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::compare_index_iters_(
    ObIBackupTabletMetaIterator *lhs, ObIBackupTabletMetaIterator *rhs, int64_t &cmp_ret)
{
  int ret = OB_SUCCESS;
  ObTabletID lvalue;
  ObTabletID rvalue;
  ObBackupTabletMetaComparator comparator;
  if (OB_ISNULL(lhs) || OB_ISNULL(rhs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(lhs), K(rhs));
  } else if (OB_UNLIKELY(lhs->is_iter_end() || rhs->is_iter_end())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected end row iters", K(ret));
  } else if (OB_FAIL(lhs->get_cur_tablet_id(lvalue))) {
    LOG_WARN("failed to get cur index", K(ret), KPC(lhs));
  } else if (OB_FAIL(rhs->get_cur_tablet_id(rvalue))) {
    LOG_WARN("failed to get cur index", K(ret), KPC(rhs));
  } else {
    cmp_ret = comparator.operator()(lvalue, rvalue);
    LOG_DEBUG("compare tablet id", K(lvalue), K(rvalue), K(cmp_ret));
  }
  return ret;
}

int ObBackupTabletMetaFuser::get_fuse_result_(
    const MERGE_ITER_ARRAY &fuse_iters,
    ObBackupTabletFuseItem &fuse_item,
    bool &need_add)
{
  int ret = OB_SUCCESS;
  if (1 == fuse_iters.count()) {
    if (OB_FAIL(fuse_tablet_when_not_exist_(fuse_iters.at(0), fuse_item, need_add))) {
      LOG_WARN("failed to fuse tablet when not exist", K(ret), K(fuse_iters));
    }
  } else if (2 == fuse_iters.count()) {
    if (OB_FAIL(fuse_tablet_when_exist_(fuse_iters.at(0),
                                        fuse_iters.at(1),
                                        fuse_item))) {
      LOG_WARN("failed to fuse tablet when exist", K(ret));
    } else {
      need_add = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not have more than 2 iterators", K(ret));
  }
  return ret;
}

int ObBackupTabletMetaFuser::fuse_tablet_when_not_exist_(
    ObIBackupTabletMetaIterator *iter,
    ObBackupTabletFuseItem &fuse_item,
    bool &need_add)
{
  int ret = OB_SUCCESS;
  need_add = true;
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter should not be null", K(ret));
  } else if (ObBackupTabletMetaIteratorType::TYPE_TABLET_INFO != iter->get_type()) {
    // this condition exist and is valid because we merge tenant level tablet index
    // and ls level tablet meta which is backed up in consistent scn stage
    // if the iterator type is tenant level meta index, which means the tablet
    // is not belong to the consistent scn stage (may be belonging to other log stream's)
    // so if the iterator type is not tablet info, we simply do not add the tablet
    need_add = false;
    LOG_WARN("iter type not correct, no need add", K(ret), "iter_type", iter->get_type(), KPC(iter));
  } else {
    ObExternBackupTabletMetaIterator *tablet_iter = static_cast<ObExternBackupTabletMetaIterator *>(iter);
    if (OB_FAIL(tablet_iter->get_cur_tablet_id(fuse_item.tablet_id_))) {
      LOG_WARN("failed to get cur tablet id", K(ret), KPC(tablet_iter));
    } else if (OB_FAIL(tablet_iter->get_cur_migration_param(fuse_item.tablet_param_v1_))) {
      LOG_WARN("failed to get cur tablet migration param", K(ret), KPC(tablet_iter));
    } else {
      fuse_item.tablet_meta_index_v2_.reset();
      fuse_item.has_tablet_meta_index_ = false;
    }
  }
  return ret;
}

int ObBackupTabletMetaFuser::fuse_tablet_when_exist_(
    const ObIBackupTabletMetaIterator *iter_v1,
    const ObIBackupTabletMetaIterator *iter_v2,
    ObBackupTabletFuseItem &fuse_item)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id_v1;
  ObTabletID tablet_id_v2;
  if (OB_ISNULL(iter_v1) || OB_ISNULL(iter_v2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iterators should not be null", K(ret), KP(iter_v1), KP(iter_v2));
  } else if (ObBackupTabletMetaIteratorType::TYPE_TABLET_INFO != iter_v1->get_type()
      && ObBackupTabletMetaIteratorType::TYPE_TENANT_META_INDEX != iter_v2->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter type not correct", K(ret), K(iter_v1->get_type()), K(iter_v2->get_type()));
  } else if (OB_FAIL(iter_v1->get_cur_tablet_id(tablet_id_v1))) {
    LOG_WARN("failed to get cur tablet id", K(ret));
  } else if (OB_FAIL(iter_v2->get_cur_tablet_id(tablet_id_v2))) {
    LOG_WARN("failed to get cur tablet migration param", K(ret));
  } else if (tablet_id_v1 != tablet_id_v2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id not match", K(ret), K(tablet_id_v1), K(tablet_id_v2));
  } else if (OB_FAIL(iter_v1->get_cur_migration_param(fuse_item.tablet_param_v1_))) {
    LOG_WARN("failed to get cur migration param", K(ret), KPC(iter_v1));
  } else if (OB_FAIL(iter_v2->get_cur_meta_index(fuse_item.tablet_meta_index_v2_))) {
    LOG_WARN("failed to get cur meta index", K(ret), KPC(iter_v2));
  } else {
    fuse_item.tablet_id_ = tablet_id_v1;
    fuse_item.has_tablet_meta_index_ = true;
  }
  return ret;
}

int ObBackupTabletMetaFuser::feed_to_external_sort_(
    const ObBackupTabletFuseItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(item));
  } else if (OB_FAIL(external_sort_.add_item(item))) {
    LOG_WARN("failed to add item to external sort", K(ret));
  } else {
    LOG_DEBUG("feed to external sort", K(item));
  }
  return ret;
}

}
}
