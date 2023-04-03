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
#include "gtest/gtest.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#define private public
#define protected public

namespace oceanbase
{
namespace backup
{

int64_t max_tablet_id = 0;

/* ObFakeBackupMacroBlockIndexIterator */

class ObFakeBackupMacroBlockIndexIterator : public ObIMacroBlockIndexIterator {
public:
  ObFakeBackupMacroBlockIndexIterator();
  virtual ~ObFakeBackupMacroBlockIndexIterator();
  int init(const int64_t task_id, const share::ObBackupDest &backup_dest, const uint64_t tenant_id,
      const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
      const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
      const int64_t file_count = 80, const int64_t item_count = 1024);
  virtual int next() override;
  virtual bool is_iter_end() const override;
  virtual int get_cur_index(ObBackupMacroRangeIndex &index) override;
  virtual ObBackupIndexIteratorType get_type() const override
  {
    return BACKUP_MACRO_BLOCK_INDEX_ITERATOR;
  }

private:
  int generate_random_macro_range_index_(
      const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id);
  void make_random_macro_range_index_(const int64_t tablet_id, const int64_t backup_set_id, const share::ObLSID &ls_id,
      const int64_t turn_id, const int64_t retry_id, const int64_t file_id, ObBackupMacroRangeIndex &range_index);

private:
  int64_t cur_idx_;
  int64_t cur_tablet_id_;
  int64_t random_count_;
  int64_t file_count_;
  int64_t per_file_item_count_;
  common::ObArray<ObBackupMacroRangeIndex> cur_index_list_;
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupMacroBlockIndexIterator);
};



ObFakeBackupMacroBlockIndexIterator::ObFakeBackupMacroBlockIndexIterator()
    : cur_idx_(0),
      cur_tablet_id_(0),
      random_count_(0),
      file_count_(0),
      per_file_item_count_(0),
      cur_index_list_()
{}

ObFakeBackupMacroBlockIndexIterator::~ObFakeBackupMacroBlockIndexIterator()
{}

int ObFakeBackupMacroBlockIndexIterator::init(const int64_t task_id, const share::ObBackupDest &backup_dest,
    const uint64_t tenant_id, const share::ObBackupSetDesc &backup_set_desc, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, const int64_t retry_id,
    const int64_t file_count, const int64_t per_file_item_count)
{
  int ret = OB_SUCCESS;
  max_tablet_id = 0;
  task_id_ = task_id;
  backup_dest_.deep_copy(backup_dest);
  tenant_id_ = tenant_id;
  backup_set_desc_ = backup_set_desc;
  ls_id_ = ls_id;
  backup_data_type_ = backup_data_type;
  turn_id_ = turn_id;
  retry_id_ = retry_id;
  cur_idx_ = 0;
  cur_file_id_ = -1;
  file_count_ = file_count;
  per_file_item_count_ = per_file_item_count;
  generate_random_macro_range_index_(backup_set_desc.backup_set_id_, ls_id, turn_id, retry_id);
  is_inited_ = true;
  return ret;
}

int ObFakeBackupMacroBlockIndexIterator::next()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ >= random_count_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret), K_(cur_idx), K_(cur_index_list));
  } else {
    cur_idx_++;
  }
  return ret;
}

bool ObFakeBackupMacroBlockIndexIterator::is_iter_end() const
{
  bool bret = false;
  bret = cur_idx_ >= cur_index_list_.count() || -1 == cur_idx_;
  return bret;
}

int ObFakeBackupMacroBlockIndexIterator::get_cur_index(ObBackupMacroRangeIndex &range_index)
{
  int ret = OB_SUCCESS;
  range_index.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("iterator do not init", K(ret));
  } else if (cur_idx_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current index is not valid",
        K(ret),
        K_(tenant_id),
        K_(ls_id),
        K_(backup_data_type),
        K(cur_idx_),
        K_(cur_file_id),
        K(file_id_list_));
  } else if (cur_idx_ >= cur_index_list_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("iterator is end", K(ret), KPC(this));
  } else {
    range_index = cur_index_list_.at(cur_idx_);
  }
  return ret;
}

int ObFakeBackupMacroBlockIndexIterator::generate_random_macro_range_index_(
    const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id)
{
  int ret = OB_SUCCESS;
  ObBackupMacroRangeIndex range_index;
  const int64_t file_count = file_count_;
  for (int64_t i = 0; OB_SUCC(ret) && i < file_count; ++i) {
    const int64_t item_count = per_file_item_count_;
    const int64_t file_id = i;
    for (int64_t j = 0; OB_SUCC(ret) && j < item_count; ++j) {
      cur_tablet_id_++;
      range_index.reset();
      make_random_macro_range_index_(cur_tablet_id_, backup_set_id, ls_id, turn_id, retry_id, file_id, range_index);
      ret = cur_index_list_.push_back(range_index);
      EXPECT_EQ(OB_SUCCESS, ret);
      random_count_++;
      max_tablet_id = std::max(max_tablet_id, cur_tablet_id_);
    }
  }
  return ret;
}

void ObFakeBackupMacroBlockIndexIterator::make_random_macro_range_index_(const int64_t tablet_id,
    const int64_t backup_set_id, const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id,
    const int64_t file_id, ObBackupMacroRangeIndex &range_index)
{
  range_index.start_key_ = blocksstable::ObLogicMacroBlockId(1, 1, tablet_id);
  range_index.end_key_ = blocksstable::ObLogicMacroBlockId(1, 1, tablet_id);
  range_index.backup_set_id_ = backup_set_id;
  range_index.ls_id_ = ls_id;
  range_index.turn_id_ = turn_id;
  range_index.retry_id_ = retry_id;
  range_index.file_id_ = file_id;
  make_random_offset(range_index.offset_);
  make_random_offset(range_index.length_);
}


/* ObFakeBackupMacroIndexMerger */

class ObFakeBackupMacroIndexMerger : public ObBackupMacroBlockIndexMerger {
public:
  ObFakeBackupMacroIndexMerger();
  virtual ~ObFakeBackupMacroIndexMerger();
  void set_count(const int64_t file_count, const int64_t per_file_item_count)
  {
    file_count_ = file_count;
    per_file_item_count_ = per_file_item_count;
  }

private:
  virtual int get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
      const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy,
      common::ObIArray<ObBackupRetryDesc> &retry_list) override;
  virtual int alloc_merge_iter_(const bool tenant_level, const ObBackupIndexMergeParam &merge_param,
      const ObBackupRetryDesc &retry_desc, ObIMacroBlockIndexIterator *&iter) override;

private:
  int64_t file_count_;
  int64_t per_file_item_count_;
  DISALLOW_COPY_AND_ASSIGN(ObFakeBackupMacroIndexMerger);
};

ObFakeBackupMacroIndexMerger::ObFakeBackupMacroIndexMerger()
{}

ObFakeBackupMacroIndexMerger::~ObFakeBackupMacroIndexMerger()
{}

int ObFakeBackupMacroIndexMerger::get_all_retries_(const int64_t task_id, const uint64_t tenant_id,
    const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id, common::ObISQLClient &sql_proxy,
    common::ObIArray<ObBackupRetryDesc> &retry_list)
{
  int ret = OB_SUCCESS;
  UNUSEDx(task_id, tenant_id, backup_data_type, sql_proxy);
  retry_list.reset();
  ObBackupRetryDesc desc;
  desc.ls_id_ = ls_id;
  desc.turn_id_ = 1;
  desc.retry_id_ = 0;
  desc.last_file_id_ = 10;
  if (OB_FAIL(retry_list.push_back(desc))) {
    LOG_WARN("failed to push back", K(ret), K(desc));
  } else {
    LOG_INFO("fake get all retries", K(retry_list));
  }
  return ret;
}

int ObFakeBackupMacroIndexMerger::alloc_merge_iter_(const bool tenant_level, const ObBackupIndexMergeParam &merge_param,
    const ObBackupRetryDesc &retry_desc, ObIMacroBlockIndexIterator *&iter)
{
  int ret = OB_SUCCESS;
  ObFakeBackupMacroBlockIndexIterator *tmp_iter = NULL;
  if (!retry_desc.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret));
  } else if (OB_ISNULL(tmp_iter = OB_NEW(ObFakeBackupMacroBlockIndexIterator, ObModIds::BACKUP))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc iterator", K(ret));
  } else if (OB_FAIL(tmp_iter->init(merge_param.task_id_,
                 merge_param.backup_dest_,
                 merge_param.tenant_id_,
                 merge_param.backup_set_desc_,
                 retry_desc.ls_id_,
                 merge_param.backup_data_type_,
                 retry_desc.turn_id_,
                 retry_desc.retry_id_,
                 file_count_,
                 per_file_item_count_))) {
    LOG_WARN("failed to init meta index iterator", K(ret), K(merge_param));
  } else {
    iter = tmp_iter;
  }
  return ret;
}

}
}
