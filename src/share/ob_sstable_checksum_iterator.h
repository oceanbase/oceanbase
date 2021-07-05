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

#ifndef OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_ITERATOR_H
#define OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_ITERATOR_H

#include "ob_sstable_checksum_operator.h"
#include "lib/list/ob_dlist.h"

namespace oceanbase {
namespace share {
class ObIMergeErrorCb;

class ObSSTableDataChecksumInfo {
public:
  ObSSTableDataChecksumInfo() = default;
  virtual ~ObSSTableDataChecksumInfo() = default;
  void reset();
  bool is_same_table(const ObSSTableDataChecksumItem& item) const;
  int compare_sstable(const ObSSTableDataChecksumInfo& other) const;
  int add_item_ignore_checksum_error(const ObSSTableDataChecksumItem& item, bool& is_checksum_error);
  int add_item(const ObSSTableDataChecksumItem& item);
  const common::ObIArray<ObSSTableDataChecksumItem>& get_replicas() const
  {
    return replicas_;
  }
  TO_STRING_KV(K_(replicas));

private:
  static const int64_t DEFAULT_REPLICA_COUNT = 7;
  common::ObSEArray<ObSSTableDataChecksumItem, DEFAULT_REPLICA_COUNT> replicas_;
};

class ObSSTableDataChecksumIterator {
  class ObIDataChecksumItemFiter : public common::ObDLinkBase<ObIDataChecksumItemFiter> {
  public:
    ObIDataChecksumItemFiter()
    {}
    virtual ~ObIDataChecksumItemFiter()
    {}
    virtual int filter(const ObSSTableDataChecksumItem& item, bool& is_filtered) = 0;

  private:
    DISALLOW_COPY_AND_ASSIGN(ObIDataChecksumItemFiter);
  };

  class ObOnlyUserTenantChecksumItemFilter : public ObIDataChecksumItemFiter {
  public:
    ObOnlyUserTenantChecksumItemFilter() = default;
    virtual ~ObOnlyUserTenantChecksumItemFilter() = default;
    int filter(const ObSSTableDataChecksumItem& item, bool& is_filtered) override
    {
      int ret = common::OB_SUCCESS;
      if (!item.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "data checksum item is not valid", K(ret), K(item));
      } else {
        is_filtered = (item.tenant_id_ < common::OB_USER_TENANT_ID);
      }
      return ret;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObOnlyUserTenantChecksumItemFilter);
  };

  class ObSpecialTableChecksumItemFilter : public ObIDataChecksumItemFiter {
  public:
    ObSpecialTableChecksumItemFilter() = default;
    virtual ~ObSpecialTableChecksumItemFilter() = default;
    int filter(const ObSSTableDataChecksumItem& item, bool& is_filtered) override
    {
      int ret = common::OB_SUCCESS;
      if (!item.is_valid()) {
        ret = common::OB_INVALID_ARGUMENT;
        SHARE_LOG(WARN, "data checksum item is not valid", K(ret), K(item));
      } else {
        is_filtered = schema::ObSysTableChecker::is_cluster_private_tenant_table(item.data_table_id_);
      }
      return ret;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(ObSpecialTableChecksumItemFilter);
  };

public:
  ObSSTableDataChecksumIterator() : cur_idx_(0), sql_proxy_(nullptr), merge_error_cb_(nullptr)
  {}
  virtual ~ObSSTableDataChecksumIterator()
  {
    reset();
  }
  int init(common::ObISQLClient* sql_proxy, ObIMergeErrorCb* merge_error_cb);
  int next(ObSSTableDataChecksumInfo& checksum_info);
  int set_special_table_filter();
  int set_only_user_tenant_filter();
  void reset();

private:
  int fetch_next_batch_();
  int filter_(const ObSSTableDataChecksumItem& item, bool& is_filtered);

private:
  static const int64_t BATCH_CNT = 999;
  common::ObSEArray<ObSSTableDataChecksumItem, BATCH_CNT> fetched_checksum_items_;
  int64_t cur_idx_;
  common::ObISQLClient* sql_proxy_;
  ObIMergeErrorCb* merge_error_cb_;
  common::ObDList<ObIDataChecksumItemFiter> filters_;
  ObOnlyUserTenantChecksumItemFilter only_user_tenant_filter_;
  ObSpecialTableChecksumItemFilter special_table_filter_;

  DISALLOW_COPY_AND_ASSIGN(ObSSTableDataChecksumIterator);
};

}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SHARE_OB_SSTABLE_CHECKSUM_ITERATOR_H */
