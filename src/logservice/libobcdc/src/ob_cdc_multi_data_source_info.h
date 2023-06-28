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
 *
 * MultiDataSourceNode and MultiDataSourceInfo
 */

#ifndef OCEANBASE_LIBOBCDC_MULTI_DATA_SOURCE_INFO_H_
#define OCEANBASE_LIBOBCDC_MULTI_DATA_SOURCE_INFO_H_

#include "logservice/palf/lsn.h"                // palf::LSN
#include "share/ls/ob_ls_operator.h"            // share::ObLSAttr
#include "storage/tx/ob_multi_data_source.h"    // transaction::ObTxBufferNode

#include "ob_cdc_tablet_to_table_info.h"        // CDCTabletChangeInfo
#include "logservice/data_dictionary/ob_data_dict_struct.h" // ObDictTenantMeta

namespace oceanbase
{
namespace libobcdc
{
class TenantSchemaInfo;
class DBSchemaInfo;

class MultiDataSourceNode
{
public:
  MultiDataSourceNode();
  ~MultiDataSourceNode() { reset(); }
  void reset();
  int init(
      const palf::LSN &lsn,
      const transaction::ObTxDataSourceType &type,
      const char *buf,
      const int64_t buf_size);
  inline bool is_valid() const
  {
    return lsn_.is_valid() && tx_buf_node_.is_valid();
  }
public:
  inline bool is_ls_table_change_node() const
  {
    return transaction::ObTxDataSourceType::LS_TABLE == tx_buf_node_.get_data_source_type();
  }

  inline bool is_tablet_change_node() const
  {
    return transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS == tx_buf_node_.get_data_source_type()
           || transaction::ObTxDataSourceType::DELETE_TABLET_NEW_MDS == tx_buf_node_.get_data_source_type();
  }

  inline bool is_ddl_trans_node() const
  {
    return transaction::ObTxDataSourceType::DDL_TRANS == tx_buf_node_.get_data_source_type();
  }

  inline const transaction::ObTxBufferNode &get_tx_buf_node() const { return tx_buf_node_; }

  TO_STRING_KV(K_(lsn), K_(tx_buf_node));
private:
  palf::LSN lsn_;
  transaction::ObTxBufferNode tx_buf_node_;
};

typedef common::ObSEArray<MultiDataSourceNode, 1> MultiDataSourceNodeArray;
typedef common::ObSEArray<ObCDCTabletChangeInfo, 1> CDCTabletChangeInfoArray;
typedef common::ObSEArray<const datadict::ObDictTenantMeta*, 1> DictTenantArray;
typedef common::ObSEArray<const datadict::ObDictDatabaseMeta*, 1> DictDatabaseArray;
typedef common::ObSEArray<const datadict::ObDictTableMeta*, 1> DictTableArray;

class MultiDataSourceInfo
{
public:
  MultiDataSourceInfo();
  ~MultiDataSourceInfo() { reset(); }
  void reset();
public:
  OB_INLINE bool is_valid() const { return has_ls_table_op() || has_tablet_change_op() || has_ddl_trans_op_; }
  OB_INLINE bool has_ls_table_op() const { return 0 < ls_attr_arr_.count(); }
  const share::ObLSAttrArray &get_ls_attr_arr() const { return ls_attr_arr_; }
  int push_back_ls_table_op(const share::ObLSAttr &ls_attr);
  bool has_tablet_change_op() const { return 0 < tablet_change_info_arr_.count(); }
  int push_back_tablet_change_info(const ObCDCTabletChangeInfo &tablet_change_info);
  const CDCTabletChangeInfoArray &get_tablet_change_info_arr() const { return tablet_change_info_arr_; }
  void set_ddl_trans() { ATOMIC_SET(&has_ddl_trans_op_, true); }
  bool is_ddl_trans() const { return ATOMIC_LOAD(&has_ddl_trans_op_); }
  DictTenantArray &get_dict_tenant_array() { return dict_tenant_metas_; }
  DictDatabaseArray &get_dict_database_array() { return dict_database_metas_; }
  DictTableArray &get_dict_table_array() { return dict_table_metas_; }

  bool is_empty_dict_info() const
  {
    return (0 == dict_tenant_metas_.count())
      && (0 == dict_database_metas_.count())
      && (0 == dict_table_metas_.count());
  }
  bool is_contains_multiple_table_metas() const
  {
    return dict_table_metas_.count() > 1;
  }

  int get_new_tenant_scehma_info(
      const uint64_t tenant_id,
      TenantSchemaInfo &tenant_schema_info);
  int get_new_database_scehma_info(
      const uint64_t tenant_id,
      const uint64_t database_id,
      DBSchemaInfo &db_schema_info);
  int get_new_table_meta(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const datadict::ObDictTableMeta *&table_meta);

  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  // ls change op(create/delete logstream), parse from ObTxBufferNode(LS_TABLE type) in commit_log
  share::ObLSAttrArray ls_attr_arr_;
  // tablet_change_op(create/delete tablet), parse from MultiDataSourceNode.
  CDCTabletChangeInfoArray tablet_change_info_arr_;

  // ddl_trans_info(incremental schema info)
  bool has_ddl_trans_op_;
  DictTenantArray dict_tenant_metas_;
  DictDatabaseArray dict_database_metas_;
  DictTableArray dict_table_metas_;
};

} // namespace libobcdc
} // namespace oceanbase
#endif
