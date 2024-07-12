/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDSDUMPNODE_WRAPPER_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDSDUMPNODE_WRAPPER_H
#include "storage/multi_data_source/mds_node.h"
#include <cstdint>

namespace oceanbase
{
namespace blocksstable
{
class ObDatumRow;
}
namespace storage
{
namespace mds
{

// reorganize MdsDumpKV inner status to make it friendly to storage layer
// but can not change MdsDumpKV directly due to some compat reasons
// so new structure converted from MdsDumpKV defined here

struct MdsDumpKVStorageMetaInfo
{
  OB_UNIS_VERSION(1);
public:
  // serializable object needed default construction
  MdsDumpKVStorageMetaInfo();
  MdsDumpKVStorageMetaInfo(const MdsDumpKV &mds_dump_kv);
  bool is_valid() const {
    return mds_table_id_ != UINT8_MAX &&
           writer_id_ != 0 &&
           seq_no_.is_valid() &&
           redo_scn_.is_valid() &&
           end_scn_.is_valid() &&
           trans_version_.is_valid();
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:// state
  uint8_t mds_table_id_;
  uint32_t key_crc_check_number_;
  uint32_t data_crc_check_number_;
  MdsNodeStatus status_;
  int64_t writer_id_;
  transaction::ObTxSEQ seq_no_;
  share::SCN redo_scn_;
  share::SCN end_scn_;
  share::SCN trans_version_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, MdsDumpKVStorageMetaInfo,\
                         mds_table_id_,
                         key_crc_check_number_,
                         data_crc_check_number_,
                         status_,
                         writer_id_,
                         seq_no_,
                         redo_scn_,
                         end_scn_,
                         trans_version_);

struct MdsDumpKVStorageAdapter// this structure dose not own it's inner status, must be used under mds_dump_node's life time protection.
{
  MdsDumpKVStorageAdapter();
  MdsDumpKVStorageAdapter(const MdsDumpKV &mds_dump_kv);
  int64_t to_string(char *buf, const int64_t buf_len) const;
public:// method
  uint8_t get_type() const { return type_; }
  common::ObString get_key() const { return key_; }
  MdsDumpKVStorageMetaInfo get_meta_info() const { return meta_info_; }
  common::ObString get_user_data() const { return user_data_; }
  bool is_valid() const {
    return type_ != UINT8_MAX &&
           meta_info_.is_valid() &&
           !user_data_.empty();
  }
  int convert_to_mds_row(ObIAllocator &allocator, blocksstable::ObDatumRow &row) const;
  // shallow copy
  int convert_from_mds_row(const blocksstable::ObDatumRow &row);
  // shallow copy
  int convert_from_mds_multi_version_row(const blocksstable::ObDatumRow &row);
public:// state
  /*********storage row key************/
  uint8_t type_;
  common::ObString key_;
  /************************************/
  MdsDumpKVStorageMetaInfo meta_info_;
  common::ObString user_data_;
};

}
}
}
#endif