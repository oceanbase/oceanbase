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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDSDUMPNODE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDSDUMPNODE_H
#include <cstdint>
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_unify_serialize.h"
#include "meta_programming/ob_type_traits.h"
#include "share/ob_ls_id.h"
#include "src/storage/multi_data_source/compile_utility/compile_mapper.h"
#include "src/storage/multi_data_source/mds_node.h"
#include "src/storage/multi_data_source/compile_utility/map_type_index_in_tuple.h"
#include "src/storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "common/meta_programming/ob_meta_serialization.h"
#include "mds_dump_kv_wrapper.h"
#include "storage/tx/ob_tx_seq.h"


namespace oceanbase
{
namespace storage
{
namespace mds
{

struct MdsDumpKey;
struct MdsDumpNode;

struct MdsDumpKey// RAII
{
  OB_UNIS_VERSION(1);
public:
  MdsDumpKey() : mds_table_id_(UINT8_MAX), mds_unit_id_(UINT8_MAX), allocator_(nullptr) {}
  ~MdsDumpKey() { reset(); }
  // disallow copy, assign and move sematic
  MdsDumpKey(const MdsDumpKey &) = delete;
  MdsDumpKey(MdsDumpKey &&) = delete;
  MdsDumpKey& operator=(const MdsDumpKey &) = delete;
  MdsDumpKey& operator=(MdsDumpKey &&) = delete;
  void swap(MdsDumpKey &other) noexcept;// for exception safty

  template <typename UnitKey>
  int init(const uint8_t mds_table_id,
           const uint8_t mds_unit_id,
           const UnitKey &key,
           ObIAllocator &alloc);
  int assign(const MdsDumpKey &rhs, common::ObIAllocator &alloc);
  template <typename UnitKey>
  int convert_to_user_key(UnitKey &user_key) const;
  void reset();
  int compare(const MdsDumpKey &rhs, int &result) const;
  uint32_t generate_hash() const;
  bool is_valid() const;
  int64_t to_string(char *, const int64_t) const;
public:
  uint8_t mds_table_id_;
  uint8_t mds_unit_id_;
  uint32_t crc_check_number_;
  ObIAllocator *allocator_;// to release serialized buffer
  common::ObString key_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, MdsDumpKey, mds_table_id_, mds_unit_id_, crc_check_number_, key_);

struct MdsDumpNode// RAII
{
public:
  MdsDumpNode();
  ~MdsDumpNode() { reset(); }
  // disallow copy, assign and move sematic
  MdsDumpNode(const MdsDumpNode &) = delete;
  MdsDumpNode(MdsDumpNode &&) = delete;
  MdsDumpNode& operator=(const MdsDumpNode &) = delete;
  MdsDumpNode& operator=(MdsDumpNode &&) = delete;
  void swap(MdsDumpNode &other) noexcept;// for exception safty

  bool is_valid() const;
  // every dump node is converted from UserMdsNode from a multi-version row in a K-V unit in MdsTable
  template <typename K, typename V>
  int init(const uint8_t mds_table_id,
           const uint8_t mds_unit_id,
           const UserMdsNode<K, V> &user_mds_node,
           ObIAllocator &alloc);
  // can be compared(only can be compared in same unit, compare key first, compare version if key is same)
  int compare(const MdsDumpNode& rhs, int &result);
  void reset();
  uint32_t generate_hash() const;
  // print
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t simple_to_string(char *buf, const int64_t buf_len, int64_t &pos) const;
  template <typename K, typename V>
  int convert_to_user_mds_node(UserMdsNode<K, V> &user_mds_node, const share::ObLSID &ls_id, const ObTabletID &tablet_id) const;

  int assign(const MdsDumpNode &rhs, ObIAllocator &alloc);

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  static constexpr int64_t UNIS_VERSION_V1 = 1;
  static constexpr int64_t UNIS_VERSION = 2;

  // member state
  uint8_t mds_table_id_;
  uint8_t mds_unit_id_;
  uint32_t crc_check_number_;
  MdsNodeStatus status_;
  ObIAllocator *allocator_;// to release serialized buffer
  int64_t writer_id_; // mostly is tx id
  transaction::ObTxSEQ seq_no_;// if one writer write multi nodes in one same row, seq_no is used to order those writes
  share::SCN redo_scn_; // log scn of redo
  share::SCN end_scn_; // log scn of commit/abort
  share::SCN trans_version_; // read as prepare version if phase is not COMMIT, or read as commit version
  common::ObString user_data_;// different user data type serialized result(may be very large, but should be less than 1.5MB)
};

struct MdsDumpKV
{
public:
  MdsDumpKV();
public:
  int convert_from_adapter(common::ObIAllocator &allocator, MdsDumpKVStorageAdapter &adapter);

  void swap(MdsDumpKV &other);
  void reset();
  bool is_valid() const;
  int assign(const MdsDumpKV &rhs, ObIAllocator &alloc);

  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos);
  int64_t get_serialize_size() const;

  int64_t to_string(char *buf, const int64_t buf_len) const;
public:
  static const int64_t UNIS_VERSION = 1;
  MdsDumpKey k_;
  MdsDumpNode v_;
};

template <typename UnitKey>
int MdsDumpKey::init(const uint8_t mds_table_id,
                     const uint8_t mds_unit_id,
                     const UnitKey &key,
                     ObIAllocator &alloc)
{
  #define PRINT_WRAPPER KR(ret), K(mds_table_id), K(mds_unit_id), K(key), K(key_size)
  static_assert(OB_TRAIT_MDS_SERIALIZEABLE(UnitKey), "UnitKey must by binary level comparable");
  static_assert(!OB_TRAIT_IS_COMPAREABLE(UnitKey), "UnitKey no need be origin comparable, but comparable after serialized, compare defination is probably by your misunderstanding");
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t key_size = key.mds_get_serialize_size();
  int64_t pos = 0;
  char *key_buffer = nullptr;
  bool need_free_key_buffer = false;
  reset();
  if (MDS_FAIL_FLAG(OB_ISNULL(key_buffer = (char*)alloc.alloc(key_size)), need_free_key_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    MDS_LOG_NONE(WARN, "fail to alloc key serlialize buffer");
  } else if (OB_FAIL(key.mds_serialize(key_buffer, key_size, pos))) {
    MDS_LOG_NONE(WARN, "fail to serialize key");
  } else {
    mds_table_id_ = mds_table_id;
    mds_unit_id_ = mds_unit_id;
    allocator_ = &alloc;
    key_.assign(key_buffer, key_size);
    crc_check_number_ = generate_hash();
  }
  if (MDS_FAIL(ret)) {
    if (need_free_key_buffer) {
      allocator_->free(key_buffer);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <>
inline int MdsDumpKey::init<DummyKey>(const uint8_t mds_table_id,
                                      const uint8_t mds_unit_id,
                                      const DummyKey &key,
                                      ObIAllocator &alloc)
{
  UNUSED(key);
  UNUSED(alloc);
  reset();
  mds_table_id_ = mds_table_id;
  mds_unit_id_ = mds_unit_id;
  crc_check_number_ = generate_hash();
  return OB_SUCCESS;
}

template <typename K, typename V>
int MdsDumpNode::init(const uint8_t mds_table_id,
                      const uint8_t mds_unit_id,
                      const UserMdsNode<K, V> &user_mds_node,
                      ObIAllocator &alloc)
{
  #define PRINT_WRAPPER KR(ret), K(user_mds_node), K(data_size)
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t pos = 0;
  bool need_free_data_buffer = false;
  int64_t data_size = user_mds_node.user_data_.get_serialize_size();
  char *data_buffer = nullptr;
  reset();
  if (MDS_FAIL_FLAG(OB_ISNULL(data_buffer = (char*)alloc.alloc(data_size)),
                    need_free_data_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    MDS_LOG_NONE(WARN, "fail to alloc data serlialize buffer");
  } else if (OB_FAIL(user_mds_node.user_data_.serialize(data_buffer, data_size, pos))) {
    MDS_LOG_NONE(WARN, "fail to serialize user data");
  } else {
    mds_table_id_ = mds_table_id;
    mds_unit_id_ = mds_unit_id;
    allocator_ = &alloc;
    writer_id_ = user_mds_node.writer_id_;
    seq_no_ = user_mds_node.seq_no_;
    redo_scn_ = user_mds_node.redo_scn_;
    end_scn_ = user_mds_node.end_scn_;
    trans_version_ = user_mds_node.trans_version_;
    status_ = user_mds_node.status_;
    user_data_.assign(data_buffer, data_size);
    crc_check_number_ = generate_hash();
  }
  // roll back path
  if (MDS_FAIL(ret)) {
    if (need_free_data_buffer) {
      alloc.free(data_buffer);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename UnitKey>
int MdsDumpKey::convert_to_user_key(UnitKey &user_key) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(generated_hash), K(typeid(UnitKey).name())
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t pos = 0;
  uint32_t generated_hash = generate_hash();
  if (UINT32_MAX != crc_check_number_ && generated_hash != crc_check_number_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_NONE(ERROR, "CRC CHECK FAILED!");
  } else if (MDS_FAIL(user_key.mds_deserialize(key_.ptr(), key_.length(), pos))) {
    MDS_LOG_NONE(ERROR, "deserialize user data failed");
  } else {
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <>
inline int MdsDumpKey::convert_to_user_key<DummyKey>(DummyKey &user_key) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(generated_hash)
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t pos = 0;
  uint32_t generated_hash = generate_hash();
  if (UINT32_MAX != crc_check_number_ && generated_hash != crc_check_number_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_NONE(ERROR, "CRC CHECK FAILED!");
  } else {
  }
  return ret;
  #undef PRINT_WRAPPER
}

template <typename K, typename V>
int MdsDumpNode::convert_to_user_mds_node(UserMdsNode<K, V> &user_mds_node, const share::ObLSID &ls_id, const ObTabletID &tablet_id) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(generated_hash), K(typeid(V).name())
  int ret = OB_SUCCESS;
  MDS_TG(1_ms);
  int64_t pos = 0;
  uint32_t generated_hash = generate_hash();
  set_mds_mem_check_thread_local_info(ls_id, tablet_id, typeid(UserMdsNode<K, V>).name());
  meta::MetaSerializer<V> serializer(MdsAllocator::get_instance(), user_mds_node.user_data_);
  if (UINT32_MAX != crc_check_number_ && generated_hash != crc_check_number_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_NONE(ERROR, "CRC CHECK FAILED!");
  } else if (MDS_FAIL(serializer.deserialize(user_data_.ptr(), user_data_.length(), pos))) {
    MDS_LOG_NONE(ERROR, "deserialize user data failed");
  } else {
    user_mds_node.writer_id_ = writer_id_;
    user_mds_node.seq_no_ = seq_no_;
    user_mds_node.redo_scn_ = redo_scn_;
    user_mds_node.end_scn_ = end_scn_;
    user_mds_node.trans_version_ = trans_version_;
    user_mds_node.status_ = status_;
  }
  reset_mds_mem_check_thread_local_info();
  return ret;
  #undef PRINT_WRAPPER
}

}
}
}

#endif