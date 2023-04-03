/**
* Copyright (c) 2022 OceanBase
* OceanBase CE is licensed under Mulan PubL v2.
* You can use this software according to the terms and conditions of the Mulan PubL v2.
* You may obtain a copy of Mulan PubL v2 at:
*          http://license.coscl.org.cn/MulanPubL-2.0
* THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
* EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
* MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
* See the Mulan PubL v2 for more details.
*
* Define DataDictionaryService
*/

#ifndef OCEANBASE_DATA_DICT_META_INFO_H_
#define OCEANBASE_DATA_DICT_META_INFO_H_

#include <cstdint>
#include "lib/utility/ob_unify_serialize.h" // NEED_SERIALIZE_AND_DESERIALIZE
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h" // TO_STRING_KV
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObMySQLProxy
#include "share/scn.h"


namespace oceanbase
{
namespace datadict
{

struct ObDataDictMetaInfoItem {
public:
  ObDataDictMetaInfoItem();
  ~ObDataDictMetaInfoItem();
  void reset();
  void reset(
      const uint64_t snapshot_scn,
      const uint64_t start_lsn,
      const uint64_t end_lsn);

  bool operator==(const ObDataDictMetaInfoItem &that) const {
    return snapshot_scn_ == that.snapshot_scn_ &&
           start_lsn_ == that.start_lsn_ &&
           end_lsn_ == that.end_lsn_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K_(snapshot_scn), K_(start_lsn), K_(end_lsn));
public:
  uint64_t snapshot_scn_;
  uint64_t start_lsn_;
  uint64_t end_lsn_;
};

typedef ObSEArray<ObDataDictMetaInfoItem, 16> DataDictMetaInfoItemArr;

class ObDataDictMetaInfoHeader
{
public:
  static const int16_t DATADICT_METAINFO_HEADER_MAGIC = 0x4444; // hex of "DD"
  static const int64_t DATADICT_METAINFO_META_VERSION = 0x0001; // version = 1
public:
  ObDataDictMetaInfoHeader();
  ~ObDataDictMetaInfoHeader();
  void reset();
  int generate(
      const uint64_t tenant_id,
      const int32_t item_cnt,
      const uint64_t max_snapshot_scn,
      const uint64_t min_snapshot_scn,
      const char *data,
      const int64_t data_size);

  uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE int32_t get_meta_version() const {
    return meta_version_;
  }
  OB_INLINE int32_t get_item_count() const {
    return item_cnt_;
  }
  OB_INLINE uint64_t get_min_snapshot_scn() const {
    return min_snapshot_scn_;
  }
  OB_INLINE uint64_t get_max_snapshot_scn() const {
    return max_snapshot_scn_;
  }
  OB_INLINE int64_t get_data_size() const {
    return data_size_;
  }

  bool check_integrity(const char *data, const int64_t data_size) const;

  bool operator==(const ObDataDictMetaInfoHeader& that) const {
    return magic_ == that.magic_ && meta_version_ == that.meta_version_ && tenant_id_ == that.tenant_id_ &&
        item_cnt_ == that.item_cnt_ && min_snapshot_scn_ == that.min_snapshot_scn_ &&
        max_snapshot_scn_ == that.max_snapshot_scn_ && data_size_ == that.data_size_ &&
        checksum_ == that.checksum_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K_(magic),
               K_(meta_version),
               K_(tenant_id),
               K_(item_cnt),
               K_(min_snapshot_scn),
               K_(max_snapshot_scn),
               K_(data_size),
               K_(checksum));
private:
  int16_t   magic_;
  int16_t   meta_version_;
  uint64_t  tenant_id_;
  int32_t   item_cnt_;
  uint64_t  min_snapshot_scn_;
  uint64_t  max_snapshot_scn_;
  int64_t   data_size_;
  int64_t   checksum_;
};

class ObDataDictMetaInfo
{
public:
  ObDataDictMetaInfo();
  ~ObDataDictMetaInfo();

  void reset();

  int push_back(const ObDataDictMetaInfoItem &item);
  bool check_integrity() const;
  int64_t get_data_size() const {
    return header_.get_data_size();
  }
  int32_t get_meta_version() const {
    return header_.get_meta_version();
  }

  uint64_t get_tenant_id() const { return header_.get_tenant_id(); }

  const ObDataDictMetaInfoHeader& get_header() const {
    return header_;
  }

  const DataDictMetaInfoItemArr& get_item_arr() const {
    return item_arr_;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;

  TO_STRING_KV(K_(header));

private:
  ObDataDictMetaInfoHeader    header_;
  DataDictMetaInfoItemArr     item_arr_;
};

class MetaInfoQueryHelper {
  static const char *QUERY_META_INFO_SQL_STR;
  static const char *DATA_DICT_META_TABLE_NAME;
public:
  explicit MetaInfoQueryHelper(common::ObMySQLProxy &sql_proxy, uint64_t tenant_id) :
      sql_proxy_(sql_proxy),
      tenant_id_(tenant_id) {}

  int get_data(
      const share::SCN &base_scn,
      char *data,
      const int64_t data_size,
      int64_t &real_size,
      share::SCN &scn);

private:
  // only get data which snapshot_scn larger than base_scn.
  int get_data_dict_meta_info_(const share::SCN &base_scn, DataDictMetaInfoItemArr &item_arr);

  int parse_record_from_result_(
      const share::SCN &base_scn,
      common::sqlclient::ObMySQLResult &result,
      int64_t &record_count,
      int64_t &valid_record_count,
      DataDictMetaInfoItemArr &item_arr);

  int generate_data_(
      const DataDictMetaInfoItemArr &arr,
      char *data,
      const int64_t data_size,
      int64_t &real_size,
      share::SCN &scn);

  // invoke data_dict_service to do dump async.
  int mark_dump_data_dict_();

private:
  common::ObMySQLProxy &sql_proxy_;
  uint64_t tenant_id_;
};

}
}
#endif
