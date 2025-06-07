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

#ifndef OCEANBASE_STORAGE_OB_TABLET_REORGANIZATION_INFO_TABLE
#define OCEANBASE_STORAGE_OB_TABLET_REORGANIZATION_INFO_TABLE

#include "share/ob_rpc_struct.h"
#include "lib/worker.h"
#include "ob_tablet_reorg_info_table_schema_helper.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{

namespace observer
{
struct VirtualTxDataRow;
}

namespace share
{
namespace schema
{
class ObTableSchema;
} // schema
} // share

namespace storage
{
class ObLS;

struct ObTabletReorgInfoDataType final
{
  enum TYPE
  {
    TRANSFER_IN = 0,
    TRANSFER_OUT = 1,
    SPLIT_SRC = 2,
    SPLIT_DST = 3,
    MAX,
  };
  static OB_INLINE bool is_valid(const TYPE &type) { return type >= 0 && type < MAX; }
  static const char *get_str(const TYPE &type);
  static bool is_transfer(const TYPE &type) {
    return type >= TRANSFER_IN && type <= TRANSFER_OUT; }
  static bool is_split(const TYPE &type) { return type >= SPLIT_SRC && type <= SPLIT_DST; }
};

class ObITabletReorgInfoDataValue
{
  OB_UNIS_VERSION_PV(); // pure virtual
public:
  ObITabletReorgInfoDataValue() {}
  virtual ~ObITabletReorgInfoDataValue() {}
  virtual bool is_valid() const = 0;
  virtual void reset() = 0;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
};

struct ObTabletReorgInfoDataKey final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletReorgInfoDataKey();
  ~ObTabletReorgInfoDataKey();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tablet_id), K_(reorganization_scn), K_(type));
  common::ObTabletID tablet_id_;
  share::SCN reorganization_scn_;
  ObTabletReorgInfoDataType::TYPE type_;
};

class ObTabletReorgInfoDataValue final
{
public:
  ObTabletReorgInfoDataValue();
  ~ObTabletReorgInfoDataValue();
  bool is_valid() const;
  int write_value(const ObITabletReorgInfoDataValue &data_value);
  int write_value(const char *buf, const int64_t buf_len);
  int get_value(ObITabletReorgInfoDataValue &data_value) const;
  int get_value(char *buf, const int64_t buf_len, int64_t &pos) const;
  void reset() { value_[0] = '\0'; }
  void reuse() { value_[0] = '\0'; }
  ObTabletReorgInfoDataValue &operator =(const ObTabletReorgInfoDataValue &value);
  int assign(const ObTabletReorgInfoDataValue &other);
  int assign(const char *str, const int64_t buf_len);
  bool is_empty() const;
  int64_t size() const { return pos_; }
  const char *get_ptr() const { return value_; }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  static const int64_t MAX_VALUE_SIZE = ObTabletReorgInfoTableSchemaDef::DATA_VALUE_COLUMN_LENGTH;
  int64_t pos_ = 0;
  char value_[MAX_VALUE_SIZE];
};

struct ObTransferDataValue : public ObITabletReorgInfoDataValue
{
  OB_UNIS_VERSION_V(1);
public:
  ObTransferDataValue();
  virtual ~ObTransferDataValue();
  virtual void reset();
  virtual bool is_valid() const;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const;
  ObTabletStatus tablet_status_;
  int64_t transfer_seq_;
  share::ObLSID relative_ls_id_;
  share::SCN transfer_scn_;
  share::SCN src_reorganization_scn_;
};

//split struct add after

struct ObTabletReorgInfoData final
{
public:
  ObTabletReorgInfoData();
  ~ObTabletReorgInfoData();
  void reset();
  bool is_valid() const;
  int data_2_datum_row(
      common::ObIAllocator &allocator,
      blocksstable::ObDatumRow *datum_row) const;
  int row_2_data(
      const blocksstable::ObDatumRow *new_row,
      share::SCN &trans_scn,
      int64_t &sql_no);
  int get_transfer_data_value(ObTransferDataValue &data_value) const;
  //NOT SUPPORTED NOW
  //int get_split_data_value();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int64_t to_value_string(char *buf, const int64_t buf_len) const;
public:
  ObTabletReorgInfoDataKey key_;
  ObTabletReorgInfoDataValue value_;
};

class ObTabletReorgInfoTable
{
public:
  ObTabletReorgInfoTable();
  ~ObTabletReorgInfoTable();

  int init(ObLS *ls);
  int start();
  void stop();
  void destroy();
  int create_tablet(const share::SCN &create_scn);
  int remove_tablet();
  int offline();
  int online();

  TO_STRING_KV(KP(this),
               K_(is_inited),
               K_(ls_id),
               K_(recycle_scn_cache));
  int update_already_recycled_scn(const share::SCN &already_recycled_scn);
  int update_can_recycle_scn(const share::SCN &can_recycle_scn);
  int get_can_recycle_scn(share::SCN &can_recycle_scn);
  int init_tablet_for_compat();

private:
  struct RecycleSCNCache final
  {
  public:
    RecycleSCNCache();
    ~RecycleSCNCache() {}
    void reset();
    int update_already_recycled_scn(
        const share::SCN &already_recycled_scn);
    int  update_can_recycle_scn(
        const share::SCN &can_recycle_scn);
    TO_STRING_KV(K_(already_recycled_scn), K_(can_recycle_scn), K_(update_ts));
    share::SCN get_already_recycle_scn();
    share::SCN get_can_reycle_scn();
  private:
    common::SpinRWLock lock_;
    share::SCN already_recycled_scn_;
    share::SCN can_recycle_scn_;
    int64_t update_ts_;
  };
private:
  int create_data_tablet_(
      const uint64_t tenant_id,
      const share::ObLSID ls_id,
      const lib::Worker::CompatMode compat_mode,
      const share::SCN &create_scn);
  int remove_tablet_(const common::ObTabletID &tablet_id);
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  ObLS *ls_;
  RecycleSCNCache recycle_scn_cache_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletReorgInfoTable);
};

}  // namespace storage
}  // namespace oceanbase
#endif
