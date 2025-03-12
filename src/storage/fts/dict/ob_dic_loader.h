/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DICT_OB_DICT_H_
#define OCEANBASE_STORAGE_DICT_OB_DICT_H_
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace storage
{
class ObTenantDicLoader
{
public:
  struct ObDicTableInfo
  {
  public:
    ObDicTableInfo(const char **raw_data = nullptr, uint64_t array_size = 0, const char *table_name = nullptr, uint64_t table_id = 0) :
        raw_data_(raw_data), array_size_(array_size), table_name_(table_name), table_id_(table_id) { }
    ~ObDicTableInfo() = default;
    TO_STRING_KV(
      KP(raw_data_),
      K(array_size_),
      K(table_name_),
      K(table_id_));
  public:
    const char **raw_data_;
    int64_t array_size_;
    const char *table_name_;
    uint64_t table_id_;
  };

  // we can add field here to adapt to dictionary tables with different structures
  struct ObDicItem
  {
  public:
    explicit ObDicItem(const char *word = nullptr) : word_(word) { }
    ~ObDicItem() = default;
  public:
    const char *word_;
  };

public:
  ObTenantDicLoader() : is_inited_(false), is_load_(false), ref_cnt_(0) { }
  virtual ~ObTenantDicLoader() { }
  virtual int init() = 0;
  int try_load_dictionary_in_trans(const uint64_t tenant_id, ObMySQLTransaction &trans);
  int try_load_dictionary_in_trans(const uint64_t tenant_id);
  OB_INLINE const ObArray<ObDicTableInfo>& get_dic_tables_info() const
  {
    return dic_tables_info_;
  }
  OB_INLINE int64_t inc_ref()
  {
    const int64_t cnt = ATOMIC_AAF(&ref_cnt_, 1);
    return cnt;
  }
  OB_INLINE int64_t dec_ref()
  {
    const int64_t cnt = ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
    return cnt;
  }
  TO_STRING_KV(
    K(is_inited_),
    K(is_load_),
    K(ref_cnt_),
    K(dic_tables_info_));
protected:
  int load_dictionary_in_trans(const uint64_t tenant_id, ObMySQLTransaction &trans);
  int check_need_load_dic(const uint64_t tenant_id, bool &check_need_load_dic);
  // When defining new loading class for some dictionary tables
  // please implement the all virtual functions in this base class to meet differentiation requirements
  virtual int get_dic_item(const uint64_t i, const uint64_t pos, ObDicItem& item) = 0;
  virtual int fill_dic_item(const ObDicItem &item, share::ObDMLSqlSplicer &dml) = 0;
  virtual ObDicTableInfo get_main_dic_info() = 0;
  virtual ObDicTableInfo get_quantifier_dic_info() = 0;
  virtual ObDicTableInfo get_stop_dic_info() = 0;

protected:
  static constexpr int64_t DEFAULT_BATCH_SIZE = 8192;
  bool is_inited_;
  bool is_load_;
  int64_t ref_cnt_;
  ObArray<ObDicTableInfo> dic_tables_info_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantDicLoader);
};

class ObTenantDicLoaderHandle
{
public:
  ObTenantDicLoaderHandle() : loader_(nullptr) {}
  ~ObTenantDicLoaderHandle() { reset(); }
  ObTenantDicLoaderHandle(const ObTenantDicLoaderHandle &other)
    : loader_(nullptr)
  {
    *this = other;
  }
  ObTenantDicLoaderHandle &operator=(const ObTenantDicLoaderHandle &other);
  void reset();
  int set_loader(ObTenantDicLoader *loader);
  OB_INLINE bool is_valid() const { return nullptr != loader_; }
  OB_INLINE ObTenantDicLoader *get_loader() const { return loader_; }
  TO_STRING_KV(KPC_(loader));

private:
  ObTenantDicLoader *loader_;
};
} //end storage
} // end oceanbase
#endif //OCEANBASE_STORAGE_DICT_OB_DICT_H_