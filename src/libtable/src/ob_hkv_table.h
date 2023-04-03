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

#ifndef _OB_KV_TABLE_H
#define _OB_KV_TABLE_H 1
#include "ob_table_define.h"
#include "ob_table.h"
#include "lib/container/ob_iarray.h"
namespace oceanbase
{
namespace table
{
class ObTableServiceClient;

/** A Key-Value Table for HBase
 * 1. every column family can be represented by a OB table
 * 2. the schema of each OB table is 'create table t1_cf1 (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T)) partition by key(K) partitions 16;'
 */
class ObHKVTable
{
public:
  /// Key type for ObHKVTable
  struct Key
  {
    ObString rowkey_;
    ObString column_qualifier_;
    int64_t version_;
    TO_STRING_KV(K_(rowkey), K_(column_qualifier), K_(version));
  };
  /// Value type for ObHKVTable
  typedef common::ObObj Value;
  /// Entity of ObHKVTable
  class Entity: public ObITableEntity
  {
  public:
    Entity();
    ~Entity();
    virtual void reset();
    virtual int set_rowkey(const ObRowkey &rowkey) override;
    virtual int set_rowkey(const ObITableEntity &other) override;
    virtual int set_rowkey_value(int64_t idx, const ObObj &value) override;
    virtual int add_rowkey_value(const ObObj &value) override;
    virtual int64_t get_rowkey_size() const override { return 3; }
    virtual int get_rowkey_value(int64_t idx, ObObj &value) const override;
    virtual ObRowkey get_rowkey() const override;
    virtual int64_t hash_rowkey() const override;
    virtual int get_property(const ObString &prop_name, ObObj &prop_value) const override;
    virtual int set_property(const ObString &prop_name, const ObObj &prop_value) override;
    virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const override;
    virtual int get_properties_names(ObIArray<ObString> &properties) const override;
    virtual int get_properties_values(ObIArray<ObObj> &values) const override;
    virtual int64_t get_properties_count() const override;

    const Key &key() const { return key_; }
    const Value &value() const { return value_; }
    void set_key(const Key &k) { key_ = k; rowkey_obj_count_ = 3; }
    void set_value(const Value &v) { value_ = v; has_property_set_ = true; }
    TO_STRING_KV(K_(key), K_(value));
  private:
    Key key_;
    Value value_;
    int64_t rowkey_obj_count_;
    ObObj key_objs_[3];
    bool has_property_set_;
  };
  typedef common::ObIArray<Key> IKeys;
  typedef common::ObIArray<Entity> IEntities;
  typedef common::ObIArray<Value> IValues;
  typedef common::ObSEArray<Key, 8> Keys;
  typedef common::ObSEArray<Entity, 8> Entities;
  typedef common::ObSEArray<Value, 8> Values;
public:
  int init(ObTableServiceClient &client, ObTable *tbl);
  void destroy();

  int get(const Key &key, Value &value);
  int multi_get(const IKeys &keys, IValues &values);

  int put(const Key &key, const Value &value);
  int multi_put(const IKeys &keys, const IValues &values);
  int multi_put(const IEntities &entities);

  int remove(const Key &key);
  int multi_remove(const IKeys &keys);
private:
  ObHKVTable();
  virtual ~ObHKVTable();
  friend class ObTableServiceClientImpl;
  // types and constants
  static const char* ROWKEY_CNAME;
  static const char* CQ_CNAME;
  static const char* VERSION_CNAME;
  static const char* VALUE_CNAME;
  static const ObString ROWKEY_CNAME_STR;
  static const ObString CQ_CNAME_STR;
  static const ObString VERSION_CNAME_STR;
  static const ObString VALUE_CNAME_STR;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHKVTable);
private:
  bool inited_;
  ObTableServiceClient *client_;
  ObTable *tbl_;
  ObTableEntityFactory<Entity> entity_factory_;
};

/**
 * @example ob_kvtable_example.cpp
 * This is an example of how to use the ObHKVTable class.
 *
 */

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_KV_TABLE_H */
