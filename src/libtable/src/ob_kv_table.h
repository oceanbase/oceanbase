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

namespace oceanbase
{
namespace table
{
/** A Key-Value Table for HBase
 * 1. provide kv inerface like memcached
 * 2. the schema of each kvtable is 'create table kv (K varbinary(1024), V varbinary(1024), primary key(K)) partition by key(K) partitions 16;'
 */
class ObKVTable
{
public:
  /// Key type for ObKVTable
  typedef ObString Key;
  /// Value type for ObKVTable
  typedef ObObj Value;
  /// Entity of ObKVTable
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
    virtual int64_t get_rowkey_size() const override { return 1; }
    virtual int get_rowkey_value(int64_t idx, ObObj &value) const override;
    virtual int64_t hash_rowkey() const override;
    virtual int get_property(const ObString &prop_name, ObObj &prop_value) const override;
    virtual int set_property(const ObString &prop_name, const ObObj &prop_value) override;
    virtual int get_properties(ObIArray<std::pair<ObString, ObObj> > &properties) const override;
    virtual int get_properties_names(ObIArray<ObString> &properties) const override;
    virtual int get_properties_values(ObIArray<ObObj> &values) const override;

    const Key &key() const { return key_; }
    const Value &value() const { return value_; }
    void set_key(const Key &k) { key_ = k;}
    void set_value(const Value &v) { value_ = v;}
    TO_STRING_KV(K_(key), K_(value));
  private:
    Key key_;
    Value value_;
  };
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
  ObKVTable();
  virtual ~ObKVTable();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObKVTable);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_KV_TABLE_H */
