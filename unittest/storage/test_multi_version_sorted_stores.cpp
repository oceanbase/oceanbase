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

#include <gtest/gtest.h>
#define private public
#include "storage/ob_multi_version_sorted_stores.h"
#include "storage/ob_ss_store.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace memtable;
namespace unittest
{

TEST(ObStoresHandle, simple)
{
  int ret = OB_SUCCESS;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *store = NULL;

  //Invalid argument
  ret = stores_handle.add_store(NULL);
  ASSERT_NE(OB_SUCCESS, ret);

  //without init
  store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_EQ(1L, store->ref_cnt_);
  ret = stores_handle.add_store(store);
  ASSERT_NE(OB_SUCCESS, ret);

  //normal
  stores_handle.init(&cp_fty);
  ret = stores_handle.add_store(store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2L, store->ref_cnt_);
  ASSERT_EQ(1L, stores_handle.get_stores().count());

  ASSERT_EQ(2L, store->ref_cnt_);
  stores_handle.reset();
  //allow repeate reset
  stores_handle.reset();
  ASSERT_EQ(1L, store->ref_cnt_);

  cp_fty.free(store);
}

TEST(ObSortedStores, simple)
{
  int ret = OB_SUCCESS;
  ObSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObVersion version;
  bool merge;

  //invalid use without init
  ret = stores.add_store(ss_store);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.remove_store(version);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_write_store(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_store(version, stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  //empty
  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));
  ret = stores.add_store(ss_store);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.remove_store(version);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_write_store(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_store(version, stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  //repeatedly add
  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  version.major_ = 1;
  ss_store->set_version(version);
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = stores.add_store(ss_store);
  ASSERT_NE(OB_SUCCESS, ret);
  ASSERT_EQ(2L, ss_store->ref_cnt_);

  //invalid handle
  ret = stores.get_write_store(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_store(version, stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  //remove
  ret = stores.remove_store(version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1L, ss_store->ref_cnt_);

  cp_fty.free(ss_store);
}

TEST(ObSortedStores, normal)
{
  int ret = OB_SUCCESS;
  ObSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  if (0 == ss_store->dec_ref()) {
    cp_fty.free(ss_store);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  mem_store->minor_freeze(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  if (0 == mem_store->dec_ref()) {
    cp_fty.free(mem_store);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 1));
  mem_store->major_freeze(ObVersion(2, 1));
  ret = stores.add_store(mem_store);
  if (0 == mem_store->dec_ref()) {
    cp_fty.free(mem_store);
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  if (0 == mem_store->dec_ref()) {
    cp_fty.free(mem_store);
  }
  ASSERT_EQ(OB_SUCCESS, ret);


  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_write_store(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(3, 0), stores_handle.get_stores().at(0)->get_version());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(1, 0), stores_handle.get_stores().at(0)->get_version());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(1, 0), stores_handle.get_stores().at(0)->get_version());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_store(ObVersion(2, 1), stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(2, 1), stores_handle.get_stores().at(0)->get_version());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, stores_handle.get_stores().count());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, stores_handle.get_stores().count());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  bool merge = false;
  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(2, 0), stores_handle.get_stores().at(0)->get_version());
  ASSERT_EQ(true, merge);

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(1, 0), stores_handle.get_stores().at(0)->get_version());

  stores_handle.reset();
  stores_handle.init(&cp_fty);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores().count());
  ASSERT_EQ(ObVersion(1, 0), stores_handle.get_stores().at(0)->get_version());
}

TEST(ObMultiVersionSortedStores, simple)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObVersion version;
  bool merge;

  //invalid use without init
  ret = stores.add_store(ss_store);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.remove_store(version);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_write_store(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_store(version, stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_version_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);

  //empty
  stores.init(&cp_fty);
  ret = stores.add_store(ss_store);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.remove_store(version);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_write_store(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_base_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_last_ssstore(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_store(version, stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_read_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_merge_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_version_stores(stores_handle);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = stores.get_all_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, stores_handle.get_stores().count());
}

TEST(ObMultiVersionSortedStores, get_minor_store_major_freeze)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;
  ObIStore *store = NULL;
  bool merge;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));

  // only ssstore
  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  // ssstore + major frozen memstore + active memstore
  mem_store->major_freeze(ObVersion(2, 0));
  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores_count());
  ASSERT_EQ(OB_SUCCESS, stores_handle.get_last_store(store));
  ASSERT_EQ(ObVersion(2, 0), store->get_version());
  ASSERT_EQ(true, merge);
}

TEST(ObMultiVersionSortedStores, get_minor_store)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;
  ObIStore *store = NULL;
  bool merge;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));

  // only ssstore
  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, stores_handle.get_stores_count());
  ASSERT_EQ(false, merge);

  // ssstore + active memstore
  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, stores_handle.get_stores_count());
  ASSERT_EQ(false, merge);

  // ssstore + minor frozen memstore + active memstore
  mem_store->minor_freeze(ObVersion(2, 0));
  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores_count());
  ASSERT_EQ(OB_SUCCESS, stores_handle.get_last_store(store));
  ASSERT_EQ(ObVersion(2, 0), store->get_version());
  ASSERT_EQ(false, merge);

  // ssstore + minor frozen memstore + major frozen memstore + active memstore
  mem_store->major_freeze(ObVersion(2, 1));
  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores_count());
  ASSERT_EQ(OB_SUCCESS, stores_handle.get_last_store(store));
  ASSERT_TRUE(NULL != store);
  ASSERT_EQ(ObVersion(2, 1), store->get_version());
  ASSERT_EQ(true, merge);
}

TEST(ObMultiVersionSortedStores, get_minor_store_merged)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;
  ObIStore *store = NULL;
  bool merge;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));

  // ssstore + minor merged sstore(major frozen memstore) + minor frozen memstore * 2 + active memstore
  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  mem_store->minor_freeze(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 1));
  mem_store->minor_freeze(ObVersion(3, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 2));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores_count());
  ASSERT_EQ(OB_SUCCESS, stores_handle.get_last_store(store));
  ASSERT_TRUE(NULL != store);
  ASSERT_EQ(ObVersion(3, 1), store->get_version());
  ASSERT_EQ(false, merge);
}

TEST(ObMultiVersionSortedStores, get_minor_store_not_across_memstore)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;
  bool merge;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));

  // ssstore + minor merged sstore + major frozen memstore + minor frozen memstore + active memstore
  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 1));
  mem_store->major_freeze(ObVersion(3, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(4, 0));
  mem_store->minor_freeze(ObVersion(4, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(4, 1));
  mem_store->minor_freeze(ObVersion(4, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.get_minor_merge_stores(stores_handle, merge);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, stores_handle.get_stores_count());
  ASSERT_EQ(false, merge);
}

TEST(ObMultiVersionSortedStores, prewarm_major_merge)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObSortedStores *new_stores = NULL;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));
  ASSERT_TRUE(NULL != (new_stores = cp_fty.get_sorted_stores()));
  ASSERT_EQ(OB_SUCCESS, new_stores->init(&cp_fty));

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 0));
  ret = new_stores->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1, stores.tail_ - stores.head_);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores_count());

  ret = stores.update(new_stores, MAJOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores.tail_ - stores.head_);

  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores_count());
}

TEST(ObMultiVersionSortedStores, prewarm_minor_merge)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObSortedStores *new_stores = NULL;
  ObSortedStores *new_stores2 = NULL;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));
  ASSERT_TRUE(NULL != (new_stores = cp_fty.get_sorted_stores()));
  ASSERT_EQ(OB_SUCCESS, new_stores->init(&cp_fty));
  ASSERT_TRUE(NULL != (new_stores2 = cp_fty.get_sorted_stores()));
  ASSERT_EQ(OB_SUCCESS, new_stores2->init(&cp_fty));

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 0));
  ret = new_stores->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1, stores.tail_ - stores.head_);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores_count());

  ret = stores.update(new_stores, MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores.tail_ - stores.head_);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores_count());

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 1));
  ret = new_stores2->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 2));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.update(new_stores2, MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores.tail_ - stores.head_);

  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, stores_handle.get_stores_count());
}

TEST(ObMultiVersionSortedStores, prewarm_major_minor_merge)
{
  int ret = OB_SUCCESS;
  ObMultiVersionSortedStores stores;
  ObStoresHandle stores_handle;
  ObSortedStores *new_stores = NULL;
  ObSortedStores *new_stores2 = NULL;
  ObPartitionComponentFactory cp_fty;
  ObSSStore *ss_store = NULL;
  ObIMemtable *mem_store = NULL;

  ASSERT_EQ(OB_SUCCESS, stores.init(&cp_fty));
  ASSERT_TRUE(NULL != (new_stores = cp_fty.get_sorted_stores()));
  ASSERT_EQ(OB_SUCCESS, new_stores->init(&cp_fty));
  ASSERT_TRUE(NULL != (new_stores2 = cp_fty.get_sorted_stores()));
  ASSERT_EQ(OB_SUCCESS, new_stores2->init(&cp_fty));

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(1, 0));
  ret = stores.add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(2, 0));
  ret = new_stores->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(2, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(1, stores.tail_ - stores.head_);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, stores_handle.get_stores_count());

  ret = stores.update(new_stores, MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores.tail_ - stores.head_);
  ret = stores.get_all_version_ssstores(stores_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, stores_handle.get_stores_count());

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 0));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ss_store = cp_fty.get_ssstore(OB_SERVER_TENANT_ID);
  ASSERT_TRUE(NULL != ss_store);
  ss_store->set_version(ObVersion(3, 0));
  ret = new_stores2->add_store(ss_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  mem_store = cp_fty.get_memtable(500);
  ASSERT_TRUE(NULL != mem_store);
  mem_store->set_version(ObVersion(3, 1));
  ret = stores.add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_stores2->add_store(mem_store);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = stores.update(new_stores2, MINOR_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, stores.tail_ - stores.head_);
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
