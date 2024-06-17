Name: oceanbase-ce
Version: %{VERSION}
Release: %{RELEASE}
Summary: oceanbase-ce
Group: Applications/Databases
License: Mulan PubL v2.
Url: https://open.oceanbase.com/
Vendor: OceanBase Inc.
Prefix: /home/admin/oceanbase
Requires: curl, jq, oceanbase-ce-libs = %{version}
BuildRequires: wget, rpm, cpio, make, glibc-devel, glibc-headers, binutils, m4, python3, python2, libtool, libaio
Autoreq: 0
%systemd_requires
%global daemon_name oceanbase
%define __os_install_post %{nil}
%define _prefix /home/admin/oceanbase
%description
OceanBase is a distributed relational database
%global __requires_exclude ^(/bin/bash|/usr/bin/.*)$
%define _unpackaged_files_terminate_build 0
%global _missing_build_ids_terminate_build 0
%global arch %{_arch}
%global lto_jobs %{?lto_jobs}
%global src_dir %{?src_dir}
%global _find_debuginfo_opts -g
%define __strip %{?src_dir}/deps/3rd/usr/local/oceanbase/devtools/bin/llvm-strip
%undefine __brp_mangle_shebangs
%define buildsubdir %{name}-%{version}-%{release}.%{arch}
%define _debugsource_template %{nil}
%global debug_package %{nil}

%package debuginfo
Summary: Debug information for OceanBase
Group: Development/Debug
%description debuginfo
This package provides debug information for package oceanbase-ce.
Debug information is useful when developing applications that use this
package or when debugging this package.

%package libs
Summary: Libraries for OceanBase
Group: System Environment/Libraries
%description libs
This package contains libraries for OceanBase.

%package libs-debuginfo
Summary: Debug information for OceanBase Libs
Group: Development/Debug
%description libs-debuginfo
This package provides debug information for package oceanbase-ce-libs.
Debug information is useful when developing applications that use this
package or when debugging this package.

%package sql-parser
Summary: Sql Parser Libraries for OceanBase
Group: System Environment/Libraries
%description sql-parser
This package contains sql pareser libraries for OceanBase.

%package table
Summary: Table Api Libraries for OceanBase
Group: System Environment/Libraries
%description table
This package contains table api libraries for OceanBase.

%package table-debuginfo
Summary: Debug information for OceanBase Table
Group: Development/Debug
%description table-debuginfo
This package provides debug information for package oceanbase-ce-table.
Debug information is useful when developing applications that use this
package or when debugging this package.

%package utils
Summary: Tools for OceanBase
Group: Development/Tools
%description utils
This package contains tools for OceanBase.

%package utils-debuginfo
Summary: Debug information for OceanBase Utils
Group: Development/Debug
%description utils-debuginfo
This package provides debug information for package oceanbase-ce-utils.
Debug information is useful when developing applications that use this
package or when debugging this package.

%build
rm -rf $RPM_BUILD_ROOT/%{_prefix}
mkdir -p $RPM_BUILD_ROOT/%{_prefix}

RPM_DIR=$OLDPWD;
SRC_DIR=$OLDPWD/../../..;
BUILD_DIR=$OLDPWD/rpmbuild

mkdir -p $BUILD_DIR
cd $BUILD_DIR
rm -rf $BUILD_DIR/*

if [ ! -e "/usr/bin/python" ]; then
    ln -s /usr/bin/python2 /usr/bin/python
fi
pip3 install PyYAML

cd $SRC_DIR
chmod u+x build.sh
bash build.sh clean
python3 tools/upgrade/gen_obcdc_compatiable_info.py
bash build.sh release -DOB_BUILD_PACKAGE=ON -DENABLE_FATAL_ERROR_HANG=OFF -DENABLE_AUTO_FDO=ON -DENABLE_THIN_LTO=ON \
   -DOB_STATIC_LINK_LGPL_DEPS=OFF -DCMAKE_INSTALL_PREFIX=%{_prefix} -DLTO_JOBS=%{lto_jobs} --init
cd build_release
make observer ob_sql_proxy_parser_static obtable obtable_static ob_admin ob_error %{_smp_mflags};
make install DESTDIR=$RPM_BUILD_ROOT

mv %{_buildrootdir}/%{buildsubdir}/%{_prefix}/bin/obshell %{_builddir}/obshell
%{_rpmconfigdir}/find-debuginfo.sh %{?_find_debuginfo_opts} %{_builddir}/%{?buildsubdir}; %{nil}
mv %{_builddir}/obshell %{_buildrootdir}/%{buildsubdir}/%{_prefix}/bin/obshell

echo "/usr/lib/debug/%{_prefix}/bin/observer.debug" > %{buildroot}/debuginfo_filelist

echo "/usr/lib/debug/%{_prefix}/lib/libaio.so.debug" > %{buildroot}/libs_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libaio.so.1.debug" >> %{buildroot}/libs_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libaio.so.1.0.1.debug" >> %{buildroot}/libs_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libmariadb.so.debug" >> %{buildroot}/libs_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libmariadb.so.3.debug" >> %{buildroot}/libs_debuginfo_filelist

echo "/usr/lib/debug/%{_prefix}/lib/libobtable.so.debug" > %{buildroot}/table_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libobtable.so.1.debug" >> %{buildroot}/table_debuginfo_filelist
echo "/usr/lib/debug/%{_prefix}/lib/libobtable.so.1.0.0.debug" >> %{buildroot}/table_debuginfo_filelist

echo "/usr/lib/debug/usr/bin/ob_admin.debug" > %{buildroot}/utils_debuginfo_filelist
echo "/usr/lib/debug/usr/bin/ob_error.debug" >> %{buildroot}/utils_debuginfo_filelist

# package infomation
%files
%defattr(-,root,root,0777)
%{_prefix}/admin
%{_prefix}/bin/import_srs_data.py
%{_prefix}/bin/import_time_zone_info.py
%{_prefix}/bin/observer
%{_prefix}/bin/obshell
%{_prefix}/etc/default_parameter.json
%{_prefix}/etc/default_system_variable.json
%{_prefix}/etc/default_srs_data_mysql.sql
%{_prefix}/etc/fill_help_tables-ob.sql
%{_prefix}/etc/oceanbase_upgrade_dep.yml
%{_prefix}/etc/timezone_V1.log
%{_prefix}/etc/upgrade_checker.py
%{_prefix}/etc/upgrade_health_checker.py
%{_prefix}/etc/upgrade_post.py
%{_prefix}/etc/upgrade_pre.py
%{_prefix}/profile

%files debuginfo -f %{buildroot}/debuginfo_filelist
%defattr(-,root,root,0777)

%files libs
%defattr(-,root,root,0777)
%{_prefix}/lib/libaio.so
%{_prefix}/lib/libaio.so.1
%{_prefix}/lib/libaio.so.1.0.1
%{_prefix}/lib/libmariadb.so
%{_prefix}/lib/libmariadb.so.3

%files libs-debuginfo -f %{buildroot}/libs_debuginfo_filelist
%defattr(-,root,root,0777)

%files sql-parser
%defattr(-,root,root,0777)
%{_prefix}/include/ob_item_type.h
%{_prefix}/include/ob_sql_mode.h
%{_prefix}/include/ob_sql_parser.h
%{_prefix}/include/parse_malloc.h
%{_prefix}/include/parse_node.h
%{_prefix}/include/parser_proxy_func.h
%{_prefix}/lib/libob_sql_proxy_parser_static.a

%files table
%defattr(-,root,root,0777)
%{_prefix}/examples
%{_prefix}/include/Time.h
%{_prefix}/include/abit_set.h
%{_prefix}/include/achunk_mgr.h
%{_prefix}/include/alloc_assist.h
%{_prefix}/include/alloc_func.h
%{_prefix}/include/alloc_struct.h
%{_prefix}/include/block_set.h
%{_prefix}/include/cond.h
%{_prefix}/include/config.h
%{_prefix}/include/data_buffer.h
%{_prefix}/include/libobtable.h
%{_prefix}/include/mprotect.h
%{_prefix}/include/murmur_hash.h
%{_prefix}/include/mutex.h
%{_prefix}/include/mysql_errno.h
%{_prefix}/include/ob_accuracy.h
%{_prefix}/include/ob_action_flag.h
%{_prefix}/include/ob_addr.h
%{_prefix}/include/ob_allocator.h
%{_prefix}/include/ob_array.h
%{_prefix}/include/ob_array_helper.h
%{_prefix}/include/ob_array_index_hash_set.h
%{_prefix}/include/ob_array_iterator.h
%{_prefix}/include/ob_array_serialization.h
%{_prefix}/include/ob_array_wrap.h
%{_prefix}/include/ob_atomic.h
%{_prefix}/include/ob_atomic_event.h
%{_prefix}/include/ob_atomic_reference.h
%{_prefix}/include/ob_bit_set.h
%{_prefix}/include/ob_bucket_lock.h
%{_prefix}/include/ob_cache_washer.h
%{_prefix}/include/ob_cached_allocator.h
%{_prefix}/include/ob_charset.h
%{_prefix}/include/ob_common_config.h
%{_prefix}/include/ob_common_utility.h
%{_prefix}/include/ob_concurrent_fifo_allocator.h
%{_prefix}/include/ob_config.h
%{_prefix}/include/ob_config_helper.h
%{_prefix}/include/ob_core_local_storage.h
%{_prefix}/include/ob_counter.h
%{_prefix}/include/ob_crc64.h
%{_prefix}/include/ob_ctype.h
%{_prefix}/include/ob_date_unit_type.h
%{_prefix}/include/ob_dedup_queue.h
%{_prefix}/include/ob_define.h
%{_prefix}/include/ob_dlink_node.h
%{_prefix}/include/ob_dlist.h
%{_prefix}/include/ob_drw_lock.h
%{_prefix}/include/ob_errno.h
%{_prefix}/include/ob_fifo_allocator.h
%{_prefix}/include/ob_fixed_array.h
%{_prefix}/include/ob_fixed_length_string.h
%{_prefix}/include/ob_fixed_queue.h
%{_prefix}/include/ob_hang_fatal_error.h
%{_prefix}/include/ob_hash_func.h
%{_prefix}/include/ob_hashmap.h
%{_prefix}/include/ob_hashset.h
%{_prefix}/include/ob_hashtable.h
%{_prefix}/include/ob_hashutils.h
%{_prefix}/include/ob_hkv_table.h
%{_prefix}/include/ob_iarray.h
%{_prefix}/include/ob_iteratable_hashmap.h
%{_prefix}/include/ob_latch.h
%{_prefix}/include/ob_latch_define.h
%{_prefix}/include/ob_lf_fifo_allocator.h
%{_prefix}/include/ob_linear_hash_map.h
%{_prefix}/include/ob_link.h
%{_prefix}/include/ob_list.h
%{_prefix}/include/ob_lock.h
%{_prefix}/include/ob_lock_guard.h
%{_prefix}/include/ob_log.h
%{_prefix}/include/ob_log_module.h
%{_prefix}/include/ob_log_print_kv.h
%{_prefix}/include/ob_macro_utils.h
%{_prefix}/include/ob_malloc.h
%{_prefix}/include/ob_malloc_allocator.h
%{_prefix}/include/ob_mod_define.h
%{_prefix}/include/ob_monitor.h
%{_prefix}/include/ob_mutex.h
%{_prefix}/include/ob_mysql_global.h
%{_prefix}/include/ob_name_def.h
%{_prefix}/include/ob_name_id_def.h
%{_prefix}/include/ob_net_util.h
%{_prefix}/include/ob_number_v2.h
%{_prefix}/include/ob_obj_cast.h
%{_prefix}/include/ob_obj_type.h
%{_prefix}/include/ob_object.h
%{_prefix}/include/ob_partition_location.h
%{_prefix}/include/ob_pcounter.h
%{_prefix}/include/ob_placement_hashutils.h
%{_prefix}/include/ob_pointer_hashmap.h
%{_prefix}/include/ob_pooled_allocator.h
%{_prefix}/include/ob_print_kv.h
%{_prefix}/include/ob_print_utils.h
%{_prefix}/include/ob_pstore.h
%{_prefix}/include/ob_random.h
%{_prefix}/include/ob_range.h
%{_prefix}/include/ob_rate_limiter.h
%{_prefix}/include/ob_region.h
%{_prefix}/include/ob_resource_mgr.h
%{_prefix}/include/ob_retire_station.h
%{_prefix}/include/ob_role.h
%{_prefix}/include/ob_rowkey.h
%{_prefix}/include/ob_rowkey_info.h
%{_prefix}/include/ob_rwlock.h
%{_prefix}/include/ob_se_array.h
%{_prefix}/include/ob_seq_event_recorder.h
%{_prefix}/include/ob_serialization.h
%{_prefix}/include/ob_serialization_helper.h
%{_prefix}/include/ob_small_allocator.h
%{_prefix}/include/ob_small_spin_lock.h
%{_prefix}/include/ob_spin_lock.h
%{_prefix}/include/ob_spin_rwlock.h
%{_prefix}/include/ob_string.h
%{_prefix}/include/ob_string_buf.h
%{_prefix}/include/ob_string_buf.ipp
%{_prefix}/include/ob_string_util.h
%{_prefix}/include/ob_strings.h
%{_prefix}/include/ob_table.h
%{_prefix}/include/ob_table_define.h
%{_prefix}/include/ob_table_rpc_proxy.h
%{_prefix}/include/ob_table_rpc_struct.h
%{_prefix}/include/ob_table_service_client.h
%{_prefix}/include/ob_table_service_config.h
%{_prefix}/include/ob_tc_malloc.h
%{_prefix}/include/ob_template_utils.h
%{_prefix}/include/ob_tenant_ctx_allocator.h
%{_prefix}/include/ob_thread_cond.h
%{_prefix}/include/ob_time_convert.h
%{_prefix}/include/ob_time_utility.h
%{_prefix}/include/ob_timeout_ctx.h
%{_prefix}/include/ob_timezone_info.h
%{_prefix}/include/ob_trace_event.h
%{_prefix}/include/ob_trace_log.h
%{_prefix}/include/ob_tsi_factory.h
%{_prefix}/include/ob_tsi_utils.h
%{_prefix}/include/ob_unify_serialize.h
%{_prefix}/include/ob_vector.h
%{_prefix}/include/ob_vector.ipp
%{_prefix}/include/ob_wait_class.h
%{_prefix}/include/ob_wait_event.h
%{_prefix}/include/ob_yson.h
%{_prefix}/include/ob_yson_encode.h
%{_prefix}/include/ob_zerofill_info.h
%{_prefix}/include/ob_zone.h
%{_prefix}/include/object_mgr.h
%{_prefix}/include/object_set.h
%{_prefix}/include/page_arena.h
%{_prefix}/include/serialization.h
%{_prefix}/include/utility.h
%{_prefix}/lib/libobtable.so
%{_prefix}/lib/libobtable.so.1
%{_prefix}/lib/libobtable.so.1.0.0
%{_prefix}/lib/libobtable_static.a

%files table-debuginfo -f %{buildroot}/table_debuginfo_filelist
%defattr(-,root,root,0777)

%files utils
%defattr(-,root,root,0777)
/usr/bin/ob_admin
/usr/bin/ob_error

%files utils-debuginfo -f %{buildroot}/utils_debuginfo_filelist
%defattr(-,root,root,0777)

%post
echo "execute post install script"
cp -f %{_prefix}/profile/oceanbase.service /etc/systemd/system/oceanbase.service
chmod 644 /etc/systemd/system/oceanbase.service
chmod +x %{_prefix}/profile/oceanbase-service.sh
cp -f %{_prefix}/profile/oceanbase.cnf /etc/oceanbase.cnf
systemctl daemon-reload

# telemetry
/bin/bash %{_prefix}/profile/telemetry.sh $1 >/dev/null 2>&1

%preun
echo "execute pre uninstall script"
systemctl stop %{daemon_name}
systemctl disable %{daemon_name}
/bin/bash %{_prefix}/profile/oceanbase-service.sh destroy
rm -f /etc/systemd/system/oceanbase.service /etc/oceanbase.cnf
systemctl daemon-reload

# telemetry
/bin/bash %{_prefix}/profile/telemetry.sh $1 >/dev/null 2>&1

%postun
echo "execute post uninstall script"
rm -rf %{_prefix}/.meta %{_prefix}/log_obshell

%changelog
* Mon Mar 18 2024 oceanbase 4.3.0
- new features: support for packaging with spec files
- new features: support for packaging on anolis
