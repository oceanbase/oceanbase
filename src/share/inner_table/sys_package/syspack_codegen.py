#/usr/bin/env python
# -*- coding: utf-8 -*-

"""
syspack_codegen.py
~~~~~~~~~~~~~~~~~~
这个脚本是系统包的配置中心, 作用如下:
1. 调用wrap程序将指定的系统包源码文件从明文转换为密文, 密文文件的后缀名为`.plw`
2. 将指定的系统包源码文件的内容以字符串的形式嵌入到`syspack_source.cpp`中, 该文件后续被编译链接到observer中
3. 将指定的系统包拷贝到编译目录的syspack_release文件夹下, 用于后续RPM打包和部署
NOTE:
1. 这个脚本会在编译期(make阶段)自动调用, 并完成上述三个步骤, 一般不需要手动调用. 也可以使用`python2 syspack_codegen.py [option]`手动调用.
2. RD需要修改的只有`syspack_config`变量, 该变量是一个列表, 每个元素是一个`SysPackConfig`对象, 该对象包含了一个系统包的所有信息.
    目前`SysPackConfig`对象的构造参数格式为:
        `SysPackConfig(group, name, header_file, body_file, wrap_type, orc_build_req)`
    其中:
        - group: 系统包的分组, 可选值为`SysPackGroup`枚举类型
        - name: 系统包的名称, 用于在`syspack_source.cpp`中标识系统包
        - header_file: 系统包的头文件, 必须是`.sql`文件
        - body_file: 系统包的实现文件, 可选(可以用None填充), 必须是`.sql`文件
        - wrap_type: 系统包的混淆方式, 可选值为`WrapType`枚举类型, 控制系统包的header和body是否需要进行加密混淆, 默认为`WrapType.NONE`, 即不混淆
        - orc_build_req: 系统包是否需要在`OB_BUILD_ORACLE_PL`环境下编译, 默认为False
"""

import os
import sys
import subprocess
import argparse
import shutil
import codecs

g_script_dir = os.path.dirname(os.path.abspath(__file__))
g_verbose = False
g_orc_build = False

class SysPackGroup:
    ORACLE         = "oracle"
    MYSQL          = "mysql"
    ORACLE_SPECIAL = "oracle_special"
    MYSQL_SPECIAL  = "mysql_special"

class WrapType:
    NONE        = 1
    HEADER_ONLY = 2
    BODY_ONLY   = 3
    BOTH        = 4

class SysPackConfig:
    def __init__(self, group, name, header_file, body_file, **kwargs):
        assert group in [SysPackGroup.ORACLE, SysPackGroup.MYSQL, SysPackGroup.ORACLE_SPECIAL, SysPackGroup.MYSQL_SPECIAL]
        self.group = group
        assert isinstance(name, str)
        self.name = name
        assert isinstance(header_file, str) and header_file.endswith(".sql")
        self.header_file = header_file
        self.body_file = None
        if body_file:
            assert isinstance(body_file, str) and body_file.endswith(".sql")
            self.body_file = body_file

        self.wrap = WrapType.NONE
        self.orc_build_req = False
        for key, value in kwargs.items():
            if key == "wrap":
                assert value in [WrapType.NONE, WrapType.HEADER_ONLY, WrapType.BODY_ONLY, WrapType.BOTH]
                self.wrap = value
            elif key == "orc_build_req":
                assert isinstance(value, bool)
                self.orc_build_req = value
            else:
                assert False, "Unknown key: {}".format(key)

        if body_file is None:
            assert self.wrap != WrapType.BODY_ONLY, "WrapType.BODY_ONLY is not allowed when body_file is None"
            assert self.wrap != WrapType.BOTH, "WrapType.BOTH is not allowed when body_file is None"

        self.release_header = self.header_file
        self.release_body = self.body_file
        if self.wrap == WrapType.HEADER_ONLY or self.wrap == WrapType.BOTH:
            self.release_header = self.header_file[:-3] + "plw"
        if self.body_file and (self.wrap == WrapType.BODY_ONLY or self.wrap == WrapType.BOTH):
            self.release_body = self.body_file[:-3] + "plw"

    def export_release_file_list(self):
        return [self.release_header] + ([self.release_body] if self.release_body else [])

    def export_source_file_config(self):
        return "{{\"{}\", \"{}\", {}}},".format(
            self.name,
            self.release_header,
            "\"{}\"".format(self.release_body) if self.release_body else "nullptr")

    def export_files_to_wrap(self):
        if self.wrap == WrapType.NONE:
            return []
        elif self.wrap == WrapType.HEADER_ONLY:
            return [self.header_file]
        elif self.wrap == WrapType.BODY_ONLY:
            return [self.body_file]
        elif self.wrap == WrapType.BOTH:
            return [self.header_file, self.body_file]

def install_syspack_files(syspack_config, release_dir):
    file_to_install = []
    for config in syspack_config:
        if not config.orc_build_req or g_orc_build:
            file_to_install += config.export_release_file_list()
    if g_verbose:
        print("installing syspack files to {}".format(release_dir))
        print("{} files to install: {}".format(len(file_to_install), file_to_install))
    for file in file_to_install:
        src_file = os.path.join(g_script_dir, file)
        dst_file = os.path.join(release_dir, file)
        shutil.copyfile(src_file, dst_file)

def wrap_syspack(syspack_config, wrap_bin_path):
    wrapped_files = []
    for config in syspack_config:
        wrapped_files += config.export_files_to_wrap()
    for file in wrapped_files:
        if g_verbose:
            print("wrap {} -o {}".format(file, file[:-3] + "plw"))
        subprocess.check_call([wrap_bin_path, file, "-o", file[:-3] + "plw"], cwd=g_script_dir)

def embed_syspack(syspack_config):
    def gen_syspack_file_list(syspack_config, group):
        orc_req_lst = []
        lst = []
        for config in syspack_config:
            if config.group == group:
                if config.orc_build_req:
                    orc_req_lst.append(config.export_source_file_config())
                else:
                    lst.append(config.export_source_file_config())
        return "#ifdef OB_BUILD_ORACLE_PL\n" \
                + "int {}_syspack_file_list_length = {};\n".format(group, len(lst) + len(orc_req_lst)) \
                + "#else\n" \
                + "int {}_syspack_file_list_length = {};\n".format(group, len(lst)) \
                + "#endif\n" \
                + "ObSysPackageFile {}_syspack_file_list[] = {{\n".format(group) \
                + "\n".join(lst) + "\n"\
                + "#ifdef OB_BUILD_ORACLE_PL\n" \
                + "\n".join(orc_req_lst) + "\n" \
                + "#endif\n" \
                + "};\n\n"

    syspack_source_file = os.path.join(g_script_dir, "syspack_source.cpp")
    with codecs.open(syspack_source_file, "w", encoding="utf8") as f:
        # 1. write header
        f.write("// This file is generated by `syspack_codegen.py`, do not edit it!\n\n"
                "#include <cstdint>\n"
                "#include <utility>\n\n"
                "namespace oceanbase {\n"
                "namespace pl {\n\n"
                "struct ObSysPackageFile {\n"
                "  const char *const package_name;\n"
                "  const char *const package_spec_file_name;\n"
                "  const char *const package_body_file_name;\n"
                "};\n\n")
        # 2. write `xxx_syspack_file_list`
        f.write(gen_syspack_file_list(syspack_config, SysPackGroup.ORACLE))
        f.write(gen_syspack_file_list(syspack_config, SysPackGroup.MYSQL))
        f.write(gen_syspack_file_list(syspack_config, SysPackGroup.ORACLE_SPECIAL))
        f.write(gen_syspack_file_list(syspack_config, SysPackGroup.MYSQL_SPECIAL))
        # 3. write `syspack_source_count`
        release_files = []
        for config in syspack_config:
            release_files += config.export_release_file_list()
        f.write("int64_t syspack_source_count = {};\n".format(len(release_files)))
        # 4. write `syspack_source_contents`
        f.write("std::pair<const char * const, const char* const> syspack_source_contents[] = {\n")
        for file in release_files:
            # 4.1 write file name
            f.write("{{\n"
                    "\"{}\",\n"
                    "R\"sys_pack_del(".format(file))
            # 4.2 write file content
            file_path = os.path.join(g_script_dir, file)
            if os.path.exists(file_path):
                try:
                    with codecs.open(file_path, "r", encoding="utf8") as file_content:
                        f.write(file_content.read())
                except Exception as e:
                    sys.stderr.write("[wrap] error! failed to embed file: {}\n".format(file_path))
                    sys.stderr.write("[wrap] error! " + str(e) + "\n")
                    sys.exit(1)
            else:
                pass # file not found
            # 4.3 write file end
            f.write(")sys_pack_del\"\n"
                    "},\n")
        f.write("};\n\n")
        # 5. write tail
        f.write("} // namespace pl\n"
                "} // namespace oceanbase\n")

syspack_config = [
    # Oracle
    SysPackConfig(SysPackGroup.ORACLE, "dbms_standard", "dbms_standard.sql", None, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_output", "dbms_output.sql", "dbms_output_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_metadata", "dbms_metadata.sql", "dbms_metadata_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_spm", "dbms_spm.sql", "dbms_spm_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_raw", "utl_raw.sql", "utl_raw_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_lob", "dbms_lob.sql", "dbms_lob_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_components", "sa_components.sql", "sa_components_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_label_admin", "sa_label_admin.sql", "sa_label_admin_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_policy_admin", "sa_policy_admin.sql", "sa_policy_admin_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_session", "sa_session.sql", "sa_session_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_sysdba", "sa_sysdba.sql", "sa_sysdba_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sa_user_admin", "sa_user_admin.sql", "sa_user_admin_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_i18n", "utl_i18n.sql", "utl_i18n_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_crypto", "dbms_crypto.sql", "dbms_crypto_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_random", "dbms_random.sql", "dbms_random_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_debug", "dbms_debug.sql", "dbms_debug_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_inaddr", "utl_inaddr.sql", "utl_inaddr_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_encode", "dbms_utl_encode.sql", "dbms_utl_encode_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_warning", "dbms_warning.sql", "dbms_warning_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_errlog", "dbms_errlog.sql", "dbms_errlog_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_lock", "dbms_lock.sql", "dbms_lock_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_sql", "dbms_sql.sql", "dbms_sql_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_xa", "dbms_xa.sql", "dbms_xa_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_resource_manager", "dbms_resource_manager.sql", "dbms_resource_manager_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_utility", "dbms_utility.sql", "dbms_utility_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "odciconst", "odciconst.sql", None, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_stats", "dbms_stats.sql", "dbms_stats_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_any", "dbms_any.sql", "dbms_any_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "xml_type", "xml_type.sql", "xml_type_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_ijob", "dbms_ijob.sql", "dbms_ijob_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_job", "dbms_job.sql", "dbms_job_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_ischeduler", "dbms_ischeduler.sql", "dbms_ischeduler_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_scheduler", "dbms_scheduler.sql", "dbms_scheduler_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "catodci", "catodci.sql", None, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_describe", "dbms_describe.sql", "dbms_describe_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_file", "utl_file.sql", "utl_file_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_plan_cache", "dbms_plancache.sql", "dbms_plancache_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_sys_error", "dbms_sys_error.sql", "dbms_sys_error_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_preprocessor", "dbms_preprocessor.sql", "dbms_preprocessor_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_audit_mgmt", "dbms_audit_mgmt.sql", "dbms_audit_mgmt_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_application", "dbms_application.sql", "dbms_application_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_session", "dbms_session.sql", "dbms_session_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_monitor", "dbms_monitor.sql", "dbms_monitor_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_xplan", "dbms_xplan.sql", "dbms_xplan_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_workload_repository", "dbms_workload_repository.sql", "dbms_workload_repository_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_ash_internal", "dbms_ash_internal.sql", "dbms_ash_internal_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_rls", "dbms_rls.sql", "dbms_rls_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_udr", "dbms_udr.sql", "dbms_udr_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "json_element_t", "json_element_type.sql", "json_element_type_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "json_object_t", "json_object_type.sql", "json_object_type_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_mview", "dbms_mview.sql", "dbms_mview_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_mview_stats", "dbms_mview_stats.sql", "dbms_mview_stats_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_space", "dbms_space.sql", "dbms_space_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "json_array_t", "json_array_type.sql", "json_array_type_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "xmlsequence", "xml_sequence_type.sql", None, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_recomp", "utl_recomp.sql", "utl_recomp_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sdo_geometry", "sdo_geometry.sql", "sdo_geometry_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "sdo_geom", "sdo_geom.sql", "sdo_geom_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_external_table", "dbms_external_table.sql", "dbms_external_table_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_profiler", "dbms_profiler.sql", "dbms_profiler_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "dbms_ddl", "dbms_ddl.sql", "dbms_ddl_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_tcp", "utl_tcp.sql", "utl_tcp_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    SysPackConfig(SysPackGroup.ORACLE, "utl_smtp", "utl_smtp.sql", "utl_smtp_body.sql", wrap=WrapType.BODY_ONLY, orc_build_req=True),
    # MySQL
    SysPackConfig(SysPackGroup.MYSQL, "dbms_stats", "dbms_stats_mysql.sql", "dbms_stats_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_scheduler", "dbms_scheduler_mysql.sql", "dbms_scheduler_mysql_body.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_ischeduler", "dbms_ischeduler_mysql.sql", "dbms_ischeduler_mysql_body.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_application", "dbms_application_mysql.sql", "dbms_application_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_session", "dbms_session_mysql.sql", "dbms_session_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_monitor", "dbms_monitor_mysql.sql", "dbms_monitor_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_resource_manager", "dbms_resource_manager_mysql.sql", "dbms_resource_manager_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_xplan", "dbms_xplan_mysql.sql", "dbms_xplan_mysql_body.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_spm", "dbms_spm_mysql.sql", "dbms_spm_body_mysql.sql", orc_build_req=True),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_udr", "dbms_udr_mysql.sql", "dbms_udr_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_workload_repository", "dbms_workload_repository_mysql.sql", "dbms_workload_repository_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_mview", "dbms_mview_mysql.sql", "dbms_mview_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_mview_stats", "dbms_mview_stats_mysql.sql", "dbms_mview_stats_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_trusted_certificate_manager", "dbms_trusted_certificate_manager_mysql.sql", "dbms_trusted_certificate_manager_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_ob_limit_calculator", "dbms_ob_limit_calculator_mysql.sql", "dbms_ob_limit_calculator_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_external_table", "dbms_external_table_mysql.sql", "dbms_external_table_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "external_table_alert_log", "external_table_alert_log.sql", None),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_vector", "dbms_vector_mysql.sql", "dbms_vector_body_mysql.sql"),
    SysPackConfig(SysPackGroup.MYSQL, "dbms_space", "dbms_space_mysql.sql", "dbms_space_body_mysql.sql"),
    # Oracle Special
    SysPackConfig(SysPackGroup.ORACLE_SPECIAL, "__dbms_upgrade", "__dbms_upgrade.sql", "__dbms_upgrade_body.sql", orc_build_req=True),
    # MySQL Special
    SysPackConfig(SysPackGroup.MYSQL_SPECIAL, "__dbms_upgrade", "__dbms_upgrade_mysql.sql", "__dbms_upgrade_body_mysql.sql"),
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="System Package Config Center")
    parser.add_argument("-wp", "--wrap-bin-path", type=str, default=None,
                        help="The path to the wrap binary. If empty, it indicates that OB_BUILD_ORACLE_PL is not set.")
    parser.add_argument("-rd", "--release-dir", type=str, default=None,
                        help="The path to the system package release directory. If empty, installation is not required.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print verbose information")
    args = parser.parse_args()

    wrap_bin_path = args.wrap_bin_path
    release_dir = args.release_dir
    g_orc_build = wrap_bin_path is not None
    g_verbose = args.verbose

    # 1. wrap syspack files
    if wrap_bin_path:
        assert os.path.exists(wrap_bin_path), "{} not exists".format(wrap_bin_path)
        wrap_syspack(syspack_config, wrap_bin_path)
    # 2. embed syspack files
    embed_syspack(syspack_config)
    # 3. install syspack files
    if release_dir:
        if not os.path.exists(release_dir):
            os.makedirs(release_dir)
        install_syspack_files(syspack_config, release_dir)
