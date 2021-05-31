
# charset objects used for proxy_parser
set(ob_sql_parser_charset_object_list
  ob_ctype_bin.c
  ob_ctype_mb.c
  ob_ctype_simple.c
  ob_ctype_utf8.c
  ob_ctype.c
  ob_dtoa.c
)

list(TRANSFORM ob_sql_parser_charset_object_list
  PREPEND ${PROJECT_SOURCE_DIR}/deps/oblib/src/lib/charset/)

# hash objects used for proxy parser
set(ob_sql_parser_hash_object_list
  murmur_hash.cpp
)

list(TRANSFORM ob_sql_parser_hash_object_list
  PREPEND ${PROJECT_SOURCE_DIR}/deps/oblib/src/lib/hash_func/)

add_library(ob_sql_parser_base INTERFACE)
target_include_directories(ob_sql_parser_base INTERFACE
  ${PROJECT_SOURCE_DIR}/deps/oblib/src/common/sql_mode/)

# parser objects for proxy parser
set(ob_inner_sql_parser_object_list
  non_reserved_keywords_mysql_mode.c
  ob_non_reserved_keywords.c
  ob_sql_parser.cpp
  parse_malloc.cpp
  parse_node.c
  parse_node_hash.cpp
  sql_parser_base.c
  sql_parser_mysql_mode_lex.c
  sql_parser_mysql_mode_tab.c
  parser_utility.cpp
  type_name.c
)

# parser objects for server parser
set(ob_extra_sql_parser_object_list
  ob_parser.cpp
  parser_proxy_func.cpp
)

ob_set_subtarget(inner_sql_parser
  ${ob_inner_sql_parser_object_list}
)

ob_set_subtarget(sql_parser_charset
  ${ob_sql_parser_charset_object_list}
)

ob_set_subtarget(sql_parser_hash
  ${ob_sql_parser_hash_object_list}
)

ob_set_subtarget(extra_sql_parser
  ${ob_extra_sql_parser_object_list}
)

# ob_sql_proxy_parser_objects is the static library for proxy, it does not link against observer
add_library(ob_sql_proxy_parser_objects OBJECT
  ${ob_inner_sql_parser_object_list}
  ${ob_sql_parser_charset_object_list}
  ${ob_sql_parser_hash_object_list}
)

# ob_sql_server_parser_object is the static library for observer
add_library(ob_sql_server_parser_objects OBJECT
  ${ob_inner_sql_parser_object_list}
  ${ob_extra_sql_parser_object_list})

target_link_libraries(ob_sql_proxy_parser_objects PUBLIC ob_base ob_sql_parser_base)
target_link_libraries(ob_sql_server_parser_objects PUBLIC ob_base)

target_compile_definitions(ob_sql_proxy_parser_objects PUBLIC SQL_PARSER_COMPILATION)

disable_pch(ob_sql_server_parser_objects)
disable_pch(ob_sql_proxy_parser_objects)

add_library(ob_sql_proxy_parser_static
  STATIC
  EXCLUDE_FROM_ALL)

add_library(ob_sql_server_parser_static
  STATIC
  EXCLUDE_FROM_ALL)

target_link_libraries(ob_sql_proxy_parser_static
  PUBLIC
  ob_sql_proxy_parser_objects)

target_link_libraries(ob_sql_server_parser_static
  PUBLIC
  ob_sql_server_parser_objects)
