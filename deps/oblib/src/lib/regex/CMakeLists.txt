# Define object library regex
ob_lib_add_target(oblib_lib_regex
  regex/regcomp.cpp
  regex/regexec.cpp
  regex/regerror.cpp
  regex/regfree.cpp
  regex/regalone.cpp
  ob_regex.cpp)
target_compile_definitions(oblib_lib_regex PRIVATE -DREGEX_STANDALONE -DREGEX_WCHAR)
disable_pch(oblib_lib_regex)
