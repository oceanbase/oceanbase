# Define object library regex
oblib_add_library(regex
  regex/regcomp.cpp
  regex/regexec.cpp
  regex/regerror.cpp
  regex/regfree.cpp
  regex/regalone.cpp
  ob_regex.cpp
  ob_regex.h)
target_link_libraries(regex oblib_base)
