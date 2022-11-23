#!/usr/bin/env python2.6

import sys
import os.path

def txtParse(src,dest):
    file = open(src)
    out = open(dest,"w")
    out.write("<body> <font size=2>")
    while 1:
        line = file.readline()
        if not line:
            break
       #print(line)
       #line.replace("[32m[==========][0m","==========")
        newline1=line.replace("[32m[==========][0m","<font color=#006400>[==========]</font>");
        newline2=newline1.replace("[32m[----------][0m","<font color=#006400>[----------]</font>");
        newline3=newline2.replace("[32m[ RUN      ][0m","<font color=#006400>[ RUN      ]</font>");
        newline4=newline3.replace("[32m[       OK ][0m","<font color=#006400>[       OK ]</font>");
        newline5=newline4.replace("[32m[  PASSED  ][0m","<font color=#006400>[   PASSED ]</font>");
        newline6=newline5.replace("[31m[  FAILED  ][0m","<font color=#FF0000>[   FAILED ]</font>");
        newline7=newline6.replace("[32m[ PASS LST ][0m","<font color=#006400>[  PASS LST ]</font>");
        newline8=newline7.replace("[31m[ FAIL LST ][0m","<font color=#FF0000>[  FAIL LST ]</font>");

#        if newline8.startswith("bianque"):
#         #   out.write("<embed src=\"bianque_output/"+newline8+".svg\" type=\"image/svg+xml\" style=\"width: 100%\"><br>")
#         #   out.write("<embed src=\"bianque_output/"+newline8+"_flame.svg\" type=\"image/svg+xml\" style=\"width: 80%\"><br>")
#        else:
        if newline8.startswith("<font") == False:
            out.write("<font color=#0000FF>")
            out.write(newline8 + "<br>")
        if newline8.startswith("<font") == False:
            out.write("</font>")
    out.write("</font></body>")

if __name__=="__main__":
    txtParse(sys.argv[1], os.path.dirname(sys.argv[1]) + "/" + os.path.basename(sys.argv[1]) + ".html")
