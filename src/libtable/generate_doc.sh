#!/bin/bash
case x$1 in
    xheader)
	/usr/local/bin/doxygen -w html new_header.html new_footer.html new_stylesheet.css Doxyfile
	;;
    xinstall)
	rm -rf ~/public_html/libobtable ; mv -n html/ ~/public_html/libobtable
	;;
    *)
	/usr/local/bin/doxygen Doxyfile
	;;
esac
		 
