import pandas as pd
import re

HTML_PART1 = '''

<html xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office" xmlns:x="urn:schemas-microsoft-com:office:excel" xmlns="http://www.w3.org/TR/REC-html40">
 <head>
  <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
  <meta name="ProgId" content="Excel.Sheet">
  <meta name="Generator" content="WPS Office ET">
  <!--[if gte mso 9]>
   <xml>
    <o:DocumentProperties>
     <o:Created>2023-03-26T13:32:25</o:Created>
     <o:LastAuthor>yuchonghuang</o:LastAuthor>
     <o:LastSaved>2023-03-26T19:36:10</o:LastSaved>
    </o:DocumentProperties>
    <o:CustomDocumentProperties>
     <o:ICV dt:dt="string">6AE16BF209A11DD8F2D81F648D76CFAA</o:ICV>
     <o:KSOProductBuildVer dt:dt="string">2052-5.1.1.7662</o:KSOProductBuildVer>
    </o:CustomDocumentProperties>
   </xml>
  <![endif]-->
  <style>
<!-- @page
	{margin:1.00in 0.75in 1.00in 0.75in;
	mso-header-margin:0.50in;
	mso-footer-margin:0.50in;}
tr
	{mso-height-source:auto;
	mso-ruby-visibility:none;}
col
	{mso-width-source:auto;
	mso-ruby-visibility:none;}
br
	{mso-data-placement:same-cell;}
.font0
	{color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font1
	{color:#000000;
	font-size:48.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font2
	{color:#000000;
	font-size:14.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font3
	{color:#FF0000;
	font-size:14.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font4
	{color:#FA7D00;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font5
	{color:#9C6500;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font6
	{color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font7
	{color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font8
	{color:#7F7F7F;
	font-size:11.0pt;
	font-weight:400;
	font-style:italic;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font9
	{color:#0000FF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:underline;
	text-underline-style:single;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font10
	{color:#44546A;
	font-size:18.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font11
	{color:#44546A;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font12
	{color:#000000;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font13
	{color:#44546A;
	font-size:13.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font14
	{color:#3F3F3F;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font15
	{color:#FFFFFF;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font16
	{color:#800080;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:underline;
	text-underline-style:single;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font17
	{color:#FF0000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font18
	{color:#006100;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font19
	{color:#9C0006;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font20
	{color:#FA7D00;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.font21
	{color:#44546A;
	font-size:15.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:134;}
.font22
	{color:#3F3F76;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:"宋体";
	mso-generic-font-family:auto;
	mso-font-charset:0;}
.style0
	{mso-number-format:"General";
	text-align:general;
	vertical-align:middle;
	white-space:nowrap;
	mso-rotate:0;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	border:none;
	mso-protection:locked visible;
	mso-style-name:"常规";
	mso-style-id:0;}
.style16
	{mso-pattern:auto none;
	background:#A9D08E;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 6";}
.style17
	{mso-pattern:auto none;
	background:#FFF2CC;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 4";}
.style18
	{mso-pattern:auto none;
	background:#FFC000;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 4";}
.style19
	{mso-pattern:auto none;
	background:#FFCC99;
	color:#3F3F76;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border:.5pt solid #7F7F7F;
	mso-style-name:"输入";}
.style20
	{mso-pattern:auto none;
	background:#DBDBDB;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 3";}
.style21
	{mso-pattern:auto none;
	background:#EDEDED;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 3";}
.style22
	{mso-number-format:"_ \\\\2\\A\\5\\\\2* \#\,\#\#0\.00_ \;_ \\\\2\\A\\5\\\\2* \\-\#\,\#\#0\.00_ \;_ \\\\2\\A\\5\\\\2* \\\\2\\-\\\\2??_ \;_ \@_ ";
	mso-style-name:"货币";
	mso-style-id:4;}
.style23
	{mso-pattern:auto none;
	background:#A5A5A5;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 3";}
.style24
	{mso-number-format:"0%";
	mso-style-name:"百分比";
	mso-style-id:5;}
.style25
	{mso-pattern:auto none;
	background:#F4B084;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 2";}
.style26
	{mso-pattern:auto none;
	background:#8EA9DB;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 5";}
.style27
	{mso-pattern:auto none;
	background:#ED7D31;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 2";}
.style28
	{mso-pattern:auto none;
	background:#9BC2E6;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 1";}
.style29
	{mso-pattern:auto none;
	background:#FFD966;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 4";}
.style30
	{mso-pattern:auto none;
	background:#F2F2F2;
	color:#FA7D00;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border:.5pt solid #7F7F7F;
	mso-style-name:"计算";}
.style31
	{mso-pattern:auto none;
	background:#5B9BD5;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 1";}
.style32
	{mso-pattern:auto none;
	background:#FFEB9C;
	color:#9C6500;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"适中";}
.style33
	{mso-pattern:auto none;
	background:#D9E1F2;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 5";}
.style34
	{mso-pattern:auto none;
	background:#C6EFCE;
	color:#006100;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"好";}
.style35
	{mso-pattern:auto none;
	background:#DDEBF7;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 1";}
.style36
	{color:#000000;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border-top:.5pt solid #5B9BD5;
	border-bottom:2.0pt double #5B9BD5;
	mso-style-name:"汇总";}
.style37
	{mso-pattern:auto none;
	background:#FFC7CE;
	color:#9C0006;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"差";}
.style38
	{mso-pattern:auto none;
	background:#A5A5A5;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border:2.0pt double #3F3F3F;
	mso-style-name:"检查单元格";}
.style39
	{mso-pattern:auto none;
	background:#F2F2F2;
	color:#3F3F3F;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border:.5pt solid #3F3F3F;
	mso-style-name:"输出";}
.style40
	{color:#44546A;
	font-size:15.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	border-bottom:1.0pt solid #5B9BD5;
	mso-style-name:"标题 1";}
.style41
	{color:#7F7F7F;
	font-size:11.0pt;
	font-weight:400;
	font-style:italic;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"解释性文本";}
.style42
	{mso-pattern:auto none;
	background:#FCE4D6;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 2";}
.style43
	{color:#44546A;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	mso-style-name:"标题 4";}
.style44
	{mso-number-format:"_ \\\\2\\A\\5\\\\2* \#\,\#\#0_ \;_ \\\\2\\A\\5\\\\2* \\-\#\,\#\#0_ \;_ \\\\2\\A\\5\\\\2* \\\\2\\-\\\\2_ \;_ \@_ ";
	mso-style-name:"货币[0]";
	mso-style-id:7;}
.style45
	{mso-pattern:auto none;
	background:#FFE699;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 4";}
.style46
	{mso-number-format:"_ * \#\,\#\#0\.00_ \;_ * \\-\#\,\#\#0\.00_ \;_ * \\\\2\\-\\\\2??_ \;_ \@_ ";
	mso-style-name:"千位分隔";
	mso-style-id:3;}
.style47
	{color:#800080;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:underline;
	text-underline-style:single;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"已访问的超链接";
	mso-style-id:9;}
.style48
	{color:#44546A;
	font-size:18.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	mso-style-name:"标题";}
.style49
	{mso-pattern:auto none;
	background:#F8CBAD;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 2";}
.style50
	{color:#FF0000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"警告文本";}
.style51
	{mso-pattern:auto none;
	background:#C9C9C9;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"60% - 强调文字颜色 3";}
.style52
	{mso-pattern:auto none;
	background:#FFFFCC;
	border:.5pt solid #B2B2B2;
	mso-style-name:"注释";}
.style53
	{mso-pattern:auto none;
	background:#E2EFDA;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"20% - 强调文字颜色 6";}
.style54
	{mso-pattern:auto none;
	background:#4472C4;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 5";}
.style55
	{mso-pattern:auto none;
	background:#C6E0B4;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 6";}
.style56
	{color:#0000FF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:underline;
	text-underline-style:single;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"超链接";
	mso-style-id:8;}
.style57
	{mso-number-format:"_ * \#\,\#\#0_ \;_ * \\-\#\,\#\#0_ \;_ * \\\\2\\-\\\\2_ \;_ \@_ ";
	mso-style-name:"千位分隔[0]";
	mso-style-id:6;}
.style58
	{color:#44546A;
	font-size:13.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	border-bottom:1.0pt solid #5B9BD5;
	mso-style-name:"标题 2";}
.style59
	{mso-pattern:auto none;
	background:#B4C6E7;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 5";}
.style60
	{color:#44546A;
	font-size:11.0pt;
	font-weight:700;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	border-bottom:1.0pt solid #ACCCEA;
	mso-style-name:"标题 3";}
.style61
	{mso-pattern:auto none;
	background:#70AD47;
	color:#FFFFFF;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"强调文字颜色 6";}
.style62
	{mso-pattern:auto none;
	background:#BDD7EE;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	mso-style-name:"40% - 强调文字颜色 1";}
.style63
	{color:#FA7D00;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:0;
	border-bottom:2.0pt double #FF8001;
	mso-style-name:"链接单元格";}
td
	{mso-style-parent:style0;
	padding-top:1px;
	padding-right:1px;
	padding-left:1px;
	mso-ignore:padding;
	mso-number-format:"General";
	text-align:general;
	vertical-align:middle;
	white-space:nowrap;
	mso-rotate:0;
	color:#000000;
	font-size:11.0pt;
	font-weight:400;
	font-style:normal;
	text-decoration:none;
	font-family:宋体;
	mso-font-charset:134;
	border:none;
	mso-protection:locked visible;}
.xl65
	{mso-style-parent:style0;
	text-align:center;
	mso-pattern:auto none;
	background:#FFFF00;
	font-size:48.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl66
	{mso-style-parent:style0;
	mso-pattern:auto none;
	background:#F8CBAD;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl67
	{mso-style-parent:style0;
	mso-number-format:"yyyy/m/d";
	text-align:left;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl68
	{mso-style-parent:style0;
	text-align:left;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl69
	{mso-style-parent:style0;
	mso-number-format:"yyyy/m/d";
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl70
	{mso-style-parent:style0;
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl71
	{mso-style-parent:style0;
	mso-number-format:"yyyy/m/d";
	text-align:left;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl72
	{mso-style-parent:style0;
	text-align:left;
	color:#FF0000;
	font-size:14.0pt;
	mso-generic-font-family:auto;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl73
	{mso-style-parent:style0;
	text-align:left;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl74
	{mso-style-parent:style0;
	mso-number-format:"yyyy/m/d";
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl75
	{mso-style-parent:style0;
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	color:#FF0000;
	font-size:14.0pt;
	mso-generic-font-family:auto;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl76
	{mso-style-parent:style0;
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl77
	{mso-style-parent:style0;
	mso-number-format:"h:mm:ss";
	text-align:left;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl78
	{mso-style-parent:style0;
	mso-number-format:"h:mm:ss";
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl79
	{mso-style-parent:style0;
	mso-number-format:"h:mm:ss";
	text-align:left;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
.xl80
	{mso-style-parent:style0;
	mso-number-format:"h:mm:ss";
	text-align:left;
	mso-pattern:auto none;
	background:#BDD7EE;
	color:#FF0000;
	font-size:14.0pt;
	mso-font-charset:134;
	border:.5pt solid windowtext;}
 -->  </style>
  <!--[if gte mso 9]>
   <xml>
    <x:ExcelWorkbook>
     <x:ExcelWorksheets>
      <x:ExcelWorksheet>
       <x:Name>AAA</x:Name>
       <x:WorksheetOptions>
        <x:DefaultRowHeight>336</x:DefaultRowHeight>
        <x:StandardWidth>2656</x:StandardWidth>
        <x:Selected/>
        <x:Panes>
         <x:Pane>
          <x:Number>3</x:Number>
          <x:ActiveCol>4</x:ActiveCol>
          <x:ActiveRow>31</x:ActiveRow>
          <x:RangeSelection>E32</x:RangeSelection>
         </x:Pane>
        </x:Panes>
        <x:ProtectContents>False</x:ProtectContents>
        <x:ProtectObjects>False</x:ProtectObjects>
        <x:ProtectScenarios>False</x:ProtectScenarios>
        <x:PageBreakZoom>100</x:PageBreakZoom>
        <x:Print>
         <x:PaperSizeIndex>9</x:PaperSizeIndex>
        </x:Print>
       </x:WorksheetOptions>
      </x:ExcelWorksheet>
     </x:ExcelWorksheets>
     <x:ProtectStructure>False</x:ProtectStructure>
     <x:ProtectWindows>False</x:ProtectWindows>
     <x:SelectedSheets>0</x:SelectedSheets>
     <x:WindowHeight>24160</x:WindowHeight>
     <x:WindowWidth>-14336</x:WindowWidth>
    </x:ExcelWorkbook>
   </xml>
  <![endif]-->
 </head>
 <body link="blue" vlink="purple">
  <table width="1275.45" border="0" cellpadding="0" cellspacing="0" style='width:1275.45pt;border-collapse:collapse;table-layout:fixed;'>
   <col width="87.45" style='mso-width-source:userset;mso-width-alt:4264;'/>
   <col width="82.50" style='mso-width-source:userset;mso-width-alt:4022;'/>
   <col width="66.55" style='mso-width-source:userset;mso-width-alt:3244;'/>
   <col width="97.85" style='mso-width-source:userset;mso-width-alt:4771;'/>
   <col width="362.50" style='mso-width-source:userset;mso-width-alt:17675;'/>
   <col width="85.80" style='mso-width-source:userset;mso-width-alt:4183;'/>
   <col width="97.85" style='mso-width-source:userset;mso-width-alt:4771;'/>
   <col width="56.65" style='mso-width-source:userset;mso-width-alt:2762;'/>
   <col width="51.90" style='mso-width-source:userset;mso-width-alt:2530;'/>
   <col width="70.90" span="2" style='mso-width-source:userset;mso-width-alt:3457;'/>
   <col width="78.05" style='mso-width-source:userset;mso-width-alt:3805;'/>
   <col width="66.55" style='mso-width-source:userset;mso-width-alt:3244;'/>
'''


HTML_PART2 = '''
   <tr height="58" style='height:58.00pt;mso-height-source:userset;mso-height-alt:1160;'>
    <td class="xl65" height="58" width="1287.85" colspan="13" style='height:58.00pt;width:1287.85pt;border-right:.5pt solid windowtext;border-bottom:.5pt solid windowtext;' x:str>2023年3月25日复盘</td>
   </tr>
'''

HTML_TITLE = '''
   <tr height="44" style='height:44.00pt;mso-height-source:userset;mso-height-alt:880;'>
    <td class="xl66" height="44" style='height:44.00pt;' x:str>日期</td>
    <td class="xl66" x:str>股票代码</td>
    <td class="xl66" x:str>股票简称</td>
    <td class="xl66" x:str>连续涨停天数</td>
    <td class="xl66" x:str>涨停原因类别</td>
    <td class="xl66" x:str>首次涨停时间</td>
    <td class="xl66" x:str>最终涨停时间</td>
    <td class="xl66" x:str>板数</td>
    <td class="xl66" x:str>成交额</td>
    <td class="xl66" x:str>成交额4%</td>
    <td class="xl66" x:str>成交额8%</td>
    <td class="xl66" x:str>成交额10%</td>
    <td class="xl66" x:str>流通市值</td>
   </tr>
'''

HTML_PART_END = '''
   <![if supportMisalignedColumns]>
    <tr width="0" style='display:none;'>
     <td width="87" style='width:87;'></td>
     <td width="83" style='width:83;'></td>
     <td width="67" style='width:67;'></td>
     <td width="98" style='width:98;'></td>
     <td width="363" style='width:363;'></td>
     <td width="86" style='width:86;'></td>
     <td width="98" style='width:98;'></td>
     <td width="57" style='width:57;'></td>
     <td width="52" style='width:52;'></td>
     <td width="71" style='width:71;'></td>
     <td width="78" style='width:78;'></td>
     <td width="67" style='width:67;'></td>
    </tr>
   <![endif]>
  </table>
 </body>
</html>
'''

def formatRow_white(row):
    tr = f'''
    <tr height="20.40" style='height:20.40pt;'>
    <td class="xl67" height="20.40" style='height:20.40pt;' x:num="45009">2023/3/24</td>
    <td class="xl68" x:str>{row["股票代码"]}</td>
    <td class="xl68" x:str>{row["股票简称"]}</td>
    <td class="xl68" x:num>{row["连续涨停天数"]}</td>
    <td class="xl68" x:str>{row["涨停原因类别"]}</td>
    <td class="xl77" x:num="0.39583333333333331">{row["首次涨停时间"]}</td>
    <td class="xl77" x:num="0.39583333333333331">{row["最终涨停时间"]}</td>
    <td class="xl68" x:str>{row["板数"]}</td>
    <td class="xl68" x:str>{row["成交额"]}</td>
    <td class="xl68" x:str>{row["成交额4%"]}</td>
    <td class="xl68" x:str>{row["成交额8%"]}</td>
    <td class="xl68" x:str>{row["成交额10%"]}</td>
    <td class="xl68" x:str>{row["流通市值"]}</td>
   </tr>

    '''
    return tr

def formatRow_white_300(row):
    tr = f'''
    <tr height="20.40" style='height:20.40pt;'>
    <td class="xl71" height="20.40" style='height:20.40pt;' x:num="45009">2023/3/24</td>
    <td class="xl72" x:str>{row["股票代码"]}</td>
    <td class="xl72" x:str>{row["股票简称"]}</td>
    <td class="xl73" x:num>{row["连续涨停天数"]}</td>
    <td class="xl72" x:str>{row["涨停原因类别"]}</td>
    <td class="xl79" x:num="0.39583333333333331">{row["首次涨停时间"]}</td>
    <td class="xl79" x:num="0.39583333333333331">{row["最终涨停时间"]}</td>
    <td class="xl72" x:str>{row["板数"]}</td>
    <td class="xl72" x:str>{row["成交额"]}</td>
    <td class="xl72" x:str>{row["成交额4%"]}</td>
    <td class="xl72" x:str>{row["成交额8%"]}</td>
    <td class="xl72" x:str>{row["成交额10%"]}</td>
    <td class="xl72" x:str>{row["流通市值"]}</td>
   </tr>
    '''
    return tr


def formatRow_blue(row):
    tr = f'''
    <tr height="20.40" style='height:20.40pt;'>
    <td class="xl69" height="20.40" style='height:20.40pt;' x:num="45009">2023/3/24</td>
    <td class="xl70" x:str>{row["股票代码"]}</td>
    <td class="xl70" x:str>{row["股票简称"]}</td>
    <td class="xl70" x:num>{row["连续涨停天数"]}</td>
    <td class="xl70" x:str>{row["涨停原因类别"]}</td>
    <td class="xl78" x:num="0.54184027777777777">{row["首次涨停时间"]}</td>
    <td class="xl78" x:num="0.54184027777777777">{row["最终涨停时间"]}</td>
    <td class="xl70" x:str>{row["板数"]}</td>
    <td class="xl70" x:str>{row["成交额"]}</td>
    <td class="xl70" x:str>{row["成交额4%"]}</td>
    <td class="xl70" x:str>{row["成交额8%"]}</td>
    <td class="xl70" x:str>{row["成交额10%"]}</td>
    <td class="xl70" x:str>{row["流通市值"]}</td>
   </tr>
    '''
    return tr

def formatRow_blue_300(row):
    tr = f'''
        <tr height="20.40" style='height:20.40pt;'>
        <td class="xl74" height="20.40" style='height:20.40pt;' x:num="45009">2023/3/24</td>
        <td class="xl75" x:str>{row["股票代码"]}</td>
        <td class="xl75" x:str>{row["股票简称"]}</td>
        <td class="xl76" x:num>{row["连续涨停天数"]}</td>
        <td class="xl75" x:str>{row["涨停原因类别"]}</td>
        <td class="xl80" x:num="0.54184027777777777">{row["首次涨停时间"]}</td>
        <td class="xl80" x:num="0.54184027777777777">{row["最终涨停时间"]}</td>
        <td class="xl75" x:str>{row["板数"]}</td>
        <td class="xl75" x:str>{row["成交额"]}</td>
        <td class="xl75" x:str>{row["成交额4%"]}</td>
        <td class="xl75" x:str>{row["成交额8%"]}</td>
        <td class="xl75" x:str>{row["成交额10%"]}</td>
        <td class="xl75" x:str>{row["流通市值"]}</td>
        </tr>
    '''
    return tr

def formatVolumn(volumn,delta = 1.0):
    newVolumn = float(volumn) * delta
    s = newVolumn /100000000.0 # 除以1亿
    ret = "亿"
    if s <1:
        t = newVolumn / 10000.0
        ret = f'''{t:.0f}万'''
    else:
        ret = f'''{s:.2f}亿'''
    return ret


def ChengjiaoLiang(df):
    df['新成交额'] = df.apply(lambda row: formatVolumn(row['成交额'],1.0), axis=1)
    df['成交额4%'] = df.apply(lambda row: formatVolumn(row['成交额'],0.04), axis=1)
    df['成交额8%'] = df.apply(lambda row: formatVolumn(row['成交额'],0.08), axis=1)
    df['成交额10%'] = df.apply(lambda row: formatVolumn(row['成交额'],0.1), axis=1)
    df['流通市值'] = df.apply(lambda row: formatVolumn(row['流通市值'],1), axis=1)


def JiSuanBanLv(info):
    res = re.search('(?P<D>.*)天(?P<B>.*)板',info)
    if res !=None:
        resDict = res.groupdict()
        day = int(resDict["D"].strip())
        ban = int(resDict["B"].strip())
        if day == 1:
            return 0
        
        return ban*1.0/day
    return 0

    

def FormatToHTML(df,tradingDays):
    tradingDay = tradingDays[-1]
    title = HTML_PART2.replace("2023年3月25日",tradingDay)
    htmlStr = HTML_PART1 + title + HTML_TITLE
    for index, row in df.iterrows():
        stockID = row['股票代码']
        if index % 2 == 0:
            if re.match('^30.*',stockID) is not None:
                htmlStr = htmlStr + formatRow_white_300(row)
            else:
                htmlStr = htmlStr + formatRow_white(row)
        else:
            if re.match('^30.*',stockID) is not None:
                htmlStr = htmlStr + formatRow_blue_300(row)
            else:
                htmlStr = htmlStr + formatRow_blue(row)

    return htmlStr


def Summary(dbConnection,tradingDays):
    lastDay = tradingDays[-1]
    sql = f'''select A.*,B.`成交额`,C.`流通市值`,C.`所属概念` from  stock.stockzhangting AS A,stock.stockdailyinfo As B, stock.stockbasicinfo AS C where A.`日期`  = "{lastDay}"  and B.`日期`  = "{lastDay}"  and A.`股票代码` = B.`股票代码` and A.`股票代码` = C.`股票代码` order by A.`连续涨停天数` DESC,A.`涨停关键词` DESC,A.`最终涨停时间` ASC;''' 
    res,column = dbConnection.Query(sql)
    df = pd.DataFrame(res,columns=column)
    ChengjiaoLiang(df)
    df['板率'] = df.apply(lambda row: JiSuanBanLv(row['涨停关键词']), axis=1)
    print(df)
    df.sort_values(["连续涨停天数","板率","最终涨停时间"],ascending=[False,False,True],inplace=True)

    newDf = pd.DataFrame()
    newDf['日期'] = df['日期']
    newDf['股票代码'] = df['股票代码'] 
    newDf['股票简称'] = df['股票简称'] 
    newDf['连续涨停天数'] = df['连续涨停天数'] 
    newDf['涨停原因类别'] = df['涨停原因类别'] 
    newDf['首次涨停时间'] = df['首次涨停时间'] 
    newDf['最终涨停时间'] = df['最终涨停时间'] 
    newDf['板数'] = df['涨停关键词']
    newDf['成交额'] = df['新成交额']
    newDf['成交额4%'] = df['成交额4%']
    newDf['成交额8%'] = df['成交额8%']
    newDf['成交额10%'] = df['成交额10%']
    newDf['流通市值'] = df['流通市值']
    
    #print(newDf)
    newDf.to_csv("/tmp/AAA.CSV",index=False)
    htmlStr = FormatToHTML(newDf,tradingDays)
    fileName = f'''/Volumes/Data/复盘/股票/{lastDay}/复盘摘要_{lastDay}.htm'''
    with open(fileName,"w+") as f:
        f.write(htmlStr)
        print("写入摘要:" + fileName + "  成功！！")


