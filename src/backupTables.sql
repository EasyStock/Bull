CREATE DATABASE `stock`;

USE `stock`; 

CREATE TABLE `bankuai_index_dailyinfo` (
  `日期` varchar(45) NOT NULL,
  `板块代码` varchar(45) NOT NULL,
  `板块名称` varchar(45) NOT NULL,
  `开盘价(点)` varchar(45) DEFAULT NULL,
  `收盘价(点)` varchar(45) DEFAULT NULL,
  `最高价(点)` varchar(45) DEFAULT NULL,
  `最低价(点)` varchar(45) DEFAULT NULL,
  `成交量(股)` varchar(45) DEFAULT NULL,
  `成交额(元)` varchar(45) DEFAULT NULL,
  `涨跌幅(%)` varchar(45) DEFAULT NULL,
  `量比` varchar(45) DEFAULT NULL,
  `换手率(%)` varchar(45) DEFAULT NULL,
  `上涨家数(家)` varchar(45) DEFAULT NULL,
  `下跌家数(家)` varchar(45) DEFAULT NULL,
  `流通市值(元)` varchar(45) DEFAULT NULL,
  `总市值(元)` varchar(45) DEFAULT NULL,
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`板块代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `bankuai_index_score_daily` (
  `日期` varchar(45) NOT NULL,
  `板块代码` varchar(45) NOT NULL,
  `板块名称` varchar(45) NOT NULL,
  `开盘价分数` float DEFAULT NULL,
  `收盘价分数` float DEFAULT NULL,
  `最高价分数` float DEFAULT NULL,
  `最低价分数` float DEFAULT NULL,
  `成交量分数` float DEFAULT NULL,
  `成交额分数` float DEFAULT NULL,
  `涨跌幅自身分数` float DEFAULT NULL,
  `涨跌幅相对分数` float DEFAULT NULL,
  `量比分数` float DEFAULT NULL,
  `换手率分数` float DEFAULT NULL,
  `上涨家数分数` float DEFAULT NULL,
  `下跌家数分数` float DEFAULT NULL,
  `流通市值分数` float DEFAULT NULL,
  `总市值分数` float DEFAULT NULL,
  PRIMARY KEY (`日期`,`板块代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `bankuai_stock_match` (
  `板块代码` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `更新日期` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`板块代码`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `caozuo` (
  `日期` varchar(45) NOT NULL,
  `股票名称` varchar(45) NOT NULL,
  `概念` varchar(45) DEFAULT NULL,
  `操作` varchar(45) DEFAULT NULL,
  `操作理由` text,
  `操作类型` varchar(45) DEFAULT NULL,
  `盈亏` varchar(45) DEFAULT NULL,
  `备注` text,
  PRIMARY KEY (`日期`,`股票名称`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `compareindex_stock` (
  `date` varchar(45) NOT NULL,
  `indexID` varchar(45) NOT NULL COMMENT '指数ID',
  `stockID` varchar(45) NOT NULL,
  `increase_rate` varchar(45) DEFAULT NULL,
  `zhangdiefu` varchar(45) DEFAULT NULL,
  `delta` float DEFAULT NULL,
  `flag` tinyint DEFAULT NULL,
  PRIMARY KEY (`date`,`indexID`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `compareindex_zai` (
  `date` varchar(45) NOT NULL,
  `indexID` varchar(45) NOT NULL COMMENT '指数ID',
  `stockID` varchar(45) NOT NULL,
  `increase_rate` varchar(45) DEFAULT NULL,
  `zhangdiefu` varchar(45) DEFAULT NULL,
  `delta` float DEFAULT NULL,
  `flag` tinyint DEFAULT NULL,
  PRIMARY KEY (`date`,`indexID`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `cookies` (
  `name` varchar(255) NOT NULL,
  `cookie` text,
  `expiry` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `dragon` (
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `stockName` varchar(255) DEFAULT NULL,
  `operator_ID` varchar(45) NOT NULL,
  `operator_Name` varchar(45) DEFAULT NULL,
  `buy` varchar(45) DEFAULT NULL,
  `sell` varchar(45) DEFAULT NULL,
  `NET` varchar(45) NOT NULL,
  `flag` varchar(45) NOT NULL,
  `reason` varchar(512) NOT NULL,
  PRIMARY KEY (`date`,`stockID`,`operator_ID`,`NET`,`flag`,`reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `dragon_guanlian` (
  `operatorIDs` varchar(255) NOT NULL,
  `operatorIDNames` varchar(512) DEFAULT NULL,
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `flag` varchar(45) NOT NULL,
  `reason` varchar(512) DEFAULT NULL,
  PRIMARY KEY (`operatorIDs`,`date`,`stockID`,`flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `fupan` (
  `日期` varchar(45) NOT NULL,
  `红盘` varchar(45) DEFAULT NULL,
  `绿盘` varchar(45) DEFAULT NULL,
  `两市量` varchar(45) DEFAULT NULL,
  `量比` varchar(45) DEFAULT NULL,
  `增量` varchar(45) DEFAULT NULL,
  `实际涨停` varchar(45) DEFAULT NULL,
  `跌停` varchar(45) DEFAULT NULL,
  `炸板` varchar(45) DEFAULT NULL,
  `炸板率` varchar(45) DEFAULT NULL,
  `连板` varchar(45) DEFAULT NULL,
  `10CM首板奖励率` varchar(45) DEFAULT NULL,
  `20CM首板奖励率` varchar(45) DEFAULT NULL,
  `10CM连板奖励率` varchar(45) DEFAULT NULL,
  `20CM连板奖励率` varchar(45) DEFAULT NULL,
  `首板个数` int DEFAULT NULL,
  `2连板个数` int DEFAULT NULL,
  `3连板个数` int DEFAULT NULL,
  `3连个股` text,
  `4连板及以上个数` int DEFAULT NULL,
  `4连及以上个股` text,
  `高度板` int DEFAULT NULL,
  `动能` float DEFAULT NULL,
  `势能` float DEFAULT NULL,
  `首板率` float DEFAULT NULL,
  `连板率` float DEFAULT NULL,
  `昨日首板溢价率` float DEFAULT NULL,
  `昨日首板晋级率` float DEFAULT NULL,
  `昨日2板溢价率` float DEFAULT NULL,
  `昨日2板晋级率` float DEFAULT NULL,
  `昨日3板溢价率` float DEFAULT NULL,
  `昨日3板晋级率` float DEFAULT NULL,
  `昨日4板及以上溢价率` float DEFAULT NULL,
  `昨日4板及以上晋级率` float DEFAULT NULL,
  `2进3成功率` float DEFAULT NULL,
  `3进高成功率` float DEFAULT NULL,
  `涨停数量` int DEFAULT NULL,
  `连板数量` int DEFAULT NULL,
  `收-5数量` int DEFAULT NULL,
  `大盘红盘比` float DEFAULT NULL,
  `亏钱效应` float DEFAULT NULL,
  `首板红盘比` float DEFAULT NULL,
  `首板大面比` float DEFAULT NULL,
  `连板股的红盘比` float DEFAULT NULL,
  `连板比例` float DEFAULT NULL,
  `连板大面比` float DEFAULT NULL,
  `昨日连板未涨停数的绿盘比` float DEFAULT NULL,
  `势能EX` float DEFAULT NULL,
  `动能EX` float DEFAULT NULL,
  `复盘笔记` text,
  `备注` text,
  PRIMARY KEY (`日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `fupansummary` (
  `date` varchar(45) NOT NULL,
  `summary` longtext,
  PRIMARY KEY (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `gainian` (
  `概念名称` varchar(45) NOT NULL,
  `更新日期` varchar(45) NOT NULL,
  PRIMARY KEY (`概念名称`,`更新日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `index_dailyinfo` (
  `日期` varchar(45) NOT NULL,
  `指数代码` varchar(45) NOT NULL,
  `指数名称` varchar(45) NOT NULL,
  `开盘价(点)` varchar(45) DEFAULT NULL,
  `收盘价(点)` varchar(45) DEFAULT NULL,
  `最高价(点)` varchar(45) DEFAULT NULL,
  `最低价(点)` varchar(45) DEFAULT NULL,
  `成交量(股)` varchar(45) DEFAULT NULL,
  `成交额(元)` varchar(45) DEFAULT NULL,
  `涨跌幅(%)` varchar(45) DEFAULT NULL,
  `量比` varchar(45) DEFAULT NULL,
  `换手率(%)` varchar(45) DEFAULT NULL,
  `上涨家数(家)` varchar(45) DEFAULT NULL,
  `下跌家数(家)` varchar(45) DEFAULT NULL,
  `流通市值(元)` varchar(45) DEFAULT NULL,
  `总市值(元)` varchar(45) DEFAULT NULL,
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`指数代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `jenkins_status` (
  `date` varchar(45) NOT NULL,
  `data` tinyint(1) DEFAULT NULL,
  `analysis` tinyint(1) DEFAULT NULL,
  PRIMARY KEY (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_dieting` (
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `time` varchar(45) DEFAULT NULL,
  `fengdan` varchar(45) DEFAULT NULL,
  `bankuai` varchar(255) DEFAULT NULL,
  `jinge` varchar(45) DEFAULT NULL,
  `volumn` varchar(45) DEFAULT NULL,
  `huanshou` varchar(45) DEFAULT NULL,
  `liutong` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`date`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_index` (
  `date` varchar(45) NOT NULL,
  `StockID` varchar(45) NOT NULL,
  `prod_name` varchar(45) DEFAULT NULL,
  `increase_amount` float DEFAULT NULL,
  `increase_rate` varchar(45) DEFAULT NULL,
  `last_px` float DEFAULT NULL,
  `turnover` bigint DEFAULT NULL,
  PRIMARY KEY (`date`,`StockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_volumn` (
  `date` varchar(45) NOT NULL,
  `volumn` varchar(45) DEFAULT NULL,
  `s_zrcs` varchar(45) DEFAULT NULL,
  `delta` varchar(45) DEFAULT NULL,
  `ratio` varchar(45) DEFAULT NULL,
  `trends` text,
  PRIMARY KEY (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_zhaban` (
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `zhangfu` varchar(45) DEFAULT NULL,
  `time` varchar(45) DEFAULT NULL,
  `timezhaban` varchar(45) DEFAULT NULL,
  `bankuai` varchar(255) DEFAULT NULL,
  `jinge` varchar(45) DEFAULT NULL,
  `volumn` varchar(45) DEFAULT NULL,
  `huanshou` varchar(45) DEFAULT NULL,
  `liutong` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`date`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_zhangting` (
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `firstTime` time DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `reason` varchar(255) DEFAULT NULL,
  `bankuai` varchar(255) DEFAULT NULL,
  `lastTime` time DEFAULT NULL,
  `fengdanMax` int DEFAULT NULL,
  `fengdan` int DEFAULT NULL,
  `jinge` int DEFAULT NULL,
  `volumn` int DEFAULT NULL,
  `huanshou` float DEFAULT NULL,
  `liutong` int DEFAULT NULL,
  PRIMARY KEY (`date`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kaipanla_ziranzhangting` (
  `date` varchar(45) NOT NULL,
  `stockID` varchar(45) NOT NULL,
  `stockName` varchar(45) DEFAULT NULL,
  `time` varchar(45) DEFAULT NULL,
  `status` varchar(45) DEFAULT NULL,
  `reason` varchar(255) DEFAULT NULL,
  `bankuai` varchar(255) DEFAULT NULL,
  `fengdanMax` varchar(45) DEFAULT NULL,
  `fengdan` varchar(45) DEFAULT NULL,
  `jinge` varchar(45) DEFAULT NULL,
  `volumn` varchar(45) DEFAULT NULL,
  `huanshou` varchar(45) DEFAULT NULL,
  `liutong` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`date`,`stockID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kezhuanzai_score` (
  `日期` varchar(45) NOT NULL,
  `转债代码` varchar(45) NOT NULL,
  `成交量分数` float DEFAULT NULL,
  `抗跌分数周期` varchar(255) DEFAULT NULL,
  `抗跌分数` float DEFAULT NULL,
  `领涨分数周期` varchar(255) DEFAULT NULL,
  `领涨分数` float DEFAULT NULL,
  `市值分数` float DEFAULT NULL,
  `剩余规模分数` float DEFAULT NULL,
  `总分` float DEFAULT NULL,
  PRIMARY KEY (`日期`,`转债代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kezhuanzai_score_everyday` (
  `日期` varchar(45) NOT NULL,
  `转债代码` varchar(45) NOT NULL,
  `成交量分数` float DEFAULT NULL,
  `比指数分数` float DEFAULT NULL,
  `剩余规模分数` float DEFAULT NULL,
  `总分` float DEFAULT NULL,
  PRIMARY KEY (`日期`,`转债代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kezhuanzai_ths` (
  `日期` varchar(45) NOT NULL,
  `转债代码` varchar(45) NOT NULL,
  `转债名称` varchar(45) DEFAULT NULL,
  `正股代码` varchar(45) DEFAULT NULL,
  `最高价` float DEFAULT NULL,
  `最低价` float DEFAULT NULL,
  `开盘价` float DEFAULT NULL,
  `收盘价` float DEFAULT NULL,
  `成交量` float DEFAULT NULL,
  `成交额` float DEFAULT NULL,
  `涨跌幅` float DEFAULT NULL,
  `上市日期` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`转债代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kezhuanzhai` (
  `日期` varchar(45) NOT NULL,
  `转债代码` varchar(45) NOT NULL,
  `转债名称` varchar(45) DEFAULT NULL,
  `现价` float DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `5日涨跌幅` varchar(45) DEFAULT NULL,
  `20日涨跌幅` varchar(45) DEFAULT NULL,
  `3月涨跌幅` varchar(45) DEFAULT NULL,
  `成交额(万元)` float DEFAULT '-1',
  `正股代码` varchar(45) DEFAULT NULL,
  `正股名称` varchar(45) DEFAULT NULL,
  `正股成交额(万元)` float DEFAULT '-1',
  `PB` float DEFAULT NULL,
  `有息负债率` float DEFAULT NULL,
  `股票质押率` float DEFAULT NULL,
  `流通市值（亿元)` float DEFAULT NULL,
  `总市值（亿元)` float DEFAULT NULL,
  `溢价率` float DEFAULT NULL,
  `行业` text,
  `评级` text,
  `回售触发价` text,
  `剩余年限` text,
  `剩余规模` text,
  `到期税前收益率` text,
  `提示` text,
  `流通市值小于50亿` text,
  `剩余规模<=3` text,
  `PB-溢价率` float DEFAULT NULL,
  PRIMARY KEY (`日期`,`转债代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `kezhuanzhai_all` (
  `日期` varchar(45) NOT NULL,
  `转债代码` varchar(45) NOT NULL,
  `转债名称` varchar(45) DEFAULT NULL,
  `现价` float DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `5日涨跌幅` varchar(45) DEFAULT NULL,
  `20日涨跌幅` varchar(45) DEFAULT NULL,
  `3月涨跌幅` varchar(45) DEFAULT NULL,
  `成交额(万元)` float DEFAULT '-1',
  `正股代码` varchar(45) DEFAULT NULL,
  `正股名称` varchar(45) DEFAULT NULL,
  `正股成交额(万元)` float DEFAULT '-1',
  `PB` float DEFAULT NULL,
  `有息负债率` float DEFAULT NULL,
  `股票质押率` float DEFAULT NULL,
  `流通市值（亿元)` float DEFAULT NULL,
  `总市值（亿元)` float DEFAULT NULL,
  `溢价率` float DEFAULT NULL,
  `行业` text,
  `评级` text,
  `回售触发价` text,
  `剩余年限` text,
  `剩余规模` text,
  `到期税前收益率` text,
  `提示` text,
  `流通市值小于50亿` text,
  `剩余规模<=3` text,
  `PB-溢价率` text,
  `筛选结果` text,
  PRIMARY KEY (`日期`,`转债代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `namemapping` (
  `shortName` varchar(45) NOT NULL,
  `fullName` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`shortName`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `newstocks` (
  `股票代码` varchar(45) NOT NULL,
  `股票名称` varchar(255) DEFAULT NULL,
  `申购日期` varchar(45) DEFAULT NULL,
  `申购日` varchar(255) DEFAULT NULL,
  `需配市值` varchar(45) DEFAULT NULL,
  `申购代码` varchar(255) DEFAULT NULL,
  `发行价` varchar(45) DEFAULT NULL,
  `申购限额` varchar(45) DEFAULT NULL,
  `缴款日` varchar(45) DEFAULT NULL,
  `中签率` varchar(45) DEFAULT NULL,
  `上市日期` varchar(45) DEFAULT NULL,
  `发行时总市值` varchar(45) DEFAULT NULL,
  `公开发行市值` varchar(45) DEFAULT NULL,
  `发行市盈率` varchar(45) DEFAULT NULL,
  `行业市盈率` varchar(45) DEFAULT NULL,
  `开板收盘价` varchar(45) DEFAULT NULL,
  `单签收益` varchar(45) DEFAULT NULL,
  `承销商` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `percentile_bankuai` (
  `板块代码` varchar(45) NOT NULL,
  `百分位数` varchar(45) NOT NULL,
  `板块名称` varchar(45) DEFAULT NULL,
  `开盘价(点)` varchar(45) DEFAULT NULL,
  `收盘价(点)` varchar(45) DEFAULT NULL,
  `最高价(点)` varchar(45) DEFAULT NULL,
  `最低价(点)` varchar(45) DEFAULT NULL,
  `成交量(股)` varchar(45) DEFAULT NULL,
  `成交额(元)` varchar(45) DEFAULT NULL,
  `涨跌幅(%)` varchar(45) DEFAULT NULL,
  `量比` varchar(45) DEFAULT NULL,
  `换手率(%)` varchar(45) DEFAULT NULL,
  `上涨家数(家)` varchar(45) DEFAULT NULL,
  `下跌家数(家)` varchar(45) DEFAULT NULL,
  `流通市值(元)` varchar(45) DEFAULT NULL,
  `总市值(元)` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`板块代码`,`百分位数`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `percentile_stock_volumn` (
  `股票代码` varchar(45) NOT NULL,
  `百分位数` varchar(45) NOT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `更新时间` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`股票代码`,`百分位数`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `rediandaily` (
  `日期` varchar(45) NOT NULL,
  `热点` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `simulate_trading` (
  `日期` varchar(45) NOT NULL,
  `名称` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `买入日期` varchar(45) DEFAULT NULL,
  `买入价格` varchar(45) DEFAULT NULL,
  `买入排名` varchar(45) DEFAULT NULL,
  `买入价格涨跌幅` varchar(45) DEFAULT NULL,
  `1日后卖出收益` varchar(45) DEFAULT NULL,
  `3日后卖出收益` varchar(45) DEFAULT NULL,
  `5日后卖出收益` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`名称`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stock_score_daily` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `板块代码` varchar(45) DEFAULT NULL,
  `板块名称` varchar(45) DEFAULT NULL,
  `板块分数` float DEFAULT NULL,
  `成交量分数` float DEFAULT NULL,
  `流通市值分数` float DEFAULT NULL,
  `比大盘分数` float DEFAULT NULL,
  `总分1` varchar(45) DEFAULT NULL,
  `总分2` varchar(45) DEFAULT NULL,
  `总分3` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockbasicinfo` (
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `所属概念` text,
  `所属概念数量` text,
  `上市日期` text,
  `上市天数` text,
  `行业` text,
  `流通市值` text,
  `更新日期` varchar(45) DEFAULT NULL,
  `istTained` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`股票代码`),
  UNIQUE KEY `股票代码_UNIQUE` (`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockbasicinfo_test` (
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `所属概念` text,
  `所属概念数量` text,
  `上市日期` text,
  `上市天数` text,
  `行业` text,
  `流通市值` text,
  `更新日期` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`股票代码`),
  UNIQUE KEY `股票代码_UNIQUE` (`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdaily_vma` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA10` varchar(45) DEFAULT NULL,
  `V/MA20` varchar(45) DEFAULT NULL,
  `V/MA30` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `V/MA90` varchar(45) DEFAULT NULL,
  `V/MA120` varchar(45) DEFAULT NULL,
  `V/MA250` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `1日后涨幅` varchar(45) DEFAULT NULL COMMENT '(后天开盘价-明天开盘价)/明天开盘价 X100%',
  `3日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第4天开盘价-第2开盘价)/第2开盘价 X100%',
  `5日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第6天开盘价-第2开盘价)/第2开盘价 X100%',
  `7日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第8天开盘价-第2开盘价)/第2开盘价 X100%',
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_2021` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `1日后涨幅` varchar(45) DEFAULT NULL COMMENT '(后天开盘价-明天开盘价)/明天开盘价 X100%',
  `3日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第4天开盘价-第2开盘价)/第2开盘价 X100%',
  `5日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第6天开盘价-第2开盘价)/第2开盘价 X100%',
  `7日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第8天开盘价-第2开盘价)/第2开盘价 X100%',
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_2022` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `1日后涨幅` varchar(45) DEFAULT NULL COMMENT '(后天开盘价-明天开盘价)/明天开盘价 X100%',
  `3日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第4天开盘价-第2开盘价)/第2开盘价 X100%',
  `5日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第6天开盘价-第2开盘价)/第2开盘价 X100%',
  `7日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第8天开盘价-第2开盘价)/第2开盘价 X100%',
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_2023` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `1日后涨幅` varchar(45) DEFAULT NULL COMMENT '(后天开盘价-明天开盘价)/明天开盘价 X100%',
  `3日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第4天开盘价-第2开盘价)/第2开盘价 X100%',
  `5日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第6天开盘价-第2开盘价)/第2开盘价 X100%',
  `7日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第8天开盘价-第2开盘价)/第2开盘价 X100%',
  `顶底分型` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_test` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_traning` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  `V/MA10` varchar(45) DEFAULT NULL,
  `V/MA20` varchar(45) DEFAULT NULL,
  `V/MA30` varchar(45) DEFAULT NULL,
  `V/MA60` varchar(45) DEFAULT NULL,
  `V/MA90` varchar(45) DEFAULT NULL,
  `V/MA120` varchar(45) DEFAULT NULL,
  `V/MA250` varchar(45) DEFAULT NULL,
  `1日后涨幅` varchar(45) DEFAULT NULL COMMENT '(后天开盘价-明天开盘价)/明天开盘价 X100%',
  `3日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第4天开盘价-第2开盘价)/第2开盘价 X100%',
  `5日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第6天开盘价-第2开盘价)/第2开盘价 X100%',
  `7日后涨幅` varchar(45) DEFAULT NULL COMMENT '(第8天开盘价-第2开盘价)/第2开盘价 X100%',
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_traning_result` (
  `stockID` varchar(45) NOT NULL,
  `VMA` varchar(45) NOT NULL,
  `VMA值` varchar(45) NOT NULL,
  `涨幅` varchar(45) NOT NULL,
  `几日后涨幅` varchar(45) NOT NULL COMMENT '几日后的涨幅',
  `概率` varchar(45) DEFAULT NULL COMMENT '概率\n',
  `平均涨幅` varchar(45) DEFAULT NULL,
  `仓位` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`stockID`,`VMA`,`涨幅`,`几日后涨幅`,`VMA值`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockdailyinfo_tushare` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `开盘价` varchar(45) DEFAULT NULL,
  `收盘价` varchar(45) DEFAULT NULL,
  `最高价` varchar(45) DEFAULT NULL,
  `最低价` varchar(45) DEFAULT NULL,
  `昨收价` varchar(45) DEFAULT NULL,
  `成交量` varchar(45) DEFAULT NULL,
  `成交额` varchar(45) DEFAULT NULL,
  `涨跌幅` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockgainiannew` (
  `日期` varchar(45) NOT NULL,
  `新概念` varchar(255) NOT NULL,
  `可转债` text,
  `股票` text,
  `是否被炒作` blob,
  PRIMARY KEY (`日期`,`新概念`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockzhangting` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `连续涨停天数` int DEFAULT NULL,
  `涨停原因类别` text,
  `首次涨停时间` varchar(45) DEFAULT NULL,
  `最终涨停时间` varchar(45) DEFAULT NULL,
  `涨停关键词` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `stockzhangting_test` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` varchar(45) DEFAULT NULL,
  `连续涨停天数` int DEFAULT NULL,
  `涨停原因类别` text,
  `首次涨停时间` varchar(45) DEFAULT NULL,
  `最终涨停时间` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `thstoken` (
  `refreshToken` varchar(512) NOT NULL,
  `access_token` text,
  `expired_time` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`refreshToken`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `treadingday` (
  `交易所` varchar(45) NOT NULL,
  `日期` varchar(45) NOT NULL,
  `开市` int DEFAULT NULL,
  PRIMARY KEY (`日期`,`交易所`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh_py`@`%` SQL SECURITY DEFINER VIEW `yaogu` AS select `stockzhangting`.`股票代码` AS `股票代码`,max(`stockzhangting`.`连续涨停天数`) AS `最大连板天数`,min(`stockzhangting`.`日期`) AS `首次涨停日期`,max(`stockzhangting`.`日期`) AS `最后涨停日期` from `stockzhangting` group by `stockzhangting`.`股票代码`;

CREATE TABLE `yiziban` (
  `日期` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票简称` text,
  `涨停原因类别` text,
  `连续涨停天数` int DEFAULT NULL,
  `封单915` varchar(45) DEFAULT NULL,
  `封单920` varchar(45) DEFAULT NULL,
  `封单925` varchar(45) DEFAULT NULL,
  `第二天表现` text,
  `第三天表现` text,
  PRIMARY KEY (`日期`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE TABLE `zhanfa` (
  `日期` varchar(45) NOT NULL,
  `战法名称` varchar(45) NOT NULL,
  `股票代码` varchar(45) NOT NULL,
  `股票名称` varchar(45) DEFAULT NULL,
  `买入日期` varchar(45) DEFAULT NULL,
  `买入价格` varchar(45) DEFAULT NULL,
  `卖出日期` varchar(45) DEFAULT NULL,
  `卖出价格` varchar(45) DEFAULT NULL,
  `盈利` varchar(45) DEFAULT NULL,
  `其他信息` text,
  PRIMARY KEY (`日期`,`战法名称`,`股票代码`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh`@`%` SQL SECURITY DEFINER VIEW `市场总体情绪` AS select `fupan`.`日期` AS `日期`,`fupan`.`红盘` AS `红盘`,`fupan`.`绿盘` AS `绿盘`,`fupan`.`两市量` AS `两市量`,`fupan`.`量比` AS `量比`,`fupan`.`增量` AS `增量`,`fupan`.`实际涨停` AS `实际涨停`,`fupan`.`跌停` AS `跌停`,`fupan`.`炸板` AS `炸板`,`fupan`.`炸板率` AS `炸板率`,`fupan`.`连板` AS `连板`,`fupan`.`备注` AS `备注` from `fupan`;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh`@`%` SQL SECURITY DEFINER VIEW `超短情绪指标` AS select `fupan`.`日期` AS `日期`,`fupan`.`涨停数量` AS `涨停数量`,`fupan`.`连板数量` AS `连板数量`,`fupan`.`收-5数量` AS `收-5数量`,`fupan`.`大盘红盘比` AS `大盘红盘比`,`fupan`.`亏钱效应` AS `亏钱效应`,`fupan`.`首板红盘比` AS `首板红盘比`,`fupan`.`首板大面比` AS `首板大面比`,`fupan`.`连板股的红盘比` AS `连板股的红盘比`,`fupan`.`连板比例` AS `连板比例`,`fupan`.`连板大面比` AS `连板大面比`,`fupan`.`昨日连板未涨停数的绿盘比` AS `昨日连板未涨停数的绿盘比`,`fupan`.`势能EX` AS `势能EX`,`fupan`.`动能EX` AS `动能EX`,`fupan`.`复盘笔记` AS `复盘笔记`,`fupan`.`备注` AS `备注` from `fupan`;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh`@`%` SQL SECURITY DEFINER VIEW `超短情绪指标2` AS select `fupan`.`日期` AS `日期`,`fupan`.`红盘` AS `红盘`,`fupan`.`绿盘` AS `绿盘`,`fupan`.`两市量` AS `两市量`,`fupan`.`实际涨停` AS `实际涨停`,`fupan`.`跌停` AS `跌停`,`fupan`.`炸板` AS `炸板`,`fupan`.`炸板率` AS `炸板率`,`fupan`.`连板` AS `连板`,`fupan`.`高度板` AS `高度板`,`fupan`.`大盘红盘比` AS `大盘红盘比`,`fupan`.`首板红盘比` AS `首板红盘比`,`fupan`.`连板股的红盘比` AS `连板股的红盘比`,`fupan`.`势能EX` AS `势能EX`,`fupan`.`动能EX` AS `动能EX`,`fupan`.`备注` AS `备注` from `fupan`;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh`@`%` SQL SECURITY DEFINER VIEW `超短环境1` AS select `fupan`.`日期` AS `日期`,`fupan`.`10CM首板奖励率` AS `10CM首板奖励率`,`fupan`.`20CM首板奖励率` AS `20CM首板奖励率`,`fupan`.`10CM连板奖励率` AS `10CM连板奖励率`,`fupan`.`20CM连板奖励率` AS `20CM连板奖励率`,`fupan`.`首板个数` AS `首板个数`,`fupan`.`2连板个数` AS `2连板个数`,`fupan`.`3连板个数` AS `3连板个数`,`fupan`.`3连个股` AS `3连个股`,`fupan`.`4连板及以上个数` AS `4连板及以上个数`,`fupan`.`4连及以上个股` AS `4连及以上个股`,`fupan`.`高度板` AS `高度板`,`fupan`.`动能` AS `动能`,`fupan`.`势能` AS `势能` from `fupan`;

CREATE ALGORITHM=UNDEFINED DEFINER=`jianpinh`@`%` SQL SECURITY DEFINER VIEW `超短环境2` AS select `fupan`.`日期` AS `日期`,`fupan`.`首板率` AS `首板率`,`fupan`.`连板率` AS `连板率`,`fupan`.`昨日首板溢价率` AS `昨日首板溢价率`,`fupan`.`昨日首板晋级率` AS `昨日首板晋级率`,`fupan`.`昨日2板溢价率` AS `昨日2板溢价率`,`fupan`.`昨日2板晋级率` AS `昨日2板晋级率`,`fupan`.`昨日3板溢价率` AS `昨日3板溢价率`,`fupan`.`昨日3板晋级率` AS `昨日3板晋级率`,`fupan`.`昨日4板及以上溢价率` AS `昨日4板及以上溢价率`,`fupan`.`昨日4板及以上晋级率` AS `昨日4板及以上晋级率`,`fupan`.`备注` AS `备注` from `fupan`;

