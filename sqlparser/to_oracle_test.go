package sqlparser

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestConvertToOracle(t *testing.T) {
	Init(map[string][]string{
		"conf_infos": {"eventId", "conferenceId"},
	})
	testCases := []struct {
		in, out string
	}{
		{
			in:  "INSERT INTO `live_channel` (`id`, `channelId`, `token`, `hostId`, `eventId`, `conferenceId`, `isRecurrence`, `billingCode`, `channelName`, `summary`, `startTime`, `endTime`, `status`, `password`, `ukey`, `necid`, `nevid`, `lastLiveTime`, `lastEndTime`, `confJoinerCnt`, `liveViewerCnt`, `demandViewerCnt`, `notifyToken`, `notifySubUrl`, `createTime`, `pushUrl`, `pushUrlUpdatedAt`, `version`, `siteId`, `hlsPullUrl`, `httpPullUrl`, `rtmpPullUrl`, `pullUrlUpdatedAt`, `authType`, `expireTime`) VALUES (0, 33823, '3beb2b05cd7b960025dcc49d1e135ff4', 88897156, 6667490, '1322', 0, '30663237', 'fll9192的会议1103', '', 1698994800, 1698998400, 0, '', 'lkR4', '', '', 0, 0, 0, 0, 0, '', '', '2023-11-03 14:53:06', '', 0, 3, '', '', '', '', 0, 0, 0)",
			out: "",
		},
		// {
		// 	in:  "update `tb_user_base` set `userBasicInfoId` = 35998836, `nameId` = null, `lastName` = '张三' where (`userBasicInfoId` = 35998836 and `provisionStatus` = 2)",
		// 	out: `update "tb_user_base" set "userBasicInfoId" = 35998836, "nameId" = null, "lastName" = '张三' where ("userBasicInfoId" = 35998836 and "provisionStatus" = 2)`,
		// },
		// {
		// 	in:  `INSERT INTO notice_status_new(id, chat_id, chat_type, conversation, user_id, count, push_count, sys_count, sender_seq, ackread_seq, created, valid) VALUES (NULL, 105, 5, 157, 88897133, 0, 0, 0, 0, 1693815665867, 1693815665868, 1),(NULL, 105, 5, 157, 88897136, 0, 0, 0, 0, 1693815665867, 1693815665868, 1),(NULL, 105, 5, 157, 88897135, 0, 0, 0, 0, 1693815665867, 1693815665868, 1) ON DUPLICATE KEY UPDATE valid = 1`,
		// 	out: `MERGE INTO notice_status_new t USING( select 105, 5, 157, 88897133, 0, 0, 0, 0, 1693815665867, 1693815665868, 1 union all select 105, 5, 157, 88897136, 0, 0, 0, 0, 1693815665867, 1693815665868, 1 union all select 105, 5, 157, 88897135, 0, 0, 0, 0, 1693815665867, 1693815665868, 1) s (chat_id, chat_type, conversation, user_id, count, push_count, sys_count, sender_seq, ackread_seq, created, valid) ON (t.conversation = s.conversation and t.user_id = s.user_id) WHEN MATCHED THEN UPDATE SET t.valid = 1 WHEN NOT MATCHED THEN INSERT (chat_id, chat_type, conversation, user_id, count, push_count, sys_count, sender_seq, ackread_seq, created, valid) VALUES (s.chat_id, s.chat_type, s.conversation, s.user_id, s.count, s.push_count, s.sys_count, s.sender_seq, s.ackread_seq, s.created, s.valid);`,
		// },
		// {
		// 	in:  `INSERT INTO webcal_entry_recurrencerule(cal_id,cal_frequency,cal_interval,cal_byday,cal_bymonth,cal_bymonthday,cal_bysetpos,cal_count,cal_enddate) VALUES (?,?,?,?,?,?,?,?,?) on duplicate key update cal_frequency=?,cal_interval=?,cal_byday=?,cal_bymonth=?,cal_bymonthday=?,cal_bysetpos=?,cal_count=?,cal_enddate=?`,
		// 	out: `merge into "webcal_entry_recurrencerule" as "t" using "dual" on "t"."cal_id" = :v1 when matched then update set "t"."cal_frequency" = :v10, "t"."cal_interval" = :v11, "t"."cal_byday" = :v12, "t"."cal_bymonth" = :v13, "t"."cal_bymonthday" = :v14, "t"."cal_bysetpos" = :v15, "t"."cal_count" = :v16, "t"."cal_enddate" = :v17 when not matched then insert ("cal_id", "cal_frequency", "cal_interval", "cal_byday", "cal_bymonth", "cal_bymonthday", "cal_bysetpos", "cal_count", "cal_enddate") values (:v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9)`,
		// },
		//{
		//	in:  `insert into webcal_live_info(cal_id,channelId,pullurl,password,extraInfo) values(634311,131722,'https://rlive1uat.rmeet.com.cn/activity/geeZWo3','','') on duplicate key update pullurl='https://rlive1uat.rmeet.com.cn/activity/geeZWo3', password='', extraInfo=''`,
		//	out: `merge into webcal_live_info as t using dual on t.cal_id = 634311 and t.channelId = 131722 when matched then update set t.pullurl = 'https://rlive1uat.rmeet.com.cn/activity/geeZWo3', t.password = '', t.extraInfo = '' when not matched then insert (cal_id, channelId, pullurl, password, extraInfo) values (634311, 131722, 'https://rlive1uat.rmeet.com.cn/activity/geeZWo3', '', '')`,
		//},
		//{
		//	in:  `REPLACE INTO exchange_bindinfo (userId,resId,bindingData) values (1,"abcd","101003")`,
		//	out: `merge into exchange_bindinfo as t using dual on t.userId = 1 when matched then update set t.resId = 'abcd', t.bindingData = '101003' when not matched then insert (userId, resId, bindingData) values (1, 'abcd', '101003')`,
		//},
		//{
		//	in:  "SELECT cal_id, creator, url, startTime, endTime, createTime, updateTime, title, type FROM conf_summary WHERE `type`=4 AND cal_id=635427;",
		//	out: "SELECT cal_id, creator, url, startTime, endTime, createTime, updateTime, title, type FROM conf_summary WHERE \"type\"=4 AND cal_id=635427;",
		//},
		//{
		//	in:  `insert into conf_infos (conferenceId,billingCode,pcode1,pcode2,hostJoinUrl,attendeeJoinUrl,joinHostUrl,guestJoinUrl,audienceJoinUrl,audienceUnionUrl,wcallMonitorUrl,eventId,confType,accessNumbers,btplRole,thirdConfId) VALUES ('239816817','95503974','201501000037705193','201501000037705194','https://stest.qsh1.cn/a/HV2GkXD29153','https://ntest.qsh1.cn/k/mLeyHKKu9re','https://stest.qsh1.cn/a/GV2GkX3C4163','https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgODeUYP4adFq0lkE2_OnlVb5d25p3aU1DP263VGa6Og0','https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgFEoJJ-bWCLvHngnwjHAgJUkY-Eoic6Htt7g-7VbPvq0','https://rlive1uat.rmeet.com.cn/activity/gefjbGl','',635497,4,'null','','')  on duplicate key update confType=4`,
		//	out: `merge into conf_infos as t using dual on t.conferenceId = '239816817' and t.eventId = 635497 when matched then update set t.confType = 4 when not matched then insert (conferenceId, billingCode, pcode1, pcode2, hostJoinUrl, attendeeJoinUrl, joinHostUrl, guestJoinUrl, audienceJoinUrl, audienceUnionUrl, wcallMonitorUrl, eventId, confType, accessNumbers, btplRole, thirdConfId) values ('239816817', '95503974', '201501000037705193', '201501000037705194', 'https://stest.qsh1.cn/a/HV2GkXD29153', 'https://ntest.qsh1.cn/k/mLeyHKKu9re', 'https://stest.qsh1.cn/a/GV2GkX3C4163', 'https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgODeUYP4adFq0lkE2_OnlVb5d25p3aU1DP263VGa6Og0', 'https://ntest.qsh1.cn/k/mLeyHKKu9re?jointid=nWMx-hSUydF8PyUXS_WWgFEoJJ-bWCLvHngnwjHAgJUkY-Eoic6Htt7g-7VbPvq0', 'https://rlive1uat.rmeet.com.cn/activity/gefjbGl', '', 635497, 4, 'null', '', '')`,
		//},
		//{
		//	in:  "update meet_stop_job  set `mark` = '172.10.157.179',`updateTime`='2023-07-13 00:00:00' where stopTime <= 1689177600 and serverUrl='http://uniform.quanshi.com'",
		//	out: `update meet_stop_job  set "mark" = '172.10.157.179',"updateTime"='2023-07-13 00:00:00' where stopTime <= 1689177600 and serverUrl='http://uniform.quanshi.com'`,
		//},
		//{
		//	in:  "delete from meet_conference_extrainfo where `conferenceId` = '239816811'",
		//	out: `delete from meet_conference_extrainfo where "conferenceId" = '239816811'`,
		//},
		// test convert ddl
		{
			in:  "CREATE TABLE IF NOT EXISTS `baas_file` (\n  `id` int(8) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT '自增ID',\n  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\n  `is_del` INTEGER(4) NOT NULL DEFAULT '0',\n  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n  `name` varchar(255) NOT NULL DEFAULT '' COMMENT '文件名',\n  `path` varchar(255) NOT NULL DEFAULT '' COMMENT '存储路径',\n  `md5` varchar(32) NOT NULL DEFAULT '' COMMENT 'md5值',\n  `size` int(16) NOT NULL DEFAULT '0' COMMENT '文件大小',\n  `suffix` varchar(255) NOT NULL COMMENT '文件类型',\n  PRIMARY KEY (`id`),\n  KEY `ix_md5_name` (`md5`,`name`)\n) ENGINE=InnoDB AUTO_INCREMENT=493 DEFAULT CHARSET=utf8 COMMENT='文件管理表'",
			out: "",
		},
		// test dm ddl
		{
			in:  "CREATE TABLE \"baas-go\".\"baas_file\"\n(\n \"id\" BIGINT IDENTITY(1,1) NOT NULL,\n \"created_at\" TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP()),\n \"is_del\" TINYINT DEFAULT (0),\n \"updated_at\" TIMESTAMP(0) DEFAULT (CURRENT_TIMESTAMP()),\n \"name\" VARCHAR(255 CHAR),\n \"path\" VARCHAR(255 CHAR),\n \"md5\" VARCHAR(32 CHAR),\n \"size\" INT DEFAULT (0),\n \"suffix\" VARCHAR(255 CHAR)\n);\n",
			out: "",
		},
		// drop table
		{
			in:  "drop table IF EXISTS webcal_live_info",
			out: "",
		},
		// create table
		{
			in:  "CREATE TABLE `bi_input_group` (\n  `bkid` bigint(20) NOT NULL AUTO_INCREMENT,\n  `bknd` bigint(20) DEFAULT NULL,\n  `nver` bigint(20) DEFAULT NULL,\n  `bfail` char(1) DEFAULT NULL,\n  `s_function_id` bigint(20) DEFAULT NULL,\n  `code` varchar(200) DEFAULT NULL,\n  `name` varchar(200) DEFAULT NULL,\n  `bnum` char(1) DEFAULT NULL,\n  `edittime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n  `succeedtime` datetime DEFAULT NULL,\n  `bsys` char(1) DEFAULT NULL,\n  `bhide` char(1) DEFAULT NULL,\n  `b_cpxid` decimal(18,0) DEFAULT NULL,\n  PRIMARY KEY (`bkid`)\n) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4;",
			out: "",
		},
		// fk
		{
			in:  "CREATE TABLE `saas_network` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n  `created_at` datetime(3) DEFAULT NULL,\n  `updated_at` datetime(3) DEFAULT NULL,\n  `is_del` bigint(20) unsigned DEFAULT NULL,\n  `NetworkID` varchar(191) NOT NULL,\n  `user_phone` char(16) NOT NULL,\n  `description` longtext,\n  `BaseCIDR` varchar(191) NOT NULL,\n  PRIMARY KEY (`id`),\n  UNIQUE KEY `uni_saas_network_network_id` (`NetworkID`),\n  UNIQUE KEY `uni_saas_network_base_c_id_r` (`BaseCIDR`),\n  KEY `fk_saas_user_networks` (`user_phone`),\n  CONSTRAINT `fk_saas_user_networks` FOREIGN KEY (`user_phone`) REFERENCES `saas_user` (`phone`)\n) COMMENT '网络表' ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8mb4;",
			out: "",
		},
		// test alter fk
		{
			in:  "alter table `saas` add CONSTRAINT `fk_saas_user_networks` FOREIGN KEY (`user_phone`) REFERENCES `saas_user` (`phone`);",
			out: "",
		},
	}

	converter := NewOracleConverter(
		map[string]map[string][]string{
			"webcal_live_info": {
				"cal_id":        {"cal_id"},
				"CONS134222551": {"channelId"},
			},
			"webcal_entry_recurrencerule": {
				"PRIMARY": {"cal_id"},
			},
			"exchange_bindinfo": {
				"user_id": {"userId"},
				"resId":   {"resId"},
			},
			"notice_status_new": {
				"indexs": {"conversation", "user_id"},
			},
		},
		nil,
		map[string]map[string]int{
			"notice_status_new": {
				"id": 1,
			},
			"live_channel": {
				"id": 1,
			},
			"tb_user_base": {
				"userBasicInfoId": 1,
			},
		},
	)
	for i, tcase := range testCases {
		t.Run(fmt.Sprintf("testcase-%d", i+1), func(t *testing.T) {
			_, oSql, _, err := converter.Convert(tcase.in)
			assert.Nil(t, err)
			assert.Equal(t, tcase.out, oSql[0])
			log.Println(oSql[0])
		})
	}
}

func TestConvertArgs(t *testing.T) {
	sql := "INSERT INTO `live_channel` (`id`, `channelId`, `token`, `hostId`, `eventId`, `conferenceId`, `isRecurrence`, `billingCode`, `channelName`, `summary`, `startTime`, `endTime`, `status`, `password`, `ukey`, `necid`, `nevid`, `lastLiveTime`, `lastEndTime`, `confJoinerCnt`, `liveViewerCnt`, `demandViewerCnt`, `notifyToken`, `notifySubUrl`, `createTime`, `pushUrl`, `pushUrlUpdatedAt`, `version`, `siteId`, `hlsPullUrl`, `httpPullUrl`, `rtmpPullUrl`, `pullUrlUpdatedAt`, `authType`, `expireTime`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	args := []interface{}{
		0, 33823, "3beb2b05cd7b960025dcc49d1e135ff4", 88897156, 6667490, "1322", 0, "30663237", "fll9192的会议1103", "", 1698994800, 1698998400, 0, "", "lkR4", "", "", 0, 0, 0, 0, 0, "", "", "2023-11-03 14:53:06", "", 0, 3, "", "", "", "", 0, 0, 0,
	}
	converter := NewOracleConverter(
		nil,
		nil,
		map[string]map[string]int{
			"notice_status_new": {
				"id": 1,
			},
			"live_channel": {
				"id": 1,
			},
			"tb_user_base": {
				"userBasicInfoId": 1,
			},
		},
	)
	_, newSQL, newArgs, err := converter.Convert(sql, args...)
	assert.Nil(t, err)
	assert.NotEqual(t, args, newArgs)
	t.Logf("newSQL: %s, newArgs: %v", newSQL, newArgs)

	formatSQL, err := Format(newSQL[0], newArgs)
	assert.Nil(t, err)
	t.Logf("formatSQL: %s", formatSQL)

}
