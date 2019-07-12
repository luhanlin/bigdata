SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for warn_message
-- ----------------------------
DROP TABLE IF EXISTS `warn_message`;
CREATE TABLE `warn_message` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `alarmRuleid` varchar(255) DEFAULT NULL,
  `alarmType` varchar(255) DEFAULT NULL,
  `sendType` varchar(255) DEFAULT NULL,
  `sendMobile` varchar(255) DEFAULT NULL,
  `sendEmail` varchar(255) DEFAULT NULL,
  `sendStatus` varchar(255) DEFAULT NULL,
  `senfInfo` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `hitTime` datetime DEFAULT NULL,
  `checkinTime` datetime DEFAULT NULL,
  `isRead` varchar(255) DEFAULT NULL,
  `readAccounts` varchar(255) DEFAULT NULL,
  `alarmaccounts` varchar(255) DEFAULT NULL,
  `accountid` varchar(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=31 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of warn_message
-- ----------------------------
