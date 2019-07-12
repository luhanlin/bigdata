SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for tz_rule
-- ----------------------------
DROP TABLE IF EXISTS `tz_rule`;
CREATE TABLE `tz_rule` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `warn_fieldname` varchar(20) DEFAULT NULL,
  `warn_fieldvalue` varchar(255) DEFAULT NULL,
  `publisher` varchar(255) DEFAULT NULL,
  `send_type` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `send_mobile` varchar(255) DEFAULT NULL,
  `send_mail` varchar(255) DEFAULT NULL,
  `send_dingding` varchar(255) DEFAULT NULL,
  `create_time` date DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;

-- ----------------------------
-- Records of tz_rule
-- ----------------------------
INSERT INTO `tz_rule` VALUES ('1', 'phone', '18609765432', 'test', '2', '13724536789', '1782324@qq.com', '32143243', '2019-05-23');
