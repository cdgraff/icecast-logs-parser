CREATE TABLE `icecast_logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `datetime_start` datetime NOT NULL,
  `datetime_end` datetime DEFAULT NULL,
  `ip` varchar(20) NOT NULL,
  `country_code` varchar(4) DEFAULT NULL,
  `mount` varchar(90) NOT NULL,
  `status_code` int(11) DEFAULT NULL,
  `duration` int(11) DEFAULT NULL,
  `sent_bytes` int(11) DEFAULT NULL,
  `agent` varchar(200) DEFAULT NULL,
  `referer` varchar(400) DEFAULT NULL,
  `server` varchar(50) DEFAULT NULL,
  `user` varchar(20) DEFAULT NULL,
  `pass` varchar(20) DEFAULT NULL,
   PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
