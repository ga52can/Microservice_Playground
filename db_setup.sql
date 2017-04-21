-- MySQL dump 10.13  Distrib 5.7.17, for Linux (x86_64)
--
-- Host: localhost    Database: sebis
-- ------------------------------------------------------
-- Server version	5.7.17-0ubuntu0.16.04.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `booked_routes`
--

DROP TABLE IF EXISTS `booked_routes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `booked_routes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `service_provider` enum('drivenow','deutschebahn') DEFAULT NULL,
  `route_id` int(11) DEFAULT NULL,
  `booking_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cities`
--

DROP TABLE IF EXISTS `cities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cities` (
  `city_id` int(11) NOT NULL,
  `city_name` varchar(25) DEFAULT NULL,
  `latitude` float DEFAULT NULL,
  `longitude` float DEFAULT NULL,
  PRIMARY KEY (`city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cities`
--

LOCK TABLES `cities` WRITE;
/*!40000 ALTER TABLE `cities` DISABLE KEYS */;
INSERT INTO `cities` VALUES (1,'Munich',48.1351,11.582),(2,'Frankfurt',50.1109,8.6821),(3,'Berlin',52.52,13.405);
/*!40000 ALTER TABLE `cities` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `deutschebahn_routes`
--

DROP TABLE IF EXISTS `deutschebahn_routes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `deutschebahn_routes` (
  `route_id` int(11) NOT NULL AUTO_INCREMENT,
  `origin` int(11) DEFAULT NULL,
  `destination` int(11) DEFAULT NULL,
  `traveldate` datetime DEFAULT NULL,
  `partner` varchar(30) DEFAULT NULL,
  `cost` int(11) DEFAULT NULL,
  PRIMARY KEY (`route_id`),
  KEY `origin` (`origin`),
  KEY `destination` (`destination`),
  CONSTRAINT `deutschebahn_routes_ibfk_1` FOREIGN KEY (`origin`) REFERENCES `cities` (`city_id`),
  CONSTRAINT `deutschebahn_routes_ibfk_2` FOREIGN KEY (`destination`) REFERENCES `cities` (`city_id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `deutschebahn_routes`
--

LOCK TABLES `deutschebahn_routes` WRITE;
/*!40000 ALTER TABLE `deutschebahn_routes` DISABLE KEYS */;
INSERT INTO `deutschebahn_routes` VALUES (1,3,2,'2017-01-01 07:00:00','DB',60),(2,2,1,'2017-01-07 10:00:00','TGV',75),(3,3,1,'2017-02-05 12:00:00','RailJet',85),(4,2,1,'2017-02-04 14:00:00','DB',65),(5,1,3,'2017-01-01 08:00:00','DB',20),(6,3,1,'2017-01-07 09:00:00','TGV',75),(7,3,2,'2017-02-05 12:00:00','RailJet',85),(8,3,1,'2017-02-04 15:00:00','DB',95);
/*!40000 ALTER TABLE `deutschebahn_routes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `drivenow_routes`
--

DROP TABLE IF EXISTS `drivenow_routes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `drivenow_routes` (
  `route_id` int(11) NOT NULL AUTO_INCREMENT,
  `origin` int(11) DEFAULT NULL,
  `destination` int(11) DEFAULT NULL,
  `traveldate` datetime DEFAULT NULL,
  `partner` varchar(30) DEFAULT NULL,
  `cost` int(11) DEFAULT NULL,
  PRIMARY KEY (`route_id`),
  KEY `origin` (`origin`),
  KEY `destination` (`destination`),
  CONSTRAINT `drivenow_routes_ibfk_1` FOREIGN KEY (`origin`) REFERENCES `cities` (`city_id`),
  CONSTRAINT `drivenow_routes_ibfk_2` FOREIGN KEY (`destination`) REFERENCES `cities` (`city_id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `drivenow_routes`
--

LOCK TABLES `drivenow_routes` WRITE;
/*!40000 ALTER TABLE `drivenow_routes` DISABLE KEYS */;
INSERT INTO `drivenow_routes` VALUES (1,1,2,'2017-01-02 07:00:00','Chris',30),(2,1,2,'2017-01-03 10:00:00','Ana',35),(3,1,3,'2017-02-03 12:00:00','John',25),(4,2,3,'2017-02-03 14:00:00','Arnold',45);
/*!40000 ALTER TABLE `drivenow_routes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `user_roles`
--

DROP TABLE IF EXISTS `user_roles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_roles` (
  `user_role_id` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(45) NOT NULL,
  `role` varchar(45) NOT NULL,
  PRIMARY KEY (`user_role_id`),
  UNIQUE KEY `uni_username_role` (`role`,`username`),
  KEY `fk_username_idx` (`username`),
  CONSTRAINT `fk_username` FOREIGN KEY (`username`) REFERENCES `users` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_roles`
--

LOCK TABLES `user_roles` WRITE;
/*!40000 ALTER TABLE `user_roles` DISABLE KEYS */;
INSERT INTO `user_roles` VALUES (1,'user','ROLE_ADMIN');
/*!40000 ALTER TABLE `user_roles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `username` varchar(45) NOT NULL,
  `password` varchar(45) NOT NULL,
  `enabled` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` VALUES ('user','password',1);
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2017-04-21 22:35:56
