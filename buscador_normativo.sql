CREATE DATABASE  IF NOT EXISTS `buscador_normativo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
USE `buscador_normativo`;
-- MySQL dump 10.13  Distrib 8.0.20, for Win64 (x86_64)
--
-- Host: 127.0.0.1    Database: buscador_normativo
-- ------------------------------------------------------
-- Server version	8.0.20

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `anexos`
--

DROP TABLE IF EXISTS `anexos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `anexos` (
  `id_anexo` int NOT NULL AUTO_INCREMENT,
  `id_documento` int NOT NULL,
  `nombre_anexo` mediumtext,
  `texto_anexo` mediumtext,
  `ruta_archivo` text,
  `embedding_completo` blob,
  `embedding_texto` blob,
  PRIMARY KEY (`id_anexo`),
  KEY `id_documento` (`id_documento`),
  CONSTRAINT `anexos_ibfk_1` FOREIGN KEY (`id_documento`) REFERENCES `documentos` (`id_documento`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `anexos`
--

LOCK TABLES `anexos` WRITE;
/*!40000 ALTER TABLE `anexos` DISABLE KEYS */;
/*!40000 ALTER TABLE `anexos` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `articulos`
--

DROP TABLE IF EXISTS `articulos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `articulos` (
  `id_articulo` int NOT NULL AUTO_INCREMENT,
  `id_documento` int NOT NULL,
  `numero_articulo` varchar(50) DEFAULT NULL,
  `texto_articulo` mediumtext,
  `embedding_articulo` blob,
  PRIMARY KEY (`id_articulo`),
  KEY `id_documento` (`id_documento`),
  CONSTRAINT `articulos_ibfk_1` FOREIGN KEY (`id_documento`) REFERENCES `documentos` (`id_documento`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `articulos`
--

LOCK TABLES `articulos` WRITE;
/*!40000 ALTER TABLE `articulos` DISABLE KEYS */;
/*!40000 ALTER TABLE `articulos` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `documentos`
--

DROP TABLE IF EXISTS `documentos`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `documentos` (
  `id_documento` int NOT NULL AUTO_INCREMENT,
  `nombre_regulacion` mediumtext,
  `ambito_aplicacion` enum('Federal','Estatal','Municipal') DEFAULT NULL,
  `tipo_de_ordenamiento` enum('Acuerdo','Aviso','Bases','Calendario','Circular','Código','Constitución','Convenio','Convocatoria','Criterios','Declaratoria','Decreto','Directiva','Disposiciones','Estatuto','Exención de MIR','Guía','Ley','Lineamientos','Lista','Manual','Metodología','Norma Oficial Mexicana','Normas','Presupuesto','Procedimiento','Programa','Reglamento','Reglas','Resolución','Otros') DEFAULT NULL,
  `fecha_publicacion` date DEFAULT NULL,
  `emisor` varchar(255) DEFAULT NULL,
  `ruta_archivo` text,
  `embedding_completo` blob,
  `embedding_nombre` blob,
  `embedding_ambito` blob,
  `embedding_tipo` blob,
  `embedding_emisor` blob,
  PRIMARY KEY (`id_documento`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `documentos`
--

LOCK TABLES `documentos` WRITE;
/*!40000 ALTER TABLE `documentos` DISABLE KEYS */;
/*!40000 ALTER TABLE `documentos` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `modificaciones`
--

DROP TABLE IF EXISTS `modificaciones`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `modificaciones` (
  `id_modificacion` int NOT NULL AUTO_INCREMENT,
  `id_documento` int NOT NULL,
  `id_articulo` int DEFAULT NULL,
  `nombre_regulacion` mediumtext,
  `tipo_modificacion` enum('Reforma','Adición','Derogación','Corrección','Otra') DEFAULT NULL,
  `texto_modificacion` mediumtext,
  `fecha_publicacion` date DEFAULT NULL,
  `fuente` text,
  `embedding_completo` blob,
  `embedding_texto_modificacion` blob,
  PRIMARY KEY (`id_modificacion`),
  KEY `id_documento` (`id_documento`),
  KEY `id_articulo` (`id_articulo`),
  CONSTRAINT `modificaciones_ibfk_1` FOREIGN KEY (`id_documento`) REFERENCES `documentos` (`id_documento`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `modificaciones_ibfk_2` FOREIGN KEY (`id_articulo`) REFERENCES `articulos` (`id_articulo`) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `modificaciones`
--

LOCK TABLES `modificaciones` WRITE;
/*!40000 ALTER TABLE `modificaciones` DISABLE KEYS */;
/*!40000 ALTER TABLE `modificaciones` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-07-31 22:46:39
