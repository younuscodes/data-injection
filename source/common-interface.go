package storage

import (
	mysql "data-injection/source/rdbms/mysql"
	postgres "data-injection/source/rdbms/postgresql"
	aws_postgresql "data-injection/source/rds/aws_postgresql"
	s3 "data-injection/source/s3"
	solr "data-injection/source/solr"

	log "github.com/sirupsen/logrus"
)

type Myinterface interface {
	Insert()
}

var CommonInterface Myinterface

func Newinterface(s string) Myinterface {
	log.Println("Newinterface triggered....")
	if s == "solr" {
		CommonInterface = solr.Solr
		return CommonInterface
	} else if s == "mysql" {
		CommonInterface = mysql.Mysql
		return CommonInterface
	} else if s == "postgres" {
		CommonInterface = postgres.Postgresql
		return CommonInterface
	} else if s == "rds" {
		CommonInterface = aws_postgresql.RdsPostgresql
		return CommonInterface
	} else if s == "s3" {
		CommonInterface = s3.S3
		return CommonInterface
	}
	return nil
}
