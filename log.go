package main

import "github.com/sirupsen/logrus"

func init() {
	logger.Formatter = &logrus.JSONFormatter{}
}

var (
	logger = logrus.New()
)
