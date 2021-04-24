package common

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func InitLogger(dir string, logName string, level log.Level) (*os.File) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.Mkdir(dir, os.ModePerm)
	}
	file, err := os.OpenFile(dir + "/" + logName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Unable to init logging to file %s", dir + "/" + logName)
	}
	log.SetFormatter(&log.JSONFormatter{PrettyPrint:true})
	log.SetLevel(level)
	log.SetOutput(file)
	return file
}
