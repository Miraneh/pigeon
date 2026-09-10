package main

import (
	"github.com/gin-gonic/gin"
	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"
	"gorm.io/gorm"

	_ "pigeon/docs"
)

func newRouter(db *gorm.DB) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", pingHandler(db))
	r.GET("/swagger/*any", ginswagger.WrapHandler(swaggerfiles.Handler))

	return r
}
