package router

import (
	"github.com/gin-gonic/gin"
	swaggerfiles "github.com/swaggo/files"
	ginswagger "github.com/swaggo/gin-swagger"
	"gorm.io/gorm"

	_ "pigeon/docs"
	"pigeon/internal/handler"
)

func New(db *gorm.DB) *gin.Engine {
	r := gin.Default()

	r.GET("/ping", handler.Ping(db))
	r.GET("/swagger/*any", ginswagger.WrapHandler(swaggerfiles.Handler))

	return r
}
