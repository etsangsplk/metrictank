package middleware

import (
	"fmt"
	"github.com/grafana/metrictank/cluster"
	macaron "gopkg.in/macaron.v1"
)

func NodeReady() macaron.Handler {
	return func(c *Context) {
		if !cluster.Manager.IsReady() {
			//fmt.Println("node not ready")
			c.Error(503, "node not ready")
		} else {
			//fmt.Println("node ready")
		}
	}
}
