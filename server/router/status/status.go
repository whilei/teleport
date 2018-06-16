package batch

import (
	"fmt"
	"github.com/pagarme/teleport/server/router"
	"net/http"
)

type statusRouter struct {
	routes []router.Route
}

func New() *statusRouter {
	r := &statusRouter{}
	r.initRoutes()
	return r
}

func (s *statusRouter) status(w http.ResponseWriter, r *http.Request) error {
	fmt.Fprintln(w, "ok")
	return nil
}

func (b *statusRouter) Routes() []router.Route {
	return b.routes
}

func (b *statusRouter) initRoutes() {
	b.routes = []router.Route{
		router.NewGetRoute("/status", b.status),
	}
}
