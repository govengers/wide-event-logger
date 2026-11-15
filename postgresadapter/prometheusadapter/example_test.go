package prometheusadapter_test

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/govengers/wide-event-logger/postgresadapter/prometheusadapter"
)

func ExampleNew() {
	registry := prometheus.NewRegistry() // the application's own registry, served on /metrics
	metrics, err := prometheusadapter.New(&prometheusadapter.Options{Registerer: registry, Namespace: "orders"})
	if err != nil {
		fmt.Println(err)
		return
	}
	_ = metrics // postgresadapter.Options{Observer: metrics, ...}
	fmt.Println("registered")
	// Output: registered
}
