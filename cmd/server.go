package main

import (
	"context"
	"flag"

	"github.com/guojianyu/swarm-mesh/pkg/server"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "4")
	flag.Parse()
	p := server.NewTunnelServer(":7000")
	p.AddPortMapping(
		9080,
		"test",
		"", //the none will is replaceed to localhost
		8080,
	)
	p.AddPortMapping(
		13306,
		"test",
		"127.0.0.1",
		3306,
	)
	// {
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/server_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/server_key.pem"
	// 	p.WithTLS(ca, cert, key)
	// }

	ctx, _ := context.WithCancel(context.Background())
	p.Run(ctx)

}
