package main

import (
	"context"
	"flag"

	"github.com/guojianyu/swarm-mesh/pkg/client"

	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)
	flag.Set("v", "4")
	flag.Parse()
	//"39.105.15.153:7000"
	p := client.NewTunnelAgent("localhost:7000", "test")
	ctx, _ := context.WithCancel(context.Background())
	// {
	// 	ca := "D:/workspace/go/src/test/multi-cluster/cert/ca_cert.pem"
	// 	cert := "D:/workspace/go/src/test/multi-cluster/cert/client_cert.pem"
	// 	key := "D:/workspace/go/src/test/multi-cluster/cert/client_key.pem"
	// 	p.WithTLS(ca, cert, key)
	// }
	p.Run(ctx)
}
