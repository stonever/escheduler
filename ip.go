package escheduler

import (
	"fmt"
	"net"
)

// GetLocalIP get local ip
func GetLocalIP() (ipv4 string, err error) {

	var (
		addrs []net.Addr
		addr  net.Addr
		ipNet *net.IPNet
		ok    bool
	)

	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}

	for _, addr = range addrs {
		// check ip address
		if ipNet, ok = addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// only ipv4 accepted
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = fmt.Errorf("ipv4 not found")
	return
}
