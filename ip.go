package main

import (
	"fmt"
	"net"
)

// GetLocalIP 获取本地ip
func GetLocalIP() (ipv4 string, err error) {

	var (
		addrs []net.Addr
		addr  net.Addr
		ipNet *net.IPNet
		ok    bool
	)

	// 获取所有网卡
	if addrs, err = net.InterfaceAddrs(); err != nil {
		return
	}

	// 取第一个非lo的网卡
	for _, addr = range addrs {
		// addr是一个接口
		// 使用类型断言
		// 判断是否为ip地址 有可能是unix socket
		if ipNet, ok = addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			// 只接受ipv4
			if ipNet.IP.To4() != nil {
				ipv4 = ipNet.IP.String()
				return
			}
		}
	}

	err = fmt.Errorf("ipNet not found")
	return
}
