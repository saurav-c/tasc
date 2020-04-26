package main

import "fmt"

type RouterServer struct {
	router []string
}

func NewRouter(ipAddr []string) (*RouterServer, error) {
	fmt.Println(ipAddr)
	return &RouterServer{
		router: ipAddr,
	}, nil
}