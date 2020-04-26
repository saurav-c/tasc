package main

type KeyRouterServer struct {
	router []string
}

func NewKeyRouter(ipAddr []string) (*KeyRouterServer, error) {
	return &KeyRouterServer{
		router: ipAddr,
	}, nil
}