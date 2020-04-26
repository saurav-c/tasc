package main

type RouterServer struct {
	router []string
}

func NewRouter(ipAddr []string) (*RouterServer, error) {
	return &RouterServer{
		router: ipAddr,
	}, nil
}