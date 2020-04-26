package main

type TxnRouterServer struct {
	router []string
}

func NewTxnRouter(ipAddr []string) (*TxnRouterServer, error) {
	return &TxnRouterServer{
		router: ipAddr,
	}, nil
}