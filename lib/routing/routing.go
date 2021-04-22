package routing

type RouterManager interface {
	// Looks up the IP Addresses for a list of keys, result received by PULLER
	Lookup(tid string, keys []string) (error)
}

type RoutingResponse struct {
	Addresses map[string][]string
}
