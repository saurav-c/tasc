package main

func InsertParticularIndex(list []*keyVersion, kv *keyVersion) []*keyVersion {
	if len(list) == 0 {
		return []*keyVersion{kv}
	}
	index := FindIndex(list, kv)
	return append(append(list[:index], kv), list[index:]...)
}

func FindIndex(list []*keyVersion, kv *keyVersion) int {
	startList := 0
	endList := len(list) - 1
	midPoint := 0
	for true {
		midPoint = (startList + endList) / 2
		midElement := list[midPoint]
		if midElement.CommitTS == kv.CommitTS {
			return midPoint
		} else if kv.CommitTS < list[startList].CommitTS {
			return startList
		} else if kv.CommitTS > list[endList].CommitTS {
			return endList
		} else if midElement.CommitTS > kv.CommitTS {
			startList = midPoint
		} else {
			endList = midPoint
		}
	}
}

type keyVersion struct {
	tid      string
	CommitTS string
}

type KeyNode struct {
	keyVersionIndex        map[string][]*keyVersion
	pendingKeyVersionIndex map[string][]*keyVersion
	committedTxnCache      map[string][]string
	readCache              map[string]byte
}

func NewKeyNode(KeyNodeIP string) {
	panic("implement me")
}
