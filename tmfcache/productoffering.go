package tmfcache

type TMFProductOffering struct {
	TMFGeneralObject
}

func (po *TMFProductOffering) Retrieve() string {
	return po.Name
}
