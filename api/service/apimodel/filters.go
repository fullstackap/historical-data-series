package apimodel

type Filters struct {
	Entity string `json:"entity,omitempty"`
	Period string `json:"period,omitempty"`
	Start  uint64 `json:"start,omitempty"`
	End    uint64 `json:"end,omitempty"`
	From   uint64 `json:"from,omitempty"`
	Limit  int64  `json:"limit,omitempty"`
}
