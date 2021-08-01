package apimodel

type Filters struct {
	Entity string `json:"entity,omitempty"`
	Period string `json:"period,omitempty"`
	Start  int64 `json:"start,omitempty"`
	End    int64 `json:"end,omitempty"`
	From   int64 `json:"from,omitempty"`
	Limit  int64  `json:"limit,omitempty"`
}
