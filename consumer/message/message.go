package message

type KeyMetric struct {
	Key     string
	Value   string
	Metrics []string
}

type KeyTag struct {
	Key   string
	Value string
	Tags  []string
}

type TagMetric struct {
	Tags    []string
	Metrics []string
}
