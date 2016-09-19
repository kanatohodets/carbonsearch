package index

type Index interface {
	Query([]string) ([]string, error)
	Name() string
}
