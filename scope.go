package cbcolumnar

// Scope represents a Columnar scope.
type Scope struct {
	client scopeClient
}

// Scope creates a new Scope instance.
func (d *Database) Scope(name string) *Scope {
	return &Scope{
		d.client.Scope(name),
	}
}

// Name returns the name of the Scope.
func (s *Scope) Name() string {
	return s.client.Name()
}
