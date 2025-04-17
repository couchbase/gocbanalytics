package cbcolumnar

// Database represents a Columnar database and provides access to Scope.
type Database struct {
	client databaseClient
}

// Database creates a new Database instance.
func (c *Cluster) Database(name string) *Database {
	return &Database{
		client: c.client.Database(name),
	}
}

// Name returns the name of the Database.
func (d *Database) Name() string {
	return d.client.Name()
}
