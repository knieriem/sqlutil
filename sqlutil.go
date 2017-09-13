package sqlutil

import (
	"errors"
	"net/url"
	"strings"

	"github.com/russross/meddler"
)

type SourceConf struct {
	Driver   string
	Host     string
	User     string
	Password string
	Database string
	Append   []string
}

type DataSource struct {
	DriverName  string
	Name        string
	DisplayName string
	Meddler     *meddler.Database
}

func NewDataSource(c *SourceConf) (*DataSource, error) {
	switch c.Driver {
	case "mssql":
		return newDataSourceMSSQL(c)
	case "":
		return nil, errors.New("data source configuration: empty driver name")
	}
	return nil, errors.New("unknown database driver: " + c.Driver)
}

func newDataSourceMSSQL(c *SourceConf) (*DataSource, error) {
	host := c.Host
	if host == "" {
		host = "localhost"
	}
	user := c.User
	if user == "" {
		user = "sa"
	}
	if c.Password == "" {
		return nil, errors.New("data source configuration: password missing")
	}
	if c.Database == "" {
		return nil, errors.New("data source configuration: database name missing")
	}

	query := url.Values{}
	query.Add("database", c.Database)
	for _, s := range c.Append {
		f := strings.SplitN(s, ":", 2)
		if len(f) != 2 {
			return nil, errors.New("append: missing colon separator")
		}
		query.Add(f[0], f[1])
	}

	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(user, c.Password),
		Host:   host,
		// Path:  instance, // if connecting to an instance instead of a port
		RawQuery: query.Encode(),
	}

	ds := new(DataSource)
	ds.Name = u.String()

	usafe := u
	usafe.User = url.UserPassword(user, "...")
	ds.DisplayName = usafe.String()

	ds.DriverName = c.Driver
	ds.Meddler = meddler.PostgreSQL
	return ds, nil
}
