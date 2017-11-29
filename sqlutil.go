package sqlutil

import (
	"errors"
	"net/url"
	"strings"

	"github.com/knieriem/meddler"
)

type SourceConf struct {
	Driver      string
	File        string
	Host        string
	User        string
	Password    string
	Database    string
	TablePrefix string
	Append      []string
}

type DataSource struct {
	DriverName      string
	Name            string
	DisplayName     string
	TablePrefix     string
	Meddler         *meddler.Database
	CastPlaceholder bool
}

func NewDataSource(c *SourceConf) (*DataSource, error) {
	switch c.Driver {
	case "mssql":
		return newDataSourceMSSQL(c)
	case "postgres":
		return newDataSourcePostgres(c)
	case "ql":
		return newDataSourceQL(c)
	case "":
		return nil, errors.New("data source configuration: empty driver name")
	}
	return nil, errors.New("unknown database driver: " + c.Driver)
}

func (ds *DataSource) QuoteTable(name string) string {
	return ds.Quote(ds.TablePrefix + name)
}

func (ds *DataSource) Quote(name string) string {
	m := ds.Meddler
	if m.Quote == "" {
		return name
	}
	return m.Quote + name + m.Quote
}

func newDataSourceMSSQL(c *SourceConf) (*DataSource, error) {
	ds, err := newSQLDataSource(c, "sqlserver", "database")
	if err != nil {
		return nil, err
	}
	ds.Meddler = meddler.MSSQL
	return ds, nil
}

func newDataSourcePostgres(c *SourceConf) (*DataSource, error) {
	ds, err := newSQLDataSource(c, "postgres", "")
	if err != nil {
		return nil, err
	}
	ds.Meddler = meddler.PostgreSQL
	return ds, nil
}

func newSQLDataSource(c *SourceConf, serverType, dbQueryParam string) (*DataSource, error) {
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
	path := ""
	if qp := dbQueryParam; qp != "" {
		query.Add(qp, c.Database)
	} else {
		path = c.Database
	}
	for _, s := range c.Append {
		f := strings.SplitN(s, ":", 2)
		if len(f) != 2 {
			return nil, errors.New("append: missing colon separator")
		}
		query.Add(f[0], f[1])
	}

	u := &url.URL{
		Scheme:   serverType,
		User:     url.UserPassword(user, c.Password),
		Host:     host,
		Path:     path,
		RawQuery: query.Encode(),
	}

	ds := new(DataSource)
	ds.Name = u.String()

	usafe := u
	usafe.User = url.UserPassword(user, "...")
	ds.DisplayName = usafe.String()

	ds.DriverName = c.Driver
	ds.TablePrefix = c.TablePrefix
	return ds, nil
}

func newDataSourceQL(c *SourceConf) (*DataSource, error) {
	ds := new(DataSource)
	file := c.File
	if file != "" {
		file = "file://" + file
	}
	ds.Name = file
	ds.DisplayName = file
	ds.DriverName = c.Driver
	ds.TablePrefix = c.TablePrefix
	ds.Meddler = meddler.QL
	ds.CastPlaceholder = true
	return ds, nil
}
