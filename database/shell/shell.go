// +build go1.9

package shell

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4/database"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/lib/pq"
)

func init() {
	shell := Shell{}
	database.Register("shell", &shell)
}

var DefaultMigrationsTable = "schema_migrations"

var (
	ErrNilConfig      = fmt.Errorf("no config")
	ErrNoDatabaseName = fmt.Errorf("no database name")
	ErrNoSchema       = fmt.Errorf("no schema")
	ErrDatabaseDirty  = fmt.Errorf("database is dirty")
)

type Config struct {
	MigrationsTable  string
	DatabaseName     string
	SchemaName       string
	StatementTimeout time.Duration
}

type Shell struct {
	// Locking and unlocking need to use the same connection
	conn     *sql.Conn
	db       *sql.DB
	isLocked bool

	// Open and WithInstance need to guarantee that config is never nil
	config *Config
}

func WithInstance(instance *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := instance.Ping(); err != nil {
		return nil, err
	}

	if config.DatabaseName == "" {
		query := `SELECT CURRENT_DATABASE()`
		var databaseName string
		if err := instance.QueryRow(query).Scan(&databaseName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(databaseName) == 0 {
			return nil, ErrNoDatabaseName
		}

		config.DatabaseName = databaseName
	}

	if config.SchemaName == "" {
		query := `SELECT CURRENT_SCHEMA()`
		var schemaName string
		if err := instance.QueryRow(query).Scan(&schemaName); err != nil {
			return nil, &database.Error{OrigErr: err, Query: []byte(query)}
		}

		if len(schemaName) == 0 {
			return nil, ErrNoSchema
		}

		config.SchemaName = schemaName
	}

	if len(config.MigrationsTable) == 0 {
		config.MigrationsTable = DefaultMigrationsTable
	}

	conn, err := instance.Conn(context.Background())

	if err != nil {
		return nil, err
	}

	px := &Shell{
		conn:   conn,
		db:     instance,
		config: config,
	}

	if err := px.ensureVersionTable(); err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Shell) Open(url string) (database.Driver, error) {
	purl, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	postgresDsn := purl.Query().Get("x-postgres-dsn")

	db, err := sql.Open("postgres", postgresDsn)
	if err != nil {
		return nil, err
	}

	migrationsTable := purl.Query().Get("x-migrations-table")
	statementTimeoutString := purl.Query().Get("x-statement-timeout")
	statementTimeout := 0
	if statementTimeoutString != "" {
		statementTimeout, err = strconv.Atoi(statementTimeoutString)
		if err != nil {
			return nil, err
		}
	}

	px, err := WithInstance(db, &Config{
		DatabaseName:     purl.Path,
		MigrationsTable:  migrationsTable,
		StatementTimeout: time.Duration(statementTimeout) * time.Millisecond,
	})

	if err != nil {
		return nil, err
	}

	return px, nil
}

func (p *Shell) Close() error {
	connErr := p.conn.Close()
	dbErr := p.db.Close()
	if connErr != nil || dbErr != nil {
		return fmt.Errorf("conn: %v, db: %v", connErr, dbErr)
	}
	return nil
}

// https://www.postgresql.org/docs/9.6/static/explicit-locking.html#ADVISORY-LOCKS
func (p *Shell) Lock() error {
	if p.isLocked {
		return database.ErrLocked
	}

	aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.SchemaName)
	if err != nil {
		return err
	}

	// This will wait indefinitely until the lock can be acquired.
	query := `SELECT pg_advisory_lock($1)`
	if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
		return &database.Error{OrigErr: err, Err: "try lock failed", Query: []byte(query)}
	}

	p.isLocked = true
	return nil
}

func (p *Shell) Unlock() error {
	if !p.isLocked {
		return nil
	}

	aid, err := database.GenerateAdvisoryLockId(p.config.DatabaseName, p.config.SchemaName)
	if err != nil {
		return err
	}

	query := `SELECT pg_advisory_unlock($1)`
	if _, err := p.conn.ExecContext(context.Background(), query, aid); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	p.isLocked = false
	return nil
}

func (p *Shell) Run(migration io.Reader) error {
	tempDir, err := ioutil.TempDir("", "migration_shell")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tempDir)

	migr, err := ioutil.ReadAll(migration)
	if err != nil {
		return err
	}

	executablePath := path.Join(tempDir, "migration")
	err = ioutil.WriteFile(executablePath, migr, 0700)
	if err != nil {
		return err
	}

	err = exec.Command(executablePath).Run()
	if err != nil {
		return err
	}

	return nil
}

func computeLineFromPos(s string, pos int) (line uint, col uint, ok bool) {
	// replace crlf with lf
	s = strings.Replace(s, "\r\n", "\n", -1)
	// pg docs: pos uses index 1 for the first character, and positions are measured in characters not bytes
	runes := []rune(s)
	if pos > len(runes) {
		return 0, 0, false
	}
	sel := runes[:pos]
	line = uint(runesCount(sel, newLine) + 1)
	col = uint(pos - 1 - runesLastIndex(sel, newLine))
	return line, col, true
}

const newLine = '\n'

func runesCount(input []rune, target rune) int {
	var count int
	for _, r := range input {
		if r == target {
			count++
		}
	}
	return count
}

func runesLastIndex(input []rune, target rune) int {
	for i := len(input) - 1; i >= 0; i-- {
		if input[i] == target {
			return i
		}
	}
	return -1
}

func (p *Shell) SetVersion(version int, dirty bool) error {
	tx, err := p.conn.BeginTx(context.Background(), &sql.TxOptions{})
	if err != nil {
		return &database.Error{OrigErr: err, Err: "transaction start failed"}
	}

	query := `TRUNCATE ` + pq.QuoteIdentifier(p.config.MigrationsTable)
	if _, err := tx.Exec(query); err != nil {
		if errRollback := tx.Rollback(); errRollback != nil {
			err = multierror.Append(err, errRollback)
		}
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	// Also re-write the schema version for nil dirty versions to prevent
	// empty schema version for failed down migration on the first migration
	// See: https://github.com/golang-migrate/migrate/issues/330
	if version >= 0 || (version == database.NilVersion && dirty) {
		query = `INSERT INTO ` + pq.QuoteIdentifier(p.config.MigrationsTable) +
			` (version, dirty) VALUES ($1, $2)`
		if _, err := tx.Exec(query, version, dirty); err != nil {
			if errRollback := tx.Rollback(); errRollback != nil {
				err = multierror.Append(err, errRollback)
			}
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}

	if err := tx.Commit(); err != nil {
		return &database.Error{OrigErr: err, Err: "transaction commit failed"}
	}

	return nil
}

func (p *Shell) Version() (version int, dirty bool, err error) {
	query := `SELECT version, dirty FROM ` + pq.QuoteIdentifier(p.config.MigrationsTable) + ` LIMIT 1`
	err = p.conn.QueryRowContext(context.Background(), query).Scan(&version, &dirty)
	switch {
	case err == sql.ErrNoRows:
		return database.NilVersion, false, nil

	case err != nil:
		if e, ok := err.(*pq.Error); ok {
			if e.Code.Name() == "undefined_table" {
				return database.NilVersion, false, nil
			}
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}

	default:
		return version, dirty, nil
	}
}

func (p *Shell) Drop() (err error) {
	// select all tables in current schema
	query := `SELECT table_name FROM information_schema.tables WHERE table_schema=(SELECT current_schema()) AND table_type='BASE TABLE'`
	tables, err := p.conn.QueryContext(context.Background(), query)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer func() {
		if errClose := tables.Close(); errClose != nil {
			err = multierror.Append(err, errClose)
		}
	}()

	// delete one table after another
	tableNames := make([]string, 0)
	for tables.Next() {
		var tableName string
		if err := tables.Scan(&tableName); err != nil {
			return err
		}
		if len(tableName) > 0 {
			tableNames = append(tableNames, tableName)
		}
	}

	if len(tableNames) > 0 {
		// delete one by one ...
		for _, t := range tableNames {
			query = `DROP TABLE IF EXISTS ` + pq.QuoteIdentifier(t) + ` CASCADE`
			if _, err := p.conn.ExecContext(context.Background(), query); err != nil {
				return &database.Error{OrigErr: err, Query: []byte(query)}
			}
		}
	}

	return nil
}

// ensureVersionTable checks if versions table exists and, if not, creates it.
// Note that this function locks the database, which deviates from the usual
// convention of "caller locks" in the Shell type.
func (p *Shell) ensureVersionTable() (err error) {
	if err = p.Lock(); err != nil {
		return err
	}

	defer func() {
		if e := p.Unlock(); e != nil {
			if err == nil {
				err = e
			} else {
				err = multierror.Append(err, e)
			}
		}
	}()

	query := `CREATE TABLE IF NOT EXISTS ` + pq.QuoteIdentifier(p.config.MigrationsTable) + ` (version bigint not null primary key, dirty boolean not null)`
	if _, err = p.conn.ExecContext(context.Background(), query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return nil
}
