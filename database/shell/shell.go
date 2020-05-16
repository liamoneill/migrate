// +build go1.9

package shell

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"os/exec"
	"path"
	"strconv"
	"time"

	"github.com/golang-migrate/migrate/v4/database"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func init() {
	shell := Shell{}
	database.Register("shell", &shell)
}

type Config struct {
	MigrationsTable string
	Timeout         time.Duration
	Verbose         bool
}

type Shell struct {
	dynamodb *dynamodb.DynamoDB
	config   *Config
}

func (s *Shell) Open(url string) (database.Driver, error) {
	shellURL, err := nurl.Parse(url)
	if err != nil {
		return nil, err
	}

	timeoutString := shellURL.Query().Get("x-timeout")
	timeout := time.Duration(0)
	if timeoutString != "" {
		timeout, err = time.ParseDuration(timeoutString)
		if err != nil {
			return nil, err
		}
	}

	verboseString := shellURL.Query().Get("x-verbose")
	verbose := true
	if verboseString != "" {
		verbose, err = strconv.ParseBool(verboseString)
		if err != nil {
			return nil, err
		}
	}

	dynamodbTable := shellURL.Query().Get("x-dynamodb-table")

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		return nil, err
	}
	dynamodbService := dynamodb.New(sess)

	shell := &Shell{
		dynamodb: dynamodbService,
		config: &Config{
			MigrationsTable: dynamodbTable,
			Timeout:         timeout,
			Verbose:         verbose,
		},
	}

	return shell, nil
}

func (s *Shell) Run(migration io.Reader) error {
	tempDir, err := ioutil.TempDir("", "shell_migration")
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

	ctx := context.Background()
	if s.config.Timeout > time.Duration(0) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.config.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(ctx, executablePath)
	if s.config.Verbose {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	err = cmd.Run()
	if err != nil {
		return err
	}

	return nil
}

func (s *Shell) SetVersion(version int, dirty bool) error {
	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(s.config.MigrationsTable),
		Item: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String("LatestMigrationVersion"),
			},
			"Dirty": {
				BOOL: aws.Bool(dirty),
			},
			"Version": {
				N: aws.String(strconv.Itoa(version)),
			},
		},
	}

	_, err := s.dynamodb.PutItem(putItemInput)
	return err
}

func (s *Shell) Version() (version int, dirty bool, err error) {
	getItemInput := &dynamodb.GetItemInput{
		TableName: aws.String(s.config.MigrationsTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String("LatestMigrationVersion"),
			},
		},
	}

	result, err := s.dynamodb.GetItem(getItemInput)
	if err != nil {
		return
	}

	if _, ok := result.Item["ID"]; !ok {
		version = -1
		dirty = false
		return
	}

	attr, ok := result.Item["Version"]
	if !ok {
		err = errors.New("could not find Version attribute on dynamodb item")
		return
	}

	versionString := attr.N
	if versionString == nil {
		err = errors.New("expected Version attribute to have type N on dynamodb item")
		return
	}

	version, err = strconv.Atoi(*versionString)
	if err != nil {
		err = fmt.Errorf("could not parse Version attribute on dynamodb item: %w", err)
	}

	attr, ok = result.Item["Dirty"]
	if !ok {
		err = errors.New("could not find Dirty attribute on dynamodb item")
		return
	}

	dirtyRef := attr.BOOL
	if dirtyRef == nil {
		err = errors.New("expected Dirty attribute to have type BOOL on dynamodb item")
		return
	}
	dirty = *dirtyRef

	return
}

func (s *Shell) Close() error {
	return nil
}

func (s *Shell) Drop() error {
	return nil
}

func (s *Shell) Lock() error {
	return nil
}

func (s *Shell) Unlock() error {
	return nil
}
