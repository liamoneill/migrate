// +build go1.9

package shell

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	nurl "net/url"
	"os"
	"os/exec"
	"path"
	"strconv"

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
}

type Shell struct {
	dynamodb *dynamodb.DynamoDB
	config   *Config
}

func (p *Shell) Open(url string) (database.Driver, error) {
	shellURL, err := nurl.Parse(url)
	if err != nil {
		return nil, err
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
		},
	}

	return shell, nil
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

func (p *Shell) SetVersion(version int, dirty bool) error {
	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(p.config.MigrationsTable),
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

	_, err := p.dynamodb.PutItem(putItemInput)
	return err
}

func (p *Shell) Version() (version int, dirty bool, err error) {
	getItemInput := &dynamodb.GetItemInput{
		TableName: aws.String(p.config.MigrationsTable),
		Key: map[string]*dynamodb.AttributeValue{
			"ID": {
				S: aws.String("LatestMigrationVersion"),
			},
		},
	}

	result, err := p.dynamodb.GetItem(getItemInput)
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

func (p *Shell) Close() error {
	return nil
}

func (p *Shell) Drop() error {
	return nil
}

func (p *Shell) Lock() error {
	return nil
}

func (p *Shell) Unlock() error {
	return nil
}
