package jdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}

	Options struct {
		Logger
	}
)

// New create a new instance of Driver
func New(dir string, opt *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := Options{}

	if opt != nil {
		opts = *opt
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("%s already exists", dir)
		return &driver, nil
	}

	opts.Logger.Debug("creating %s database", dir)

	return &driver, os.MkdirAll(dir, 0755)
}

func (d *Driver) Write(collection, identifier string, v interface{}) (string, error) {
	if collection == "" {
		return "", fmt.Errorf("missing collection, no place to save data")
	}

	if identifier == "" {
		return "", fmt.Errorf("missing identifier")
	}

	return d.doWrite(collection, identifier, v)
}

func (d *Driver) doWrite(collection, ID string, v interface{}) (string, error) {
	mutex := d.getMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, ID+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return ID, err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return ID, err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return ID, err
	}

	d.log.Info("done creating: %s", ID)
	return ID, os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, identifier string) (string, error) {
	if collection == "" {
		return "", fmt.Errorf("missing collection, no place to get data")
	}

	if identifier == "" {
		return "", fmt.Errorf("missing ID, no identifier to get data")
	}

	record := filepath.Join(d.dir, collection, identifier)

	if _, err := stat(record); err != nil {
		return "", err
	}

	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("missing collection, no place to get data")
	}

	var records []string

	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Update(collection, ID string, v interface{}) (string, error) {
	if err := d.doDelete(collection, ID); err != nil {
		return ID, err
	}

	return d.doWrite(collection, ID, v)
}

func (d *Driver) Delete(collection, ID string) error {
	return d.doDelete(collection, ID)
}

func (d *Driver) doDelete(collection, ID string) error {
	path := filepath.Join(collection, ID)
	mutex := d.getMutex(collection)

	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch file, err := stat(dir); {
	case file == nil, err != nil:
		return fmt.Errorf("unable to find directory %q", path)
	case file.Mode().IsDir():
		return os.RemoveAll(dir)
	case file.Mode().IsRegular():
		os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) getMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (file os.FileInfo, err error) {
	if file, err = os.Stat(path); os.IsNotExist(err) {
		file, err = os.Stat(path + ".json")
	}
	return
}
