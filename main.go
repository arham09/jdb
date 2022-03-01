package jdb

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/google/uuid"
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

func (d *Driver) Write(collection string, v interface{}) (string, error) {
	if collection == "" {
		return "", fmt.Errorf("missing collection, no place to save data")
	}

	mutex := d.getMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	ID := uuid.New().String()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, ID+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}

	value := reflect.ValueOf(v).Elem()
	field := value.FieldByName("ID")
	if field.IsValid() {
		field.SetString(ID)
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return "", err
	}

	b = append(b, byte('\n'))

	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return "", err
	}

	return ID, os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection, ID string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("missing collection, no place to get data")
	}

	if ID == "" {
		return fmt.Errorf("missing ID, no identifier to get data")
	}

	record := filepath.Join(d.dir, collection, ID)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
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

func (d *Driver) Delete(collection, ID string) error {
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

type User struct {
	ID   string
	Name string
	Age  int
}

// func main() {
// 	dir := "./db"

// 	db, err := New(dir, nil)
// 	if err != nil {
// 		fmt.Println("ERROR: ", err)
// 		panic(err)
// 	}

// 	users := []User{
// 		{
// 			Name: "Andra",
// 			Age:  10,
// 		},
// 		{
// 			Name: "Anggun",
// 			Age:  15,
// 		},
// 	}

// 	for _, user := range users {
// 		db.Write("users", &user)
// 	}
// }
