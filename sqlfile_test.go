package sqlfs

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

var (
	testCfg *mysql.Config
	testDB  *sql.DB
)

func TestCreateHasDropTable(t *testing.T) {
	assert := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	assert.NoError(createTable(testDB, fn))
	has, e := HasTable(testDB, fn)
	assert.NoError(e)
	assert.True(has)
	assert.NoError(DropTable(testDB, fn))
}

func TestWriterCreate(t *testing.T) {
	assert := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())
	w, e := Create(testDB, fn)
	assert.NoError(e)
	assert.NotNil(w)
	defer w.Close()

	has, e1 := HasTable(testDB, fn)
	assert.NoError(e1)
	assert.True(has)

	assert.NoError(DropTable(testDB, fn))
}

func writeAndRead(t *testing.T, block, size int) {
	a := assert.New(t)

	fn := fmt.Sprintf("sqlfs.unitest%d", rand.Int())

	w, e := Create(testDB, fn)
	a.NoError(e)
	a.NotNil(w)

	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte('x')
	}
	for b := 0; b < block; b++ {
		n, e := w.Write(buf)
		a.NoError(e)
		a.Equal(size, n)
	}
	a.NoError(w.Close())

	r, e := Open(testDB, fn)
	a.NoError(e)
	a.NotNil(r)

	for b := 0; b < block; b++ {
		n, e := r.Read(buf)
		a.NoError(e)
		a.Equal(size, n)
		for i := 0; i < size; i++ {
			a.Equal(byte('x'), buf[i])
		}
	}

	a.NoError(r.Close())
	a.NoError(DropTable(testDB, fn))
}

func TestWriteAndRead(t *testing.T) {
	for b := 0; b <= 100; b += 50 {
		t.Logf("TestWriteAndRead: writeAndRead(t, %d, %d)\n", b, 100-b)
		writeAndRead(t, b, 100-b)
	}
}

func TestMain(m *testing.M) {
	testCfg = &mysql.Config{
		User:   "root",
		Passwd: "root",
		Addr:   "localhost:3306",
	}
	db, e := sql.Open("mysql", testCfg.FormatDSN())
	if e != nil {
		log.Panicf("TestMain cannot connect to MySQL: %q.\n"+
			"Please run MySQL server as in example/churn/README.md.", e)
	}
	testDB = db

	defer testDB.Close()
	os.Exit(m.Run())
}
