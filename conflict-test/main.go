package main

import (
	"database/sql"
	"flag"
	"fmt"
	"sort"
	"os"
	"time"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/Sirupsen/logrus"
	"github.com/tealeg/xlsx"
)

var query = "update test set id=id+1"

var wg sync.WaitGroup

var (
	host      = flag.String("h", "127.0.0.1", "host address")
	port      = flag.Int("P", 4000, "host port")
	user      = flag.String("u", "root", "set user of the database")
	password  = flag.String("p", "", "set password of the database ")
	dbname    = flag.String("D", "test", "set the default database name")
	tablename = flag.String("T", "test", "the default table name")
	parallel  = flag.Int("w", 10, "set worker number")
	taskCount = flag.Int("t", 100, "set task number")

	tidbVersion = os.Getenv("TIDB_VERSION")
	tikvVersion = os.Getenv("TIKV_VERSION")
	pdVersion   = os.Getenv("PD_VERSION")
)

func createDB(user string, password string, host string, port int, dbname string) (*sql.DB, error) {
	dbaddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", user, password, host, port, dbname)
	db, err := sql.Open("mysql", dbaddr)
	if err != nil {
		return nil, err
	}
	return db, nil
}

type tester struct {
	db         *sql.DB
	taskChan   chan *string
	resultChan chan float64
}

func (t *tester) run() {
	defer wg.Done()
	log.Debug("worker start.")
	for {
		select {
		case query := <-t.taskChan:
			start := time.Now()
			for {
				if _, err := t.db.Exec(*query); err == nil {
					break
				}
			}
			end := time.Since(start).Seconds()
			select {
			case t.resultChan <- end:
			default:
				log.Fatal("cant put result, quit")
			}
		default:
			log.Debug("worker quit")
			return
		}
	}
}

func addTask(c chan *string) {
	i := 0
	for i = 0; i < *taskCount; i++ {
		c <- &query
	}

	if i != *taskCount {
		log.Fatalf("only add %d tasks, exit.", i)
	}

	log.Info("finish to add tasks.")
}

func exportResult(c chan float64, totalTime float64) {
	var res []float64
	for elapse := range c {
		res = append(res, elapse)
	}

	sort.Float64s(res)

	var file *xlsx.File
	var sheet *xlsx.Sheet
	var row *xlsx.Row
	var cell *xlsx.Cell
	var err error

	file = xlsx.NewFile()

	//log.Info("open new xlsx file.")
	sheet, err = file.AddSheet("conflict test")
	if err != nil {
		log.Fatal(err)
	}

	//log.Info("add titles.")
	row = sheet.AddRow()
	cell = row.AddCell()
	title := fmt.Sprintf("worker-%d-task-%d", *parallel, *taskCount)
	cell.SetString(title)
	for _, elapse := range res {
		row = sheet.AddRow()
		cell = row.AddCell()
		cell.SetFloat(elapse * 1000)
	}
	//log.Info("add all rows.")
	fileName := fmt.Sprintf("%s-time-%.6gs.xlsx", title, totalTime)
	err = file.Save(fileName)
	//log.Info("save file.")
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	flag.Parse()

	taskChan := make(chan *string, *taskCount)
	resultChan := make(chan float64, *taskCount)

	addTask(taskChan)
	fmt.Printf("=== test start ===\n"+
		"query is \"%s\"\n"+
		"task count is %d\n"+
		"worker number is %d\n",
		query, *taskCount, *parallel)
	start := time.Now()
	for i := 0; i < *parallel; i++ {
		db, err := createDB(*user, *password, *host, *port, *dbname)
		if err != nil {
			log.Fatal(err)
		}
		defer db.Close()

		tester := tester{
			db:         db,
			taskChan:   taskChan,
			resultChan: resultChan,
		}

		wg.Add(1)
		go tester.run()
	}
	wg.Wait()
	close(resultChan)
	end := time.Since(start).Seconds()

	exportResult(resultChan, end)

	fmt.Printf("task time is %.6gs\n"+
		"=== test end ===\n", end)
}
