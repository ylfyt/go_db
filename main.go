package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

var db *DB

func test(worker int, wg *sync.WaitGroup) {
	defer wg.Done()
	type Product struct {
		Id          int
		StoreId     int
		Name        string
		Description string
		Price       int
		CreatedAt   time.Time
		Detail      string
	}

	var products []Product
	err := db.Fetch(&products, `SELECT * FROM product`)
	if err != nil {
		panic(err)
	}

	// fmt.Println(worker, ":", len(products))
}

var peek uint64 = 0
var minMem uint64 = 999999999999

func printMemoryUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Allocated memory: %v MB\n", m.Alloc/(1024*1024))
	if peek < m.Alloc {
		peek = m.Alloc
	}
	if minMem > m.Alloc {
		minMem = m.Alloc
	}
	fmt.Printf("Total memory allocated (including freed): %v bytes\n", m.TotalAlloc)
	fmt.Printf("System memory used: %v bytes\n", m.Sys)
	fmt.Printf("Memory currently in use by Go runtime: %v bytes\n", m.Mallocs-m.Frees)
	fmt.Printf("=============== %d MB | %d MB ==================\n", peek/(1024*1024), minMem/(1024*1024))
}

func main() {
	connStr := "postgresql://postgres:postgres@localhost/db_product?sslmode=disable"
	dbTmp, err := New(connStr, Option{
		MaxOpenConn:     100,
		MaxIdleConn:     10,
		MaxIdleLifeTime: 300,
	})
	if err != nil {
		panic(err)
	}
	db = dbTmp

	go func() {
		for {
			printMemoryUsage()
			time.Sleep(10 * time.Second)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 40000; i++ {
		wg.Add(1)
		go test(i, &wg)
	}
	wg.Wait()
	fmt.Println("Done==============")

	func() {
		for {
			time.Sleep(10 * time.Second)
		}
	}()
}
