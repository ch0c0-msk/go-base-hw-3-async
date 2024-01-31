package main

import (
	"sort"
	"strconv"
	"strings"
	"sync"
)

func SingleHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for data := range in {
		wg.Add(1)
		go func(data interface{}) {
			defer wg.Done()
			strData := strconv.Itoa(data.(int))
			mu.Lock()
			md5data := DataSignerMd5(strData)
			mu.Unlock()
			crc32ch := make(chan string)
			md5ch := make(chan string)

			go func(strData string, ch chan<- string) {
				defer close(ch)
				crc32 := DataSignerCrc32(strData)
				ch <- crc32
			}(strData, crc32ch)

			go func(md5Data string, ch chan<- string) {
				defer close(ch)
				crc32 := DataSignerCrc32(md5Data)
				ch <- crc32
			}(md5data, md5ch)

			firstRes := <-crc32ch
			secondRes := <-md5ch
			res := firstRes + "~" + secondRes
			out <- res
		}(data)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	var wg sync.WaitGroup
	const maxTh = 6
	for data := range in {
		strData := data.(string)
		wg.Add(1)
		go func(data string) {
			defer wg.Done()
			var wgTh sync.WaitGroup
			var mu sync.Mutex
			hashList := make([]string, 6)

			for th := 0; th < maxTh; th++ {
				wgTh.Add(1)
				go func(th int) {
					defer wgTh.Done()
					hash := DataSignerCrc32(strconv.Itoa(th) + data)
					mu.Lock()
					hashList[th] = hash
					mu.Unlock()
				}(th)
			}
			wgTh.Wait()
			res := strings.Join(hashList, "")
			out <- res
		}(strData)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var inputList []string

	for data := range in {
		inputList = append(inputList, data.(string))
	}
	sort.Strings(inputList)
	res := strings.Join(inputList, "_")
	out <- res
}

func ExecutePipeline(jobs ...job) {
	in := make(chan interface{})
	var wg sync.WaitGroup
	for _, j := range jobs {
		out := make(chan interface{})
		wg.Add(1)
		go func(in, out chan interface{}, j job) {
			defer wg.Done()
			j(in, out)
			close(out)
		}(in, out, j)
		in = out
	}
	wg.Wait()
}
