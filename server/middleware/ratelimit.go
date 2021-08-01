package middleware

import "net/http"

// https://eli.thegreenplace.net/2019/on-concurrency-in-go-http-servers

// LimitNumOfConcurrentClients is HTTP handling middleware that ensures no more than
// maxClients requests are passed concurrently to the given handler f.
func LimitNumOfConcurrentClients(f http.HandlerFunc, maxClients int) http.HandlerFunc {
	// Counting semaphore using a buffered channel
	sema := make(chan struct{}, maxClients)

	return func(w http.ResponseWriter, req *http.Request) {
		sema <- struct{}{}
		defer func() { <-sema }()
		f(w, req)
	}
}
