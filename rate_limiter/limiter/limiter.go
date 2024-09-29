package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    client := redis.NewClient(&redis.Options{
        Addr:	  "localhost:6379",
        Password: "", // no password set
        DB:		  0,  // use default DB
    })
    
    
    target, err := url.Parse("http://localhost:6000")
    if err != nil {
        log.Fatalf("Error parsing target URL: %v", err)
    }

    proxy := httputil.NewSingleHostReverseProxy(target)
    
    // Create a new HTTP server
	srv := &http.Server{Addr: ":8080"}

	// Define a handler function
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request){
        if isRateLimited(r, client, ctx, 4){
            http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
        } else {
            r.Host = target.Host
            proxy.ServeHTTP(w, r)
        }
    })
    // Start the server in a separate goroutine
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
		}
	}()
    // Create a channel to receive OS signals
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt)

    // Block until a signal is received
    <-c

    // Create a context with a timeout
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Gracefully shutdown the server
    fmt.Println("Shutting down server...")
    if err := srv.Shutdown(ctx); err != nil {
        fmt.Printf("Server shutdown error: %v\n", err)
    }

    fmt.Println("Server shutdown complete")

}

func isRateLimited(r *http.Request, client *redis.Client, ctx context.Context, allowedRequestsPerSecond int32) bool {
    ip := r.RemoteAddr
    timeStamp := float64(time.Now().UnixMilli())
    
    windowEnd := strconv.FormatInt(time.Now().UnixMilli() - 1000, 10)
    _, err := client.ZRemRangeByScore(ctx, ip, "-inf", windowEnd).Result()
	if err != nil {
        fmt.Printf("error removing old entries: %v\n", err)
		return false
	}
    
    client.ZAdd(ctx, ip, redis.Z{Member: timeStamp, Score: timeStamp})

    numRequests := int32(client.ZCard(ctx, ip).Val())
    fmt.Printf("User: %v\n", ip)
    fmt.Printf("%d requests in the past second\n", numRequests)

    if numRequests < allowedRequestsPerSecond {
        fmt.Println("Allowed")
        return false
    } else {
        fmt.Println("Rate limited")
        return true
    }
}
