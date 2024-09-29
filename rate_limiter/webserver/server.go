package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Handler for the root ("/") route
func homeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Welcome to the Home Page!")
}

// Handler for the "/about" route
func aboutHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "This is the About Page!")
}

// Handler for the "/contact" route
func contactHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Contact us at: example@example.com")
}

func main() {
	// Create a new ServeMux (which lets us map routes to handlers)
	mux := http.NewServeMux()

	// Register route handlers to the ServeMux
	mux.HandleFunc("/", homeHandler)
	mux.HandleFunc("/about", aboutHandler)
	mux.HandleFunc("/contact", contactHandler)

	// Create a custom server object
	server := &http.Server{
		Addr:         ":6000",          // Address and port to listen on
		Handler:      mux,              // Use the mux to handle routes
		ReadTimeout:  10 * time.Second, // Max time to read request from the client
		WriteTimeout: 10 * time.Second, // Max time to write response to the client
		IdleTimeout:  15 * time.Second, // Max time for idle connections
	}

		// Channel to listen for interrupt signals for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	
	go func() {	
		// Start the server
		fmt.Println("Starting server on :6000")
		if err := server.ListenAndServe(); err != nil {
			fmt.Printf("Server error: %s\n", err)
		}
	}()

	// block until recieve interrupt signal
	<-signalChan

	fmt.Println("Shutting down server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed :%v\n", err)
	}

	fmt.Println("Server exited gracefully")
}
