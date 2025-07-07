package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	monolithProxy    *httputil.ReverseProxy
	moviesProxy      *httputil.ReverseProxy
	eventsProxy      *httputil.ReverseProxy
	gradualMigration bool
	migrationPercent int
	random           *rand.Rand
)

func main() {
	random = rand.New(rand.NewSource(time.Now().UnixNano()))

	// Environment variables
	port := getEnv("PORT", "8000")
	monolithURL := getEnv("MONOLITH_URL", "http://monolith:8080")
	moviesURL := getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081")
	eventsURL := getEnv("EVENTS_SERVICE_URL", "http://events-service:8082")
	gradualMigration = os.Getenv("GRADUAL_MIGRATION") == "true"
	migrationPercent, _ = strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "50"))

	// Create reverse proxies
	monolithProxy = createProxy(monolithURL)
	moviesProxy = createProxy(moviesURL)
	eventsProxy = createProxy(eventsURL)

	// Start server
	http.HandleFunc("/", gatewayHandler)
	log.Println("Starting proxy service on port", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func createProxy(target string) *httputil.ReverseProxy {
	url, _ := url.Parse(target)
	return httputil.NewSingleHostReverseProxy(url)
}

func gatewayHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		eventsProxy.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		handleMoviesRequest(w, r)
	default:
		monolithProxy.ServeHTTP(w, r)
	}
}

func handleMoviesRequest(w http.ResponseWriter, r *http.Request) {
	if gradualMigration && random.Intn(100) < migrationPercent {
		moviesProxy.ServeHTTP(w, r)
	} else {
		monolithProxy.ServeHTTP(w, r)
	}
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}
