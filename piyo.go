package main

import (
	"log"
)

func main() {
	port := 8080
	log.Printf("starting server at %d", port)
	log.Fatal(StartService(NewInMemoryStore(), port))
}
