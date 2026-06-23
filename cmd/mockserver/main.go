// Copyright 2026 Query Farm LLC - https://query.farm

// Command mockserver runs a standalone in-memory HL7 FHIR R4 server exposing
// /Patient and /Observation (searchset Bundles), /Patient/{id}, and /metadata
// (CapabilityStatement). It is used by the haybarn SQL end-to-end tests: the
// Makefile starts it on a free port, reads the printed PORT line, and points
// the worker's fhir functions at it.
//
// Usage:
//
//	mockserver [--addr 127.0.0.1:0] [--token TOKEN] [--page-size N]
//
// On startup it prints "PORT:<n>" (the bound TCP port) to stdout so a caller
// can discover the port even when binding to :0.
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/Query-farm/vgi-fhir/internal/mockfhir"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:0", "TCP address to listen on (host:port; port 0 = pick a free port)")
	token := flag.String("token", mockfhir.Token, "Required bearer token (empty = no auth)")
	// Force tiny Patient pages so the E2E exercises Bundle next-link pagination.
	pageSize := flag.Int("page-size", 2, "Patient page size to exercise pagination (0 = single page)")
	flag.Parse()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("mockserver: listen %q: %v", *addr, err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	fmt.Printf("PORT:%d\n", port)
	_ = os.Stdout.Sync()

	handler := mockfhir.NewHandler(mockfhir.Config{Token: *token, PageSize: *pageSize})
	srv := &http.Server{Handler: handler}
	if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
		log.Fatalf("mockserver: serve: %v", err)
	}
}
