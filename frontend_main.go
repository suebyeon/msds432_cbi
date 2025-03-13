package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

// TripSummary structure to hold the trip summary data
type TripSummary struct {
	DropoffZipCode string  `json:"dropoff_zip_code"`
	NumberOfTrips  int     `json:"number_of_trips"`
	TotalPosCases  float64 `json:"total_pos_cases"`
}

type CCVITripSummary struct {
	NeighborhoodZipCode string `json:"neighborhood_zip_code"`
	CommunityAreaName   string `json:"community_area_name"`
	NumberOfTripsTo     int    `json:"number_of_trips_to"`
	NumberOfTripsFrom   int    `json:"number_of_trips_from"`
}

type UnemployNeighborhoodSummary struct {
	CommunityArea     string  `json:"community_area"`
	Unemployment      float64 `json:"unemployment"`
	BelowPovertyLevel float64 `json:"below_poverty_level"`
}

type LoanNeighborhoodSummary struct {
	CommunityArea   string `json:"community_area"`
	PermitCount     int    `json:"permit_count"`
	PerCapitaIncome int    `json:"per_capita_income"`
}

// Report interface that all report functions will implement
type Report interface {
	GenerateReport() error
}

// Function to fetch data from the backend
func fetchreq2Data(backendURL string) ([]TripSummary, error) {
	resp, err := http.Get(backendURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var summaries []TripSummary
	err = json.Unmarshal(body, &summaries)
	if err != nil {
		return nil, err
	}

	return summaries, nil
}

func fetchreq3Data(backendURL string) ([]CCVITripSummary, error) {
	resp, err := http.Get(backendURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var summaries []CCVITripSummary
	err = json.Unmarshal(body, &summaries)
	if err != nil {
		return nil, err
	}

	return summaries, nil
}

func fetchreq5Data(backendURL string) ([]UnemployNeighborhoodSummary, error) {
	resp, err := http.Get(backendURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var summaries []UnemployNeighborhoodSummary
	err = json.Unmarshal(body, &summaries)
	if err != nil {
		return nil, err
	}

	return summaries, nil
}

func fetchreq6Data(backendURL string) ([]LoanNeighborhoodSummary, error) {
	resp, err := http.Get(backendURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch data: %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var summaries []LoanNeighborhoodSummary
	err = json.Unmarshal(body, &summaries)
	if err != nil {
		return nil, err
	}

	return summaries, nil
}

// Function to display the report
func displayreq2Report(summaries []TripSummary) {
	fmt.Printf("%-20s %-20s %-20s\n", "Dropoff Zip Code", "Number of Trips", "Total Positive COVID-19 Cases")
	fmt.Println(strings.Repeat("-", 60))

	for _, summary := range summaries {
		fmt.Printf("%-20s %-20d %-20.2f\n", summary.DropoffZipCode, summary.NumberOfTrips, summary.TotalPosCases)
	}
}

func displayreq3Report(summaries []CCVITripSummary) {
	fmt.Printf("%-20s %-20s %-20s %-20s\n", "Neighborhood Zip Code", "Community Area Name", "Number of Trips To", "Number of Trips From")
	fmt.Println(strings.Repeat("-", 60))

	for _, summary := range summaries {
		fmt.Printf("%-20s %-20s %-20d %-20d\n", summary.NeighborhoodZipCode, summary.CommunityAreaName, summary.NumberOfTripsTo, summary.NumberOfTripsFrom)
	}
}

func displayreq5Report(summaries []UnemployNeighborhoodSummary) {
	fmt.Printf("%-20s %-20s %-20s\n", "Community Area", "Unemployment Rate", "Below Poverty Level")
	fmt.Println(strings.Repeat("-", 60))

	for _, summary := range summaries {
		fmt.Printf("%-20s %-20.2f %-20.2f\n", summary.CommunityArea, summary.Unemployment, summary.BelowPovertyLevel)
	}
}

func displayreq6Report(summaries []LoanNeighborhoodSummary) {
	fmt.Printf("%-20s %-20s %-20s\n", "Community Area", "Permit Count", "Per Capita Income")
	fmt.Println(strings.Repeat("-", 60))

	for _, summary := range summaries {
		fmt.Printf("%-20s %-20d %-20d\n", summary.CommunityArea, summary.PermitCount, summary.PerCapitaIncome)
	}
}

// Report for trips from airport to zip codes
type AirportToZipReport struct {
	backendURL string
}
type HighCCVINeighborhoodReport struct {
	backendURL string
}
type UnemployNeighborhooddReport struct {
	backendURL string
}
type LoanNeighborhooddReport struct {
	backendURL string
}

func (r *AirportToZipReport) GenerateReport() error {
	summaries, err := fetchreq2Data(r.backendURL)
	if err != nil {
		return err
	}
	displayreq2Report(summaries)
	return nil
}

func (r *HighCCVINeighborhoodReport) GenerateReport()error {
	summaries, err := fetchreq3Data(r.backendURL)
	if err != nil {
		return err
	}
	displayreq3Report(summaries)
	return nil
}

func (r *UnemployNeighborhoodReport) GenerateReport()error {
	summaries, err := fetchreq5Data(r.backendURL)
	if err != nil {
		return err
	}
	displayreq5Report(summaries)
	return nil
}

func (r *LoanNeighborhoodReport) GenerateReport()error {
	summaries, err := fetchreq6Data(r.backendURL)
	if err != nil {
		return err
	}
	displayreq6Report(summaries)
	return nil
}

func main() {
	if len(os.Args) < 3 {
        fmt.Println("Usage: transportation_report <report_type> <backend_url>")
        fmt.Println("report_type: airport_to_zip, HighCCVINeighborhoodreport, UnemployNeighborhoodreport, LoanNeighborhoodreport")
        fmt.Println("backend_url: The URL of the backend API endpoint")
        return
    }
	reportType := os.Args[1]
	backendURL := os.Args[2]

	var report Report

	switch reportType {
	case "airport_to_zip":
		report = &AirportToZipReport{backendURL: backendURL}
	case "HighCCVINeighborhoodreport":
		report = &HighCCVINeighborhoodReport{backendURL: backendURL}
	case "UnemployNeighborhooddreport":
		report = &UnemployNeighborhoodReport{backendURL: backendURL}
	case "LoanNeighborhooddreport":
		report = &LoanNeighborhoodReport{backendURL: backendURL}
	default:
		fmt.Println("Invalid report type. Use airport_to_zip or another_report.")
		return
	}

	err := report.GenerateReport()
	if err != nil {
		log.Fatal(err)
	}
}

// to run the code:
// go build -o main main.go
// ./main airport_to_zip https://go-microservice-424531809163.us-central1.run.app 