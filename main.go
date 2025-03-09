package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentRecords []struct {
	Community_area      string `json:"community_area"`
	Below_poverty_level string `json:"below_poverty_level"`
	Per_capita_income   string `json:"per_capita_income"`
	Unemployment        string `json:"unemployment"`
}

type PermitRecords []struct {
	ID             string `json:"id"`
	Permit_type    string `json:"permit_type"`
	Community_area string `json:"community_area"`
}

type CCCVIRecords []struct {
	Geography_type        string `json:"geography_type"`
	Community_area_or_zip string `json:"community_area_or_zip"`
	Community_area_name   string `json:"community_area_name"`
	Ccvi_category         string `json:"ccvi_category"`
}

type CovidRecords []struct {
	Zip_code         string `json:"zip_code"`
	Week_number      string `json:"week_number"`
	Tests            string `json:"tests_weekly"`
	Percent_positive string `json:"percent_tested_positive_weekly"`
}

const apiKey = "AIzaSyC0c7zFxovSnma6BhX60prrCaAjtmFCE1w"

// Declare my database connection
var db *sql.DB

// The main package can has the init function.
// The init function will be triggered before the main function

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=localhost sslmode=disable port = 5432"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=162.123.0.9 sslmode=disable port = 5433"

	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=sql host=/cloudsql/wide-hexagon-452908-m3:us-central1:mypostgres sslmode=disable port=5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
		panic(err)
	}

	// Test the database connection
	//err = db.Ping()
	//if err != nil {
	//	fmt.Println("Couldn't Connect to database")
	//	panic(err)
	//}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	geocoder.ApiKey = apiKey

	// Spin in a loop and pull data from the city of chicago data portal
	// Once every hour, day, week, etc.
	// Though, please note that Not all datasets need to be pulled on daily basis
	// fine-tune the following code-snippet as you see necessary

	// For now while you are doing protyping and unit-testing,
	// it is a good idea to use Cloud Run and start an HTTP server, and manually you kick-start
	// the microservices (goroutines) for data collection from the different sources
	// Once you are done with protyping and unit-testing,
	// you could port your code Cloud Run to  Compute Engine, App Engine, Kubernetes Engine, Google Functions, etc.

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service

		log.Print("starting CBI Microservices ...")

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		// This code snippet is only for prototypying and unit-testing

		// build and fine-tune the functions to pull data from the different data sources
		// The following code snippets show you how to pull data from different data sources

		go GetTrips(db)
		go GetUnemploymentRates(db)
		go GetBuildingPermits(db)
		go GetCovidDetails(db)
		go GetCCVIDetails(db)

		http.HandleFunc("/", handler)

		// Determine port for HTTP service.
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("defaulting to port %s", port)
		}

		// Start HTTP server.
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")

		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}

		time.Sleep(24 * time.Hour)
	}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("PROJECT_ID")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTrips(db *sql.DB) {

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	drop_table := `drop table if exists transportation`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "transportation" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Taxi Trips")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.

	// Get the the Taxi Trips for Taxi medallions list

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=500"

	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	body_1, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list_1 TripsJsonRecords
	json.Unmarshal(body_1, &taxi_trips_list_1)

	// Get the Taxi Trip list for rideshare companies like Uber/Lyft list
	// Transportation-Network-Providers-Trips:
	var url_2 = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=500"

	res_2, err := http.Get(url_2)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Transportation-Network-Providers-Trips")

	body_2, _ := ioutil.ReadAll(res_2.Body)
	var taxi_trips_list_2 TripsJsonRecords
	json.Unmarshal(body_2, &taxi_trips_list_2)

	s := fmt.Sprintf("\n\n Transportation-Network-Providers-Trips number of SODA records received = %d\n\n", len(taxi_trips_list_2))
	io.WriteString(os.Stdout, s)

	// Add the Taxi medallions list & rideshare companies like Uber/Lyft list

	taxi_trips_list := append(taxi_trips_list_1, taxi_trips_list_2...)

	// Process the list

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		fmt.Println(pickup_location)

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO transportation ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the TaxiTrips Table")

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetUnemploymentRates(db *sql.DB) {
	fmt.Println("GetCommunityAreaUnemployment: Collecting Unemployment Rates Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Health-Human-Services/Public-Health-Statistics-Selected-public-health-in/iqnk-2tcu/data

	drop_table := `drop table if exists unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
		"id"   SERIAL ,
		"community_area" VARCHAR(255) ,
		"below_poverty_level" DOUBLE PRECISION,
		"per_capita_income" INTEGER,
		"unemployment" DOUBLE PRECISION,
		PRIMARY KEY ("id")
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for community_area_unemployment")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Community Areas Unemplyment: Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(unemployment_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}

		below_poverty_level, err := strconv.ParseFloat(unemployment_data_list[i].Below_poverty_level, 64)
		if err != nil {
			continue
		}

		per_capita_income, err := strconv.Atoi(unemployment_data_list[i].Per_capita_income)
		if err != nil {
			continue // Skip the record if conversion fails
		}

		unemployment, err := strconv.ParseFloat(unemployment_data_list[i].Unemployment, 64)
		if err != nil {
			continue
		}

		sql := `INSERT INTO unemployment ("community_area", "below_poverty_level", "per_capita_income", "unemployment") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			community_area,
			below_poverty_level,
			per_capita_income,
			unemployment)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the community_area_unemployment Table")

}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	// This function is NOT complete
	// It provides code-snippets for the data source: https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	// Data Collection needed from data source:
	// https://data.cityofchicago.org/Buildings/Building-Permits/ydr8-5enu/data

	drop_table := `drop table if exists permit`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "permit" (
		"id"   VARCHAR(255) ,
		"permit_type" VARCHAR(255) ,
		"community_area" INTEGER,
		PRIMARY KEY ("id")
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/building-permits.json?$limit=500"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list PermitRecords
	json.Unmarshal(body, &building_data_list)

	s := fmt.Sprintf("\n\n Building Permits: number of SODA records received = %d\n\n", len(building_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(building_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		id := building_data_list[i].ID
		if id == "" {
			continue
		}

		permit_type := building_data_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		community_area, err := strconv.Atoi(building_data_list[i].Community_area)
		if err != nil {
			continue
		}

		sql := `INSERT INTO permit ("id", "permit_type", "community_area") values($1, $2, $3)`

		_, err = db.Exec(
			sql,
			id,
			permit_type,
			community_area)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Building Permits Table")
}

func GetCovidDetails(db *sql.DB) {
	fmt.Println("GetCovidDetails: Collecting Covid Data")

	drop_table := `drop table if exists covid`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid" (
		"id" SERIAL,	
		"zip_code" VARCHAR(255),
		"week_number" INTEGER,
		"tests" INTEGER,
		"percentage_positive" FLOAT,
		PRIMARY KEY ("id")
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Covid")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=500"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Covid")

	body, _ := ioutil.ReadAll(res.Body)
	var covid_list CovidRecords
	json.Unmarshal(body, &covid_list)

	s := fmt.Sprintf("\n\n Covid: number of SODA records received = %d\n\n", len(covid_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(covid_list); i++ {

		zip_code := covid_list[i].Zip_code
		if zip_code == "" {
			continue
		}

		week_number, err := strconv.Atoi(covid_list[i].Week_number)
		if err != nil {
			continue
		}

		tests_weekly, err := strconv.Atoi(covid_list[i].Tests)
		if err != nil {
			continue
		}

		percent_tested_positive_weekly := covid_list[i].Percent_positive
		if percent_tested_positive_weekly == "" {
			continue
		}

		sql := `INSERT INTO covid ("zip_code" ,"week_number", "tests", "percentage_positive") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			zip_code,
			week_number,
			tests_weekly,
			percent_tested_positive_weekly)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Covid Table")

}

func GetCCVIDetails(db *sql.DB) {
	fmt.Println("GetCCVIDetails: Collecting CCVI Data")

	drop_table := `drop table if exists ccvi`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "ccvi" (
		"community_area_or_zip" INTEGER,
		"geography_type" VARCHAR(255),
		"community_area_name" VARCHAR(255),
		"ccvi_category" VARCHAR(255),
		PRIMARY KEY ("community_area_or_zip")
	);`
	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for CCVI")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=500"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for CCVI")

	body, _ := ioutil.ReadAll(res.Body)
	var ccvi_list CCCVIRecords
	json.Unmarshal(body, &ccvi_list)

	s := fmt.Sprintf("\n\n CCVI: number of SODA records received = %d\n\n", len(ccvi_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(ccvi_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		geography_type := ccvi_list[i].Geography_type
		if geography_type == "" {
			continue
		}

		community_area_or_zip, err := strconv.Atoi(ccvi_list[i].Community_area_or_zip)
		if err != nil {
			continue
		}

		community_area_name := ccvi_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		ccvi_category := ccvi_list[i].Ccvi_category
		if ccvi_category == "" {
			continue
		}

		sql := `INSERT INTO ccvi ("geography_type", "community_area_or_zip", "community_area_name", "ccvi_category") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			geography_type,
			community_area_or_zip,
			community_area_name,
			ccvi_category)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the CCVI Table")

}
