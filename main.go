package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/url"
	"os"
	"time"
)

func main() {

	debug := flag.Bool("debug", false, "sets log level to debug")

	help := flag.Bool("help", false, "Print help")
	port := flag.Int("port", 1433, "SQL Server port (default: 1433)")
	username := flag.String("username", "", "SQL Server username (required)")
	password := flag.String("password", "", "SQL Server password (required)")
	hostname := flag.String("hostname", "localhost", "SQL Server hostname (default: localhost)")
	flag.Parse()

	if *help || len(*username) == 0 || len(*password) == 0 || len(*hostname) == 0 || *port < 0 || *port > 65535 {
		printUsage()
	}

	initializeLogging(*debug)

	targetHost := *hostname
	if targetHost == "127.0.0.1" || targetHost == "localhost" {
		targetHost, _ = os.Hostname()
	}

	queryParams := url.Values{}

	u := &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(*username, *password),
		Host:     fmt.Sprintf("%s:%d", *hostname, *port),
		RawQuery: queryParams.Encode(),
	}

	connectionString := u.String()
	log.Logger.Info().Msgf("Connection to database: %s", connectionString)
	db, err := sql.Open("sqlserver", connectionString)
	if err != nil {
		log.Logger.
			Error().
			Interface("url", u).
			Err(err).
			Msg("Error opening connection to database")
		os.Exit(1)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to start tx")
	}
	defer tx.Commit()

	_, err = tx.Exec("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to set tx isolation level")
	}

	log.Info().Msg("Executing query")
	/*
	  Below query doesn't return any records.  It hangs, never returning from tx.Query.

	  Interesting observations:
	    - works when ORDER BY is removed;
	    - works when TOP 50 is removed
	    - continues to hang when columns are removed
	*/
	rows, err := tx.Query(
		`SELECT  TOP 50 'Top 50 queries' as Description,
	                  a.*,
	                  SUBSTRING(SqlText, (qs.statement_start_offset/2)+1,
	   	((CASE qs.statement_end_offset
	   		WHEN -1 THEN DATALENGTH(SqlText)
	   		ELSE qs.statement_end_offset
	   		END - qs.statement_start_offset)/2) + 1) as statement,
	       		qs.*,
	       		queryplan.query_plan as query_plan_ext_xml
	   FROM (SELECT DB_NAME(dbid) as [Database],
	                plan_handle,
	                UseCounts,
	                RefCounts,
	                size_in_bytes,
	                Cacheobjtype,
	                Objtype,
	                st.text as SqlText
	         FROM sys.dm_exec_cached_plans cp
	                  CROSS APPLY sys.dm_exec_sql_text(plan_handle) st
	         WHERE (LEFT(TEXT,300) LIKE '%SOME_MATCHING_TEXT%')) a
	            CROSS APPLY sys.dm_exec_query_plan(a.plan_handle) queryplan
	            INNER JOIN sys.dm_exec_query_stats qs on qs.plan_handle = a.plan_handle
	   	  WHERE queryplan.query_plan IS NOT NULL AND DATEDIFF(hour,qs.last_execution_time,GETDATE()) < 12
	         ORDER BY qs.total_elapsed_time DESC
	         `)

	if err != nil {
		log.Fatal().Err(err).Msg("Failed to execute query")
	}
	log.Info().Msg("Executed query")
	defer rows.Close()
	for rows.Next() {
		log.Info().Msgf("Reading row...")
	}
	log.Info().Msg("Completed successfully")
	os.Exit(0)
}

func initializeLogging(debug bool) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}
	log.Logger = log.Output(
		zerolog.ConsoleWriter{
			Out:        os.Stderr,
			NoColor:    true,
			TimeFormat: time.RFC3339,
		},
	)
}

func printUsage() {
	flag.PrintDefaults()
	fmt.Printf("Example: --username user --password pwd --hostname 108.234.456.789 --clario-version 4.5")
	os.Exit(1)
}
