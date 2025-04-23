// main.go
package main

import (
	"DailyTransactionBatchProcessing/detector"
	"DailyTransactionBatchProcessing/ingestion"
	"DailyTransactionBatchProcessing/output"
	"DailyTransactionBatchProcessing/processor"

	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Parse command line arguments
	dateFlag := flag.String("date", "", "Processing date in YYYY-MM-DD format (defaults to yesterday)")
	inputDirFlag := flag.String("input", "./data", "Directory containing transaction data files")
	outputDirFlag := flag.String("output", "./output", "Directory for output files")
	logFileFlag := flag.String("log", "", "Log file path (defaults to stdout)")
	flag.Parse()

	// Configure logging
	if *logFileFlag != "" {
		logFile, err := os.OpenFile(*logFileFlag, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file: %v", err)
		}
		defer func(logFile *os.File) {
			err := logFile.Close()
			if err != nil {

			}
		}(logFile)
		log.SetOutput(logFile)
	}

	// Determine processing date
	var processDate time.Time
	var err error
	if *dateFlag != "" {
		processDate, err = time.Parse("2006-01-02", *dateFlag)
		if err != nil {
			log.Fatalf("Invalid date format: %v", err)
		}
	} else {
		// Default to yesterday
		processDate = time.Now().AddDate(0, 0, -1)
	}
	dateStr := processDate.Format("2006-01-02")

	log.Printf("Starting batch processing for date: %s", dateStr)

	// Ensure output directory exists
	if err := os.MkdirAll(*outputDirFlag, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Step 1: Load account data from the previous day
	accountsFilePath := filepath.Join(*inputDirFlag, fmt.Sprintf("accounts_%s.csv", dateStr))
	if _, err := os.Stat(accountsFilePath); os.IsNotExist(err) {
		accountsFilePath = filepath.Join(*inputDirFlag, "accounts.csv")
	}
	accounts, err := processor.LoadAccounts(accountsFilePath)
	if err != nil {
		log.Fatalf("Failed to load accounts: %v", err)
	}
	log.Printf("Loaded %d accounts", len(accounts))

	// Step 2: Ingest transactions
	transactionsFilePath := filepath.Join(*inputDirFlag, fmt.Sprintf("transactions_%s.csv", dateStr))
	transactions, err := ingestion.LoadTransactions(transactionsFilePath)
	if err != nil {
		log.Fatalf("Failed to load transactions: %v", err)
	}
	log.Printf("Loaded %d transactions", len(transactions))

	// Step 3: Validate transactions
	validTransactions, invalidTransactions := ingestion.ValidateTransactions(transactions, accounts)
	log.Printf("Validated transactions: %d valid, %d invalid", len(validTransactions), len(invalidTransactions))

	// Log invalid transactions
	if len(invalidTransactions) > 0 {
		invalidPath := filepath.Join(*outputDirFlag, fmt.Sprintf("invalid_transactions_%s.csv", dateStr))
		if err := output.WriteInvalidTransactions(invalidTransactions, invalidPath); err != nil {
			log.Printf("Warning: Failed to write invalid transactions: %v", err)
		}
	}

	// Step 4: Process valid transactions
	processedAccounts, processedTransactions := processor.ProcessTransactions(validTransactions, accounts)
	log.Printf("Processed %d transactions", len(processedTransactions))

	// Step 5: Detect anomalies
	anomalies := detector.DetectAnomalies(processedTransactions, processedAccounts)
	log.Printf("Detected %d anomalies", len(anomalies))

	// Write anomalies to output
	if len(anomalies) > 0 {
		anomalyPath := filepath.Join(*outputDirFlag, fmt.Sprintf("fraud_alerts_%s.csv", dateStr))
		if err := output.WriteAnomalies(anomalies, anomalyPath); err != nil {
			log.Printf("Warning: Failed to write anomalies: %v", err)
		}
	}

	// Step 6: Generate account summaries
	summary := output.GenerateAccountSummary(processedAccounts, processedTransactions, dateStr)

	// Write updated accounts
	accountsOutputPath := filepath.Join(*outputDirFlag, fmt.Sprintf("accounts_%s.csv", time.Now().Format("2006-01-02")))
	if err := output.WriteAccounts(processedAccounts, accountsOutputPath); err != nil {
		log.Fatalf("Failed to write updated accounts: %v", err)
	}

	// Write transaction log
	transactionsOutputPath := filepath.Join(*outputDirFlag, fmt.Sprintf("processed_transactions_%s.csv", dateStr))
	if err := output.WriteProcessedTransactions(processedTransactions, transactionsOutputPath); err != nil {
		log.Printf("Warning: Failed to write processed transactions: %v", err)
	}

	// Write account summary
	summaryPath := filepath.Join(*outputDirFlag, fmt.Sprintf("account_summary_%s.csv", dateStr))
	if err := output.WriteAccountSummary(summary, summaryPath); err != nil {
		log.Fatalf("Failed to write account summary: %v", err)
	}

	log.Printf("Batch processing completed successfully for date: %s", dateStr)
}
