// output/generate_reports.go
package output

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"DailyTransactionBatchProcessing/models"
)

// WriteAccounts writes account data to a CSV file
func WriteAccounts(accounts map[string]models.Account, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating accounts file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"account_id", "balance", "overdraft_count", "last_transaction_time"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write account data
	for _, account := range accounts {
		lastTxTime := ""
		if !account.LastTransactionTime.IsZero() {
			lastTxTime = account.LastTransactionTime.Format(time.RFC3339)
		}

		record := []string{
			account.ID,
			fmt.Sprintf("%.2f", account.Balance),
			strconv.Itoa(account.OverdraftCount),
			lastTxTime,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing account record: %w", err)
		}
	}

	return nil
}

// WriteProcessedTransactions writes processed transactions to a CSV file
func WriteProcessedTransactions(transactions []models.Transaction, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating transactions file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"transaction_id",
		"account_id",
		"timestamp",
		"amount",
		"type",
		"status",
		"description",
		"destination_account_id",
		"processing_message",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write transaction data
	for _, transaction := range transactions {
		record := []string{
			transaction.ID,
			transaction.AccountID,
			transaction.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.2f", transaction.Amount),
			transaction.Type,
			transaction.Status,
			transaction.Description,
			transaction.DestinationAccountID,
			transaction.ProcessingMessage,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing transaction record: %w", err)
		}
	}

	return nil
}

// WriteInvalidTransactions writes invalid transactions to a CSV file
func WriteInvalidTransactions(transactions []models.Transaction, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating invalid transactions file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"transaction_id",
		"account_id",
		"timestamp",
		"amount",
		"type",
		"status",
		"validation_message",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write invalid transaction data
	for _, transaction := range transactions {
		record := []string{
			transaction.ID,
			transaction.AccountID,
			transaction.Timestamp.Format(time.RFC3339),
			fmt.Sprintf("%.2f", transaction.Amount),
			transaction.Type,
			transaction.Status,
			transaction.ValidationMessage,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing invalid transaction record: %w", err)
		}
	}

	return nil
}

// WriteAnomalies writes detected anomalies to a CSV file
func WriteAnomalies(anomalies []models.Anomaly, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating anomalies file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"transaction_id",
		"account_id",
		"timestamp",
		"type",
		"description",
		"severity",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write anomaly data
	for _, anomaly := range anomalies {
		record := []string{
			anomaly.TransactionID,
			anomaly.AccountID,
			anomaly.Timestamp.Format(time.RFC3339),
			anomaly.Type,
			anomaly.Description,
			anomaly.Severity,
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing anomaly record: %w", err)
		}
	}

	return nil
}

// GenerateAccountSummary generates account summaries for the day
func GenerateAccountSummary(
	accounts map[string]models.Account,
	transactions []models.Transaction,
	dateStr string,
) []models.AccountSummary {
	// Track opening balances and create summaries
	summaries := make(map[string]*models.AccountSummary)

	// Create initial summaries with closing balances (current account balances)
	for accountID, account := range accounts {
		summaries[accountID] = &models.AccountSummary{
			AccountID:        accountID,
			Date:             dateStr,
			ClosingBalance:   account.Balance,
			OpeningBalance:   account.Balance, // Will be adjusted below
			TotalDebits:      0,
			TotalCredits:     0,
			TransactionCount: 0,
			OverdraftCount:   account.OverdraftCount,
		}
	}

	// Process transactions to calculate opening balances and transaction totals
	for _, transaction := range transactions {
		// Skip non-completed transactions
		if transaction.Status != "completed" {
			continue
		}

		// Update source account summary
		if summary, exists := summaries[transaction.AccountID]; exists {
			summary.TransactionCount++

			// Update opening balance and transaction totals based on transaction type
			switch transaction.Type {
			case "credit":
				summary.OpeningBalance -= transaction.Amount
				summary.TotalCredits += transaction.Amount

			case "debit":
				summary.OpeningBalance += transaction.Amount
				summary.TotalDebits += transaction.Amount

			case "transfer":
				summary.OpeningBalance += transaction.Amount
				summary.TotalDebits += transaction.Amount

				// Update destination account for transfers
				if destSummary, exists := summaries[transaction.DestinationAccountID]; exists {
					destSummary.TransactionCount++
					destSummary.OpeningBalance -= transaction.Amount
					destSummary.TotalCredits += transaction.Amount
				}
			}
		}
	}

	// Convert map to slice for return
	result := make([]models.AccountSummary, 0, len(summaries))
	for _, summary := range summaries {
		result = append(result, *summary)
	}

	return result
}

// WriteAccountSummary writes account summaries to a CSV file
func WriteAccountSummary(summaries []models.AccountSummary, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating account summary file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"account_id",
		"date",
		"opening_balance",
		"closing_balance",
		"total_debits",
		"total_credits",
		"transaction_count",
		"overdraft_count",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("error writing header: %w", err)
	}

	// Write summary data
	for _, summary := range summaries {
		record := []string{
			summary.AccountID,
			summary.Date,
			fmt.Sprintf("%.2f", summary.OpeningBalance),
			fmt.Sprintf("%.2f", summary.ClosingBalance),
			fmt.Sprintf("%.2f", summary.TotalDebits),
			fmt.Sprintf("%.2f", summary.TotalCredits),
			strconv.Itoa(summary.TransactionCount),
			strconv.Itoa(summary.OverdraftCount),
		}

		if err := writer.Write(record); err != nil {
			return fmt.Errorf("error writing summary record: %w", err)
		}
	}

	return nil
}
