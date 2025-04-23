// ingestion/load_transactions.go
package ingestion

import (
	"DailyTransactionBatchProcessing/models"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"
)

// LoadTransactions loads transaction data from a CSV file
func LoadTransactions(filePath string) ([]models.Transaction, error) {
	file, err := os.Open("data/transactions_2025-04-15.csv")
	if err != nil {
		return nil, fmt.Errorf("error opening transactions file: %w", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Ensure file is not empty and has headers
	if len(records) < 2 {
		return nil, fmt.Errorf("transaction file is empty or missing data rows")
	}

	// Skip header row
	transactions := make([]models.Transaction, 0, len(records)-1)
	for i, record := range records {
		// Skip header row
		if i == 0 {
			continue
		}

		// Ensure we have the expected number of fields
		if len(record) < 6 {
			return nil, fmt.Errorf("invalid record format at line %d: insufficient fields", i+1)
		}

		// Parse transaction data
		transaction, err := parseTransaction(record, i+1)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, transaction)
	}

	return transactions, nil
}

// parseTransaction parses a CSV record into a Transaction struct
func parseTransaction(record []string, lineNum int) (models.Transaction, error) {
	// Expected format: [transactionID, accountID, timestamp, amount, transactionType, status, description(optional)]
	transaction := models.Transaction{
		ID:          record[0],
		AccountID:   record[1],
		Description: "",
	}

	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, record[2])
	if err != nil {
		return transaction, fmt.Errorf("invalid timestamp at line %d: %w", lineNum, err)
	}
	transaction.Timestamp = timestamp

	// Parse amount
	amount, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		return transaction, fmt.Errorf("invalid amount at line %d: %w", lineNum, err)
	}
	transaction.Amount = amount

	// Parse transaction type
	transactionType := record[4]
	if transactionType != "credit" && transactionType != "debit" && transactionType != "transfer" {
		return transaction, fmt.Errorf("invalid transaction type at line %d: must be 'credit', 'debit', or 'transfer'", lineNum)
	}
	transaction.Type = transactionType

	// Parse status
	status := record[5]
	if status != "pending" && status != "completed" && status != "rejected" {
		return transaction, fmt.Errorf("invalid status at line %d: must be 'pending', 'completed', or 'rejected'", lineNum)
	}
	transaction.Status = status

	// Parse optional description field
	if len(record) > 6 {
		transaction.Description = record[6]
	}

	// For transfer transactions, ensure destination account is specified
	if transactionType == "transfer" && len(record) < 8 {
		return transaction, fmt.Errorf("transfer transaction at line %d is missing destination account", lineNum)
	} else if transactionType == "transfer" {
		transaction.DestinationAccountID = record[7]
	}

	return transaction, nil
}

// ValidateTransactions validates a slice of transactions against a map of accounts
func ValidateTransactions(transactions []models.Transaction, accounts map[string]models.Account) ([]models.Transaction, []models.Transaction) {
	validTransactions := make([]models.Transaction, 0)
	invalidTransactions := make([]models.Transaction, 0)

	for _, transaction := range transactions {
		valid := true
		reason := ""

		// Skip already rejected transactions
		if transaction.Status == "rejected" {
			transaction.ValidationMessage = "Already rejected in input file"
			invalidTransactions = append(invalidTransactions, transaction)
			continue
		}

		// Only process pending transactions
		if transaction.Status != "pending" {
			transaction.ValidationMessage = "Only pending transactions can be processed"
			invalidTransactions = append(invalidTransactions, transaction)
			continue
		}

		// Validate amount is positive
		if transaction.Amount <= 0 {
			valid = false
			reason = "Transaction amount must be positive"
		}

		// Validate account exists
		if _, exists := accounts[transaction.AccountID]; !exists {
			valid = false
			reason = fmt.Sprintf("Account %s does not exist", transaction.AccountID)
		}

		// For transfers, validate destination account exists
		if transaction.Type == "transfer" {
			if transaction.DestinationAccountID == "" {
				valid = false
				reason = "Transfer is missing destination account"
			} else if _, exists := accounts[transaction.DestinationAccountID]; !exists {
				valid = false
				reason = fmt.Sprintf("Destination account %s does not exist", transaction.DestinationAccountID)
			} else if transaction.DestinationAccountID == transaction.AccountID {
				valid = false
				reason = "Source and destination accounts cannot be the same"
			}
		}

		if valid {
			validTransactions = append(validTransactions, transaction)
		} else {
			transaction.ValidationMessage = reason
			invalidTransactions = append(invalidTransactions, transaction)
		}
	}

	return validTransactions, invalidTransactions
}
