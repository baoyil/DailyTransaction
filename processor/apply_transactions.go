// Package processor //processor/apply_transactions.go
package processor

import (
	"DailyTransactionBatchProcessing/models"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

// Constants for business rules
const (
	OverdraftLimit          = -1000.0 // Maximum allowed overdraft
	MaxDailyWithdrawalLimit = 5000.0  // Maximum daily withdrawal limit
)

// LoadAccounts loads account data from a CSV file
func LoadAccounts(filePath string) (map[string]models.Account, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening accounts file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV: %w", err)
	}

	// Ensure file is not empty and has headers
	if len(records) < 2 {
		return nil, fmt.Errorf("accounts file is empty or missing data rows")
	}

	// Skip header row
	accounts := make(map[string]models.Account)
	for i, record := range records {
		// Skip header row
		if i == 0 {
			continue
		}

		// Ensure we have the expected number of fields
		if len(record) < 2 {
			return nil, fmt.Errorf("invalid record format at line %d: insufficient fields", i+1)
		}

		// Parse account data
		accountID := record[0]
		balance, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid balance at line %d: %w", i+1, err)
		}

		// Create account
		account := models.Account{
			ID:             accountID,
			Balance:        balance,
			DailyDebits:    0,
			DailyCredits:   0,
			OverdraftCount: 0,
		}

		// If available, parse additional fields
		if len(record) > 2 {
			overdraftCount, err := strconv.Atoi(record[2])
			if err == nil {
				account.OverdraftCount = overdraftCount
			}
		}

		accounts[accountID] = account
	}

	return accounts, nil
}

// ProcessTransactions applies transactions to account balances
func ProcessTransactions(
	transactions []models.Transaction,
	accounts map[string]models.Account,
) (map[string]models.Account, []models.Transaction) {
	// Create a copy of accounts to avoid modifying the original
	processedAccounts := make(map[string]models.Account)
	for id, account := range accounts {
		processedAccounts[id] = account
	}

	// Process transactions
	processedTransactions := make([]models.Transaction, len(transactions))
	copy(processedTransactions, transactions)

	// Sort transactions by timestamp
	// In a real system, we would sort here, but for simplicity we'll assume
	// transactions are already in chronological order

	for i, transaction := range processedTransactions {
		// Get the account
		account := processedAccounts[transaction.AccountID]

		switch transaction.Type {
		case "credit":
			// Handle deposit
			processedTransactions[i], processedAccounts = processCredit(transaction, account, processedAccounts)

		case "debit":
			// Handle withdrawal
			processedTransactions[i], processedAccounts = processDebit(transaction, account, processedAccounts)

		case "transfer":
			// Handle transfer
			processedTransactions[i], processedAccounts = processTransfer(transaction, processedAccounts)
		}

		// Update last transaction time
		if processedTransactions[i].Status == "completed" {
			account = processedAccounts[transaction.AccountID]
			account.LastTransactionTime = transaction.Timestamp
			processedAccounts[transaction.AccountID] = account
		}
	}

	return processedAccounts, processedTransactions
}

// processCredit handles deposit transactions
func processCredit(
	transaction models.Transaction,
	account models.Account,
	accounts map[string]models.Account,
) (models.Transaction, map[string]models.Account) {
	// Apply credit to account
	account.Balance += transaction.Amount
	account.DailyCredits += transaction.Amount
	accounts[transaction.AccountID] = account

	// Update transaction status
	transaction.Status = "completed"
	return transaction, accounts
}

// processDebit handles withdrawal transactions
func processDebit(
	transaction models.Transaction,
	account models.Account,
	accounts map[string]models.Account,
) (models.Transaction, map[string]models.Account) {
	// Check if withdrawal would exceed daily limit
	if account.DailyDebits+transaction.Amount > MaxDailyWithdrawalLimit {
		transaction.Status = "rejected"
		transaction.ProcessingMessage = fmt.Sprintf("Exceeds daily withdrawal limit of $%.2f", MaxDailyWithdrawalLimit)
		return transaction, accounts
	}

	// Check if withdrawal would exceed overdraft limit
	newBalance := account.Balance - transaction.Amount
	if newBalance < OverdraftLimit {
		transaction.Status = "rejected"
		transaction.ProcessingMessage = fmt.Sprintf("Would exceed overdraft limit of $%.2f", -OverdraftLimit)
		return transaction, accounts
	}

	// Apply debit to account
	account.Balance = newBalance
	account.DailyDebits += transaction.Amount

	// Check if account is in overdraft after this transaction
	if newBalance < 0 {
		account.OverdraftCount++
		transaction.ProcessingMessage = "Account in overdraft"
	}

	accounts[transaction.AccountID] = account

	// Update transaction status
	transaction.Status = "completed"
	return transaction, accounts
}

// processTransfer handles transfer transactions
func processTransfer(
	transaction models.Transaction,
	accounts map[string]models.Account,
) (models.Transaction, map[string]models.Account) {
	sourceAccount := accounts[transaction.AccountID]
	destAccount := accounts[transaction.DestinationAccountID]

	// Check if transfer would exceed overdraft limit
	newBalance := sourceAccount.Balance - transaction.Amount
	if newBalance < OverdraftLimit {
		transaction.Status = "rejected"
		transaction.ProcessingMessage = fmt.Sprintf("Would exceed overdraft limit of $%.2f", -OverdraftLimit)
		return transaction, accounts
	}

	// Apply transfer
	sourceAccount.Balance = newBalance
	sourceAccount.DailyDebits += transaction.Amount
	destAccount.Balance += transaction.Amount
	destAccount.DailyCredits += transaction.Amount

	// Check if source account is in overdraft after this transaction
	if newBalance < 0 {
		sourceAccount.OverdraftCount++
		transaction.ProcessingMessage = "Source account in overdraft"
	}

	accounts[transaction.AccountID] = sourceAccount
	accounts[transaction.DestinationAccountID] = destAccount

	// Update transaction status
	transaction.Status = "completed"
	return transaction, accounts
}
