package models

import (
	"time"
)

// Account represents a bank account
type Account struct {
	ID                  string    `json:"id"`
	Balance             float64   `json:"balance"`
	DailyDebits         float64   `json:"daily_debits"`
	DailyCredits        float64   `json:"daily_credits"`
	LastTransactionTime time.Time `json:"last_transaction_time"`
	OverdraftCount      int       `json:"overdraft_count"`
}

// Transaction represents a bank transaction
type Transaction struct {
	ID                   string    `json:"id"`
	AccountID            string    `json:"account_id"`
	DestinationAccountID string    `json:"destination_account_id,omitempty"`
	Timestamp            time.Time `json:"timestamp"`
	Amount               float64   `json:"amount"`
	Type                 string    `json:"type"` // credit, debit, transfer
	Status               string    `json:"status"`
	Description          string    `json:"description,omitempty"`
	ValidationMessage    string    `json:"validation_message,omitempty"`
	ProcessingMessage    string    `json:"processing_message,omitempty"`
}

// Anomaly represents a detected anomaly in transaction processing
type Anomaly struct {
	TransactionID string    `json:"transaction_id"`
	AccountID     string    `json:"account_id"`
	Timestamp     time.Time `json:"timestamp"`
	Type          string    `json:"type"`
	Description   string    `json:"description"`
	Severity      string    `json:"severity"` // low, medium, high
}

// AccountSummary represents a daily summary for an account
type AccountSummary struct {
	AccountID        string  `json:"account_id"`
	Date             string  `json:"date"`
	OpeningBalance   float64 `json:"opening_balance"`
	ClosingBalance   float64 `json:"closing_balance"`
	TotalDebits      float64 `json:"total_debits"`
	TotalCredits     float64 `json:"total_credits"`
	TransactionCount int     `json:"transaction_count"`
	OverdraftCount   int     `json:"overdraft_count"`
}
