// detectors/anomaly_detector.go
package detector

import (
	"fmt"

	"DailyTransactionBatchProcessing/models"
)

// Constants for anomaly detection
const (
	LargeTransactionThreshold     = 10000.0 // Transactions above this amount are considered large
	RapidWithdrawalThreshold      = 3       // Number of withdrawals in short period considered suspicious
	RapidWithdrawalTimeWindowMins = 60      // Time window in minutes for rapid withdrawal detection
	OverdraftLimit                = -1000.0 // Maximum allowed overdraft
	MaxDailyWithdrawalLimit       = 5000.0  // Maximum daily withdrawal limit
)

// DetectAnomalies analyzes processed transactions for suspicious patterns
func DetectAnomalies(
	transactions []models.Transaction,
	accounts map[string]models.Account,
) []models.Anomaly {
	anomalies := []models.Anomaly{}

	// Track withdrawals by account for rapid withdrawal detection
	withdrawalsByAccount := make(map[string][]models.Transaction)

	// Process each transaction for anomalies
	for _, transaction := range transactions {
		// Skip rejected transactions
		if transaction.Status != "completed" {
			continue
		}

		// Check for large transactions
		if transaction.Amount >= LargeTransactionThreshold {
			anomalies = append(anomalies, models.Anomaly{
				TransactionID: transaction.ID,
				AccountID:     transaction.AccountID,
				Timestamp:     transaction.Timestamp,
				Type:          "large_transaction",
				Description:   fmt.Sprintf("Large transaction: $%.2f", transaction.Amount),
				Severity:      "medium",
			})
		}

		// Track withdrawals for rapid withdrawal detection
		if transaction.Type == "debit" {
			withdrawalsByAccount[transaction.AccountID] = append(
				withdrawalsByAccount[transaction.AccountID],
				transaction,
			)
		}

		// Check for accounts in overdraft
		account := accounts[transaction.AccountID]
		if account.Balance < 0 {
			severity := "low"
			if account.Balance < OverdraftLimit/2 {
				severity = "medium"
			}
			if account.Balance < OverdraftLimit*0.8 {
				severity = "high"
			}

			anomalies = append(anomalies, models.Anomaly{
				TransactionID: transaction.ID,
				AccountID:     transaction.AccountID,
				Timestamp:     transaction.Timestamp,
				Type:          "account_overdraft",
				Description:   fmt.Sprintf("Account in overdraft: $%.2f", account.Balance),
				Severity:      severity,
			})
		}
	}

	// Detect rapid withdrawals (multiple withdrawals in a short time period)
	for accountID, withdrawals := range withdrawalsByAccount {
		// Sort withdrawals by timestamp (in a real system)
		// For simplicity, we assume they're already in order

		// Check for rapid withdrawals
		if len(withdrawals) >= RapidWithdrawalThreshold {
			for i := RapidWithdrawalThreshold - 1; i < len(withdrawals); i++ {
				start := i - (RapidWithdrawalThreshold - 1)
				timeWindow := withdrawals[i].Timestamp.Sub(withdrawals[start].Timestamp)

				// If the time window between N withdrawals is less than the threshold
				if timeWindow.Minutes() <= RapidWithdrawalTimeWindowMins {
					totalAmount := 0.0
					for j := start; j <= i; j++ {
						totalAmount += withdrawals[j].Amount
					}

					anomalies = append(anomalies, models.Anomaly{
						TransactionID: withdrawals[i].ID,
						AccountID:     accountID,
						Timestamp:     withdrawals[i].Timestamp,
						Type:          "rapid_withdrawals",
						Description: fmt.Sprintf("%d withdrawals totaling $%.2f in %d minutes",
							RapidWithdrawalThreshold, totalAmount, int(timeWindow.Minutes())),
						Severity: "high",
					})

					// Only report once per series to avoid duplicate alerts
					break
				}
			}
		}
	}

	return anomalies
}
