//! Native PT Wealth Hub auto-sync for Hyperliquid and Trade Republic.
//!
//! Replaces external addons by running natively as a background worker
//! in the Wealthfolio server container.
//!
//! Features:
//! - Exact Trade Republic day-by-day ACT/360 interest accrual:
//!   Calculates interest on each day's exact closing balance (supporting multiple
//!   deposits throughout the month), maintains current month's accruing interest,
//!   and automatically compounds/settles the payout at the start of each new month.
//! - Hyperliquid L1 bot balance & spot sync via official info API.
//!
//! Configured via environment variables:
//! - `HL_WALLET_ADDRESS`: 0x... (Hyperliquid L1 bot wallet address)
//! - `HL_POLL_INTERVAL_SECS`: Polling interval in seconds (default: 600 = 10 min)
//! - `HL_ACCOUNT_ID`: Optional explicit account UUID for Hyperliquid
//! - `TR_BASE_PRINCIPAL`: Fallback base principal if no initial deposit exists
//! - `TR_TANB_RATE`: Annual interest rate percentage (default: 2.50)
//! - `TR_DISPLAY_MODE`: "gross" or "net" (after 28% IRS retention)
//! - `TR_ACCOUNT_ID`: Optional explicit account UUID for Trade Republic

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{Datelike, NaiveDate};
use rust_decimal::Decimal;
use serde_json::Value;
use tracing::{debug, info, warn};

use wealthfolio_core::accounts::{Account, AccountServiceTrait, TrackingMode};
use wealthfolio_core::activities::{Activity, ActivityStatus, NewActivity};
use wealthfolio_core::portfolio::snapshot::{AccountStateSnapshot, SnapshotSource};
use wealthfolio_core::utils::time_utils::{parse_user_timezone_or_default, user_today};

use crate::main_lib::AppState;

/// Starts the PT Wealth Hub background worker.
pub fn start_pt_wealth_hub(state: Arc<AppState>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Initial delay to let the database and event bus fully initialize
        tokio::time::sleep(Duration::from_secs(10)).await;

        let poll_secs = std::env::var("HL_POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(600)
            .max(30);

        info!(
            "PT Wealth Hub background worker started (polling interval: {}s)",
            poll_secs
        );

        let mut interval = tokio::time::interval(Duration::from_secs(poll_secs));

        loop {
            interval.tick().await;

            if let Err(err) = sync_trade_republic(&state).await {
                warn!("[PT-Wealth-Hub] Trade Republic auto-sync error: {err}");
            }

            if let Err(err) = sync_hyperliquid(&state).await {
                warn!("[PT-Wealth-Hub] Hyperliquid auto-sync error: {err}");
            }
        }
    })
}

// ─── Trade Republic Banking-Grade Accrual Engine ─────────────────────────────

async fn sync_trade_republic(state: &Arc<AppState>) -> anyhow::Result<()> {
    let tr_base_env = std::env::var("TR_BASE_PRINCIPAL").ok();
    let tr_acc_id_env = std::env::var("TR_ACCOUNT_ID").ok();

    // Skip quietly if not configured
    if tr_base_env.is_none() && tr_acc_id_env.is_none() {
        return Ok(());
    }

    let accounts = state.account_service.get_all_accounts()?;
    let account = find_account(&accounts, tr_acc_id_env.as_deref(), &["trade", "republic"]);

    let Some(account) = account else {
        debug!("[PT-Wealth-Hub] Trade Republic account not found, skipping sync");
        return Ok(());
    };

    let timezone = state
        .timezone
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let today = user_today(parse_user_timezone_or_default(&timezone));

    let tanb_rate = std::env::var("TR_TANB_RATE")
        .ok()
        .and_then(|s| Decimal::from_str(s.trim()).ok())
        .unwrap_or_else(|| Decimal::new(250, 2)); // default 2.50%

    let display_mode = std::env::var("TR_DISPLAY_MODE")
        .unwrap_or_else(|_| "gross".to_string())
        .to_lowercase();

    let fallback_base = tr_base_env
        .and_then(|s| Decimal::from_str(s.trim()).ok())
        .unwrap_or(Decimal::ZERO);

    // Fetch existing activities on this account
    let activities = state
        .activity_service
        .get_activities_by_account_id(&account.id)?;

    let mut manual_activities: Vec<Activity> = Vec::new();
    let mut old_temp_auto_ids = Vec::new();
    let mut existing_temp_amount = Decimal::ZERO;

    for act in &activities {
        let is_payout = act
            .notes
            .as_deref()
            .is_some_and(|n| n.contains("[Auto-TR-Payout]"));

        let is_temp_auto = !is_payout
            && act.notes.as_deref().is_some_and(|notes| {
                notes.contains("[Auto-TR]")
                    || notes.starts_with("Ajuste de Saldo")
                    || notes.starts_with("Reset Trade Republic")
                    || notes.starts_with("Trade Republic PT50 Juros")
            });

        if is_temp_auto {
            old_temp_auto_ids.push(act.id.clone());
            if let Some(amt) = act.amount {
                existing_temp_amount = amt;
            }
        } else {
            // Manual deposits, withdrawals, or previous month settled payouts
            manual_activities.push(act.clone());
        }
    }

    let first_of_current_month =
        NaiveDate::from_ymd_opt(today.year(), today.month(), 1).unwrap_or(today);

    // ── Step 1: Automatic Monthly Compounding Payout (on 1st of month) ─────────
    if let Some(last_day_prev_month) = first_of_current_month.pred_opt() {
        let prev_month_payout_note = format!(
            "[Auto-TR-Payout] Juros Pagos Trade Republic ({:02}/{})",
            last_day_prev_month.month(),
            last_day_prev_month.year()
        );

        let already_paid = manual_activities.iter().any(|a| {
            a.notes
                .as_deref()
                .is_some_and(|n| n.contains(&prev_month_payout_note))
        });

        if !already_paid {
            let first_of_prev_month =
                NaiveDate::from_ymd_opt(last_day_prev_month.year(), last_day_prev_month.month(), 1)
                    .unwrap_or(last_day_prev_month);

            let mut prev_month_interest = Decimal::ZERO;
            let mut d = first_of_prev_month;

            while d <= last_day_prev_month {
                let bal = compute_balance_on_date(&manual_activities, d, fallback_base);
                if bal > Decimal::ZERO {
                    prev_month_interest +=
                        bal * (tanb_rate / Decimal::from(100)) / Decimal::from(360);
                }
                if let Some(next) = d.succ_opt() {
                    d = next;
                } else {
                    break;
                }
            }

            if display_mode == "net" {
                prev_month_interest = (prev_month_interest * Decimal::new(72, 2)).round_dp(2);
            } else {
                prev_month_interest = prev_month_interest.round_dp(2);
            }

            if prev_month_interest > Decimal::ZERO {
                let payout_act = NewActivity {
                    id: None,
                    account_id: account.id.clone(),
                    asset: None,
                    activity_type: "INTEREST".to_string(),
                    subtype: None,
                    activity_date: format!(
                        "{}T00:00:00Z",
                        first_of_current_month.format("%Y-%m-%d")
                    ),
                    quantity: None,
                    unit_price: None,
                    currency: account.currency.clone(),
                    fee: None,
                    tax: None,
                    amount: Some(prev_month_interest),
                    status: Some(ActivityStatus::Posted),
                    notes: Some(format!(
                        "{} (+{:.2} {})",
                        prev_month_payout_note, prev_month_interest, account.currency
                    )),
                    fx_rate: None,
                    metadata: None,
                    needs_review: Some(false),
                    source_system: None,
                    source_record_id: None,
                    source_group_id: None,
                    idempotency_key: None,
                    import_run_id: None,
                };

                if let Ok(created) = state.activity_service.create_activity(payout_act).await {
                    info!(
                        "[PT-Wealth-Hub] Trade Republic monthly interest compounded into principal: +{:.2} {}",
                        prev_month_interest, account.currency
                    );
                    manual_activities.push(created);
                }
            }
        }
    }

    // ── Step 2: Day-by-Day Interest Accrual for Current Month ─────────────────
    let last_accrual_day = today.pred_opt().unwrap_or(today);
    let mut current_month_accrued = Decimal::ZERO;
    let mut d = first_of_current_month;

    while d <= last_accrual_day {
        let bal = compute_balance_on_date(&manual_activities, d, fallback_base);
        if bal > Decimal::ZERO {
            current_month_accrued += bal * (tanb_rate / Decimal::from(100)) / Decimal::from(360);
        }
        if let Some(next) = d.succ_opt() {
            d = next;
        } else {
            break;
        }
    }

    if display_mode == "net" {
        current_month_accrued = (current_month_accrued * Decimal::new(72, 2)).round_dp(2);
    } else {
        current_month_accrued = current_month_accrued.round_dp(2);
    }

    let today_balance = compute_balance_on_date(&manual_activities, today, fallback_base);
    let mut daily_rate =
        (today_balance * (tanb_rate / Decimal::from(100)) / Decimal::from(360)).round_dp(2);
    if display_mode == "net" {
        daily_rate = (daily_rate * Decimal::new(72, 2)).round_dp(2);
    }

    // ── Step 3: Update Account Balances in Wealthfolio ────────────────────────
    if account.tracking_mode == TrackingMode::Holdings {
        let total = today_balance + current_month_accrued;
        let mut cash_balances = std::collections::HashMap::new();
        cash_balances.insert(account.currency.clone(), total);
        let snapshot = AccountStateSnapshot {
            id: AccountStateSnapshot::stable_id(&account.id, today),
            account_id: account.id.clone(),
            snapshot_date: today,
            currency: account.currency.clone(),
            source: SnapshotSource::ManualEntry,
            cash_balances,
            cash_total_account_currency: total,
            ..Default::default()
        };

        state
            .snapshot_service
            .save_manual_snapshot(&account.id, snapshot)
            .await?;

        info!(
            "[PT-Wealth-Hub] Trade Republic snapshot updated: {:.2} {} (Saldo base: {:.2}, Juros decorrentes: {:.2})",
            total, account.currency, today_balance, current_month_accrued
        );
    } else {
        // In Transactions mode, keep all deposits intact and update the accrual entry
        if current_month_accrued == existing_temp_amount && !old_temp_auto_ids.is_empty() {
            debug!(
                "[PT-Wealth-Hub] Trade Republic interest up to date (+{:.2} {})",
                current_month_accrued, account.currency
            );
            return Ok(());
        }

        // Remove old temporary month accrual activity
        for old_id in old_temp_auto_ids {
            let _ = state.activity_service.delete_activity(old_id).await;
        }

        if current_month_accrued > Decimal::ZERO {
            let activity = NewActivity {
                id: None,
                account_id: account.id.clone(),
                asset: None,
                activity_type: "INTEREST".to_string(),
                subtype: None,
                activity_date: format!("{}T00:00:00Z", today.format("%Y-%m-%d")),
                quantity: None,
                unit_price: None,
                currency: account.currency.clone(),
                fee: None,
                tax: None,
                amount: Some(current_month_accrued),
                status: Some(ActivityStatus::Posted),
                notes: Some(format!(
                    "[Auto-TR] Juros a Decorrer ({:02}/{}): +{:.2} {} (~{:.2} {}/dia)",
                    today.month(),
                    today.year(),
                    current_month_accrued,
                    account.currency,
                    daily_rate,
                    account.currency
                )),
                fx_rate: None,
                metadata: None,
                needs_review: Some(false),
                source_system: None,
                source_record_id: None,
                source_group_id: None,
                idempotency_key: None,
                import_run_id: None,
            };

            state.activity_service.create_activity(activity).await?;

            info!(
                "[PT-Wealth-Hub] Trade Republic juros a decorrer atualizados: +{:.2} {} (~{:.2} {}/dia sobre saldo {:.2} {})",
                current_month_accrued, account.currency, daily_rate, account.currency, today_balance, account.currency
            );
        }
    }

    Ok(())
}

fn compute_balance_on_date(
    activities: &[Activity],
    target_date: NaiveDate,
    fallback_base: Decimal,
) -> Decimal {
    let mut balance = Decimal::ZERO;
    let mut count = 0;

    for act in activities {
        if act.activity_date.date_naive() <= target_date {
            count += 1;
            let amount = act.amount.unwrap_or(Decimal::ZERO);
            match act.activity_type.as_str() {
                "DEPOSIT" | "CREDIT" | "INTEREST" | "DIVIDEND" | "SELL" | "TRANSFER_IN" => {
                    balance += amount;
                }
                "WITHDRAWAL" | "FEE" | "TAX" | "BUY" | "TRANSFER_OUT" => {
                    balance -= amount;
                }
                _ => {}
            }
        }
    }

    if count == 0 {
        fallback_base
    } else {
        balance
    }
}

// ─── Hyperliquid Auto-Sync ───────────────────────────────────────────────────

async fn sync_hyperliquid(state: &Arc<AppState>) -> anyhow::Result<()> {
    let wallet_env = match std::env::var("HL_WALLET_ADDRESS") {
        Ok(w) if !w.trim().is_empty() => w.trim().to_string(),
        _ => return Ok(()),
    };

    let hl_acc_id_env = std::env::var("HL_ACCOUNT_ID").ok();
    let accounts = state.account_service.get_all_accounts()?;
    let account = find_account(
        &accounts,
        hl_acc_id_env.as_deref(),
        &["hyperliquid", "hyprliquid", "bot"],
    );

    let Some(account) = account else {
        debug!("[PT-Wealth-Hub] Hyperliquid account not found, skipping sync");
        return Ok(());
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;

    let perps_req = serde_json::json!({
        "type": "clearinghouseState",
        "user": &wallet_env
    });
    let spot_req = serde_json::json!({
        "type": "spotClearinghouseState",
        "user": &wallet_env
    });

    let (perps_res, spot_res) = tokio::join!(
        client
            .post("https://api.hyperliquid.xyz/info")
            .json(&perps_req)
            .send(),
        client
            .post("https://api.hyperliquid.xyz/info")
            .json(&spot_req)
            .send()
    );

    let perps_val: Value = perps_res?.json().await?;
    let mut total_equity = Decimal::ZERO;

    // Perps accountValue
    if let Some(val_str) = perps_val
        .get("marginSummary")
        .and_then(|m| m.get("accountValue"))
        .and_then(|v| v.as_str())
    {
        if let Ok(val) = Decimal::from_str(val_str) {
            total_equity += val;
        }
    }

    // Spot balances (USDC, USDT, USDH)
    if let Ok(res) = spot_res {
        if let Ok(spot_val) = res.json::<Value>().await {
            if let Some(balances) = spot_val.get("balances").and_then(|b| b.as_array()) {
                for b in balances {
                    let coin = b.get("coin").and_then(|c| c.as_str()).unwrap_or("");
                    if coin == "USDC" || coin == "USDT" || coin == "USDH" {
                        if let Some(total_str) = b.get("total").and_then(|t| t.as_str()) {
                            if let Ok(amt) = Decimal::from_str(total_str) {
                                total_equity += amt;
                            }
                        }
                    }
                }
            }
        }
    }

    let total_usd = total_equity.round_dp(2);
    let timezone = state
        .timezone
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    let today = user_today(parse_user_timezone_or_default(&timezone));

    if account.tracking_mode == TrackingMode::Holdings {
        let mut cash_balances = std::collections::HashMap::new();
        cash_balances.insert("USD".to_string(), total_usd);
        let snapshot = AccountStateSnapshot {
            id: AccountStateSnapshot::stable_id(&account.id, today),
            account_id: account.id.clone(),
            snapshot_date: today,
            currency: "USD".to_string(),
            source: SnapshotSource::ManualEntry,
            cash_balances,
            cash_total_account_currency: total_usd,
            ..Default::default()
        };

        state
            .snapshot_service
            .save_manual_snapshot(&account.id, snapshot)
            .await?;

        info!(
            "[PT-Wealth-Hub] Hyperliquid equity updated: {:.2} USD",
            total_usd
        );
    } else {
        // In Transactions mode, create or adjust balance to match total_usd
        let activities = state
            .activity_service
            .get_activities_by_account_id(&account.id)?;

        let mut manual_sum = Decimal::ZERO;
        let mut old_auto_hl_ids = Vec::new();

        for act in &activities {
            let is_auto = act.notes.as_deref().is_some_and(|notes| {
                notes.contains("[Auto-HL]") || notes.contains("Hyperliquid L1 Bot Equity")
            });

            if is_auto {
                old_auto_hl_ids.push(act.id.clone());
                continue;
            }

            let amount = act.amount.unwrap_or(Decimal::ZERO);
            match act.activity_type.as_str() {
                "DEPOSIT" | "CREDIT" | "INTEREST" | "DIVIDEND" | "SELL" | "TRANSFER_IN" => {
                    manual_sum += amount;
                }
                "WITHDRAWAL" | "FEE" | "TAX" | "BUY" | "TRANSFER_OUT" => {
                    manual_sum -= amount;
                }
                _ => {}
            }
        }

        let diff = total_usd - manual_sum;
        let (activity_type, amount) = if diff >= Decimal::ZERO {
            ("CREDIT", diff)
        } else {
            ("WITHDRAWAL", -diff)
        };

        for old_id in old_auto_hl_ids {
            let _ = state.activity_service.delete_activity(old_id).await;
        }

        if amount >= Decimal::new(1, 2) {
            let activity = NewActivity {
                id: None,
                account_id: account.id.clone(),
                asset: None,
                activity_type: activity_type.to_string(),
                subtype: None,
                activity_date: format!("{}T00:00:00Z", today.format("%Y-%m-%d")),
                quantity: None,
                unit_price: None,
                currency: "USD".to_string(),
                fee: None,
                tax: None,
                amount: Some(amount),
                status: Some(ActivityStatus::Posted),
                notes: Some(format!("[Auto-HL] (Alvo: {:.2} USD)", total_usd)),
                fx_rate: None,
                metadata: None,
                needs_review: Some(false),
                source_system: None,
                source_record_id: None,
                source_group_id: None,
                idempotency_key: None,
                import_run_id: None,
            };

            state.activity_service.create_activity(activity).await?;

            info!(
                "[PT-Wealth-Hub] Hyperliquid balance adjusted to match live equity: {:.2} USD",
                total_usd
            );
        }
    }

    Ok(())
}

fn find_account(
    accounts: &[Account],
    explicit_id: Option<&str>,
    keywords: &[&str],
) -> Option<Account> {
    if let Some(id) = explicit_id {
        if let Some(acc) = accounts.iter().find(|a| a.id == id) {
            return Some(acc.clone());
        }
    }

    accounts
        .iter()
        .find(|a| {
            let lower = a.name.to_lowercase();
            keywords.iter().any(|k| lower.contains(k))
        })
        .cloned()
}
