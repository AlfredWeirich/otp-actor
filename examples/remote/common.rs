//! Shared domain types for remote actor examples.

use otp_actor::actor::{ActorContext, StatefulActor};
use serde::{Deserialize, Serialize};

/// Resilient Bank Account Actor state machine.
#[derive(Clone, Default)]
pub struct BankAccountActor {
    pub account_id: String,
    pub balance: i64,
}

/// Messages accepted by the [`BankAccountActor`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccountMsg {
    /// Deposit funds into account.
    Deposit(i64),
    /// Withdraw funds from account.
    Withdraw(i64),
    /// Query current balance.
    GetBalance,
    /// Simulates an unhandled crash inside the actor handler.
    SimulateCrash,
    /// Requests graceful domain termination of this actor via `ctx.stop()`.
    CloseAccount,
}

impl StatefulActor for BankAccountActor {
    type Message = AccountMsg;
    type Response = i64;
    type Error = String;

    async fn on_start(&mut self, _ctx: &mut ActorContext<Self>) -> Result<(), Self::Error> {
        tracing::info!(
            account = %self.account_id,
            initial_balance = self.balance,
            "BankAccountActor starting up"
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext<Self>,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            AccountMsg::Deposit(amount) => {
                if amount <= 0 {
                    return Err("Deposit amount must be positive".into());
                }
                self.balance += amount;
                tracing::info!(
                    account = %self.account_id,
                    amount,
                    balance = self.balance,
                    "Deposit processed"
                );
                Ok(self.balance)
            }
            AccountMsg::Withdraw(amount) => {
                if amount <= 0 {
                    return Err("Withdrawal amount must be positive".into());
                }
                if amount > self.balance {
                    return Err(format!(
                        "Insufficient funds: requested {amount}, available {}",
                        self.balance
                    ));
                }
                self.balance -= amount;
                tracing::info!(
                    account = %self.account_id,
                    amount,
                    balance = self.balance,
                    "Withdrawal processed"
                );
                Ok(self.balance)
            }
            AccountMsg::GetBalance => Ok(self.balance),
            AccountMsg::SimulateCrash => {
                tracing::warn!(
                    account = %self.account_id,
                    "💥 Simulating unhandled failure in BankAccountActor!"
                );
                ctx.fail("Fatal panic/crash simulation in actor handler");
                Err("Actor crashed".into())
            }
            AccountMsg::CloseAccount => {
                tracing::info!(
                    account = %self.account_id,
                    final_balance = self.balance,
                    "🔒 Client requested account closure. Stopping actor gracefully via ctx.stop()..."
                );
                ctx.stop();
                Ok(self.balance)
            }
        }
    }

    async fn on_stop(&mut self, _ctx: &mut ActorContext<Self>) {
        tracing::info!(
            account = %self.account_id,
            final_balance = self.balance,
            "BankAccountActor stopped gracefully"
        );
    }
}
