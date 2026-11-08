//! # Root Supervisor Implementation
//!
//! The top-level supervisor actor in the supervision tree. Manages the lifecycle of
//! domain-level service supervisors, isolates subsystem panics, and accepts dynamic
//! self-registrations from autonomous service supervisors.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot, watch,
};
use tokio::task::{JoinError, JoinHandle, JoinSet};

use super::model::{RestartPolicy, RestartTracker, RootError, ShutdownPolicy, TaskResult};
use super::service_supervisor::{ServiceSupervisorCommand, ServiceSupervisorHandle};

/// Factory closure used by RootSupervisor to instantiate new or replacement service supervisors.
pub(crate) type ServiceFactory = Arc<
    dyn Fn(usize) -> (Sender<ServiceSupervisorCommand>, JoinHandle<TaskResult>)
        + Send
        + Sync
        + 'static,
>;

/// Commands accepted by the [`RootSupervisor`].
pub(crate) enum RootCommand {
    /// An autonomous service supervisor announces itself to the root supervisor.
    RegisterServiceSelf {
        name: String,
        sender: Sender<ServiceSupervisorCommand>,
        task_handle: JoinHandle<TaskResult>,
        factory: ServiceFactory,
        restart_policy: RestartPolicy,
        shutdown_policy: ShutdownPolicy,
        respond_to: oneshot::Sender<Result<(), RootError>>,
    },
    /// Resolve the current active communication channel for a registered service supervisor.
    ResolveServiceChannel {
        service_name: String,
        respond_to: oneshot::Sender<Result<Sender<ServiceSupervisorCommand>, RootError>>,
    },
    /// Triggers an intentional panic in a specific service supervisor for chaos testing.
    TriggerServiceCrash {
        service_name: String,
        respond_to: oneshot::Sender<Result<(), RootError>>,
    },
    /// Manually restarts or recovers a service supervisor that has permanently failed or needs a reset.
    RestartService {
        service_name: String,
        respond_to: oneshot::Sender<Result<(), RootError>>,
    },
    /// Gracefully stops a specific service supervisor and all of its supervised child actors.
    StopService {
        service_name: String,
        respond_to: oneshot::Sender<Result<(), RootError>>,
    },
    /// Gracefully shuts down the entire supervision tree (all services and their child actors).
    Shutdown { respond_to: oneshot::Sender<()> },
}

/// Metadata stored for each actively supervised service.
struct SupervisedService {
    sender: Sender<ServiceSupervisorCommand>,
    abort_handle: tokio::task::AbortHandle,
    term_rx: watch::Receiver<bool>,
    factory: ServiceFactory,
    shutdown_policy: ShutdownPolicy,
    instance_id: usize,
    restart_ticket: u64,
    restart_tracker: RestartTracker,
    is_failed: bool,
    is_stopped: bool,
    is_stopping: bool,
    is_restarting: bool,
    pending_resolutions: Vec<oneshot::Sender<Result<Sender<ServiceSupervisorCommand>, RootError>>>,
}

/// The top-level Root Supervisor actor overseeing all sub-services.
pub struct RootSupervisor {
    receiver: Receiver<RootCommand>,
    services: HashMap<String, SupervisedService>,
    service_order: Vec<String>,
    next_restart_ticket: u64,
    join_set: JoinSet<(String, usize, Result<TaskResult, JoinError>)>,
}

impl RootSupervisor {
    /// Spawns the root supervisor actor onto the current Tokio runtime and returns a handle for dynamic registration.
    ///
    /// # Panics
    /// Panics if called outside the context of an active Tokio runtime.
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> RootSupervisorHandle {
        let (tx, rx) = mpsc::channel(256);
        let supervisor = Self {
            receiver: rx,
            services: HashMap::new(),
            service_order: Vec::new(),
            next_restart_ticket: 0,
            join_set: JoinSet::new(),
        };

        tokio::spawn(supervisor.run());
        RootSupervisorHandle { sender: tx }
    }

    /// Allocates a monotonically increasing ticket to uniquely identify a restart wave.
    fn allocate_restart_ticket(&mut self) -> u64 {
        self.next_restart_ticket = self.next_restart_ticket.wrapping_add(1);
        self.next_restart_ticket
    }

    /// Signals graceful shutdown (or brutal kill) to a running service supervisor according to its
    /// [`ShutdownPolicy`] and returns a watch receiver that resolves once the service task terminates.
    fn initiate_service_termination(
        service_name: String,
        entry: &mut SupervisedService,
    ) -> watch::Receiver<bool> {
        let policy = entry.shutdown_policy;
        let abort_handle = entry.abort_handle.clone();
        let term_rx = entry.term_rx.clone();
        let sender = entry.sender.clone();

        match policy {
            ShutdownPolicy::BrutalKill => {
                tracing::info!(
                    service = %service_name,
                    "[RootSupervisor] ⚡ BrutalKill policy: Immediately aborting ServiceSupervisor."
                );
                abort_handle.abort();
            }
            ShutdownPolicy::Timeout(timeout) => {
                tracing::info!(
                    service = %service_name,
                    timeout = ?timeout,
                    "[RootSupervisor] 🛑 Signaling graceful shutdown to ServiceSupervisor with timeout..."
                );
                let mut rx_for_timeout = term_rx.clone();
                tokio::spawn(async move {
                    let timed_out = tokio::time::timeout(timeout, async {
                        let (ack_tx, _ack_rx) = oneshot::channel();
                        let _ = sender
                            .send(ServiceSupervisorCommand::Shutdown { respond_to: ack_tx })
                            .await;
                        let _ = rx_for_timeout.wait_for(|&done| done).await;
                    })
                    .await
                    .is_err();
                    if timed_out {
                        tracing::warn!(
                            service = %service_name,
                            timeout = ?timeout,
                            "[RootSupervisor] ⚠️ ServiceSupervisor exceeded graceful shutdown timeout; forcefully aborting!"
                        );
                        abort_handle.abort();
                    }
                });
            }
            ShutdownPolicy::Infinity => {
                tracing::info!(
                    service = %service_name,
                    "[RootSupervisor] 🛑 Signaling graceful shutdown to ServiceSupervisor (Infinity policy)..."
                );
                tokio::spawn(async move {
                    let (ack_tx, _ack_rx) = oneshot::channel();
                    let _ = sender
                        .send(ServiceSupervisorCommand::Shutdown { respond_to: ack_tx })
                        .await;
                });
            }
        }

        term_rx
    }

    /// Initiates a non-blocking shutdown of a single service supervisor according to its [`ShutdownPolicy`].
    fn trigger_service_shutdown(
        service_name: String,
        entry: &mut SupervisedService,
        restart_ticket: u64,
        respond_to: Option<oneshot::Sender<Result<(), RootError>>>,
    ) {
        entry.is_stopping = true;
        entry.is_stopped = true;
        entry.is_restarting = false;
        entry.restart_ticket = restart_ticket;
        for waiter in entry.pending_resolutions.drain(..) {
            let _ = waiter.send(Err(RootError::ServiceNotFound(service_name.clone())));
        }

        let mut term_rx = Self::initiate_service_termination(service_name, entry);
        tokio::spawn(async move {
            let _ = term_rx.wait_for(|&done| done).await;
            if let Some(tx) = respond_to {
                let _ = tx.send(Ok(()));
            }
        });
    }

    /// Gracefully shuts down all supervised services sequentially in reverse registration order.
    async fn graceful_shutdown_all(&mut self) {
        tracing::info!(
            services = self.services.len(),
            "[RootSupervisor] 🛑 Initiating sequential graceful shutdown of entire supervision tree..."
        );

        let ordered_services: Vec<String> = self.service_order.iter().rev().cloned().collect();
        for service_name in ordered_services {
            let ticket = self.allocate_restart_ticket();
            let maybe_rx = if let Some(entry) = self.services.get_mut(&service_name)
                && !entry.is_stopped
            {
                let (tx, rx) = oneshot::channel();
                Self::trigger_service_shutdown(service_name, entry, ticket, Some(tx));
                Some(rx)
            } else {
                None
            };
            if let Some(rx) = maybe_rx {
                let _ = rx.await;
            }
        }

        while self.join_set.join_next().await.is_some() {}

        tracing::info!("[RootSupervisor] ✅ Supervision tree graceful shutdown complete.");
    }

    /// Executes a service supervisor restart immediately if its restart ticket matches.
    fn execute_service_restart(&mut self, service_name: &str, expected_ticket: u64) {
        if let Some(service_entry) = self.services.get_mut(service_name)
            && !service_entry.is_failed
            && !service_entry.is_stopping
            && service_entry.restart_ticket == expected_ticket
        {
            service_entry.abort_handle.abort();
            service_entry.is_stopped = false;
            service_entry.instance_id += 1;
            let new_inst = service_entry.instance_id;
            service_entry.is_restarting = false;

            tracing::info!(
                service = %service_name,
                instance = new_inst,
                "[RootSupervisor] 🔄 One-for-One Restart: Spawning replacement ServiceSupervisor..."
            );

            let (new_tx, task_handle) = (service_entry.factory)(new_inst);
            let abort_handle = task_handle.abort_handle();
            let (term_tx, term_rx) = watch::channel(false);
            let name_owned = service_name.to_string();

            self.join_set.spawn(async move {
                let res = task_handle.await;
                let _ = term_tx.send(true);
                (name_owned, new_inst, res)
            });

            service_entry.abort_handle = abort_handle;
            service_entry.term_rx = term_rx;
            service_entry.sender = new_tx.clone();
            for waiter in service_entry.pending_resolutions.drain(..) {
                let _ = waiter.send(Ok(new_tx.clone()));
            }
            tracing::info!(
                service = %service_name,
                instance = new_inst,
                "[RootSupervisor] 🌐 ServiceSupervisor online & updated in root registry."
            );
        }
    }

    /// Main event loop of the Root Supervisor.
    async fn run(mut self) {
        tracing::info!(
            "[RootSupervisor] 👑 Root Supervisor online. Awaiting autonomous service registrations..."
        );

        let (scheduled_restart_tx, mut scheduled_restart_rx) =
            mpsc::channel::<(String, u64, Option<oneshot::Sender<Result<(), RootError>>>)>(64);

        loop {
            tokio::select! {
                Some((service_name, expected_ticket, maybe_respond_to)) = scheduled_restart_rx.recv() => {
                    self.execute_service_restart(&service_name, expected_ticket);
                    if let Some(respond_to) = maybe_respond_to {
                        let _ = respond_to.send(Ok(()));
                    }
                }

                cmd_opt = self.receiver.recv() => {
                    let Some(cmd) = cmd_opt else {
                        tracing::info!("[RootSupervisor] All root supervisor handles dropped. Shutting down.");
                        break;
                    };

                    match cmd {
                        RootCommand::RegisterServiceSelf {
                            name,
                            sender,
                            task_handle,
                            factory,
                            restart_policy,
                            shutdown_policy,
                            respond_to,
                        } => {
                            let already_running = self
                                .services
                                .get(&name)
                                .is_some_and(|s| !s.is_stopped);
                            if already_running {
                                let _ = respond_to.send(Err(RootError::ServiceAlreadyExists(name)));
                                continue;
                            }

                            let inst_id = self
                                .services
                                .get(&name)
                                .map_or(1, |s| s.instance_id + 1);
                            let ticket = self.allocate_restart_ticket();

                            tracing::info!(
                                service = %name,
                                instance = inst_id,
                                "[RootSupervisor] 📥 ServiceSupervisor announced itself and self-registered with root."
                            );

                            let abort_handle = task_handle.abort_handle();
                            let (term_tx, term_rx) = watch::channel(false);
                            let name_for_task = name.clone();
                            self.join_set.spawn(async move {
                                let res = task_handle.await;
                                let _ = term_tx.send(true);
                                (name_for_task, inst_id, res)
                            });

                            if !self.service_order.contains(&name) {
                                self.service_order.push(name.clone());
                            }

                            self.services.insert(
                                name,
                                SupervisedService {
                                    sender,
                                    abort_handle,
                                    term_rx,
                                    factory,
                                    shutdown_policy,
                                    instance_id: inst_id,
                                    restart_ticket: ticket,
                                    restart_tracker: RestartTracker::new(restart_policy),
                                    is_failed: false,
                                    is_stopped: false,
                                    is_stopping: false,
                                    is_restarting: false,
                                    pending_resolutions: Vec::new(),
                                },
                            );

                            let _ = respond_to.send(Ok(()));
                        }

                        RootCommand::ResolveServiceChannel { service_name, respond_to } => {
                            if let Some(service) = self.services.get_mut(&service_name) {
                                if service.is_failed {
                                    let _ = respond_to.send(Err(RootError::MaxRestartsExceeded(
                                        service_name,
                                        service.restart_tracker.policy.max_restarts,
                                        service.restart_tracker.policy.time_window,
                                    )));
                                } else if service.is_stopped || service.is_stopping {
                                    let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                                } else if service.is_restarting || service.sender.is_closed() {
                                    service.pending_resolutions.push(respond_to);
                                } else {
                                    let _ = respond_to.send(Ok(service.sender.clone()));
                                }
                            } else {
                                let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                            }
                        }

                        RootCommand::TriggerServiceCrash { service_name, respond_to } => {
                            if let Some(service) = self.services.get(&service_name) {
                                if service.is_failed {
                                    let _ = respond_to.send(Err(RootError::MaxRestartsExceeded(
                                        service_name,
                                        service.restart_tracker.policy.max_restarts,
                                        service.restart_tracker.policy.time_window,
                                    )));
                                } else if service.is_stopped || service.is_stopping {
                                    let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                                } else {
                                    let sender = service.sender.clone();
                                    tokio::spawn(async move {
                                        let res = sender.send(ServiceSupervisorCommand::SimulateCrash).await;
                                        let _ = respond_to.send(res.map_err(|_| RootError::RootUnavailable));
                                    });
                                }
                            } else {
                                let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                            }
                        }

                        RootCommand::RestartService { service_name, respond_to } => {
                            let ticket = self.allocate_restart_ticket();
                            if let Some(service) = self.services.get_mut(&service_name) {
                                let is_running = !service.is_stopped && !*service.term_rx.borrow();
                                service.is_failed = false;
                                service.is_stopped = false;
                                service.is_stopping = false;
                                service.is_restarting = true;
                                service.restart_ticket = ticket;
                                service.restart_tracker = RestartTracker::new(service.restart_tracker.policy);

                                tracing::info!(
                                    service = %service_name,
                                    "🔄 Manual service restart triggered via RootSupervisor."
                                );

                                if is_running {
                                    let mut term_rx = Self::initiate_service_termination(
                                        service_name.clone(),
                                        service,
                                    );
                                    let tx = scheduled_restart_tx.clone();
                                    tokio::spawn(async move {
                                        let _ = term_rx.wait_for(|&done| done).await;
                                        let _ = tx.send((service_name, ticket, Some(respond_to))).await;
                                    });
                                } else {
                                    self.execute_service_restart(&service_name, ticket);
                                    let _ = respond_to.send(Ok(()));
                                }
                            } else {
                                let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                            }
                        }

                        RootCommand::StopService { service_name, respond_to } => {
                            let ticket = self.allocate_restart_ticket();
                            if let Some(entry) = self.services.get_mut(&service_name) {
                                if entry.is_stopped {
                                    let _ = respond_to.send(Ok(()));
                                } else {
                                    Self::trigger_service_shutdown(
                                        service_name,
                                        entry,
                                        ticket,
                                        Some(respond_to),
                                    );
                                }
                            } else {
                                let _ = respond_to.send(Err(RootError::ServiceNotFound(service_name)));
                            }
                        }

                        RootCommand::Shutdown { respond_to } => {
                            self.receiver.close();
                            self.graceful_shutdown_all().await;
                            let _ = respond_to.send(());
                            return;
                        }
                    }
                }

                Some(join_res) = self.join_set.join_next() => {
                    if let Ok((service_name, inst_id, task_res)) = join_res {
                        if let Some(entry) = self.services.get(&service_name)
                            && (inst_id < entry.instance_id
                                || entry.is_stopping
                                || entry.is_restarting
                                || entry.is_failed)
                        {
                            continue;
                        }

                        let crash_reason: Option<String> = match task_res {
                            Ok(Ok(())) => {
                                tracing::info!(
                                    service = %service_name,
                                    "[RootSupervisor] Service completed lifecycle cleanly."
                                );
                                if let Some(entry) = self.services.get_mut(&service_name)
                                    && entry.instance_id == inst_id
                                {
                                    entry.is_stopped = true;
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(RootError::ServiceNotFound(service_name.clone())));
                                    }
                                }
                                None
                            }
                            Ok(Err(reason)) => Some(reason),
                            Err(panic_err) if panic_err.is_cancelled() => {
                                continue;
                            }
                            Err(panic_err) => Some(format!("Task panicked: {:?}", panic_err)),
                        };

                        let Some(crash_reason) = crash_reason else {
                            continue;
                        };

                        tracing::error!(
                            service = %service_name,
                            instance = inst_id,
                            reason = %crash_reason,
                            "[RootSupervisor] 🚨 ALARM: ServiceSupervisor crashed!"
                        );

                        let maybe_backoff = if let Some(service_entry) = self.services.get_mut(&service_name) {
                            service_entry.restart_tracker.record_crash()
                        } else {
                            Some(Duration::ZERO)
                        };

                        match maybe_backoff {
                            Some(backoff) => {
                                let ticket = self.allocate_restart_ticket();
                                if let Some(entry) = self.services.get_mut(&service_name) {
                                    entry.is_restarting = true;
                                    entry.restart_ticket = ticket;
                                }

                                let s_name = service_name.clone();
                                if backoff.is_zero() {
                                    self.execute_service_restart(&s_name, ticket);
                                } else {
                                    tracing::info!(
                                        service = %service_name,
                                        backoff = ?backoff,
                                        "[RootSupervisor] ⏳ Backoff delay: Scheduling non-blocking restart of service..."
                                    );
                                    let tx = scheduled_restart_tx.clone();
                                    tokio::spawn(async move {
                                        tokio::time::sleep(backoff).await;
                                        let _ = tx.send((s_name, ticket, None)).await;
                                    });
                                }
                            }
                            None => {
                                tracing::error!(
                                    service = %service_name,
                                    "[RootSupervisor] 🛑 CRASH-LOOP DETECTED: ServiceSupervisor exceeded max restarts! Permanently failed."
                                );
                                let ticket = self.allocate_restart_ticket();
                                if let Some(entry) = self.services.get_mut(&service_name) {
                                    entry.is_failed = true;
                                    entry.is_restarting = false;
                                    entry.restart_ticket = ticket;
                                    let err = RootError::MaxRestartsExceeded(
                                        service_name.clone(),
                                        entry.restart_tracker.policy.max_restarts,
                                        entry.restart_tracker.policy.time_window,
                                    );
                                    for waiter in entry.pending_resolutions.drain(..) {
                                        let _ = waiter.send(Err(err.clone()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::info!(
            "[RootSupervisor] Root supervisor shutdown. Aborting supervised service tasks."
        );
        self.join_set.abort_all();
    }
}

// ============================================================================
// Root Supervisor Public Handle
// ============================================================================

/// Public thread-safe client handle for registering services with the [`RootSupervisor`].
#[derive(Clone)]
pub struct RootSupervisorHandle {
    sender: Sender<RootCommand>,
}

impl RootSupervisorHandle {
    /// Accepts a self-registration from an autonomously running service supervisor with specific RestartPolicy and ShutdownPolicy.
    pub(crate) async fn register_service_self_with_policies(
        &self,
        name: impl Into<String>,
        sender: Sender<ServiceSupervisorCommand>,
        task_handle: JoinHandle<TaskResult>,
        factory: ServiceFactory,
        restart_policy: RestartPolicy,
        shutdown_policy: ShutdownPolicy,
    ) -> Result<(), RootError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RootCommand::RegisterServiceSelf {
                name: name.into(),
                sender,
                task_handle,
                factory,
                restart_policy,
                shutdown_policy,
                respond_to: tx,
            })
            .await
            .map_err(|_| RootError::RootUnavailable)?;

        rx.await.map_err(|_| RootError::RootUnavailable)?
    }

    /// Queries the root supervisor to resolve the active command channel for a service.
    pub async fn resolve_service_channel(
        &self,
        service_name: &str,
    ) -> Result<Sender<ServiceSupervisorCommand>, RootError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RootCommand::ResolveServiceChannel {
                service_name: service_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| RootError::RootUnavailable)?;

        rx.await.map_err(|_| RootError::RootUnavailable)?
    }

    /// Resolves a running service and wraps it in a resilient [`ServiceSupervisorHandle`].
    pub async fn resolve_service_handle(
        &self,
        service_name: &str,
    ) -> Result<ServiceSupervisorHandle, RootError> {
        let sender = self.resolve_service_channel(service_name).await?;
        Ok(ServiceSupervisorHandle::new(
            service_name,
            sender,
            Some(self.clone()),
        ))
    }

    /// Triggers an intentional crash in a specific registered service supervisor.
    pub async fn trigger_service_crash(&self, service_name: &str) -> Result<(), RootError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RootCommand::TriggerServiceCrash {
                service_name: service_name.to_string(),
                respond_to: tx,
            })
            .await
            .map_err(|_| RootError::RootUnavailable)?;

        rx.await.map_err(|_| RootError::RootUnavailable)?
    }

    /// Manually restarts or recovers a service supervisor, resetting its failed state and restart rate-limits.
    pub async fn restart_service(&self, service_name: impl Into<String>) -> Result<(), RootError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RootCommand::RestartService {
                service_name: service_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| RootError::RootUnavailable)?;

        rx.await.map_err(|_| RootError::RootUnavailable)?
    }

    /// Gracefully stops a specific service supervisor and all of its supervised child actors.
    pub async fn stop_service(&self, service_name: impl Into<String>) -> Result<(), RootError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(RootCommand::StopService {
                service_name: service_name.into(),
                respond_to: tx,
            })
            .await
            .map_err(|_| RootError::RootUnavailable)?;

        rx.await.map_err(|_| RootError::RootUnavailable)?
    }

    /// Gracefully shuts down the entire supervision tree (RootSupervisor -> ServiceSupervisors -> Child Actors).
    pub async fn shutdown(&self) -> Result<(), RootError> {
        if self.sender.is_closed() {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(RootCommand::Shutdown { respond_to: tx })
            .await
            .is_err()
        {
            return Ok(());
        }
        let _ = rx.await;
        Ok(())
    }
}
