//! # Einfaches Lehrbeispiel: Resilienter Resource-Actor, StatefulActor & Supervision
//!
//! Dieses Beispiel demonstriert die wesentlichen Kernkonzepte des Frameworks:
//!
//! 1. **Supervision-Hierarchie (Control Plane)**:
//!    `RootSupervisor` -> `ServiceSupervisor` zur Lebenszyklusverwaltung.
//! 2. **Worker-Definition (Zero-Cost Abstraktion)**:
//!    Eine leichtgewichtige async Closure zur Beschaffung einer ablaufenden Ressource (`TimedResource`).
//! 3. **Actor-Erstellung & Registrierung**:
//!    Deklarative Konfiguration via Builder (mit Pre-Warming und Pufferzeiten).
//! 4. **Direkter Datenzugriff (Data Plane)**:
//!    Hochperformante 1-Hop-Kommunikation ohne Supervisor-Overhead.
//! 5. **Transparente Selbstheilung (Self-Healing)**:
//!    Automatischer Neustart nach einem Absturz und nahtloser Rebind des Clients.
//! 6. **Zustandsbehafteter Aktor (`StatefulActor`)**:
//!    Klassisches `gen_server`-Modell (`call` & `cast`, `on_start` / `on_stop`) im selben Supervisor.

use std::time::Duration;
use tokio::time::Instant;

use aixtor::actor::{
    ActorContext, GenericResourceActor, RestartType, RootSupervisor, ServiceSupervisor,
    StatefulActor, StatefulActorBuilder, TimedResource, WorkerContext,
};

// ============================================================================
// Beispiel für einen zustandsbehafteten Nicht-Ressourcen-Aktor (StatefulActor)
// ============================================================================

/// Zustand unseres Audit-Zähler-Aktors
#[derive(Clone, Default)]
struct AuditCounterActor {
    requests_logged: u64,
}

/// Nachrichtenprotokoll für `AuditCounterActor`
#[derive(Clone, Debug)]
enum AuditMsg {
    /// Erhöht den Zähler um `n` und gibt `(aktueller_stand, instance_id)` zurück
    RecordEvents(u64),
    /// Gibt den aktuellen Stand und die `instance_id` zurück
    GetStats,
    /// Simuliert einen kritischen Zustandsfehler über `ctx.fail(...)`
    TriggerFatalError,
}

impl StatefulActor for AuditCounterActor {
    type Message = AuditMsg;
    type Response = (u64, usize);
    type Error = String;

    async fn on_start(&mut self, ctx: &mut ActorContext) -> Result<(), String> {
        println!(
            "  🟢 [StatefulActor '{}'] on_start Hook ausgeführt (Instanz #{})",
            ctx.name(),
            ctx.instance_id()
        );
        Ok(())
    }

    async fn handle(
        &mut self,
        msg: Self::Message,
        ctx: &mut ActorContext,
    ) -> Result<Self::Response, Self::Error> {
        match msg {
            AuditMsg::RecordEvents(n) => {
                self.requests_logged += n;
                Ok((self.requests_logged, ctx.instance_id()))
            }
            AuditMsg::GetStats => Ok((self.requests_logged, ctx.instance_id())),
            AuditMsg::TriggerFatalError => {
                // `ctx.fail(...)` eskaliert den Fehler an den ServiceSupervisor (triggert Neustart),
                // während `Err(...)` die Fehlermeldung direkt an den wartenden Aufrufer (`call`) liefert.
                ctx.fail("Simulierter kritischer Zustandsfehler im AuditCounterActor");
                Err("Zustand korrumpiert".to_string())
            }
        }
    }

    async fn on_stop(&mut self, ctx: &mut ActorContext) {
        println!(
            "  🔵 [StatefulActor '{}'] on_stop Hook beim Graceful Shutdown ausgeführt (Instanz #{}, gezählte Events: {})",
            ctx.name(),
            ctx.instance_id(),
            self.requests_logged
        );
    }
}

/// Maskiert sensible Token-Ausgaben auf der Konsole.
fn mask_secret(s: &str) -> String {
    let chars: Vec<char> = s.chars().collect();
    if chars.len() <= 12 {
        "***".to_string()
    } else {
        let prefix: String = chars[..6.min(chars.len())].iter().collect();
        let suffix: String = chars[chars.len().saturating_sub(4)..].iter().collect();
        format!("{prefix}…{suffix} (len: {})", chars.len())
    }
}

#[tokio::main]
async fn main() {
    // Optional: Tracing-Logs aktivieren (hier auf WARN gestellt, um die Konsole übersichtlich zu halten)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::WARN.into()),
        )
        .with_target(false)
        .init();

    println!("========================================================");
    println!("  Einfaches Lehrbeispiel: Resource-Actor & StatefulActor");
    println!("========================================================\n");

    // ------------------------------------------------------------------
    // Schritt 1: Control Plane aufsetzen (Supervision-Baum)
    // ------------------------------------------------------------------
    println!("--- Schritt 1: Supervision-Hierarchie initialisieren ---");

    // Der RootSupervisor überwacht ServiceSupervisoren auf oberster Ebene.
    let root = RootSupervisor::new();

    // Der ServiceSupervisor verwaltet die konkreten Worker-Aktoren einer Domäne.
    // Er registriert sich autonom beim RootSupervisor.
    let auth_service = ServiceSupervisor::spawn_and_register("auth-service", &root)
        .await
        .expect("Fehler bei der Registrierung des ServiceSupervisors");

    println!("  ✅ RootSupervisor und ServiceSupervisor ('auth-service') aktiv.\n");

    // ------------------------------------------------------------------
    // Schritt 2: Worker definieren
    // ------------------------------------------------------------------
    println!("--- Schritt 2: Worker definieren (Ressourcen-Beschaffung) ---");

    // Ein Worker ist eine beliebige async-Funktion oder Closure mit der Signatur:
    //   Fn(WorkerContext) -> Future<Output = Result<TimedResource<T>, E>>
    // Dank impl Future entfällt jegliches `Box::pin` oder dynamische Heap-Dispatching.
    let token_worker = |ctx: WorkerContext| async move {
        println!(
            "  ⚙️  [Worker-Aufruf] Hole neuen Token vom Backend (Generation: {}, Instanz: {})...",
            ctx.generation, ctx.instance_id
        );

        // Simuliere kurze Latenz eines externen API-Aufrufs (z.B. OAuth2-Server)
        tokio::time::sleep(Duration::from_millis(50)).await;

        let token_value = format!("TOKEN_SECRET_GEN_{:02}", ctx.generation);

        // Die Ressource wird mit einer Time-to-Live (TTL) versehen:
        Ok::<_, String>(TimedResource::with_ttl(
            token_value,
            Duration::from_secs(5), // 5 Sekunden gültig
        ))
    };

    println!("  ✅ Worker-Closure für Token-Generierung vorbereitet.\n");

    // ------------------------------------------------------------------
    // Schritt 3: Actor über den Builder konfigurieren & starten
    // ------------------------------------------------------------------
    println!("--- Schritt 3: Actor starten & registrieren ---");

    let client = GenericResourceActor::builder("auth-token")
        .worker(token_worker)
        .restart_type(RestartType::Permanent) // Kritischer Daemon: Wird immer neu gestartet
        .shutdown_timeout(Duration::from_secs(2)) // Graceful Shutdown mit 2s Frist für laufende Worker
        .eager_prewarm(true) // Lädt die Ressource sofort beim Booten im Hintergrund
        .expiry_buffer(Duration::from_secs(1)) // Erneuert 1 Sekunde vor Ablauf proaktiv
        .spawn_and_register(&auth_service, &root)
        .await
        .expect("Fehler beim Starten des Actors");

    // Kurze Pause (100ms), damit der asynchrone Pre-Warm-Task im Hintergrund fertig wird
    tokio::time::sleep(Duration::from_millis(100)).await;
    println!("  ✅ Actor 'auth-token' gestartet. Client-Handle erhalten.\n");

    // ------------------------------------------------------------------
    // Schritt 4: Data-Plane Zugriff & Caching (Direct 1-Hop)
    // ------------------------------------------------------------------
    println!("--- Schritt 4: Data-Plane Zugriff & Caching testen ---");

    // 1. Abruf: Sofortiger Cache-Hit dank Eager Pre-Warming!
    let start = Instant::now();
    let token1 = client.get().await.expect("Abruf fehlgeschlagen");
    let elapsed1 = start.elapsed();
    println!(
        "  👉 1. Abruf: \"{}\" (Dauer: {:?}) -> Schneller Cache-Hit!",
        mask_secret(&token1),
        elapsed1
    );

    // 2. Abruf: Weiterer Cache-Hit (Worker wird NICHT erneut aufgerufen)
    let start = Instant::now();
    let token2 = client.get().await.expect("Abruf fehlgeschlagen");
    let elapsed2 = start.elapsed();
    println!(
        "  👉 2. Abruf: \"{}\" (Dauer: {:?}) -> Serviert direkt aus dem RAM!",
        mask_secret(&token2),
        elapsed2
    );

    println!("  ℹ️  Beachte: Anfragen gehen direkt an den Actor, die Supervisoren haben 0% Overhead.\n");

    // ------------------------------------------------------------------
    // Schritt 5: Resilienz & Transparente Selbstheilung (Self-Healing)
    // ------------------------------------------------------------------
    println!("--- Schritt 5: Resilienz-Test (Absturz & Selbstheilung) ---");
    println!("  💥 Wir provozieren einen kontrollierten Absturz des Actors...");

    // Simuliert einen Panic/Crash des Actor-Tasks
    let _ = client.simulate_crash().await;

    // Kurze Pause: Der ServiceSupervisor erkennt den Ausfall und startet eine neue Instanz (#2)
    tokio::time::sleep(Duration::from_millis(80)).await;

    println!("  🔄 Client führt get() aus (bemerkt geschlossenen Kanal & verbindet sich neu)...");
    let start = Instant::now();
    let token_recovered = client
        .get()
        .await
        .expect("Selbstheilung des Clients fehlgeschlagen");
    let elapsed_rec = start.elapsed();

    println!(
        "  ✅ Nach Crash & Rebind: \"{}\" (Dauer: {:?})",
        mask_secret(&token_recovered),
        elapsed_rec
    );
    println!("  ℹ️  Der Aufrufer musste keinen Reconnect-Code schreiben – alles geschah transparent!\n");

    // ------------------------------------------------------------------
    // Schritt 6: Manuelles Forcieren eines Refreshes
    // ------------------------------------------------------------------
    println!("--- Schritt 6: Manuelles Erneuern (force_refresh) ---");
    println!("  🔄 Forciere vorzeitiges Neuladen der Ressource...");

    client
        .force_refresh()
        .await
        .expect("Force refresh fehlgeschlagen");

    // Der Refresh läuft asynchron im Hintergrund (Non-Blocking), damit bestehende Aufrufer
    // nicht blockiert werden. Wir warten kurz (70ms), bis der Worker fertig ist:
    tokio::time::sleep(Duration::from_millis(70)).await;

    let fresh_token = client.get().await.expect("Abruf fehlgeschlagen");
    println!(
        "  ✅ Neuer Token nach Refresh: \"{}\"\n",
        mask_secret(&fresh_token)
    );

    // ------------------------------------------------------------------
    // Schritt 7: Zustandsbehafteter StatefulActor im selben Supervisor
    // ------------------------------------------------------------------
    println!("--- Schritt 7: Zustandsbehafteter StatefulActor (call, cast & Crash-Recovery) ---");

    let audit_client = StatefulActorBuilder::from_default::<AuditCounterActor>("audit-counter")
        .restart_type(RestartType::Permanent)
        .shutdown_timeout(Duration::from_secs(2))
        .spawn(&auth_service)
        .await
        .expect("Fehler beim Starten des StatefulActors");

    // 1. Fire-and-Forget Nachrichten senden (cast)
    audit_client
        .cast(AuditMsg::RecordEvents(3))
        .await
        .expect("cast fehlgeschlagen");
    audit_client
        .cast(AuditMsg::RecordEvents(2))
        .await
        .expect("cast fehlgeschlagen");

    // 2. Request-Response Abfrage (call)
    let _ = audit_client
        .call(AuditMsg::RecordEvents(5))
        .await
        .expect("call fehlgeschlagen");
    let (count, inst) = audit_client
        .call(AuditMsg::GetStats)
        .await
        .expect("call fehlgeschlagen");
    println!(
        "  👉 [StatefulActor] Zählerstand nach 2x cast (3+2) + 2x call (RecordEvents(5) + GetStats): {} Events (Instanz #{})",
        count, inst
    );

    // 3. Kritischer Fehler im Handler via ctx.fail(...) -> Supervisor startet Instanz #2
    println!("  💥 Provoziere kritischen Handler-Fehler im StatefulActor via ctx.fail(...)...");
    let _ = audit_client.call(AuditMsg::TriggerFatalError).await;

    // ActorClient bindet sich beim nächsten Aufruf automatisch an die neue Instanz #2!
    let (new_count, new_inst) = audit_client
        .call(AuditMsg::RecordEvents(1))
        .await
        .expect("Automatischer Rebind des ActorClient fehlgeschlagen");
    println!(
        "  ✅ [StatefulActor] Nach Selbstheilung: Zählerstand = {} Event (frische Instanz #{})\n",
        new_count, new_inst
    );

    // ------------------------------------------------------------------
    // Schritt 8: Kontrolliertes Herunterfahren (Graceful Shutdown)
    // ------------------------------------------------------------------
    println!("--- Schritt 8: Graceful Shutdown des gesamten Supervision-Baums ---");
    println!("  🛑 Fahre RootSupervisor, ServiceSupervisoren und alle Aktoren geordnet herunter...");

    root.shutdown()
        .await
        .expect("Fehler beim Graceful Shutdown");

    println!("  ✅ Alle laufenden Worker und StatefulActors (inkl. on_stop) sauber beendet.\n");

    println!("========================================================");
    println!("  Zusammenfassung:");
    println!("  - GenericResourceActor (TTL-Cache) + StatefulActor im selben Baum.");
    println!("  - Klare Trennung: Control Plane (Supervision) vs. Data Plane (Client).");
    println!("  - OTP RestartType (Permanent/Transient/Temporary) & Graceful Shutdown.");
    println!("  - Automatische Selbstheilung ohne manuelles Reconnect-Handling.");
    println!("========================================================");
}
