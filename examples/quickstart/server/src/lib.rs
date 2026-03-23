use spacetimedb::{ReducerContext, Table};

// ── Tables ─────────────────────────────────────────────────────────────────

#[spacetimedb::table(accessor = user, public)]
pub struct User {
    #[primary_key]
    identity: spacetimedb::Identity,
    name: Option<String>,
    online: bool,
}

#[spacetimedb::table(accessor = message, public)]
pub struct Message {
    sender: spacetimedb::Identity,
    sent: spacetimedb::Timestamp,
    text: String,
}

// ── Lifecycle reducers ──────────────────────────────────────────────────────

#[spacetimedb::reducer(init)]
pub fn init(_ctx: &ReducerContext) {
    // Called when the module is initially published.
}

#[spacetimedb::reducer(client_connected)]
pub fn identity_connected(ctx: &ReducerContext) {
    if let Some(user) = ctx.db.user().identity().find(ctx.sender()) {
        // Returning user — mark online, preserve name and identity.
        ctx.db.user().identity().update(User { online: true, ..user });
    } else {
        // New user — create a row for this identity.
        ctx.db.user().insert(User {
            identity: ctx.sender(),
            name: None,
            online: true,
        });
    }
}

#[spacetimedb::reducer(client_disconnected)]
pub fn identity_disconnected(ctx: &ReducerContext) {
    if let Some(user) = ctx.db.user().identity().find(ctx.sender()) {
        ctx.db.user().identity().update(User { online: false, ..user });
    } else {
        // Should be unreachable: a client cannot disconnect without connecting first.
        log::warn!(
            "Disconnect event for unknown user with identity {:?}",
            ctx.sender()
        );
    }
}

// ── Business reducers ───────────────────────────────────────────────────────

#[spacetimedb::reducer]
pub fn set_name(ctx: &ReducerContext, name: String) -> Result<(), String> {
    if name.is_empty() {
        return Err("Names must not be empty".into());
    }
    match ctx.db.user().identity().find(ctx.sender()) {
        Some(user) => {
            ctx.db
                .user()
                .identity()
                .update(User { name: Some(name), ..user });
            Ok(())
        }
        None => Err("Cannot set name for unknown user".into()),
    }
}

#[spacetimedb::reducer]
pub fn send_message(ctx: &ReducerContext, text: String) -> Result<(), String> {
    // Consider: rate-limit per user, reject messages from unnamed users.
    if text.is_empty() {
        return Err("Messages must not be empty".into());
    }
    ctx.db.message().insert(Message {
        sender: ctx.sender(),
        text,
        sent: ctx.timestamp,
    });
    Ok(())
}
