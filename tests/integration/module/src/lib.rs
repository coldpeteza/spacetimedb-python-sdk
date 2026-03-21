use spacetimedb::ReducerContext;

// ── Tables ─────────────────────────────────────────────────────────────────

#[spacetimedb::table(public)]
pub struct User {
    #[primary_key]
    identity: spacetimedb::Identity,
    name: Option<String>,
    online: bool,
}

#[spacetimedb::table(public)]
pub struct Message {
    sender: spacetimedb::Identity,
    sent: spacetimedb::Timestamp,
    text: String,
}

// ── Lifecycle reducers ──────────────────────────────────────────────────────

#[spacetimedb::reducer(client_connected)]
pub fn identity_connected(ctx: &ReducerContext) {
    if let Some(user) = ctx.db.user().identity().find(ctx.sender()) {
        ctx.db.user().identity().update(User { online: true, ..user });
    } else {
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
        None => Err("User not found".into()),
    }
}

#[spacetimedb::reducer]
pub fn send_message(ctx: &ReducerContext, text: String) -> Result<(), String> {
    if text.is_empty() {
        return Err("Messages must not be empty".into());
    }
    ctx.db.message().insert(Message {
        sender: ctx.sender(),
        sent: ctx.timestamp,
        text,
    });
    Ok(())
}
