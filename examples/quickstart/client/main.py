import asyncio
from multiprocessing import Queue
import threading

from spacetimedb_sdk.spacetimedb_async_client import SpacetimeDBAsyncClient
import spacetimedb_sdk.local_config as local_config

import module_bindings
from module_bindings.user import User
from module_bindings.message import Message
from module_bindings.system_notification import SystemNotification
import module_bindings.send_message_reducer as send_message_reducer
import module_bindings.set_name_reducer as set_name_reducer

input_queue = Queue()
local_identity = None


def run_client(spacetime_client):
    asyncio.run(
        spacetime_client.run(
            local_config.get_token(),
            "localhost:3000",
            "chat",
            False,
            on_connect,
            ["SELECT * FROM User", "SELECT * FROM Message"],
        )
    )


def input_loop():
    global input_queue

    while True:
        user_input = input()
        if len(user_input) == 0:
            return
        elif user_input.startswith("/name "):
            input_queue.put(("name", user_input[6:]))
        else:
            input_queue.put(("message", user_input))


def on_connect(auth_token, identity):
    global local_identity
    local_identity = identity

    local_config.set_token(auth_token)


def check_commands():
    global input_queue

    if not input_queue.empty():
        choice = input_queue.get()
        if choice[0] == "name":
            set_name_reducer.set_name(
                choice[1],
                then=lambda evt: print(f"Failed to set name: {evt.message}")
                if evt.status == "failed"
                else None,
            )
        else:
            send_message_reducer.send_message(
                choice[1],
                then=lambda evt: print(f"Failed to send message: {evt.message}")
                if evt.status == "failed"
                else None,
            )

    spacetime_client.schedule_event(0.1, check_commands)


def print_messages_in_order():
    all_messages = sorted(Message.iter(), key=lambda x: x.sent)
    for entry in all_messages:
        print(
            f"{user_name_or_identity(User.filter_by_identity(entry.sender))}: {entry.text}"
        )


def on_subscription_applied():
    print(f"\nSYSTEM: Connected.")
    print_messages_in_order()


def on_message_row_update(row_op, message_old, message, reducer_event):
    if reducer_event is not None and row_op == "insert":
        print_message(message)


def print_message(message):
    user = User.filter_by_identity(message.sender)
    user_name = "unknown"
    if user is not None:
        user_name = user_name_or_identity(user)

    print(f"{user_name}: {message.text}")


def user_name_or_identity(user):
    if user.name:
        return user.name
    else:
        return (str(user.identity))[:8]


def on_user_row_update(row_op, user_old, user, reducer_event):
    if row_op == "insert":
        if user.online:
            print(f"User {user_name_or_identity(user)} connected.")
    elif row_op == "update":
        if user_old.online and not user.online:
            print(f"User {user_name_or_identity(user)} disconnected.")
        elif not user_old.online and user.online:
            print(f"User {user_name_or_identity(user)} connected.")

        if user_old.name != user.name:
            print(
                f"User {user_name_or_identity(user_old)} renamed to {user_name_or_identity(user)}."
            )


def on_system_notification(row_op, _old, notification, reducer_event):
    if row_op == "insert":
        print(f"[{notification.level.upper()}] {notification.message}")


def register_callbacks(spacetime_client):
    spacetime_client.register_on_subscription_applied(on_subscription_applied)

    User.register_row_update(on_user_row_update)
    Message.register_row_update(on_message_row_update)
    SystemNotification.register_row_update(on_system_notification)

    spacetime_client.schedule_event(0.1, check_commands)


if __name__ == "__main__":
    local_config.init(".spacetimedb-python-quickstart")

    spacetime_client = SpacetimeDBAsyncClient(module_bindings)

    register_callbacks(spacetime_client)

    thread = threading.Thread(target=run_client, args=(spacetime_client,))
    thread.start()

    input_loop()

    spacetime_client.force_close()
    thread.join()
