# P2P Chat System

Run all commands from the `code` folder.

## Install

```powershell
cd code
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```
## IMPORTANT NOTE
Nếu clone code từ git về thì luôn phải truy cập vào thư mục code trước khi chạy lệnh
```powershell
cd code
```

Còn không có thể mở trực tiếp thư mục code để không phải cd code

Đợi môi trường .venv active trước khi chạy câu lệnh dưới.

## Start Bootstrap Server

```powershell
python -m server.bootstrap_server --host 127.0.0.1 --port 9000
```

## Start Admin UI

```powershell
python -m server.admin_ui --bootstrap-host 127.0.0.1 --bootstrap-port 9000 --admin-host 127.0.0.1 --admin-port 9100
```

## Start Peer Web UI 1

```powershell
python -m peer.web_app --web-host 127.0.0.1 --web-port 8001
```

## Start Peer Web UI 2

```powershell
python -m peer.web_app --web-host 127.0.0.1 --web-port 8002
```

## Start Peer Web UI 3

```powershell
python -m peer.web_app --web-host 127.0.0.1 --web-port 8003
```

## Open

Admin:

```text
http://127.0.0.1:9100
```

Peers:

```text
http://127.0.0.1:8001
http://127.0.0.1:8002
http://127.0.0.1:8003
```

## Register

peer1:

```text
username 1
peer_host 127.0.0.1
peer_port 5001
bootstrap_host 127.0.0.1
bootstrap_port 9000
```

peer2:

```text
username 2
peer_host 127.0.0.1
peer_port 5002
bootstrap_host 127.0.0.1
bootstrap_port 9000
```

peer3:

```text
username 3
peer_host 127.0.0.1
peer_port 5003
bootstrap_host 127.0.0.1
bootstrap_port 9000
```

## Architecture

Bootstrap Server is only for discovery, status, groups, and offline queue APIs. It starts with a fresh in-memory session and does not depend on the Admin UI.

Admin UI is only for monitoring Bootstrap Server state. It fetches Bootstrap Server admin APIs and shows offline status if Bootstrap is unreachable.

Peer Web UI runs the actual peer node. Each Web UI starts unregistered, starts a local TCP peer server after registration, and keeps in-memory caches of known peers and groups.

Online chat is sent directly peer-to-peer by TCP socket:

```text
Alice -> Bob
```

Normal online chat and file transfer are not relayed through Bootstrap or Admin UI. After peers discover each other, stopping Bootstrap should not stop direct chat, group chat, broadcast, or file transfer between cached reachable peers. While Bootstrap is offline, discovery, status refresh, group sync, admin monitoring, and offline queue are unavailable.

## File Transfer

Peer Web UI supports direct, group, and broadcast file transfer. Files stream directly over TCP between peers using file metadata, raw 64 KB chunks, and SHA-256 verification. Bootstrap only helps peers discover addresses and group membership; it never relays or stores file contents.

Received files are saved under:

```text
storage/received_files/<username>/
```

Files can be opened or downloaded from the receiver Web UI in the Message Log and Received Files panel. If a received filename already exists, the receiver saves a numbered copy such as `report_1.pdf`. Empty files are rejected, and the default maximum file size is 50 MB. Offline peers cannot receive files and files are not placed in the Bootstrap offline message queue.

Quick manual tests:

```text
Direct file:
1. Start Bootstrap and two Peer Web UIs.
2. Register Alice and Bob.
3. In Alice Direct Chat, select Bob, choose a file, and click Send File.
4. Bob should see "Received file from alice: <filename>" and the file under storage/received_files/bob/.

Group file:
1. Create or join a group with Alice, Bob, and Charlie.
2. In Alice Send Group Message, select the group, choose a file, and click Send File to Group.
3. Bob and Charlie receive direct TCP transfers. Alice does not receive her own file.

Broadcast file:
1. In Alice Broadcast, choose a file and click Send File to All.
2. All cached online peers except Alice receive the file.

Bootstrap offline:
1. Let Alice and Bob discover each other.
2. Stop Bootstrap.
3. Send a direct file from Alice to Bob.
4. Transfer still works if Bob's cached peer TCP address is reachable.
```

Port roles:

```text
web_port 8001/8002/8003 = browser UI only
peer_port 5001/5002/5003 = TCP P2P communication only
bootstrap_port 9000 = discovery/status/group/offline queue API
admin_port 9100 = management UI only
```
