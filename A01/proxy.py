"""
HTTP Proxy Server - Assignment 1
CS3001 Computer Networks, Spring 2026
FAST-NUCES Karachi

Usage:
    python proxy.py <port>
    Example: python proxy.py 8888

Then configure your browser to use localhost:<port> as HTTP proxy.
"""

import sys
import os
import signal
import socket
from urllib.parse import urlparse

# ─── Configuration ───────────────────────────────────────────────
MAX_CONNECTIONS = 100        # max concurrent child processes
BUFFER_SIZE     = 4096       # bytes to read at a time
TIMEOUT         = 10         # socket timeout in seconds

# Track active child processes
active_children = 0


# ══════════════════════════════════════════════════════════════════
#  Helper: build an HTTP error response
# ══════════════════════════════════════════════════════════════════
def http_error(status_code, reason):
    """Return a complete HTTP/1.0 error response as bytes."""
    body = (
        f"<html><body>"
        f"<h1>{status_code} {reason}</h1>"
        f"</body></html>"
    )
    response = (
        f"HTTP/1.0 {status_code} {reason}\r\n"
        f"Content-Type: text/html\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
        f"{body}"
    )
    return response.encode()


# ══════════════════════════════════════════════════════════════════
#  Helper: parse the raw HTTP request from the client
# ══════════════════════════════════════════════════════════════════
def parse_request(raw_request):
    """
    Parse the raw HTTP request bytes.
    Returns a dict with: method, url, version, host, port, path, headers
    Raises ValueError on bad request.
    """
    try:
        # Decode and split into lines
        request_text = raw_request.decode("utf-8", errors="replace")
        lines = request_text.split("\r\n")

        # --- Parse the request line ---
        request_line = lines[0]
        parts = request_line.split()
        if len(parts) != 3:
            raise ValueError("Malformed request line")

        method, url, version = parts

        # Validate version
        if version not in ("HTTP/1.0", "HTTP/1.1"):
            raise ValueError("Unsupported HTTP version")

        # --- Parse headers ---
        headers = {}
        i = 1
        while i < len(lines) and lines[i] != "":
            header_line = lines[i]
            colon_pos = header_line.find(":")
            if colon_pos == -1:
                raise ValueError(f"Malformed header: {header_line}")
            key = header_line[:colon_pos].strip()
            value = header_line[colon_pos + 1:].strip()
            headers[key] = value
            i += 1

        # --- Parse the absolute URI ---
        parsed = urlparse(url)
        if not parsed.hostname:
            raise ValueError("URL must be in absolute form (e.g. http://host/path)")

        host = parsed.hostname
        port = parsed.port if parsed.port else 80
        path = parsed.path if parsed.path else "/"
        if parsed.query:
            path += "?" + parsed.query

        return {
            "method":  method,
            "url":     url,
            "version": version,
            "host":    host,
            "port":    port,
            "path":    path,
            "headers": headers,
        }

    except ValueError:
        raise  # re-raise our own ValueErrors
    except Exception as e:
        raise ValueError(f"Failed to parse request: {e}")


# ══════════════════════════════════════════════════════════════════
#  Core: handle one client connection
# ══════════════════════════════════════════════════════════════════
def handle_client(client_socket, client_address):
    """Handle one client: read request, forward to server, relay response."""
    try:
        client_socket.settimeout(TIMEOUT)

        # ---- 1. Receive the full request from the client ----
        raw_request = b""
        while True:
            chunk = client_socket.recv(BUFFER_SIZE)
            raw_request += chunk
            # End of headers is marked by \r\n\r\n
            if b"\r\n\r\n" in raw_request or not chunk:
                break

        if not raw_request:
            client_socket.close()
            return

        # ---- 2. Parse the request ----
        try:
            req = parse_request(raw_request)
        except ValueError as e:
            print(f"  [!] Bad Request from {client_address}: {e}")
            client_socket.sendall(http_error(400, "Bad Request"))
            client_socket.close()
            return

        print(f"  [>] {req['method']} {req['url']} from {client_address}")

        # ---- 3. Only GET is supported ----
        if req["method"] != "GET":
            print(f"  [!] Method '{req['method']}' not implemented")
            client_socket.sendall(http_error(501, "Not Implemented"))
            client_socket.close()
            return

        # ---- 4. Build the request to send to the remote server ----
        # Convert the absolute URI to a relative path for the origin server
        forward_request = f"GET {req['path']} HTTP/1.0\r\n"
        forward_request += f"Host: {req['host']}\r\n"

        # Forward other headers (skip proxy-specific ones)
        for key, value in req["headers"].items():
            lower = key.lower()
            if lower in ("proxy-connection", "connection"):
                continue
            if lower == "host":
                continue  # already added
            forward_request += f"{key}: {value}\r\n"

        forward_request += "Connection: close\r\n"
        forward_request += "\r\n"

        # ---- 5. Connect to the remote server ----
        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.settimeout(TIMEOUT)
            server_socket.connect((req["host"], req["port"]))
            server_socket.sendall(forward_request.encode())
        except Exception as e:
            print(f"  [!] Could not connect to {req['host']}:{req['port']} — {e}")
            client_socket.sendall(http_error(502, "Bad Gateway"))
            client_socket.close()
            return

        # ---- 6. Receive the response from server and relay to client ----
        try:
            while True:
                data = server_socket.recv(BUFFER_SIZE)
                if not data:
                    break
                client_socket.sendall(data)
        except socket.timeout:
            pass  # remote server stopped sending
        except Exception as e:
            print(f"  [!] Error relaying data: {e}")

        # ---- 7. Clean up ----
        server_socket.close()
        client_socket.close()
        print(f"  [<] Done: {req['url']}")

    except Exception as e:
        print(f"  [!] Unexpected error for {client_address}: {e}")
        try:
            client_socket.close()
        except:
            pass


# ══════════════════════════════════════════════════════════════════
#  Main: start the proxy server
# ══════════════════════════════════════════════════════════════════
def reap_children():
    """Reap any finished child processes to avoid zombies."""
    global active_children
    while active_children > 0:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break  # no more finished children
            active_children -= 1
        except ChildProcessError:
            break  # no child processes


def sigchld_handler(signum, frame):
    """Handle SIGCHLD to reap zombie child processes."""
    global active_children
    while True:
        try:
            pid, _ = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
            active_children -= 1
        except ChildProcessError:
            break


def main():
    global active_children

    # --- Get port from command line ---
    if len(sys.argv) != 2:
        print("Usage: python proxy.py <port>")
        print("Example: python proxy.py 8888")
        sys.exit(1)

    try:
        port = int(sys.argv[1])
    except ValueError:
        print("Error: port must be a number")
        sys.exit(1)

    # --- Set up SIGCHLD handler to reap zombie processes ---
    signal.signal(signal.SIGCHLD, sigchld_handler)

    # --- Create the listening socket ---
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy_socket.bind(("0.0.0.0", port))
    proxy_socket.listen(MAX_CONNECTIONS)

    print("=" * 55)
    print(f"  HTTP Proxy Server running on port {port}")
    print(f"  Max concurrent child processes: {MAX_CONNECTIONS}")
    print("=" * 55)
    print("  Configure your browser proxy to: localhost:" + str(port))
    print("  Press Ctrl+C to stop.\n")

    # --- Main accept loop ---
    try:
        while True:
            try:
                client_socket, client_address = proxy_socket.accept()
            except OSError:
                # accept() can be interrupted by SIGCHLD, just retry
                continue

            # Reap any finished child processes
            reap_children()

            if active_children >= MAX_CONNECTIONS:
                print(f"  [!] Max connections reached, rejecting {client_address}")
                client_socket.sendall(http_error(503, "Service Unavailable"))
                client_socket.close()
                continue

            # Fork a new process for each client request
            pid = os.fork()

            if pid == 0:
                # ── Child process ──
                proxy_socket.close()  # child doesn't need the listening socket
                handle_client(client_socket, client_address)
                os._exit(0)  # exit child process
            else:
                # ── Parent process ──
                active_children += 1
                client_socket.close()  # parent doesn't need the client socket

    except KeyboardInterrupt:
        print("\n  [*] Shutting down proxy server...")
        proxy_socket.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
