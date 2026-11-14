import socket
import json

def test_ping():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(3.0)
    try:
        s.connect(('127.0.0.1', 9001))
        # Send PING
        s.sendall(b'{"cmd":"PING"}\n')
        reply = s.recv(4096).decode('utf-8', errors='ignore').strip()
        print("PING reply:", reply)

        # Send GET_STATE
        s.sendall(b'{"cmd":"GET_STATE"}\n')
        reply = s.recv(8192).decode('utf-8', errors='ignore').strip()
        state = json.loads(reply)
        print("GET_STATE reply:")
        print(f"  Login: {state.get('login')}, Server: {state.get('server')}, Balance: {state.get('balance')}, Equity: {state.get('equity')}")
        print(f"  Positions count: {len(state.get('positions', []))}")
        print(f"  Orders count: {len(state.get('orders', []))}")

        # Send GET_SYMBOLS
        s.sendall(b'{"cmd":"GET_SYMBOLS"}\n')
        reply = s.recv(16384).decode('utf-8', errors='ignore').strip()
        syms = json.loads(reply)
        symbols_list = syms.get('symbols', [])
        print(f"GET_SYMBOLS reply ({len(symbols_list)} symbols):")
        print("  Sample symbols:", symbols_list[:15])

        # Test GET_TICK for a couple symbols like EURUSD, GBPUSD, XAUUSD
        for test_sym in ['EURUSD', 'GBPUSD', 'XAUUSD', 'EURUSDm', 'GOLD']:
            if test_sym in symbols_list:
                s.sendall(f'{{"cmd":"GET_TICK","symbol":"{test_sym}"}}\n'.encode('utf-8'))
                t_reply = s.recv(4096).decode('utf-8', errors='ignore').strip()
                print(f"GET_TICK for {test_sym}: {t_reply}")
                break
    except Exception as e:
        print("Error connecting to MT4 Bridge:", e)
    finally:
        s.close()

if __name__ == "__main__":
    test_ping()
