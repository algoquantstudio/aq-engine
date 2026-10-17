// Run the actual EA HTTP admission function against a tiny C++ test harness.
// Does not connect to MT5, a broker, or a bridge.
import assert from 'node:assert/strict';
import { readFileSync, mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const ea = readFileSync(new URL('./AqeMt5BridgeEA.mq5', import.meta.url), 'utf8');
const begin = ea.indexOf('bool HttpTransportAllowed(');
const end = ea.indexOf('\nbool ParseBridgeSocketEndpoint(', begin);
assert(begin >= 0 && end > begin);
const shim = `
#include <vector>
#include <cassert>
using uint = unsigned int;
enum { AQE_TRANSPORT_AUTO, AQE_TRANSPORT_WEBSOCKET, AQE_TRANSPORT_HTTP };
int InpTransportMode = AQE_TRANSPORT_AUTO;
struct Bridge { uint ws_first_attempt_ms; bool active; };
std::vector<Bridge> g_bridges;
int ArraySize(const std::vector<Bridge>& v) { return v.size(); }
bool IsValidBridgeIndex(int i) { return i >= 0 && i < ArraySize(g_bridges); }
bool IsWebSocketActive(int i) { return IsValidBridgeIndex(i) && g_bridges[i].active; }
int HttpFallbackAfterMs() { return 5000; }
bool HasElapsedMs(uint last, int duration) { return 10000 - last >= uint(duration); }
${ea.slice(begin, end)}
int main() {
  g_bridges = {{1, false}, {1, true}, {1, false}};
  assert(!HttpTransportAllowed(0)); // dead primary cannot block a healthy chart bridge
  assert(!HttpTransportAllowed(1));
  assert(!HttpTransportAllowed(2));
  g_bridges[1].active = false;
  assert(!HttpTransportAllowed(0)); // still no blocking HTTP during socket recovery
  g_bridges = {{1, false}};
  assert(HttpTransportAllowed(0)); // legacy single-bridge fallback preserved
  g_bridges[0].ws_first_attempt_ms = 9000;
  assert(!HttpTransportAllowed(0)); // negotiation grace
  InpTransportMode = AQE_TRANSPORT_HTTP;
  assert(HttpTransportAllowed(0));
  InpTransportMode = AQE_TRANSPORT_WEBSOCKET;
  assert(!HttpTransportAllowed(0));
}
`;
const dir = mkdtempSync(join(tmpdir(), 'aqe-stream-transport-'));
try {
  const source = join(dir, 'test.cpp'), binary = join(dir, 'test');
  writeFileSync(source, shim);
  const compile = spawnSync('clang++', ['-std=c++17', source, '-o', binary], { encoding: 'utf8' });
  assert.equal(compile.status, 0, compile.stderr);
  const result = spawnSync(binary, [], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  console.log('MT5 stream transport isolation: 8 assertions passed');
} finally { rmSync(dir, { recursive: true, force: true }); }
