#pragma once

// Port B's client connects to.  The injector owns it from the moment it starts,
// long before Minecraft exists, so B can be sitting in "connecting" while A is
// still booting.
#define RELAY_LISTEN_PORT 25565

// Where the in-game proxy actually serves.  Loopback only -- the only client is
// the bridge in this process.
#define PROXY_UPSTREAM_PORT 25566

// Binds RELAY_LISTEN_PORT and starts bridging accepted connections to
// PROXY_UPSTREAM_PORT.  Returns nonzero on success.  Connections are accepted
// and held even while nothing is listening upstream yet, which is the whole
// point: they are only forwarded once the DLL has been injected and has brought
// its own listener up.
int relay_start(unsigned short listenPort, unsigned short upstreamPort);
