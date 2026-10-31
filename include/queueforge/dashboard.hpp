#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <functional>
#include <iostream>
#include <string>
#include <thread>

namespace qf {

// The whole dashboard page. It asks /metrics once a second and draws the numbers.
inline const char* kDashboardHtml = R"HTML(<!doctype html>
<html><head><meta charset="utf-8"><title>QueueForge</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body { font-family: system-ui, sans-serif; background:#0f1115; color:#e6e6e6; margin:0; padding:24px; }
  h1 { margin:0 0 4px; font-size:22px; } .sub { color:#8a8f98; margin-bottom:20px; font-size:14px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(150px,1fr)); gap:12px; }
  .card { background:#181b22; border:1px solid #262a33; border-radius:10px; padding:14px; }
  .card .n { font-size:28px; font-weight:600; } .card .l { color:#8a8f98; font-size:13px; margin-top:2px; }
  .dead .n { color:#ff6b6b; } .done .n { color:#51cf66; } .run .n { color:#fcc419; }
  canvas { width:100%; height:140px; background:#181b22; border:1px solid #262a33; border-radius:10px; margin-top:16px; }
  #status { float:right; font-size:13px; } .ok { color:#51cf66; } .bad { color:#ff6b6b; }
</style></head><body>
<h1>QueueForge <span id="status" class="bad">connecting...</span></h1>
<div class="sub">Live broker metrics. Refreshes every second.</div>
<div class="grid">
  <div class="card"><div class="n" id="queued">-</div><div class="l">queued</div></div>
  <div class="card"><div class="n" id="delayed">-</div><div class="l">delayed</div></div>
  <div class="card run"><div class="n" id="running">-</div><div class="l">running</div></div>
  <div class="card done"><div class="n" id="done">-</div><div class="l">done</div></div>
  <div class="card dead"><div class="n" id="dead">-</div><div class="l">dead</div></div>
  <div class="card"><div class="n" id="timeouts">-</div><div class="l">visibility timeouts</div></div>
  <div class="card"><div class="n" id="rate">-</div><div class="l">jobs finished / sec</div></div>
  <div class="card"><div class="n" id="uptime">-</div><div class="l">uptime</div></div>
</div>
<canvas id="chart" width="900" height="140"></canvas>
<script>
const hist = []; let last = null;
function fmt(s) { const m = Math.floor(s/60); return m + "m " + (s%60) + "s"; }
function draw() {
  const c = document.getElementById("chart"), g = c.getContext("2d");
  g.clearRect(0,0,c.width,c.height);
  if (hist.length < 2) return;
  const max = Math.max(1, ...hist.map(h => h.queued));
  g.strokeStyle = "#4dabf7"; g.lineWidth = 2; g.beginPath();
  hist.forEach((h,i) => {
    const x = i/(hist.length-1)*c.width, y = c.height-8-(h.queued/max)*(c.height-24);
    i ? g.lineTo(x,y) : g.moveTo(x,y);
  });
  g.stroke(); g.fillStyle = "#8a8f98"; g.font = "12px sans-serif";
  g.fillText("queue depth, last 60 s (max " + max + ")", 8, 14);
}
async function tick() {
  try {
    const m = await (await fetch("/metrics", {cache:"no-store"})).json();
    for (const k of ["queued","delayed","running","done","dead","timeouts"])
      document.getElementById(k).textContent = m[k];
    document.getElementById("uptime").textContent = fmt(m.uptime_s);
    if (last) document.getElementById("rate").textContent = Math.max(0, m.done - last.done);
    last = m; hist.push(m); if (hist.length > 60) hist.shift(); draw();
    const s = document.getElementById("status"); s.textContent = "online"; s.className = "ok";
  } catch (e) {
    const s = document.getElementById("status"); s.textContent = "broker offline"; s.className = "bad";
  }
}
tick(); setInterval(tick, 1000);
</script></body></html>)HTML";

// Tiny HTTP server: GET / is the page, GET /metrics is JSON from the broker.
inline void run_dashboard(int port, std::function<std::string()> get_stats_json) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);  // this computer only
    addr.sin_port = htons(port);
    if (bind(server_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "dashboard: cannot bind port " << port << "\n";
        return;
    }
    listen(server_fd, 16);
    std::cout << "Dashboard on http://localhost:" << port << "\n";

    while (true) {
        int fd = accept(server_fd, nullptr, nullptr);
        if (fd < 0) continue;
        std::thread([fd, get_stats_json] {
            char buf[4096];
            ssize_t n = read(fd, buf, sizeof(buf) - 1);
            std::string req(buf, n > 0 ? static_cast<size_t>(n) : 0);

            std::string path = "/";
            size_t a = req.find(' ');
            size_t b = (a == std::string::npos) ? a : req.find(' ', a + 1);
            if (a != std::string::npos && b != std::string::npos) path = req.substr(a + 1, b - a - 1);

            std::string status = "200 OK", type = "text/html; charset=utf-8", body;
            if (path == "/metrics") {
                type = "application/json";
                body = get_stats_json();
            } else if (path == "/") {
                body = kDashboardHtml;
            } else {
                status = "404 Not Found";
                type = "text/plain";
                body = "not found";
            }
            std::string resp = "HTTP/1.1 " + status + "\r\nContent-Type: " + type +
                               "\r\nContent-Length: " + std::to_string(body.size()) +
                               "\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n" + body;
            send(fd, resp.data(), resp.size(), MSG_NOSIGNAL);
            close(fd);
        }).detach();
    }
}

} 