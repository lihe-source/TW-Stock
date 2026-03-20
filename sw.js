const CACHE_NAME = 'taiwan-stock-radar-v3.72';
const STATIC_ASSETS = [
  './',
  './index.html',
  './style.css',
  './app.js',
  './manifest.json',
  './icon-192.svg',
  './icon-512.svg'
  // ⚠ screener.json 故意不放這裡，每次都從網路取最新版
];

// Install: cache static assets (不含 screener.json)
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => cache.addAll(STATIC_ASSETS))
  );
  self.skipWaiting();
});

// Activate: clean old caches
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k)))
    )
  );
  self.clients.claim();
});

// Fetch strategy:
//   data/ 路徑（screener.json）→ network-first，不快取
//   外部 API               → network-only
//   其他靜態資源            → cache-first
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // 外部 API → network only
  if (url.hostname.includes('finmindtrade.com') || url.hostname.includes('twse.com.tw')) {
    event.respondWith(
      fetch(event.request).catch(() =>
        new Response(JSON.stringify({ status: 0, msg: '離線模式' }), {
          headers: { 'Content-Type': 'application/json' }
        })
      )
    );
    return;
  }

  // screener.json → 永遠走網路，不使用任何快取
  if (url.pathname.includes('/data/screener.json') || url.pathname.endsWith('screener.json')) {
    event.respondWith(
      fetch(event.request, { cache: 'no-store' })
        .catch(() => caches.match('./data/screener.json'))  // 離線時才用快取
    );
    return;
  }

  // 靜態資源 → cache-first
  event.respondWith(
    caches.match(event.request).then(cached => cached || fetch(event.request))
  );
});
