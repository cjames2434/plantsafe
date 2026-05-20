// Crop Sentry Service Worker — push notifications + offline-first caching

const CACHE_NAME = 'cropsentry-v4';
const APP_SHELL = [
  '/',
  '/static/css/style.css?v=6',
  '/static/js/app.js',
  '/static/js/results.js',
  '/static/manifest.json',
];

// Cache app shell on install
self.addEventListener('install', function(event) {
  event.waitUntil(
    caches.open(CACHE_NAME).then(function(cache) {
      return cache.addAll(APP_SHELL).catch(function() {});
    }).then(function() { self.skipWaiting(); })
  );
});

// Clean old caches on activate
self.addEventListener('activate', function(event) {
  event.waitUntil(
    caches.keys().then(function(names) {
      return Promise.all(
        names.filter(function(n) { return n !== CACHE_NAME; })
             .map(function(n) { return caches.delete(n); })
      );
    }).then(function() { return clients.claim(); })
  );
});

// Network-first for API calls, cache-first for static assets
self.addEventListener('fetch', function(event) {
  var url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // API calls: network-first with cache fallback
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(
      fetch(event.request).then(function(response) {
        if (response.ok) {
          var clone = response.clone();
          caches.open(CACHE_NAME).then(function(cache) {
            cache.put(event.request, clone);
          });
        }
        return response;
      }).catch(function() {
        return caches.match(event.request).then(function(cached) {
          if (cached) return cached;
          return new Response(JSON.stringify({ error: 'Offline', offline: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        });
      })
    );
    return;
  }

  // Static assets: cache-first
  if (url.pathname.startsWith('/static/')) {
    event.respondWith(
      caches.match(event.request).then(function(cached) {
        if (cached) return cached;
        return fetch(event.request).then(function(response) {
          if (response.ok) {
            var clone = response.clone();
            caches.open(CACHE_NAME).then(function(cache) {
              cache.put(event.request, clone);
            });
          }
          return response;
        });
      })
    );
    return;
  }

  // HTML pages: network-first
  event.respondWith(
    fetch(event.request).then(function(response) {
      if (response.ok) {
        var clone = response.clone();
        caches.open(CACHE_NAME).then(function(cache) {
          cache.put(event.request, clone);
        });
      }
      return response;
    }).catch(function() {
      return caches.match(event.request).then(function(cached) {
        if (cached) return cached;
        return caches.match('/');
      });
    })
  );
});

// Push notifications
self.addEventListener('push', function(event) {
  if (!event.data) return;

  var data;
  try {
    data = event.data.json();
  } catch (e) {
    data = { title: 'Crop Sentry Alert', body: event.data.text() };
  }

  var title = data.title || 'Crop Sentry Alert';
  var options = {
    body: data.message || data.body || '',
    icon: '/static/img/icon-192.png',
    badge: '/static/img/badge-72.png',
    tag: data.type || 'cropsentry-alert',
    data: {
      url: data.url || '/results?' + (data.lat && data.lon ? 'lat=' + data.lat + '&lon=' + data.lon : ''),
      type: data.type,
    },
    actions: [
      { action: 'view', title: 'View Assessment' },
      { action: 'dismiss', title: 'Dismiss' },
    ],
    vibrate: [200, 100, 200],
    requireInteraction: data.urgency === 'critical',
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', function(event) {
  event.notification.close();
  if (event.action === 'dismiss') return;

  var url = (event.notification.data && event.notification.data.url) || '/';
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then(function(clientList) {
      for (var i = 0; i < clientList.length; i++) {
        if (clientList[i].url.includes('/results') && 'focus' in clientList[i]) {
          return clientList[i].focus();
        }
      }
      return clients.openWindow(url);
    })
  );
});
