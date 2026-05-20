// Click-wrap consent gate. Shows on first visit and whenever the server-declared
// terms_version / privacy_version differs from what's stored locally.
//
// Why click-wrap (vs. browse-wrap): U.S. courts (Specht v. Netscape and its
// progeny) consistently enforce ToS only when the user took an affirmative
// action — checkbox + button — to agree. A footer link alone is rarely enough.

(function () {
  const KEY = 'cropsentry.tosAccepted';
  const gate = document.getElementById('consentGate');
  if (!gate) return;

  const wantedTerms = gate.dataset.termsVersion || '';
  const wantedPrivacy = gate.dataset.privacyVersion || '';

  let stored = null;
  try { stored = JSON.parse(localStorage.getItem(KEY) || 'null'); } catch { stored = null; }

  const accepted = stored
    && stored.terms_version === wantedTerms
    && stored.privacy_version === wantedPrivacy;

  if (accepted) return;  // already on the current versions

  // Open the modal.
  gate.hidden = false;
  document.body.classList.add('consent-locked');

  // Ensure Terms/Privacy links open reliably even if default behavior is blocked.
  gate.querySelectorAll('a[href="/terms"], a[href="/privacy"]').forEach(function (link) {
    link.addEventListener('click', function (e) {
      e.preventDefault();
      e.stopPropagation();
      window.open(link.href, '_blank', 'noopener');
    });
  });

  const check = document.getElementById('consentCheck');
  const accept = document.getElementById('consentAccept');
  const errBox = document.getElementById('consentError');

  check.addEventListener('change', () => {
    accept.disabled = !check.checked;
    if (errBox) errBox.hidden = true;
  });

  accept.addEventListener('click', async () => {
    if (!check.checked) return;
    accept.disabled = true;
    const acceptedAt = new Date().toISOString();
    const record = {
      terms_version: wantedTerms,
      privacy_version: wantedPrivacy,
      accepted_at: acceptedAt,
    };
    try {
      const res = await fetch('/api/consent', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(record),
      });
      // Server-side log is best-effort. Persist locally regardless so the
      // user isn't blocked by a transient logging failure — the lack of a
      // server record will simply mean we can't produce evidence later for
      // that one user; the local record still proves a UI-level click.
      if (!res.ok && errBox) {
        errBox.textContent = 'We could not record your acceptance on the server, but you may continue.';
        errBox.hidden = false;
      }
    } catch {
      if (errBox) {
        errBox.textContent = 'Network error recording acceptance — you may continue.';
        errBox.hidden = false;
      }
    }
    localStorage.setItem(KEY, JSON.stringify({
      terms_version: wantedTerms,
      privacy_version: wantedPrivacy,
      accepted_at: acceptedAt,
    }));
    gate.hidden = true;
    document.body.classList.remove('consent-locked');
  });
})();
