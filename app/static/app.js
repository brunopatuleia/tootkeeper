async function triggerSync() {
    const btn = document.querySelector('.btn-sync');
    const statusEl = document.getElementById('sync-status');

    btn.disabled = true;
    btn.textContent = 'Syncing...';
    statusEl.style.display = 'block';
    statusEl.className = 'sync-status running';
    statusEl.textContent = 'Sync started. This may take a moment...';

    try {
        const response = await fetch('/api/sync', { method: 'POST' });
        const data = await response.json();

        if (data.status === 'already_running') {
            statusEl.textContent = 'A sync is already running. Please wait.';
        } else {
            statusEl.textContent = 'Sync started! The page will refresh shortly.';
            setTimeout(() => location.reload(), 10000);
        }
        statusEl.className = 'sync-status done';
    } catch (err) {
        statusEl.textContent = 'Sync request failed: ' + err.message;
        statusEl.className = 'sync-status';
        statusEl.style.borderColor = 'var(--red)';
        statusEl.style.color = 'var(--red)';
    } finally {
        btn.disabled = false;
        btn.textContent = 'Sync Now';
    }
}

async function regenerateRoast() {
    const btn = document.getElementById('roast-btn');
    const text = document.getElementById('roast-text');

    btn.disabled = true;
    btn.textContent = 'Generating...';

    try {
        const response = await fetch('/api/roast', { method: 'POST' });
        const data = await response.json();

        if (data.roast) {
            text.textContent = data.roast;
        } else {
            text.textContent = 'AI could not generate a roast. Check your API settings.';
        }
    } catch (err) {
        text.textContent = 'Failed to generate roast: ' + err.message;
    } finally {
        btn.disabled = false;
        btn.textContent = 'Roast Me Again';
    }
}

async function checkVersion() {
    const container = document.getElementById('version-check');
    if (!container) return;

    try {
        const response = await fetch('/api/version');
        const data = await response.json();

        if (data.update_available) {
            container.innerHTML = `
                <div style="margin-top: 10px; padding: 10px; border: 1px solid var(--accent); border-radius: 6px; background: rgba(255,255,255,0.05);">
                    <strong style="color: var(--accent);">Update available: v${data.latest}</strong><br>
                    <small>You are on v${data.current}. <a href="https://github.com/brunopatuleia/tootkeeper" target="_blank" style="color: inherit; text-decoration: underline;">View on GitHub</a></small>
                </div>
            `;
        } else if (data.latest) {
            container.innerHTML = `<small style="color: var(--green); display: block; margin-top: 5px;">You are up to date (v${data.current})</small>`;
        }
    } catch (err) {
        console.error('Failed to check version:', err);
    }
}

document.addEventListener('DOMContentLoaded', checkVersion);
