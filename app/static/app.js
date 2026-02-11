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
