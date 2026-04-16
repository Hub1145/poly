document.addEventListener('DOMContentLoaded', () => {
    const socket = io();
    let botRunning = false;
    let settingsTabActive = false;

    // UI Elements
    const statusDot      = document.getElementById('status-dot');
    const statusText     = document.getElementById('status-text');
    const mainControlBtn = document.getElementById('main-control-btn');
    const logDisplay     = document.getElementById('log-display');

    // Metrics
    const mTrades  = document.getElementById('m-trades');
    const mWinrate = document.getElementById('m-winrate');
    const mProfit  = document.getElementById('m-profit');
    const mBalance = document.getElementById('m-balance');
    const mMarkets = document.getElementById('m-markets');

    // Tables
    const positionsTable = document.getElementById('positions-table');
    const resolvedTable  = document.getElementById('resolved-table');
    const scanTable      = document.getElementById('scan-table');
    const newsTable      = document.getElementById('news-table');
    const devTable       = document.getElementById('dev-table');
    const downloadLogsBtn = document.getElementById('download-logs-btn');

    // ── Tab Logic with active-tab memory ─────────────────────────────────
    const tabs  = document.querySelectorAll('.tab-btn');
    const panes = document.querySelectorAll('.tab-pane');

    function switchTab(tabName) {
        tabs.forEach(t  => t.classList.remove('active'));
        panes.forEach(p => p.style.display = 'none');
        const btn  = document.querySelector(`.tab-btn[data-tab="${tabName}"]`);
        const pane = document.getElementById(tabName);
        if (btn)  btn.classList.add('active');
        if (pane) pane.style.display = 'block';
        sessionStorage.setItem('activeTab', tabName);
    }

    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            switchTab(tab.dataset.tab);
            settingsTabActive = (tab.dataset.tab === 'settings');
        });
    });

    // Restore the last active tab on page load (survives refresh)
    const savedTab = sessionStorage.getItem('activeTab') || 'dashboard';
    settingsTabActive = (savedTab === 'settings');
    switchTab(savedTab);

    // ── Bot Control ───────────────────────────────────────────────────────
    mainControlBtn.addEventListener('click', () => {
        const action = botRunning ? 'stop' : 'start';
        fetch('/api/control', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ action })
        })
        .then(res => res.json())
        .then(data => updateBotUI(data.is_trading));
    });

    function updateBotUI(isTrading) {
        botRunning = isTrading;
        if (isTrading) {
            statusDot.classList.add('active');
            statusText.innerText = 'Trading Active';
            mainControlBtn.innerText = 'Stop Trading';
            mainControlBtn.classList.remove('btn-success');
            mainControlBtn.classList.add('btn-danger');
        } else {
            statusDot.classList.remove('active');
            statusText.innerText = 'Scanning Only';
            mainControlBtn.innerText = 'Start Trading';
            mainControlBtn.classList.remove('btn-danger');
            mainControlBtn.classList.add('btn-success');
        }
    }

    // ── WebSocket state updates ───────────────────────────────────────────
    let lastStatusJson = '';

    socket.on('bot_status', (data) => {
        // Guard: skip identical payloads
        const statusJson = JSON.stringify(data);
        if (statusJson === lastStatusJson) return;
        lastStatusJson = statusJson;

        try { updateBotUI(data.is_trading || false); } catch (_) {}

        // ── Metrics ──────────────────────────────────────────────────────
        try {
            const m = data.metrics || {};
            if (mTrades)  mTrades.innerText  = m.total_trades  ?? 0;
            if (mWinrate) mWinrate.innerText = (m.win_rate     ?? 0) + '%';
            if (mProfit)  mProfit.innerText  = '$' + ((m.total_profit ?? 0).toFixed(2));
            if (mBalance) mBalance.innerText = '$' + ((m.balance      ?? 0).toLocaleString());
            if (mMarkets) mMarkets.innerText = data.total_scanned ?? 0;
        } catch (_) {}

        // ── Settings form — only sync from socket when user is NOT on settings tab
        // to prevent socket from overwriting in-progress edits.
        if (!settingsTabActive) {
            try {
                const cfg = data.config || {};
                const inputs = {
                    's-mode':       (cfg.paper_mode ?? true).toString(),
                    's-strategy':   cfg.strategy     ?? '',
                    's-amount':     cfg.trade_amount ?? '',
                    's-edge':       cfg.min_edge      ?? '',
                    's-interval':   cfg.scan_interval ?? '',
                    's-balance':    cfg.paper_balance ?? '',
                    's-max-trades': cfg.max_trades    ?? '',
                };
                for (const [id, val] of Object.entries(inputs)) {
                    const el = document.getElementById(id);
                    if (el) el.value = val;
                }
            } catch (_) {}
        }

        // ── Logs ─────────────────────────────────────────────────────────
        try {
            const logs = data.logs || [];
            logDisplay.innerHTML = logs.map(log => `<div class="log-entry">${log}</div>`).join('');
            logDisplay.scrollTop = logDisplay.scrollHeight;
        } catch (_) {}

        // ── Open Positions ───────────────────────────────────────────────
        try {
            positionsTable.innerHTML = (data.open_positions || []).map(p => `
                <tr>
                    <td title="${p.market}">${(p.market || '').substring(0, 50)}...</td>
                    <td><span class="side-badge ${(p.side || '').toLowerCase()}">${p.side || ''}</span></td>
                    <td>$${p.size || 0}</td>
                    <td>${(p.price || 0).toFixed(3)}</td>
                    <td><span class="success">${p.signal_type || 'Bayesian'}</span></td>
                </tr>
            `).join('');
        } catch (_) {}

        // ── Resolved Positions ───────────────────────────────────────────
        try {
            if (resolvedTable) {
                resolvedTable.innerHTML = (data.resolved_positions || []).map(p => {
                    const cls = (p.profit || 0) >= 0 ? 'success' : 'danger';
                    return `
                        <tr>
                            <td title="${p.market}">${(p.market || '').substring(0, 40)}...</td>
                            <td><span class="side-badge ${(p.side || '').toLowerCase()}">${p.side || ''}</span></td>
                            <td>$${p.size || 0}</td>
                            <td class="${cls}">$${(p.profit || 0).toFixed(2)}</td>
                            <td>${p.resolved_at || ''}</td>
                        </tr>
                    `;
                }).reverse().join('');
            }
        } catch (_) {}

        // ── Alpha Scan ───────────────────────────────────────────────────
        try {
            const scanCountBadge = document.getElementById('scan-count-badge');
            const realCount = data.total_scanned || 0;
            const markets   = data.scanned_markets || [];
            const warming   = realCount === 0 && markets.length > 0;

            if (scanCountBadge) {
                if (warming) {
                    scanCountBadge.className = 'side-badge no';
                    scanCountBadge.innerText = 'Scanning — computing signals...';
                } else {
                    scanCountBadge.className = 'side-badge yes';
                    scanCountBadge.innerText = `${realCount} Alpha Signal${realCount !== 1 ? 's' : ''} Found`;
                }
            }

            // Show loading overlay while markets are being fetched
            const scanCard = scanTable ? scanTable.closest('.card') : null;
            let scanOverlay = document.getElementById('scan-loading-overlay');
            const showOverlay = data.is_initializing || data.is_scanning;
            const overlayMsg  = data.is_initializing
                ? 'Initializing — loading market data...'
                : 'Refreshing markets for strategy...';
            if (showOverlay) {
                if (!scanOverlay && scanCard) {
                    scanCard.style.position = 'relative';
                    scanOverlay = document.createElement('div');
                    scanOverlay.id = 'scan-loading-overlay';
                    scanOverlay.style.cssText = 'position:absolute;inset:0;background:rgba(0,0,0,0.55);display:flex;flex-direction:column;align-items:center;justify-content:center;border-radius:inherit;z-index:10;gap:12px;';
                    if (!document.getElementById('scan-spin-style')) {
                        const s = document.createElement('style');
                        s.id = 'scan-spin-style';
                        s.textContent = '@keyframes spin{to{transform:rotate(360deg)}}';
                        document.head.appendChild(s);
                    }
                    scanCard.appendChild(scanOverlay);
                }
                if (scanOverlay) {
                    scanOverlay.innerHTML = `<div class="spinner" style="width:36px;height:36px;border:4px solid var(--primary,#6366f1);border-top-color:transparent;border-radius:50%;animation:spin 0.8s linear infinite;"></div><span style="color:#fff;font-size:0.9rem;font-weight:600;">${overlayMsg}</span>`;
                }
            } else if (scanOverlay) {
                scanOverlay.remove();
            }

            if (markets.length > 0) {
                scanTable.innerHTML = markets.slice(0, 100).map(m => {
                    const isWarm     = m.warming_up === true;
                    const score      = m.alpha_score || 0;
                    const scoreColor = isWarm ? 'text-dim' : (score > 70 ? 'success' : (score > 40 ? 'primary' : 'text-dim'));
                    const bias       = (m.bias || 'N/A');
                    const liq        = (m.liquidity || 'High');
                    const question   = (m.question  || '').substring(0, 80);
                    const rowStyle   = isWarm ? 'opacity:0.45;' : '';
                    const scoreText  = isWarm ? '—' : score.toFixed(1);
                    const biasCell   = isWarm
                        ? `<span style="color:var(--text-dim);font-size:0.8rem;">pending</span>`
                        : `<span class="side-badge ${bias.toLowerCase()}">${bias}</span>`;
                    const liqCell    = isWarm
                        ? `<span style="color:var(--text-dim);font-size:0.8rem;">pending</span>`
                        : `<span class="side-badge ${liq.toLowerCase()}">${liq}</span>`;
                    return `
                        <tr style="${rowStyle}">
                            <td title="${m.question || ''}">${question}${(m.question||'').length > 80 ? '...' : ''}</td>
                            <td class="${scoreColor}" style="font-weight:bold;">${scoreText}</td>
                            <td>${biasCell}</td>
                            <td>${liqCell}</td>
                            <td style="font-size:0.8rem;color:var(--text-dim);font-style:${isWarm?'italic':'normal'};">${m.reasoning || ''}</td>
                        </tr>
                    `;
                }).join('');
            } else {
                const msg = data.is_initializing
                    ? 'Initializing — loading market data for the first time...'
                    : data.is_syncing
                        ? 'Computing signals...'
                        : 'No signals above threshold yet — engine is scanning.';
                scanTable.innerHTML = `<tr><td colspan="5" style="text-align:center;padding:40px;color:var(--text-dim);">${msg}</td></tr>`;
            }
        } catch (_) {}

        // ── Alpha Feed ───────────────────────────────────────────────────
        try {
            newsTable.innerHTML = (data.news_events || []).map(e => `
                <tr>
                    <td style="font-family:'JetBrains Mono';font-size:0.8rem;">${e.trader || ''}</td>
                    <td><span class="side-badge yes" style="font-size:0.7rem;">${e.label || ''}</span></td>
                    <td>${e.activity || ''}</td>
                    <td class="success">${e.impact || ''}</td>
                    <td style="font-size:0.75rem">${e.summary || ''}</td>
                </tr>
            `).join('');
        } catch (_) {}

        // ── Signal Attribution ───────────────────────────────────────────
        try {
            if (devTable) {
                devTable.innerHTML = (data.dev_check_logs || []).map(log => {
                    const traders = (log.top_traders || []).map(t =>
                        `<div style="margin-bottom:5px;border-left:2px solid var(--primary);padding-left:5px;">${(t.address||'').substring(0,8)}... (${t.label||''})</div>`
                    ).join('');
                    return `
                        <tr>
                            <td style="font-size:0.7rem;white-space:nowrap;">${log.timestamp || ''}</td>
                            <td style="font-size:0.8rem;"><span class="side-badge ${(log.directional_bias||'').toLowerCase()}">${log.directional_bias||''}</span> ${(log.question||'').substring(0,40)}...</td>
                            <td style="font-size:0.8rem;color:var(--primary-light);">${log.explanation || 'Aggregated Signal'}</td>
                            <td style="font-size:0.7rem;">${traders || 'General Momentum'}</td>
                            <td style="font-size:0.7rem;color:var(--text-dim);">
                                Strength: ${(log.signal_strength||0).toFixed(4)} |
                                Count: ${(log.top_traders||[]).length}
                            </td>
                        </tr>
                    `;
                }).reverse().join('');
            }
        } catch (_) {}
    });

    // ── Download Signal Logs ──────────────────────────────────────────────
    if (downloadLogsBtn) {
        downloadLogsBtn.addEventListener('click', () => {
            const logs = lastStatusJson ? (JSON.parse(lastStatusJson).dev_check_logs || []) : [];
            if (logs.length === 0) { alert('No signals available to download.'); return; }
            const logText = logs.map(l =>
                `[${l.timestamp}] SIGNAL: ${l.directional_bias} | MARKET: ${l.question}\n` +
                `STRENGTH: ${(l.signal_strength||0).toFixed(4)}\n` +
                `EXPLANATION: ${l.explanation || ''}\n` +
                `TRADERS: ${(l.top_traders||[]).map(t => `${t.address} (${t.label})`).join(', ')}\n` +
                `${'─'.repeat(80)}\n`
            ).join('\n');
            const blob = new Blob([logText], { type: 'text/plain' });
            const url  = window.URL.createObjectURL(blob);
            const a    = document.createElement('a');
            a.href = url;
            a.download = `polymarket_signals_${new Date().toISOString().slice(0,10)}.log`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        });
    }

    // ── Settings Form ─────────────────────────────────────────────────────
    const settingsForm   = document.getElementById('settings-form');
    const settingsSaveBtn = settingsForm ? settingsForm.querySelector('button[type="submit"]') : null;

    // Inline save feedback element (no blocking alert)
    const saveFeedback = document.createElement('span');
    saveFeedback.style.cssText = 'margin-left:12px;font-size:0.85rem;color:var(--success,#4ade80);display:none;';
    if (settingsSaveBtn) settingsSaveBtn.parentNode.insertBefore(saveFeedback, settingsSaveBtn.nextSibling);

    function showSaveFeedback(msg, isError) {
        saveFeedback.innerText = msg;
        saveFeedback.style.color = isError ? 'var(--danger,#f87171)' : 'var(--success,#4ade80)';
        saveFeedback.style.display = 'inline';
        setTimeout(() => { saveFeedback.style.display = 'none'; }, 3000);
    }

    if (settingsForm) {
        settingsForm.addEventListener('submit', (e) => {
            e.preventDefault();
            const pk = (document.getElementById('s-pk') || {}).value || '';
            const config = {
                paper_mode:    document.getElementById('s-mode').value === 'true',
                strategy:      document.getElementById('s-strategy').value,
                trade_amount:  parseFloat(document.getElementById('s-amount').value),
                min_edge:      parseFloat(document.getElementById('s-edge').value),
                scan_interval: parseInt(document.getElementById('s-interval').value),
                paper_balance: parseFloat(document.getElementById('s-balance').value),
                max_trades:    parseInt(document.getElementById('s-max-trades').value),
            };
            // Only include private_key if the user actually typed something
            if (pk.trim()) config.private_key = pk.trim();

            fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            })
            .then(res => res.json())
            .then(() => {
                showSaveFeedback('✓ Saved', false);
                // Clear private key field after successful save (security)
                const pkField = document.getElementById('s-pk');
                if (pkField) pkField.value = '';
            })
            .catch(() => showSaveFeedback('Save failed — check connection', true));
        });
    }

    // ── Load settings on page load (before WebSocket arrives) ────────────
    fetch('/api/config')
        .then(res => res.json())
        .then(cfg => {
            // Populate editable settings from saved config
            const inputs = {
                's-mode':       (cfg.paper_mode ?? true).toString(),
                's-strategy':   cfg.strategy     ?? '',
                's-amount':     cfg.trade_amount  ?? '',
                's-edge':       cfg.min_edge       ?? '',
                's-interval':   cfg.scan_interval  ?? '',
                's-balance':    cfg.paper_balance  ?? '',
                's-max-trades': cfg.max_trades     ?? '',
            };
            for (const [id, val] of Object.entries(inputs)) {
                const el = document.getElementById(id);
                if (el) el.value = val;
            }
            // Select correct paper mode option
            const modeEl = document.getElementById('s-mode');
            if (modeEl) modeEl.value = (cfg.paper_mode ?? true).toString();
        })
        .catch(() => {});

    // Request initial state push from server
    socket.emit('request_update');
});
