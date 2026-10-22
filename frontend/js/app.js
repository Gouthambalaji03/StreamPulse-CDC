// StreamPulse CDC Frontend Application Logic
(function() {
  'use strict';

  // App State
  const state = {
    connected: false,
    ws: null,
    currentTab: 'dashboard',
    activeFilter: 'ALL',
    activeTableFilter: 'ALL',
    searchQuery: '',
    isStreaming: true,
    soundEnabled: true,
    events: [],
    metrics: {
      total_events: 12482,
      insert_events: 6321,
      update_events: 4186,
      delete_events: 1975,
      sparkline_inserts: [12, 18, 25, 30, 42, 38, 55, 60, 48, 52, 65, 70],
      sparkline_updates: [20, 24, 18, 30, 35, 40, 32, 28, 45, 50, 42, 48],
      sparkline_deletes: [5, 8, 12, 10, 15, 8, 14, 20, 18, 12, 16, 22]
    },
    connectors: [],
    consumerGroups: [],
    schemas: [],
    selectedEvent: null
  };

  // Audio Blip using Web Audio API
  let audioCtx = null;
  function playBlipSound(type) {
    if (!state.soundEnabled) return;
    try {
      if (!audioCtx) {
        audioCtx = new (window.AudioContext || window.webkitAudioContext)();
      }
      if (audioCtx.state === 'suspended') {
        audioCtx.resume();
      }
      const osc = audioCtx.createOscillator();
      const gain = audioCtx.createGain();
      osc.connect(gain);
      gain.connect(audioCtx.destination);

      let freq = 480;
      if (type === 'INSERT') freq = 620;
      else if (type === 'UPDATE') freq = 520;
      else if (type === 'DELETE') freq = 360;

      osc.frequency.setValueAtTime(freq, audioCtx.currentTime);
      gain.gain.setValueAtTime(0.04, audioCtx.currentTime);
      gain.gain.exponentialRampToValueAtTime(0.0001, audioCtx.currentTime + 0.08);

      osc.start();
      osc.stop(audioCtx.currentTime + 0.08);
    } catch (e) {
      // Audio might be blocked until user gesture
    }
  }

  // DOM Elements
  const el = {
    // Navigation & Mobile Drawer
    appSidebar: document.getElementById('appSidebar'),
    mobileMenuBtn: document.getElementById('mobileMenuBtn'),
    sidebarCloseBtn: document.getElementById('sidebarCloseBtn'),
    sidebarBackdrop: document.getElementById('sidebarBackdrop'),
    navItems: document.querySelectorAll('.nav-item'),
    tabViews: document.querySelectorAll('.tab-view'),
    
    // Top Bar
    headerTitle: document.getElementById('headerTitle'),
    connStatusPill: document.getElementById('connStatusPill'),
    timeRangeSelect: document.getElementById('timeRangeSelect'),
    btnSimulateModal: document.getElementById('btnSimulateModal'),
    btnToggleStream: document.getElementById('btnToggleStream'),
    
    // Dashboard Metrics
    metricTotal: document.getElementById('metricTotal'),
    metricInserts: document.getElementById('metricInserts'),
    metricUpdates: document.getElementById('metricUpdates'),
    metricDeletes: document.getElementById('metricDeletes'),
    
    sparkInserts: document.getElementById('sparkInserts'),
    sparkUpdates: document.getElementById('sparkUpdates'),
    sparkDeletes: document.getElementById('sparkDeletes'),
    
    // Recent Events Table
    recentEventsBody: document.getElementById('recentEventsBody'),
    dashboardFilterPills: document.querySelectorAll('.dashboard-filter-pill'),
    
    // Events Explorer
    eventsExplorerBody: document.getElementById('eventsExplorerBody'),
    eventsSearchInput: document.getElementById('eventsSearchInput'),
    eventsTableSelect: document.getElementById('eventsTableSelect'),
    eventsOpSelect: document.getElementById('eventsOpSelect'),
    
    // Consumers Tab
    consumersList: document.getElementById('consumersList'),
    
    // Schemas Tab
    schemasGrid: document.getElementById('schemasGrid'),
    
    // Connectors Tab
    connectorsFlow: document.getElementById('connectorsFlow'),
    connectorsList: document.getElementById('connectorsList'),
    
    // Settings Tab
    settingSpeedRange: document.getElementById('settingSpeedRange'),
    settingSpeedVal: document.getElementById('settingSpeedVal'),
    settingSoundToggle: document.getElementById('settingSoundToggle'),
    btnResetStats: document.getElementById('btnResetStats'),
    btnSpike50: document.getElementById('btnSpike50'),
    
    // Modal
    mutationModal: document.getElementById('mutationModal'),
    modalCloseBtn: document.getElementById('modalCloseBtn'),
    modalCancelBtn: document.getElementById('modalCancelBtn'),
    mutationForm: document.getElementById('mutationForm'),
    mutateTable: document.getElementById('mutateTable'),
    mutateOp: document.getElementById('mutateOp'),
    mutateRecordId: document.getElementById('mutateRecordId'),
    mutateData: document.getElementById('mutateData'),
    
    // Drawer
    eventDrawer: document.getElementById('eventDrawer'),
    drawerCloseBtn: document.getElementById('drawerCloseBtn'),
    drawerTime: document.getElementById('drawerTime'),
    drawerOpBadge: document.getElementById('drawerOpBadge'),
    drawerTable: document.getElementById('drawerTable'),
    drawerRecordId: document.getElementById('drawerRecordId'),
    drawerWalLsn: document.getElementById('drawerWalLsn'),
    drawerTxId: document.getElementById('drawerTxId'),
    drawerKafkaTopic: document.getElementById('drawerKafkaTopic'),
    drawerLatency: document.getElementById('drawerLatency'),
    drawerRawJson: document.getElementById('drawerRawJson'),
    drawerBeforeBox: document.getElementById('drawerBeforeBox'),
    drawerAfterBox: document.getElementById('drawerAfterBox'),
  };

  // Format numbers with commas
  function fmtNum(n) {
    return Number(n).toLocaleString();
  }

  // Format short data preview for the table row
  function formatDataSnippet(data) {
    if (!data) return '{}';
    const keys = Object.keys(data);
    if (keys.length === 0) return '{}';
    
    let parts = [];
    keys.slice(0, 3).forEach(k => {
      let val = data[k];
      if (typeof val === 'string' && val.length > 20) {
        val = val.substring(0, 20) + '...';
      }
      parts.push(`<span class="data-key">"${k}"</span>: <span class="data-val">${JSON.stringify(val)}</span>`);
    });
    
    let suffix = keys.length > 3 ? ', ...' : '';
    return `{ ${parts.join(', ')}${suffix} }`;
  }

  // Render Sparklines
  function renderSparkline(container, series, colorClass) {
    if (!container || !series || series.length === 0) return;
    const max = Math.max(...series, 1);
    const html = series.map(val => {
      const heightPercent = Math.max(15, Math.round((val / max) * 100));
      return `<div class="spark-bar ${colorClass}" style="height: ${heightPercent}%;"></div>`;
    }).join('');
    container.innerHTML = html;
  }

  // Render Dashboard Metrics
  function updateMetricsUI(metrics) {
    if (!metrics) return;
    state.metrics = metrics;
    
    el.metricTotal.textContent = fmtNum(metrics.total_events);
    el.metricInserts.textContent = fmtNum(metrics.insert_events);
    el.metricUpdates.textContent = fmtNum(metrics.update_events);
    el.metricDeletes.textContent = fmtNum(metrics.delete_events);
    
    renderSparkline(el.sparkInserts, metrics.sparkline_inserts || [20, 35, 50, 45, 60], 'green');
    renderSparkline(el.sparkUpdates, metrics.sparkline_updates || [15, 25, 40, 30, 48], 'blue');
    renderSparkline(el.sparkDeletes, metrics.sparkline_deletes || [5, 10, 15, 8, 20], 'red');
  }

  // Build Table Row
  function createTableRow(ev, isNew = false) {
    const tr = document.createElement('tr');
    tr.className = isNew ? 'new-event-row' : '';
    tr.dataset.eventId = ev.id;
    
    const opLower = (ev.operation || 'INSERT').toLowerCase();
    
    tr.innerHTML = `
      <td class="time-cell">${ev.time || '10:24:31'}</td>
      <td><span class="badge-op ${opLower}">${ev.operation}</span></td>
      <td class="table-cell-badge">${ev.table}</td>
      <td class="record-id-cell">${ev.record_id}</td>
      <td class="data-cell">${formatDataSnippet(ev.data)}</td>
    `;
    
    tr.addEventListener('click', () => openEventDrawer(ev));
    return tr;
  }

  // Render Table Rows
  function renderRecentEvents(events) {
    if (!el.recentEventsBody) return;
    el.recentEventsBody.innerHTML = '';
    
    let filtered = events;
    if (state.activeFilter !== 'ALL') {
      filtered = filtered.filter(e => e.operation.toUpperCase() === state.activeFilter);
    }
    
    // Show 7 most recent on the dashboard
    filtered.slice(0, 7).forEach(ev => {
      el.recentEventsBody.appendChild(createTableRow(ev, false));
    });
  }

  // Handle incoming real-time event
  function handleNewEvent(ev, metrics) {
    state.events.unshift(ev);
    if (state.events.length > 200) {
      state.events.pop();
    }
    
    if (metrics) {
      updateMetricsUI(metrics);
    }
    
    playBlipSound(ev.operation);
    
    // Check if event matches dashboard filter
    const matchesFilter = state.activeFilter === 'ALL' || ev.operation.toUpperCase() === state.activeFilter;
    if (matchesFilter && el.recentEventsBody) {
      const newTr = createTableRow(ev, true);
      el.recentEventsBody.insertBefore(newTr, el.recentEventsBody.firstChild);
      
      // Keep row count limited to 7 on the dashboard
      while (el.recentEventsBody.children.length > 7) {
        el.recentEventsBody.removeChild(el.recentEventsBody.lastChild);
      }
    }
    
    // Also update Events Explorer if open
    if (state.currentTab === 'events') {
      renderEventsExplorer();
    }
  }

  // Render Events Explorer view
  function renderEventsExplorer() {
    if (!el.eventsExplorerBody) return;
    el.eventsExplorerBody.innerHTML = '';
    
    let filtered = state.events;
    if (state.activeTableFilter !== 'ALL') {
      filtered = filtered.filter(e => e.table === state.activeTableFilter);
    }
    const opVal = el.eventsOpSelect ? el.eventsOpSelect.value : 'ALL';
    if (opVal !== 'ALL') {
      filtered = filtered.filter(e => e.operation.toUpperCase() === opVal);
    }
    if (state.searchQuery) {
      const q = state.searchQuery.toLowerCase();
      filtered = filtered.filter(e => 
        String(e.record_id).toLowerCase().includes(q) ||
        JSON.stringify(e.data).toLowerCase().includes(q) ||
        e.table.toLowerCase().includes(q)
      );
    }
    
    if (filtered.length === 0) {
      el.eventsExplorerBody.innerHTML = `<tr><td colspan="5" style="text-align:center; padding: 24px; color: var(--text-muted);">No CDC events match current criteria</td></tr>`;
      return;
    }
    
    filtered.slice(0, 50).forEach(ev => {
      el.eventsExplorerBody.appendChild(createTableRow(ev, false));
    });
  }

  // Open Drawer
  function openEventDrawer(ev) {
    state.selectedEvent = ev;
    el.drawerTime.textContent = ev.time || ev.timestamp;
    
    const opLower = (ev.operation || 'insert').toLowerCase();
    el.drawerOpBadge.className = `badge-op ${opLower}`;
    el.drawerOpBadge.textContent = ev.operation;
    
    el.drawerTable.textContent = `${ev.schema_name || 'public'}.${ev.table}`;
    el.drawerRecordId.textContent = ev.record_id;
    el.drawerWalLsn.textContent = ev.wal_lsn || '0/1A4D800';
    el.drawerTxId.textContent = ev.tx_id || '10428';
    el.drawerKafkaTopic.textContent = ev.kafka_topic || `cdc.postgres.public.${ev.table}`;
    el.drawerLatency.textContent = `${ev.latency_ms || '2.4'} ms`;
    
    el.drawerRawJson.textContent = JSON.stringify(ev, null, 2);
    
    if (el.drawerBeforeBox) {
      el.drawerBeforeBox.textContent = ev.before ? JSON.stringify(ev.before, null, 2) : '/* nil (INSERT op) */';
    }
    if (el.drawerAfterBox) {
      el.drawerAfterBox.textContent = ev.after ? JSON.stringify(ev.after, null, 2) : '/* nil (DELETE op) */';
    }
    
    el.eventDrawer.classList.add('open');
  }

  function closeEventDrawer() {
    el.eventDrawer.classList.remove('open');
  }

  // Render Consumers View
  function renderConsumers() {
    if (!el.consumersList) return;
    if (!state.consumerGroups || state.consumerGroups.length === 0) {
      el.consumersList.innerHTML = `<div style="color:var(--text-muted); padding:20px;">Loading consumer groups...</div>`;
      return;
    }
    
    el.consumersList.innerHTML = state.consumerGroups.map(cg => {
      const maxLag = 50;
      const pct = Math.min(100, Math.round((cg.lag / maxLag) * 100));
      const lagColor = cg.lag > 25 ? 'var(--red)' : (cg.lag > 10 ? 'var(--amber)' : 'var(--emerald)');
      return `
        <div class="consumer-item">
          <div>
            <div style="font-weight: 600; color: #fff; margin-bottom: 4px; display:flex; align-items:center; gap:8px;">
              <span>${cg.group_id}</span>
              <span class="live-pill" style="font-size:0.65rem;">${cg.state}</span>
            </div>
            <div style="font-size: 0.8rem; color: var(--text-muted);">
              Topic: <strong style="color:var(--text-secondary);">${cg.topic}</strong> &bull; Partitions: ${cg.partitions} &bull; Consumers: ${cg.members}
            </div>
          </div>
          <div class="lag-meter">
            <div style="display:flex; justify-content:space-between; font-size:0.75rem;">
              <span style="color:var(--text-muted);">Consumer Lag</span>
              <span style="color:${lagColor}; font-weight:700;">${cg.lag} msgs</span>
            </div>
            <div class="lag-bar-bg">
              <div class="lag-bar-fill" style="width: ${pct}%; background-color: ${lagColor};"></div>
            </div>
            <div style="font-size:0.7rem; color:var(--text-muted); font-family:var(--font-mono);">
              Offset: ${fmtNum(cg.current_offset)} / ${fmtNum(cg.log_end_offset)}
            </div>
          </div>
        </div>
      `;
    }).join('');
  }

  // Render Schemas View
  function renderSchemas() {
    if (!el.schemasGrid) return;
    if (!state.schemas || state.schemas.length === 0) {
      el.schemasGrid.innerHTML = `<div style="color:var(--text-muted); padding:20px;">Loading schemas...</div>`;
      return;
    }
    
    el.schemasGrid.innerHTML = state.schemas.map(s => {
      const colRows = s.columns.map(c => `
        <tr>
          <td>${c.is_pk ? '<span class="pk-icon">🔑</span>' : ''} ${c.name}</td>
          <td style="color:#38bdf8;">${c.type}</td>
          <td style="color:${c.nullable ? 'var(--text-muted)' : 'var(--emerald)'};">${c.nullable ? 'NULL' : 'NOT NULL'}</td>
        </tr>
      `).join('');
      
      return `
        <div class="schema-card">
          <div class="schema-card-header">
            <div>
              <div style="font-weight:700; color:#fff;">public.${s.table_name}</div>
              <div style="font-size:0.75rem; color:var(--text-muted);">${fmtNum(s.total_records)} records</div>
            </div>
            <span class="badge-op insert" style="font-size:0.7rem;">CDC REPLICA: ${s.wal_replica_identity}</span>
          </div>
          <div class="schema-card-body">
            <table class="schema-table">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Type</th>
                  <th>Nullability</th>
                </tr>
              </thead>
              <tbody>
                ${colRows}
              </tbody>
            </table>
          </div>
        </div>
      `;
    }).join('');
  }

  // Render Connectors View
  function renderConnectors() {
    if (!el.connectorsList) return;
    if (!state.connectors || state.connectors.length === 0) return;
    
    el.connectorsList.innerHTML = state.connectors.map(c => {
      const isRunning = c.status === 'RUNNING';
      const badgeClass = isRunning ? 'insert' : 'delete';
      return `
        <div class="consumer-item">
          <div style="display:flex; align-items:center; gap:16px;">
            <div class="brand-icon" style="background:#1e293b; color:var(--emerald);">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="10"/><path d="M12 6v6l4 2"/></svg>
            </div>
            <div>
              <div style="font-weight: 600; color: #fff; display:flex; align-items:center; gap:10px;">
                <span>${c.name}</span>
                <span class="badge-op ${badgeClass}">${c.status}</span>
              </div>
              <div style="font-size:0.78rem; color:var(--text-muted); margin-top:2px;">
                Technology: ${c.technology} &bull; Target: ${c.target}
              </div>
            </div>
          </div>
          <div style="display:flex; align-items:center; gap:20px;">
            <div style="text-align:right;">
              <div style="font-family:var(--font-mono); font-size:0.95rem; font-weight:700; color:#fff;">${fmtNum(c.events_processed)}</div>
              <div style="font-size:0.72rem; color:var(--text-muted);">processed (${c.throughput_eps} eps)</div>
            </div>
            <button class="btn-secondary" onclick="window.toggleConnector('${c.id}')" style="font-size:0.78rem; padding:6px 12px;">
              ${isRunning ? 'Pause' : 'Resume'}
            </button>
          </div>
        </div>
      `;
    }).join('');
  }

  // Toggle Connector Action
  window.toggleConnector = async function(id) {
    try {
      const res = await fetch(`/api/connectors/${id}/toggle`, { method: 'POST' });
      if (res.ok) {
        const updated = await res.json();
        const idx = state.connectors.findIndex(c => c.id === id);
        if (idx !== -1) {
          state.connectors[idx] = updated;
          renderConnectors();
        }
      }
    } catch (e) {
      console.error(e);
    }
  };

  // Switch Active Tab
  function switchTab(tabId) {
    state.currentTab = tabId;
    el.navItems.forEach(item => {
      item.classList.toggle('active', item.dataset.tab === tabId);
    });
    el.tabViews.forEach(view => {
      view.classList.toggle('active', view.id === `tab-${tabId}`);
    });
    
    // Set Header Title
    const titles = {
      dashboard: 'Change Events',
      events: 'CDC Event Log Stream',
      consumers: 'Kafka Consumer Groups',
      schema: 'Database Schema Registry',
      connectors: 'Pipeline Connectors & Topology',
      settings: 'Engine & Platform Settings'
    };
    if (el.headerTitle) {
      el.headerTitle.textContent = titles[tabId] || 'Change Events';
    }
    
    if (tabId === 'events') renderEventsExplorer();
    else if (tabId === 'consumers') renderConsumers();
    else if (tabId === 'schema') renderSchemas();
    else if (tabId === 'connectors') renderConnectors();
  }

  // Fetch Initial Data via REST (in addition to WS)
  async function fetchInitialData() {
    try {
      const [metricsRes, eventsRes, connectorsRes, consumersRes, schemasRes] = await Promise.all([
        fetch('/api/metrics').then(r => r.json()),
        fetch('/api/events?limit=25').then(r => r.json()),
        fetch('/api/connectors').then(r => r.json()),
        fetch('/api/consumers').then(r => r.json()),
        fetch('/api/schemas').then(r => r.json())
      ]);
      
      state.metrics = metricsRes;
      state.events = eventsRes;
      state.connectors = connectorsRes;
      state.consumerGroups = consumersRes;
      state.schemas = schemasRes;
      
      updateMetricsUI(state.metrics);
      renderRecentEvents(state.events);
      renderConsumers();
      renderSchemas();
      renderConnectors();
    } catch (e) {
      console.warn("REST initialization fallback error:", e);
    }
  }

  // Connect WebSocket
  function connectWebSocket() {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${proto}//${window.location.host}/ws/events`;
    
    state.ws = new WebSocket(wsUrl);
    
    state.ws.onopen = () => {
      state.connected = true;
      if (el.connStatusPill) {
        el.connStatusPill.innerHTML = `<span class="pulse-dot"></span> LIVE`;
        el.connStatusPill.style.color = 'var(--emerald)';
      }
    };
    
    state.ws.onmessage = (msgEvent) => {
      try {
        const data = JSON.parse(msgEvent.data);
        if (data.type === 'INIT') {
          if (data.metrics) updateMetricsUI(data.metrics);
          if (data.events) {
            state.events = data.events;
            renderRecentEvents(state.events);
          }
          if (data.connectors) state.connectors = data.connectors;
          if (data.consumerGroups) state.consumerGroups = data.consumerGroups;
          state.isStreaming = data.is_streaming;
          updateStreamingBtnUI();
        } else if (data.type === 'NEW_EVENT') {
          handleNewEvent(data.event, data.metrics);
        }
      } catch (err) {
        console.error("WS Parse error:", err);
      }
    };
    
    state.ws.onclose = () => {
      state.connected = false;
      if (el.connStatusPill) {
        el.connStatusPill.innerHTML = `<span style="width:8px;height:8px;border-radius:50%;background:#ef4444;display:inline-block;"></span> RECONNECTING`;
        el.connStatusPill.style.color = 'var(--red)';
      }
      setTimeout(connectWebSocket, 3000);
    };
    
    state.ws.onerror = () => {
      state.ws.close();
    };
  }

  function updateStreamingBtnUI() {
    if (!el.btnToggleStream) return;
    if (state.isStreaming) {
      el.btnToggleStream.innerHTML = `
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="6" y="4" width="4" height="16"/><rect x="14" y="4" width="4" height="16"/></svg>
        Pause Stream
      `;
    } else {
      el.btnToggleStream.innerHTML = `
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>
        Resume Stream
      `;
    }
  }

  // Event Listeners
  function setupListeners() {
    // Mobile Drawer Controls
    function closeMobileSidebar() {
      if (el.appSidebar) el.appSidebar.classList.remove('mobile-open');
      if (el.sidebarBackdrop) el.sidebarBackdrop.classList.remove('open');
    }

    if (el.mobileMenuBtn) {
      el.mobileMenuBtn.addEventListener('click', () => {
        if (el.appSidebar) el.appSidebar.classList.add('mobile-open');
        if (el.sidebarBackdrop) el.sidebarBackdrop.classList.add('open');
      });
    }

    if (el.sidebarCloseBtn) {
      el.sidebarCloseBtn.addEventListener('click', closeMobileSidebar);
    }

    if (el.sidebarBackdrop) {
      el.sidebarBackdrop.addEventListener('click', closeMobileSidebar);
    }

    // Navigation with auto-close for mobile
    el.navItems.forEach(item => {
      item.addEventListener('click', () => {
        switchTab(item.dataset.tab);
        if (window.innerWidth <= 1024) {
          closeMobileSidebar();
        }
      });
    });
    
    // Dashboard Filter Pills
    el.dashboardFilterPills.forEach(pill => {
      pill.addEventListener('click', () => {
        el.dashboardFilterPills.forEach(p => p.classList.remove('active'));
        pill.classList.add('active');
        state.activeFilter = pill.dataset.filter;
        renderRecentEvents(state.events);
      });
    });
    
    // Toggle Stream
    if (el.btnToggleStream) {
      el.btnToggleStream.addEventListener('click', async () => {
        state.isStreaming = !state.isStreaming;
        updateStreamingBtnUI();
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
          state.ws.send(JSON.stringify({
            action: 'SET_STREAMING',
            value: state.isStreaming
          }));
        }
      });
    }
    
    // Modal Open & Close
    if (el.btnSimulateModal) {
      el.btnSimulateModal.addEventListener('click', () => {
        el.mutationModal.classList.add('open');
      });
    }
    if (el.modalCloseBtn) {
      el.modalCloseBtn.addEventListener('click', () => {
        el.mutationModal.classList.remove('open');
      });
    }
    if (el.modalCancelBtn) {
      el.modalCancelBtn.addEventListener('click', () => {
        el.mutationModal.classList.remove('open');
      });
    }
    
    // Drawer Close
    if (el.drawerCloseBtn) {
      el.drawerCloseBtn.addEventListener('click', closeEventDrawer);
    }
    
    // Mutation Form Submit
    if (el.mutationForm) {
      el.mutationForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const table = el.mutateTable.value;
        const op = el.mutateOp.value;
        const recordId = el.mutateRecordId.value ? parseInt(el.mutateRecordId.value) : Math.floor(Math.random() * 8999) + 1000;
        
        let dataPayload = null;
        try {
          dataPayload = JSON.parse(el.mutateData.value);
        } catch (err) {
          dataPayload = { note: el.mutateData.value || "Interactive Mutation" };
        }
        
        try {
          const res = await fetch('/api/mutate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              table: table,
              operation: op,
              record_id: recordId,
              data: dataPayload
            })
          });
          if (res.ok) {
            el.mutationModal.classList.remove('open');
            // reset record id
            el.mutateRecordId.value = '';
          }
        } catch (err) {
          console.error("Mutation failed:", err);
        }
      });
    }
    
    // Quick Trigger Mutation preset helper
    if (el.mutateTable && el.mutateOp && el.mutateData) {
      function updateDefaultMutationPayload() {
        const t = el.mutateTable.value;
        const o = el.mutateOp.value;
        if (t === 'users') {
          el.mutateData.value = o === 'DELETE' ? '{"id": 1024, "reason": "user_account_closure"}' : '{"name": "John Doe", "email": "john.doe@enterprise.io", "role": "engineer"}';
        } else if (t === 'orders') {
          el.mutateData.value = o === 'UPDATE' ? '{"status": "shipped", "carrier": "FedEx", "tracking": "FX-99201"}' : '{"customer_id": 1024, "amount": 199.50, "status": "pending"}';
        } else if (t === 'products') {
          el.mutateData.value = o === 'DELETE' ? '{"id": 554, "sku": "PRD-554-BLK"}' : '{"name": "Ergonomic Mechanical Keyboard", "price": 149.00, "stock": 50}';
        } else if (t === 'payments') {
          el.mutateData.value = '{"amount": 129.99, "currency": "USD", "method": "stripe", "status": "captured"}';
        }
      }
      el.mutateTable.addEventListener('change', updateDefaultMutationPayload);
      el.mutateOp.addEventListener('change', updateDefaultMutationPayload);
    }
    
    // Events Explorer Filters
    if (el.eventsSearchInput) {
      el.eventsSearchInput.addEventListener('input', (e) => {
        state.searchQuery = e.target.value;
        renderEventsExplorer();
      });
    }
    if (el.eventsTableSelect) {
      el.eventsTableSelect.addEventListener('change', (e) => {
        state.activeTableFilter = e.target.value;
        renderEventsExplorer();
      });
    }
    if (el.eventsOpSelect) {
      el.eventsOpSelect.addEventListener('change', () => {
        renderEventsExplorer();
      });
    }
    
    // Settings Listeners
    if (el.settingSpeedRange) {
      el.settingSpeedRange.addEventListener('input', (e) => {
        const val = parseFloat(e.target.value);
        if (el.settingSpeedVal) el.settingSpeedVal.textContent = `${val.toFixed(1)}s`;
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
          state.ws.send(JSON.stringify({
            action: 'SET_SPEED',
            delay: val
          }));
        }
      });
    }
    if (el.settingSoundToggle) {
      el.settingSoundToggle.addEventListener('change', (e) => {
        state.soundEnabled = e.target.checked;
      });
    }
    if (el.btnSpike50) {
      el.btnSpike50.addEventListener('click', async () => {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
          state.ws.send(JSON.stringify({
            action: 'TRIGGER_BATCH',
            count: 30
          }));
        }
      });
    }
    if (el.btnResetStats) {
      el.btnResetStats.addEventListener('click', () => {
        state.events = [];
        renderRecentEvents([]);
      });
    }
  }

  // Initialize
  async function init() {
    setupListeners();
    await fetchInitialData();
    connectWebSocket();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
